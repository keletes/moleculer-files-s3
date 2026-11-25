import { randomUUID } from "node:crypto";
import { PassThrough } from "node:stream";
import type { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { Client } from "minio";
import type { ClientOptions, ItemBucketMetadata } from "minio";
import { Errors } from "moleculer";
import type { Service, ServiceBroker } from "moleculer";
import type { AdapterSchema, QueryParams } from "@keletes/moleculer-files";

const { MoleculerError, ServiceSchemaError } = Errors;

const isReadable = (value: unknown): value is NodeJS.ReadableStream =>
	typeof value === "object" && value !== null && typeof (value as { pipe?: unknown }).pipe === "function";

/** S3 error codes of a key that does not exist. HEAD answers carry no body, hence `NotFound`. */
const MISSING_KEY = new Set(["NoSuchKey", "NotFound"]);

/** System headers the client mixes into `metaData` next to the user metadata. */
const STANDARD_HEADERS = new Set(["content-type", "cache-control", "content-disposition", "content-encoding", "content-language", "expires"]);

/** User metadata key listing (as a JSON array of lowercase key names) the values that hold encoded JSON. */
const JSON_KEYS = "json-keys";

/** S3 limit for all user metadata of an object (key names plus values), in bytes. */
const MAX_METADATA_BYTES = 2048;

/** JSON text restricted to printable ASCII (S3 user metadata is ASCII header text): other characters become `\uXXXX`. */
const encodeJson = (value: unknown) => JSON.stringify(value).replace(/[^\x20-\x7e]/g, (c) => `\\u${c.charCodeAt(0).toString(16).padStart(4, "0")}`);

const metadataSize = (entries: Record<string, string>) => Object.entries(entries).reduce((sum, [k, v]) => sum + Buffer.byteLength(k) + Buffer.byteLength(v), 0);

class S3Adapter implements AdapterSchema {
	endPoint: string;
	accessKey: string;
	secretKey: string;
	opts: S3Adapter.Options;
	broker!: ServiceBroker;
	service!: Service;
	/** Bucket name: `collection` of the service schema. */
	collection!: string;
	client!: Client;

	constructor(endpoint: string, accessKey: string, secretKey: string, opts: S3Adapter.Options = {}) {
		this.endPoint = endpoint;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.opts = opts;
	}

	init(broker: ServiceBroker, service: Service) {
		this.broker = broker;
		this.service = service;

		if (!this.endPoint) throw new ServiceSchemaError("Missing `endPoint` definition!", undefined);
		if (!this.accessKey) throw new ServiceSchemaError("Missing `accessKey` definition!", undefined);
		if (!this.secretKey) throw new ServiceSchemaError("Missing `secretKey` definition!", undefined);
		if (!this.service.schema.collection) {
			throw new ServiceSchemaError("Missing `collection` definition in schema of service!", undefined);
		}
		this.collection = this.service.schema.collection;
	}

	async connect() {
		const { endPoint, accessKey, secretKey } = this;
		const { useSSL, sessionToken, region, transport, partSize, pathStyle } = this.opts;
		const port = this.opts.port == null ? NaN : parseInt(String(this.opts.port));
		try {
			this.client = new Client({
				endPoint,
				...(Number.isFinite(port) ? { port } : {}),
				useSSL,
				accessKey,
				secretKey,
				sessionToken,
				region,
				transport,
				partSize,
				pathStyle,
			});
			const exists = await this.client.bucketExists(this.collection);
			if (!exists) {
				if (!this.opts.createBucket) {
					this.service.logger.warn(`S3 bucket \`${this.collection}\` does not exist and \`createBucket\` is off.`);
				} else {
					await this.client.makeBucket(this.collection, region ?? "");
				}
			}
		} catch (err) {
			this.service.logger.error("S3 error.", err);
			throw err;
		}
	}

	async disconnect() {
		// The client holds no connection
	}

	/**
	 * Objects of the bucket as `{ _id, name, size, lastModified, etag }`. `params.prefix` restricts the listing,
	 * `limit`/`offset` page it; other filters are not supported by S3 and ignored.
	 */
	async find(params: QueryParams = {}) {
		const prefix = typeof params.prefix === "string" ? params.prefix : "";
		const items = await new Promise<ObjectEntry[]>((resolve, reject) => {
			const found: ObjectEntry[] = [];
			const stream = this.client.listObjects(this.collection, prefix, true);
			stream.on("data", (obj) => {
				if (obj.name) found.push({ _id: obj.name, name: obj.name, size: obj.size, lastModified: obj.lastModified, etag: obj.etag });
			});
			stream.on("end", () => resolve(found));
			stream.on("error", (err) => {
				this.service.logger.error("Error during find operation in S3 bucket.", err);
				reject(err);
			});
		});
		const offset = typeof params.offset === "number" && params.offset > 0 ? params.offset : 0;
		const limit = typeof params.limit === "number" && params.limit > 0 ? params.limit : undefined;
		return items.slice(offset, limit === undefined ? undefined : offset + limit);
	}

	/** Stream of the first object matching the params (see `find`), `undefined` when there is none. */
	async findOne(params: QueryParams = {}) {
		const [first] = await this.find({ ...params, limit: 1 });
		return first ? this.client.getObject(this.collection, first.name) : undefined;
	}

	/** Stream of the object `fd`; rejects with a 404 `MoleculerError` when it does not exist. */
	async findById(fd: string | number) {
		try {
			return await this.client.getObject(this.collection, String(fd));
		} catch (err) {
			if (MISSING_KEY.has((err as { code?: string }).code ?? "")) {
				throw new MoleculerError(`File \`${fd}\` not found`, 404, "ERR_NOT_FOUND");
			}
			throw err;
		}
	}

	/**
	 * Metadata of the object `id` from one HEAD on the key `findById` reads, `undefined` when it does not exist.
	 * `filename` is the `filename` user metadata `save` stored, else the key; `metadata` is the user metadata.
	 */
	async stat(id: string | number) {
		const key = String(id);
		let stat;
		try {
			stat = await this.client.statObject(this.collection, key);
		} catch (err) {
			if (MISSING_KEY.has((err as { code?: string }).code ?? "")) return undefined;
			throw err;
		}
		const metadata: Record<string, unknown> = {};
		for (const [name, value] of Object.entries(stat.metaData ?? {})) {
			if (!STANDARD_HEADERS.has(name.toLowerCase()) && !name.toLowerCase().startsWith("x-amz-")) metadata[name] = value;
		}
		// Only the keys the companion entry lists are decoded; every other value stays a string
		const encoded = metadata[JSON_KEYS];
		delete metadata[JSON_KEYS];
		for (const name of typeof encoded === "string" ? safeParseList(encoded) : []) {
			if (typeof metadata[name] === "string") metadata[name] = safeParse(metadata[name] as string);
		}
		const contentType = (stat.metaData as Record<string, unknown> | undefined)?.["content-type"];
		const stored = metadata.filename;
		return {
			filename: typeof stored === "string" && stored ? stored : key,
			contentType: typeof contentType === "string" ? contentType : undefined,
			size: stat.size,
			metadata,
		};
	}

	/** Number of objects matching the params (see `find`). */
	async count(params: QueryParams = {}) {
		return (await this.find({ ...params, limit: undefined, offset: undefined })).length;
	}

	/**
	 * Uploads the stream as `meta.id` (else `meta.filename`, else a random UUID). The primitive entries of `meta`
	 * (not the `$`-prefixed Moleculer ones) become the object's user metadata as strings. Object and array values are
	 * stored as JSON in printable ASCII and listed in the reserved `json-keys` entry so `stat` can decode them. When
	 * all metadata would pass S3's 2 KB limit, those JSON values are dropped, largest first (logged as a warning);
	 * metadata never fails the upload.
	 */
	async save(entity: NodeJS.ReadableStream, meta: Record<string, unknown> = {}) {
		if (!isReadable(entity)) throw new MoleculerError("Entity is not a stream", 400, "E_BAD_REQUEST");

		const filename = String(meta.id || meta.filename || randomUUID());
		const metaData: ItemBucketMetadata = {};
		const json: Record<string, string> = {};
		for (const [key, value] of Object.entries(meta)) {
			if (key.startsWith("$") || key.toLowerCase() === JSON_KEYS) continue;
			if (["string", "number", "boolean"].includes(typeof value)) metaData[key] = String(value);
			else if (typeof value === "object" && value !== null) json[key.toLowerCase()] = encodeJson(value);
		}
		const dropped: string[] = [];
		const withJson = () => (Object.keys(json).length ? { ...metaData, ...json, [JSON_KEYS]: JSON.stringify(Object.keys(json)) } : metaData);
		while (Object.keys(json).length && metadataSize(withJson() as Record<string, string>) > MAX_METADATA_BYTES) {
			const [largest] = Object.entries(json).sort((a, b) => b[1].length - a[1].length)[0];
			delete json[largest];
			dropped.push(largest);
		}
		if (dropped.length) this.service.logger.warn(`Metadata of \`${filename}\` over the S3 limit of ${MAX_METADATA_BYTES} bytes, not stored: ${dropped.join(", ")}.`);

		// The client does not watch the source for errors (a failing source would leave the upload hanging), so the
		// source goes through `pipeline`, which rejects on the first error of either side.
		const body = new PassThrough();
		const upload = this.client.putObject(this.collection, filename, body, undefined, withJson());
		upload.catch(() => undefined);
		try {
			const [, result] = await Promise.all([pipeline(entity as NodeJS.ReadableStream & Readable, body), upload]);
			return result;
		} catch (err) {
			body.destroy();
			// Drop the parts already sent of the unfinished multipart upload
			await this.client.removeIncompleteUpload(this.collection, filename).catch(() => undefined);
			throw err;
		}
	}

	/** Replaces the object `id` (moleculer-files passes the id; a metadata object is accepted too). */
	async updateById(entity: NodeJS.ReadableStream, id: string | number | Record<string, unknown>) {
		const meta = typeof id === "object" && id !== null ? id : { id: String(id) };
		return this.save(entity, meta);
	}

	/** Deletes the object; resolves to `{ _id, name }`, or `undefined` when it does not exist. */
	async removeById(_id: string | number) {
		const name = String(_id);
		try {
			await this.client.statObject(this.collection, name);
		} catch (err) {
			if (MISSING_KEY.has((err as { code?: string }).code ?? "")) return undefined;
			throw err;
		}
		await this.client.removeObject(this.collection, name);
		return { _id: name, name };
	}

	/**
	 * Deletes the keys with the multi-object delete (`DeleteObjects`), at most 1000 keys per request (the S3 limit;
	 * the client splits longer lists). Resolves to the ids deleted. A missing key is not an error in S3; any other
	 * per-key error rejects with the failing keys listed (never the credentials). An empty list makes no request.
	 */
	async removeMany(ids: string[]) {
		const keys = ids.map(String);
		if (!keys.length) return [];
		// The client returns the `<Error>` entries of the response as `{ Code, Message, Key }`, its typings say otherwise
		const results = (await this.client.removeObjects(this.collection, keys)) as unknown as { Code?: string; Message?: string; Key?: string }[];
		const failed = results.filter((e) => !MISSING_KEY.has(e.Code ?? ""));
		if (failed.length) {
			const detail = failed.map((e) => `${e.Key} (${e.Code ?? "unknown"}${e.Message ? `: ${e.Message}` : ""})`).join(", ");
			throw new MoleculerError(`Could not delete ${failed.length} of ${keys.length} objects: ${detail}`, 500, "ERR_S3_DELETE");
		}
		return keys;
	}

	clear() {
		// To be implemented
	}
}

const safeParse = (text: string): unknown => {
	try {
		return JSON.parse(text);
	} catch {
		return text;
	}
};

const safeParseList = (text: string): string[] => {
	const list = safeParse(text);
	return Array.isArray(list) ? list.filter((k): k is string => typeof k === "string") : [];
};

interface ObjectEntry {
	_id: string;
	name: string;
	size?: number;
	lastModified?: Date;
	etag?: string;
}

// eslint-disable-next-line @typescript-eslint/no-namespace
declare namespace S3Adapter {
	interface Options extends Pick<ClientOptions, "useSSL" | "sessionToken" | "region" | "transport" | "partSize" | "pathStyle"> {
		/** Port of the endpoint; a string is parsed. Omit for the protocol default. */
		port?: number | string | null;
		/** Create the bucket on connect when it does not exist. */
		createBucket?: boolean;
	}
}

// `export =`: `require("@keletes/moleculer-files-s3")` returns the class itself, as before the TypeScript rewrite.
export = S3Adapter;
