import { randomBytes } from "node:crypto";
import { PassThrough, Readable } from "node:stream";
import { Errors, ServiceBroker } from "moleculer";
import type { Service } from "moleculer";
import FilesService, { FileNotFoundError } from "@keletes/moleculer-files";
import S3Adapter from "../../src";
import { FakeS3 } from "../fake-s3";

const readAll = async (stream: NodeJS.ReadableStream) => {
	const chunks: Buffer[] = [];
	for await (const c of stream) chunks.push(Buffer.from(c as Buffer));
	return Buffer.concat(chunks);
};

describe("moleculer-files-s3 (against an in-process fake S3 over HTTP)", () => {
	const fake = new FakeS3("test-access");
	let port: number;
	let broker: ServiceBroker;
	let svc: Service;
	let adapter: S3Adapter;
	let n = 0;

	const build = (opts: Partial<S3Adapter.Options> = {}, accessKey = "test-access", collection = `bucket-${++n}`) => {
		broker = new ServiceBroker({ logger: false });
		svc = broker.createService({
			name: "cache",
			mixins: [FilesService],
			adapter: new S3Adapter("127.0.0.1", accessKey, "test-secret", { port: String(port), useSSL: false, createBucket: true, ...opts }),
			collection,
		});
		adapter = (svc as unknown as { adapter: S3Adapter }).adapter;
	};

	const save = (content: Buffer | string, meta: Record<string, unknown>) =>
		broker.call("cache.save", Readable.from([Buffer.from(content)]), { meta });
	const get = async (id: string) => readAll((await broker.call("cache.get", { id })) as NodeJS.ReadableStream);

	beforeAll(async () => {
		port = await fake.start();
	});
	afterAll(() => fake.stop());
	beforeEach(async () => {
		build();
		await broker.start();
	});
	afterEach(() => broker.stop());

	describe("connect", () => {
		it("creates the bucket when createBucket is on", () => {
			expect(fake.buckets.has(adapter.collection)).toBe(true);
		});

		it("leaves a missing bucket alone when createBucket is off", async () => {
			await broker.stop();
			build({ createBucket: false });
			await broker.start();
			expect(fake.buckets.has(adapter.collection)).toBe(false);
		});

		it("reuses an existing bucket", async () => {
			await save("kept", { id: "k.txt" });
			await broker.stop();
			build({}, "test-access", adapter.collection);
			await broker.start();
			expect((await get("k.txt")).toString()).toBe("kept");
		});

		it("accepts the port as a number as well as a string", async () => {
			await broker.stop();
			build({ port });
			await broker.start();
			expect(await broker.call("cache.count")).toBe(0);
		});

		it("rejects bad credentials", async () => {
			const bad = new S3Adapter("127.0.0.1", "wrong", "x", { port, useSSL: false });
			bad.init(broker, svc);
			await expect(bad.connect()).rejects.toMatchObject({ code: "InvalidAccessKeyId" });
		});

		it("validates its configuration", () => {
			const check = (args: [string, string, string]) => () => new S3Adapter(...args).init(broker, svc);
			expect(check(["", "a", "b"])).toThrow(Errors.ServiceSchemaError);
			expect(check(["e", "", "b"])).toThrow(Errors.ServiceSchemaError);
			expect(check(["e", "a", ""])).toThrow(Errors.ServiceSchemaError);
			expect(() =>
				broker.createService({ name: "nobucket", mixins: [FilesService], adapter: new S3Adapter("e", "a", "b") }),
			).toThrow(Errors.ServiceSchemaError);
		});

		it("disconnect is a no-op", async () => {
			await expect(adapter.disconnect()).resolves.toBeUndefined();
		});
	});

	describe("save / get", () => {
		it("stores a file and streams it back", async () => {
			await save("hello s3", { id: "hello.txt" });
			expect((await get("hello.txt")).toString()).toBe("hello s3");
		});

		it("round-trips a binary file larger than one chunk", async () => {
			const data = randomBytes(3 * 1024 * 1024 + 7);
			await save(data, { id: "big.bin" });
			expect(Buffer.compare(await get("big.bin"), data)).toBe(0);
		});

		it("names the object after meta.id, then meta.filename, then a random UUID", async () => {
			await save("a", { id: "by-id" });
			await save("b", { filename: "by-filename" });
			await save("c", {});
			const names = ((await broker.call("cache.find")) as { name: string }[]).map((o) => o.name);
			expect(names).toContain("by-id");
			expect(names).toContain("by-filename");
			expect(names.filter((x) => /^[0-9a-f-]{36}$/.test(x))).toHaveLength(1);
		});

		it("supports keys with slashes and spaces", async () => {
			await save("nested", { id: "dir/sub dir/file name.txt" });
			expect((await get("dir/sub dir/file name.txt")).toString()).toBe("nested");
		});

		it("stores primitive metadata as user metadata and skips Moleculer internals and null", async () => {
			await save("m", { id: "m.txt", owner: "me", count: 3, flag: true, $requestID: "r", empty: null });
			const bucket = fake.buckets.get(adapter.collection);
			const headers = bucket?.get("m.txt")?.headers;
			expect(headers).toMatchObject({ "x-amz-meta-owner": "me", "x-amz-meta-count": "3", "x-amz-meta-flag": "true" });
			expect(Object.keys(headers ?? {}).some((h) => h.includes("requestid") || h.includes("empty") || h.includes("json-keys"))).toBe(false);
		});

		it("overwrites on update and takes the id as the name", async () => {
			await save("old", { id: "u.txt" });
			await broker.call("cache.update", Readable.from([Buffer.from("new")]), { meta: { id: "u.txt" } });
			expect((await get("u.txt")).toString()).toBe("new");
			expect(await broker.call("cache.count")).toBe(1);
		});

		it("accepts a metadata object in updateById", async () => {
			await adapter.updateById(Readable.from([Buffer.from("z")]), { id: "z.txt" });
			expect((await get("z.txt")).toString()).toBe("z");
		});
	});

	describe("save result", () => {
		it("resolves to the stored file's details, matching stat", async () => {
			const doc = { name: "Città", list: [1] };
			const result = await broker.call("cache.save", Readable.from([Buffer.from("hello")]), {
				meta: { id: "d1", filename: "orig.txt", "content-type": "text/plain", tag: "x", doc },
			});
			expect(result).toEqual({
				id: "d1",
				filename: "orig.txt",
				contentType: "text/plain",
				size: 5,
				metadata: { id: "d1", filename: "orig.txt", "content-type": "text/plain", tag: "x", doc },
			});
			expect(((await adapter.stat("d1")) as { metadata: unknown }).metadata).toMatchObject({ tag: "x", doc });
		});

		it("falls back to the key as filename and updateById resolves the same", async () => {
			const result = await adapter.updateById(Readable.from([Buffer.from("zz")]), "d2");
			expect(result).toMatchObject({ id: "d2", filename: "d2", size: 2 });
		});
	});

	describe("errors", () => {
		it("get of a missing key is a 404 FileNotFoundError", async () => {
			await expect(broker.call("cache.get", { id: "nope" })).rejects.toBeInstanceOf(FileNotFoundError);
		});

		it("get surfaces other S3 errors", async () => {
			fake.buckets.delete(adapter.collection);
			await expect(broker.call("cache.get", { id: "x" })).rejects.toMatchObject({ code: "NoSuchBucket" });
		});

		it("rejects an entity that is not a stream", async () => {
			await expect(adapter.save("text" as never, { id: "x" })).rejects.toMatchObject({ code: 400, type: "E_BAD_REQUEST" });
		});

		it("rejects when the source stream fails and stores nothing", async () => {
			const source = new PassThrough();
			const pending = adapter.save(source, { id: "broken.bin" });
			source.write(Buffer.from("partial"));
			setTimeout(() => source.destroy(new Error("client went away")), 30);
			await expect(pending).rejects.toThrow();
			await expect(broker.call("cache.get", { id: "broken.bin" })).rejects.toBeInstanceOf(FileNotFoundError);
			expect(fake.buckets.get(adapter.collection)?.has("broken.bin")).toBe(false);
			expect(fake.uploads.size).toBe(0); // the unfinished multipart upload was aborted
		});

		it("find rejects when listing fails", async () => {
			fake.buckets.delete(adapter.collection);
			await expect(broker.call("cache.find")).rejects.toMatchObject({ code: "NoSuchBucket" });
		});
	});

	describe("find / count / list", () => {
		beforeEach(async () => {
			await save("1", { id: "a/1.txt" });
			await save("22", { id: "a/2.txt" });
			await save("333", { id: "b/3.txt" });
		});

		it("lists the objects with name, size and etag", async () => {
			const rows = (await broker.call("cache.find")) as { _id: string; name: string; size: number; etag: string }[];
			expect(rows.map((r) => r.name)).toEqual(["a/1.txt", "a/2.txt", "b/3.txt"]);
			expect(rows.map((r) => r.size)).toEqual([1, 2, 3]);
			expect(rows[0]._id).toBe("a/1.txt");
			expect(rows[0].etag).toBeTruthy();
		});

		it("honours limit and offset", async () => {
			const rows = (await broker.call("cache.find", { limit: 1, offset: 1 })) as { name: string }[];
			expect(rows.map((r) => r.name)).toEqual(["a/2.txt"]);
		});

		it("filters by prefix", async () => {
			const rows = (await adapter.find({ prefix: "a/" })) as { name: string }[];
			expect(rows.map((r) => r.name)).toEqual(["a/1.txt", "a/2.txt"]);
		});

		it("pages through more than 1000 objects", async () => {
			const bucket = fake.buckets.get(adapter.collection) as Map<string, unknown>;
			for (let i = 0; i < 2500; i++) bucket.set(`p/${String(i).padStart(5, "0")}`, { body: Buffer.from("x"), headers: {}, modified: new Date() });
			fake.requests.length = 0;
			const rows = (await adapter.find({ prefix: "p/" })) as { name: string }[];
			expect(rows).toHaveLength(2500);
			expect(new Set(rows.map((r) => r.name)).size).toBe(2500);
			expect(fake.requests.filter((r) => r.startsWith("GET") && r.includes("prefix=p%2F"))).toHaveLength(3);
			expect(await adapter.count({ prefix: "p/" })).toBe(2500);
		});

		it("counts the objects", async () => {
			expect(await broker.call("cache.count")).toBe(3);
			expect(await adapter.count({ prefix: "b/" })).toBe(1);
		});

		it("list paginates with a real total", async () => {
			const page = (await broker.call("cache.list", { page: 2, pageSize: 2 })) as { rows: { name: string }[]; total: number; totalPages: number };
			expect(page.total).toBe(3);
			expect(page.totalPages).toBe(2);
			expect(page.rows.map((r) => r.name)).toEqual(["b/3.txt"]);
		});

		it("findOne streams the first match, undefined for none", async () => {
			expect((await readAll((await adapter.findOne({ prefix: "b/" })) as NodeJS.ReadableStream)).toString()).toBe("333");
			expect(await adapter.findOne({ prefix: "zzz/" })).toBeUndefined();
		});
	});

	describe("remove", () => {
		it("deletes the object and returns it", async () => {
			await save("x", { id: "r.txt" });
			expect(await broker.call("cache.remove", { id: "r.txt" })).toMatchObject({ name: "r.txt" });
			await expect(broker.call("cache.get", { id: "r.txt" })).rejects.toBeInstanceOf(FileNotFoundError);
		});

		it("is a 404 for a missing object", async () => {
			await expect(broker.call("cache.remove", { id: "ghost" })).rejects.toBeInstanceOf(FileNotFoundError);
		});

		it("surfaces other S3 errors", async () => {
			fake.buckets.delete(adapter.collection);
			await expect(adapter.removeById("x")).rejects.toBeDefined();
		});

		it("stat reads size, type and user metadata with one HEAD", async () => {
			await save("hello", { id: "k1", filename: "orig.txt", tag: "x", "content-type": "text/plain" });
			const stat = await adapter.stat("k1");
			expect(stat).toMatchObject({ filename: "orig.txt", size: 5, contentType: "text/plain" });
			expect(stat?.metadata).toMatchObject({ tag: "x", filename: "orig.txt" });
		});

		it("stores object and array metadata as ASCII JSON and stat decodes them", async () => {
			const doc = { name: "Città – 日本語 😀", list: [1, "é", { deep: true }] };
			await save("m", { id: "j1", Tags: ["a", "è"], doc, plain: "x", n: 3 });
			const headers = fake.buckets.get(adapter.collection)?.get("j1")?.headers ?? {};
			// eslint-disable-next-line no-control-regex
			expect(Object.values(headers).every((v) => /^[\x20-\x7e]*$/.test(v))).toBe(true);
			expect(headers["x-amz-meta-doc"]).toContain("\\u00e0");
			expect(headers["x-amz-meta-json-keys"]).toBe("[\"tags\",\"doc\"]");
			const { metadata } = (await adapter.stat("j1")) as { metadata: Record<string, unknown> };
			expect(metadata).toEqual({ id: "j1", tags: ["a", "è"], doc, plain: "x", n: "3" });
		});

		it("keeps a string that merely looks like JSON as a string", async () => {
			await save("m", { id: "j2", looks: "{\"a\":1}", arr: "[1,2]", real: { a: 1 } });
			const { metadata } = (await adapter.stat("j2")) as { metadata: Record<string, unknown> };
			expect(metadata).toEqual({ id: "j2", looks: "{\"a\":1}", arr: "[1,2]", real: { a: 1 } });
		});

		it("ignores a user-supplied json-keys entry", async () => {
			await save("m", { id: "j3", "json-keys": "[\"a\"]", a: "[1]" });
			const { metadata } = (await adapter.stat("j3")) as { metadata: Record<string, unknown> };
			expect(metadata).toEqual({ id: "j3", a: "[1]" });
		});

		it("drops JSON metadata over 2 KB, largest first, warns and still uploads", async () => {
			const warn = jest.spyOn(adapter.service.logger, "warn").mockImplementation(() => undefined);
			await save("body", { id: "j4", keep: { s: "x" }, big: { s: "y".repeat(1500) }, bigger: { s: "z".repeat(1800) } });
			expect(warn).toHaveBeenCalledWith(expect.stringContaining("bigger"));
			expect((await get("j4")).toString()).toBe("body");
			const { metadata } = (await adapter.stat("j4")) as { metadata: Record<string, unknown> };
			expect(metadata).toEqual({ id: "j4", keep: { s: "x" }, big: { s: "y".repeat(1500) } });
			warn.mockRestore();
		});

		it("stat falls back to the key and resolves undefined for a missing object", async () => {
			await save("hi", { id: "k2" });
			expect((await adapter.stat("k2"))?.filename).toBe("k2");
			expect(await adapter.stat("nope")).toBeUndefined();
		});

		it("stat rethrows other errors", async () => {
			await broker.stop();
			build({}, "wrong-access");
			await expect(adapter.stat("k")).rejects.toBeDefined();
		});

		it("clear is a placeholder", () => {
			expect(() => adapter.clear()).not.toThrow();
		});
	});

	describe("removeMany", () => {
		const seed = (count: number) => {
			const bucket = fake.buckets.get(adapter.collection) as Map<string, unknown>;
			const ids: string[] = [];
			for (let i = 0; i < count; i++) {
				ids.push(`k/${String(i).padStart(5, "0")}`);
				bucket.set(ids[i], { body: Buffer.from("x"), headers: {}, modified: new Date() });
			}
			return { bucket, ids };
		};
		const deleteRequests = () => fake.requests.filter((r) => r.startsWith("POST") && r.includes("?delete"));

		beforeEach(() => {
			fake.requests.length = 0;
			fake.deleteErrors.clear();
		});

		it("deletes the keys in one request and resolves to the ids", async () => {
			const { bucket, ids } = seed(3);
			expect(await adapter.removeMany(ids.slice(0, 2))).toEqual(ids.slice(0, 2));
			expect([...bucket.keys()]).toEqual([ids[2]]);
			expect(deleteRequests()).toHaveLength(1);
		});

		it("makes no request for an empty list", async () => {
			expect(await adapter.removeMany([])).toEqual([]);
			expect(fake.requests).toEqual([]);
		});

		it("sends 1000 keys in one request", async () => {
			const { bucket, ids } = seed(1000);
			expect(await adapter.removeMany(ids)).toHaveLength(1000);
			expect(bucket.size).toBe(0);
			expect(deleteRequests()).toHaveLength(1);
		});

		it("splits 2001 keys into 3 requests", async () => {
			const { bucket, ids } = seed(2001);
			expect(await adapter.removeMany(ids)).toEqual(ids);
			expect(bucket.size).toBe(0);
			expect(deleteRequests()).toHaveLength(3);
		});

		it("does not fail on a missing key and handles keys with special characters", async () => {
			const bucket = fake.buckets.get(adapter.collection) as Map<string, unknown>;
			bucket.set("a & b <1>.txt", { body: Buffer.from("x"), headers: {}, modified: new Date() });
			expect(await adapter.removeMany(["a & b <1>.txt", "missing"])).toEqual(["a & b <1>.txt", "missing"]);
			expect(bucket.size).toBe(0);
		});

		it("ignores NoSuchKey errors but rejects with the failing keys for any other error", async () => {
			const { ids } = seed(3);
			fake.deleteErrors.set(ids[0], "NoSuchKey");
			expect(await adapter.removeMany(ids)).toEqual(ids);

			fake.deleteErrors.set(ids[1], "AccessDenied");
			const err = await adapter.removeMany(ids).catch((e) => e);
			expect(err.message).toContain(ids[1]);
			expect(err.message).toContain("AccessDenied");
			expect(err.message).not.toContain(ids[2]);
			expect(err.message).not.toContain("test-secret");
		});

		it("reports the failures of every chunk", async () => {
			const { ids } = seed(2001);
			fake.deleteErrors.set(ids[5], "InternalError");
			fake.deleteErrors.set(ids[2000], "InternalError");
			const err = await adapter.removeMany(ids).catch((e) => e);
			expect(err.message).toContain("2 of 2001");
			expect(err.message).toContain(ids[5]);
			expect(err.message).toContain(ids[2000]);
		});
	});
});
