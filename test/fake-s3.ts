import { createHash } from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";

type StoredObject = { body: Buffer; headers: Record<string, string>; modified: Date };
type Upload = { bucket: string; key: string; headers: Record<string, string>; parts: Map<number, Buffer> };

const userHeaders = (req: http.IncomingMessage) => {
	const headers: Record<string, string> = {};
	for (const [name, value] of Object.entries(req.headers)) {
		if (name.startsWith("x-amz-meta-") || name === "content-type") headers[name] = String(value);
	}
	return headers;
};

const xml = (body: string) => `<?xml version="1.0" encoding="UTF-8"?>\n${body}`;
const escapeXml = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

/**
 * Minimal in-process S3 over real HTTP: just what the MinIO client calls for bucket exists/make/location, object
 * put/get/head/delete and listing (v1 and v2). Requests must be signed with the configured access key (the
 * SigV4 signature itself is not verified), so wrong credentials fail like on a real server.
 */
export class FakeS3 {
	buckets = new Map<string, Map<string, StoredObject>>();
	requests: string[] = [];
	/** Multipart uploads started and not completed or aborted. */
	uploads = new Map<string, Upload>();
	private uploadSeq = 0;
	private server = http.createServer((req, res) => {
		const chunks: Buffer[] = [];
		req.on("data", (c) => chunks.push(c));
		req.on("end", () => {
			try {
				this.handle(req, res, Buffer.concat(chunks));
			} catch (err) {
				this.error(res, 500, "InternalError", String(err));
			}
		});
	});

	constructor(public accessKey = "test-access") {}

	async start() {
		await new Promise<void>((resolve) => this.server.listen(0, "127.0.0.1", resolve));
		return (this.server.address() as AddressInfo).port;
	}

	stop() {
		this.server.closeAllConnections();
		return new Promise<void>((resolve) => this.server.close(() => resolve()));
	}

	private error(res: http.ServerResponse, status: number, code: string, message: string, head = false) {
		res.statusCode = status;
		if (head) return res.end();
		res.setHeader("Content-Type", "application/xml");
		res.end(xml(`<Error><Code>${code}</Code><Message>${escapeXml(message)}</Message></Error>`));
	}

	private handle(req: http.IncomingMessage, res: http.ServerResponse, body: Buffer) {
		const url = new URL(req.url as string, "http://fake");
		const method = req.method as string;
		const head = method === "HEAD";
		this.requests.push(`${method} ${url.pathname}${url.search}`);

		const credential = /Credential=([^/]+)\//.exec(String(req.headers.authorization ?? ""));
		if (!credential || credential[1] !== this.accessKey) {
			return this.error(res, 403, "InvalidAccessKeyId", "The access key does not exist.", head);
		}

		const [, bucketName, ...rest] = decodeURIComponent(url.pathname).split("/");
		const key = rest.join("/");
		const bucket = this.buckets.get(bucketName);

		if (!key) {
			if (method === "PUT") {
				if (!bucket) this.buckets.set(bucketName, new Map());
				return res.end();
			}
			if (!bucket) return this.error(res, 404, "NoSuchBucket", "The specified bucket does not exist", head);
			if (head) return res.end();
			if (url.searchParams.has("location")) {
				res.setHeader("Content-Type", "application/xml");
				return res.end(xml("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></LocationConstraint>"));
			}
			if (method === "GET" && url.searchParams.has("uploads")) {
				res.setHeader("Content-Type", "application/xml");
				const prefix = url.searchParams.get("prefix") ?? "";
				const pending = [...this.uploads.entries()]
					.filter(([, u]) => u.bucket === bucketName && u.key.startsWith(prefix))
					.map(([id, u]) => `<Upload><Key>${escapeXml(u.key)}</Key><UploadId>${id}</UploadId><Initiated>${new Date().toISOString()}</Initiated></Upload>`)
					.join("");
				return res.end(xml(`<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>${bucketName}</Bucket><IsTruncated>false</IsTruncated>${pending}</ListMultipartUploadsResult>`));
			}
			if (method === "GET") return this.list(url, bucket, res);
			return this.error(res, 405, "MethodNotAllowed", method);
		}

		if (!bucket) return this.error(res, 404, "NoSuchBucket", "The specified bucket does not exist", head);

		const uploadId = url.searchParams.get("uploadId");
		if (method === "POST" && url.searchParams.has("uploads")) {
			const id = `upload-${++this.uploadSeq}`;
			this.uploads.set(id, { bucket: bucketName, key, headers: userHeaders(req), parts: new Map() });
			res.setHeader("Content-Type", "application/xml");
			return res.end(xml(`<InitiateMultipartUploadResult><Bucket>${bucketName}</Bucket><Key>${escapeXml(key)}</Key><UploadId>${id}</UploadId></InitiateMultipartUploadResult>`));
		}
		if (uploadId) {
			const upload = this.uploads.get(uploadId);
			if (!upload) return this.error(res, 404, "NoSuchUpload", "The specified upload does not exist.", head);
			if (method === "PUT") {
				upload.parts.set(Number(url.searchParams.get("partNumber")), body);
				res.setHeader("ETag", `"${createHash("md5").update(body).digest("hex")}"`);
				return res.end();
			}
			if (method === "GET") {
				const parts = [...upload.parts.entries()]
					.sort(([a], [b]) => a - b)
					.map(([nr, buf]) => `<Part><PartNumber>${nr}</PartNumber><LastModified>${new Date().toISOString()}</LastModified><ETag>"${createHash("md5").update(buf).digest("hex")}"</ETag><Size>${buf.length}</Size></Part>`)
					.join("");
				res.setHeader("Content-Type", "application/xml");
				return res.end(xml(`<ListPartsResult><Bucket>${bucketName}</Bucket><Key>${escapeXml(key)}</Key><UploadId>${uploadId}</UploadId><IsTruncated>false</IsTruncated>${parts}</ListPartsResult>`));
			}
			if (method === "DELETE") {
				this.uploads.delete(uploadId);
				res.statusCode = 204;
				return res.end();
			}
			if (method === "POST") {
				const numbers = [...upload.parts.keys()].sort((a, b) => a - b);
				const whole = Buffer.concat(numbers.map((nr) => upload.parts.get(nr) as Buffer));
				bucket.set(key, { body: whole, headers: upload.headers, modified: new Date() });
				this.uploads.delete(uploadId);
				res.setHeader("Content-Type", "application/xml");
				return res.end(xml(`<CompleteMultipartUploadResult><Location>http://fake/${bucketName}/${escapeXml(key)}</Location><Bucket>${bucketName}</Bucket><Key>${escapeXml(key)}</Key><ETag>"${createHash("md5").update(whole).digest("hex")}-${numbers.length}"</ETag></CompleteMultipartUploadResult>`));
			}
		}

		if (method === "PUT") {
			const headers = userHeaders(req);
			bucket.set(key, { body, headers, modified: new Date() });
			res.setHeader("ETag", `"${createHash("md5").update(body).digest("hex")}"`);
			return res.end();
		}

		if (method === "DELETE") {
			bucket.delete(key);
			res.statusCode = 204;
			return res.end();
		}

		const obj = bucket.get(key);
		if (!obj) return this.error(res, 404, "NoSuchKey", "The specified key does not exist.", head);
		for (const [name, value] of Object.entries(obj.headers)) res.setHeader(name, value);
		res.setHeader("Content-Length", obj.body.length);
		res.setHeader("ETag", `"${createHash("md5").update(obj.body).digest("hex")}"`);
		res.setHeader("Last-Modified", obj.modified.toUTCString());
		res.end(head ? undefined : obj.body);
	}

	private list(url: URL, bucket: Map<string, StoredObject>, res: http.ServerResponse) {
		const prefix = url.searchParams.get("prefix") ?? "";
		const keys = [...bucket.keys()].filter((k) => k.startsWith(prefix)).sort();
		const contents = keys
			.map((k) => {
				const o = bucket.get(k) as StoredObject;
				const etag = createHash("md5").update(o.body).digest("hex");
				return `<Contents><Key>${encodeURIComponent(k)}</Key><LastModified>${o.modified.toISOString()}</LastModified><ETag>"${etag}"</ETag><Size>${o.body.length}</Size><StorageClass>STANDARD</StorageClass></Contents>`;
			})
			.join("");
		const v2 = url.searchParams.get("list-type") === "2";
		res.setHeader("Content-Type", "application/xml");
		res.end(
			xml(
				`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix>${escapeXml(prefix)}</Prefix>` +
					`${v2 ? `<KeyCount>${keys.length}</KeyCount>` : ""}<MaxKeys>1000</MaxKeys><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated>${contents}</ListBucketResult>`,
			),
		);
	}
}
