import { randomBytes } from "node:crypto";
import { PassThrough, Readable } from "node:stream";
import { Errors, ServiceBroker } from "moleculer";
import type { Service } from "moleculer";
import FilesService, { FileNotFoundError } from "moleculer-files";
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

		it("stores primitive metadata as user metadata and skips Moleculer internals and objects", async () => {
			await save("m", { id: "m.txt", owner: "me", count: 3, flag: true, $requestID: "r", nested: { a: 1 } });
			const bucket = fake.buckets.get(adapter.collection);
			const headers = bucket?.get("m.txt")?.headers;
			expect(headers).toMatchObject({ "x-amz-meta-owner": "me", "x-amz-meta-count": "3", "x-amz-meta-flag": "true" });
			expect(Object.keys(headers ?? {}).some((h) => h.includes("requestid") || h.includes("nested"))).toBe(false);
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

		it("removeMany and clear are placeholders", () => {
			expect(() => adapter.removeMany()).not.toThrow();
			expect(() => adapter.clear()).not.toThrow();
		});
	});
});
