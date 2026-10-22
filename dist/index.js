"use strict";
const node_crypto_1 = require("node:crypto");
const node_stream_1 = require("node:stream");
const promises_1 = require("node:stream/promises");
const minio_1 = require("minio");
const moleculer_1 = require("moleculer");
const { MoleculerError, ServiceSchemaError } = moleculer_1.Errors;
const isReadable = (value) => typeof value === "object" && value !== null && typeof value.pipe === "function";
/** S3 error codes of a key that does not exist. HEAD answers carry no body, hence `NotFound`. */
const MISSING_KEY = new Set(["NoSuchKey", "NotFound"]);
class S3Adapter {
    constructor(endpoint, accessKey, secretKey, opts = {}) {
        this.endPoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.opts = opts;
    }
    init(broker, service) {
        this.broker = broker;
        this.service = service;
        if (!this.endPoint)
            throw new ServiceSchemaError("Missing `endPoint` definition!", undefined);
        if (!this.accessKey)
            throw new ServiceSchemaError("Missing `accessKey` definition!", undefined);
        if (!this.secretKey)
            throw new ServiceSchemaError("Missing `secretKey` definition!", undefined);
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
            this.client = new minio_1.Client({
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
                }
                else {
                    await this.client.makeBucket(this.collection, region ?? "");
                }
            }
        }
        catch (err) {
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
    async find(params = {}) {
        const prefix = typeof params.prefix === "string" ? params.prefix : "";
        const items = await new Promise((resolve, reject) => {
            const found = [];
            const stream = this.client.listObjects(this.collection, prefix, true);
            stream.on("data", (obj) => {
                if (obj.name)
                    found.push({ _id: obj.name, name: obj.name, size: obj.size, lastModified: obj.lastModified, etag: obj.etag });
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
    async findOne(params = {}) {
        const [first] = await this.find({ ...params, limit: 1 });
        return first ? this.client.getObject(this.collection, first.name) : undefined;
    }
    /** Stream of the object `fd`; rejects with a 404 `MoleculerError` when it does not exist. */
    async findById(fd) {
        try {
            return await this.client.getObject(this.collection, String(fd));
        }
        catch (err) {
            if (MISSING_KEY.has(err.code ?? "")) {
                throw new MoleculerError(`File \`${fd}\` not found`, 404, "ERR_NOT_FOUND");
            }
            throw err;
        }
    }
    /** Number of objects matching the params (see `find`). */
    async count(params = {}) {
        return (await this.find({ ...params, limit: undefined, offset: undefined })).length;
    }
    /**
     * Uploads the stream as `meta.id` (else `meta.filename`, else a random UUID). The primitive entries of `meta`
     * (not the `$`-prefixed Moleculer ones) become the object's user metadata.
     */
    async save(entity, meta = {}) {
        if (!isReadable(entity))
            throw new MoleculerError("Entity is not a stream", 400, "E_BAD_REQUEST");
        const filename = String(meta.id || meta.filename || (0, node_crypto_1.randomUUID)());
        const metaData = {};
        for (const [key, value] of Object.entries(meta)) {
            if (!key.startsWith("$") && ["string", "number", "boolean"].includes(typeof value))
                metaData[key] = String(value);
        }
        // The client does not watch the source for errors (a failing source would leave the upload hanging), so the
        // source goes through `pipeline`, which rejects on the first error of either side.
        const body = new node_stream_1.PassThrough();
        const upload = this.client.putObject(this.collection, filename, body, undefined, metaData);
        upload.catch(() => undefined);
        try {
            const [, result] = await Promise.all([(0, promises_1.pipeline)(entity, body), upload]);
            return result;
        }
        catch (err) {
            body.destroy();
            // Drop the parts already sent of the unfinished multipart upload
            await this.client.removeIncompleteUpload(this.collection, filename).catch(() => undefined);
            throw err;
        }
    }
    /** Replaces the object `id` (moleculer-files passes the id; a metadata object is accepted too). */
    async updateById(entity, id) {
        const meta = typeof id === "object" && id !== null ? id : { id: String(id) };
        return this.save(entity, meta);
    }
    /** Deletes the object; resolves to `{ _id, name }`, or `undefined` when it does not exist. */
    async removeById(_id) {
        const name = String(_id);
        try {
            await this.client.statObject(this.collection, name);
        }
        catch (err) {
            if (MISSING_KEY.has(err.code ?? ""))
                return undefined;
            throw err;
        }
        await this.client.removeObject(this.collection, name);
        return { _id: name, name };
    }
    removeMany(_query) {
        // To be implemented
    }
    clear() {
        // To be implemented
    }
}
module.exports = S3Adapter;
//# sourceMappingURL=index.js.map