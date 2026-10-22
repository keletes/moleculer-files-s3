import type { Readable } from "node:stream";
import { Client } from "minio";
import type { ClientOptions } from "minio";
import type { Service, ServiceBroker } from "moleculer";
import type { AdapterSchema, QueryParams } from "moleculer-files";
declare class S3Adapter implements AdapterSchema {
    endPoint: string;
    accessKey: string;
    secretKey: string;
    opts: S3Adapter.Options;
    broker: ServiceBroker;
    service: Service;
    /** Bucket name: `collection` of the service schema. */
    collection: string;
    client: Client;
    constructor(endpoint: string, accessKey: string, secretKey: string, opts?: S3Adapter.Options);
    init(broker: ServiceBroker, service: Service): void;
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    /**
     * Objects of the bucket as `{ _id, name, size, lastModified, etag }`. `params.prefix` restricts the listing,
     * `limit`/`offset` page it; other filters are not supported by S3 and ignored.
     */
    find(params?: QueryParams): Promise<ObjectEntry[]>;
    /** Stream of the first object matching the params (see `find`), `undefined` when there is none. */
    findOne(params?: QueryParams): Promise<Readable | undefined>;
    /** Stream of the object `fd`; rejects with a 404 `MoleculerError` when it does not exist. */
    findById(fd: string | number): Promise<Readable>;
    /** Number of objects matching the params (see `find`). */
    count(params?: QueryParams): Promise<number>;
    /**
     * Uploads the stream as `meta.id` (else `meta.filename`, else a random UUID). The primitive entries of `meta`
     * (not the `$`-prefixed Moleculer ones) become the object's user metadata.
     */
    save(entity: NodeJS.ReadableStream, meta?: Record<string, unknown>): Promise<import("minio/dist/main/internal/type").UploadedObjectInfo>;
    /** Replaces the object `id` (moleculer-files passes the id; a metadata object is accepted too). */
    updateById(entity: NodeJS.ReadableStream, id: string | number | Record<string, unknown>): Promise<import("minio/dist/main/internal/type").UploadedObjectInfo>;
    /** Deletes the object; resolves to `{ _id, name }`, or `undefined` when it does not exist. */
    removeById(_id: string | number): Promise<{
        _id: string;
        name: string;
    } | undefined>;
    removeMany(_query?: unknown): void;
    clear(): void;
}
interface ObjectEntry {
    _id: string;
    name: string;
    size?: number;
    lastModified?: Date;
    etag?: string;
}
declare namespace S3Adapter {
    interface Options extends Pick<ClientOptions, "useSSL" | "sessionToken" | "region" | "transport" | "partSize" | "pathStyle"> {
        /** Port of the endpoint; a string is parsed. Omit for the protocol default. */
        port?: number | string | null;
        /** Create the bucket on connect when it does not exist. */
        createBucket?: boolean;
    }
}
export = S3Adapter;
