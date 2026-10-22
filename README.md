# moleculer-files-s3

S3 driver for [`moleculer-files`](https://github.com/keletes/moleculer-files), on the [MinIO JS client](https://github.com/minio/minio-js) (v8), so it works with AWS S3, MinIO,
DigitalOcean Spaces and any S3-compatible endpoint. TypeScript, compiled output and declarations included.

## Install

Not on npm; installed from a git tag (compiled output is committed on the release commit, no build step, no auth: the repo is public).

```json
"moleculer-files-s3": "git+https://github.com/keletes/moleculer-files-s3.git#vX.Y.Z",
"moleculer-files": "git+https://github.com/keletes/moleculer-files.git#vX.Y.Z",
"moleculer": "^0.14.32"
```

Node >= 22. `moleculer` and `moleculer-files` are peer dependencies.

## Usage

```ts
import FilesService from "moleculer-files";
import S3 from "moleculer-files-s3"; // `require("moleculer-files-s3")` returns the same class

export default {
	name: "cache",
	mixins: [FilesService],
	adapter: new S3(process.env.S3_ENDPOINT!, process.env.S3_ACCESS_KEY!, process.env.S3_SECRET!, {
		port: process.env.S3_PORT, // number or string, omit for the protocol default
		useSSL: true,
		createBucket: true, // create the bucket on connect when missing
		// also: region, sessionToken, pathStyle, partSize, transport
	}),
	collection: "my-bucket", // the bucket
};
```

## Behaviour

- **`save`**: key is `meta.id`, else `meta.filename`, else a random UUID; existing keys are overwritten. Primitive entries of `meta` (not `$`-prefixed) become user metadata (`x-amz-meta-*`).
  A failing source stream rejects the call and aborts the unfinished multipart upload.
- **`get`**: readable stream; a missing key is a 404 (`FileNotFoundError` through the mixin).
- **`find`/`count`/`list`**: lists the bucket as `{ _id, name, size, lastModified, etag }`; `prefix`, `limit` and `offset` are supported, other filters are ignored (S3 has none).
- **`update`**: replaces the object `id`. **`remove`**: deletes it and returns it; missing key is a 404.

### Changes from 0.1.x

Same constructor and class export. `minio` 7 -> 8 (promise API). Fixes: `find` crashed (`resolve(data)` on an undefined variable) so `find`/`list` never worked; `save` threw a `TypeError` instead of a 400 for a non-stream;
`remove` always answered 404; `update` stored under a random name; `makeBucket` was not awaited; a failing source stream hung the upload; non-primitive metadata values broke the request headers;
`port: null` produced `NaN`. `uuid` and `is-stream` dependencies dropped.
