"use strict";

const { MoleculerError, ServiceSchemaError } = require("moleculer").Errors;
const uuidv4 = require("uuid/v4");
const isStream = require("is-stream");
const Minio = require("minio");

class FSAdapter {

	constructor(endpoint, accessKey, secretKey, opts) {
	  this.endpoint = endpoint;
		this.opts = opts;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
	}

	init(broker, service) {
		this.broker = broker;
		this.service = service;

		if (!this.uri) {
  		throw new ServiceSchemaError("Missing `uri` definition!");
    }
		if (!this.accessKey) {
			throw new ServiceSchemaError("Missing S3 access key!");
		}
		if (!this.secretKey) {
			throw new ServiceSchemaError("Missing S3 secret key!");
		}

		if (!this.service.schema.collection) {
			/* istanbul ignore next */
			throw new ServiceSchemaError("Missing `collection` definition in schema of service!");
		}
		this.collection = this.service.schema.collection;
	}

	async connect() {
		const {endpoint, accessKey, secretKey} = this;
		const {
			port,
			useSSL,
			sessionToken,
			region,
			transport,
			partSize,
			pathStyle
		} = this.opts;
		try {
			this.client = new Minio.Client({
				endpoint,
				port,
				useSSL,
				accessKey,
				secretKey,
				sessionToken,
				region,
				transport,
				partSize,
				pathStyle
			});
			const exists = await this.client.bucketExists(this.collection);
			if (!exists && !!this.opts.createBucket) {
				await this.client.createBucket(this.collection);
			}
		} catch(err) {
			this.service.logger.error("S3 error.", err);
			throw err;
		}
	}

	disconnect() {
		return Promise.resolve();
	}

	async find(filters) {
		if (filters)
			this.service.logger.warn('Filters not yet implemented for S3 driver.');
		return new Promise((resolve, reject) => {
			try {
				const items = [];
				const stream = this.client.listObjects(this.collection, '', true);
				stream.on('data', (obj) => { items.push(obj); } );
				stream.on('end',  () => { resolve(data); });
				stream.on('error', (err) => {
					this.service.logger.error('Error during find operation in S3 bucket.', err);
					reject(err);
				})
			} catch (error) {
				reject(error);
			}
		});
	}

	findOne(query) {
		// To be implemented
		return;
	}

	findById(fd) {
		return this.client.getObject(this.collection, fd);
	}

	async count(filters = {}) {
		// To be implemented
		return;
	}

	async save(entity, meta) {
		if (!isStream(entity)) reject(new MoleculerError("Entity is not a stream", 400, "E_BAD_REQUEST"));

		const filename = meta.id || meta.filename || uuidv4();
		return this.client.putObject(this.collection, filename, entity, null, meta);
	}

	async updateById(entity, meta) {
		return await this.save(entity, meta);
	}

	removeMany(query) {
		// To Be Implemented.
	}

	removeById(_id) {
		return this.client.removeObject(this.collection, _id);
	}

	clear() {
		// To be implemented
		return;
	}
}

module.exports = FSAdapter;
