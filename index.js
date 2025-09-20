'use strict';

const { MoleculerError, ServiceSchemaError } = require('moleculer').Errors;
const uuidv4 = require('uuid/v4');
const isStream = require('is-stream');
const Minio = require('minio');

class S3Adapter {
	constructor(endpoint, accessKey, secretKey, opts) {
		this.endPoint = endpoint;
		this.opts = opts;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
	}

	init(broker, service) {
		this.broker = broker;
		this.service = service;

		if (!this.endPoint) {
			throw new ServiceSchemaError('Missing `endPoint` definition!');
		}
		if (!this.accessKey) {
			throw new ServiceSchemaError('Missing `accessKey` definition!');
		}
		if (!this.secretKey) {
			throw new ServiceSchemaError('Missing `secretKey` definition!');
		}

		if (!this.service.schema.collection) {
			/* istanbul ignore next */
			throw new ServiceSchemaError(
				'Missing `collection` definition in schema of service!',
			);
		}
		this.collection = this.service.schema.collection;
	}

	async connect() {
		const { endPoint, accessKey, secretKey } = this;
		const {
			port,
			useSSL,
			sessionToken,
			region,
			transport,
			partSize,
			pathStyle,
		} = this.opts;
		try {
			this.client = new Minio.Client({
				endPoint,
				port: parseInt(port),
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
			if (!exists && !!this.opts.createBucket) {
				this.client.makeBucket(this.collection);
			}
		} catch (err) {
			this.service.logger.error('S3 error.', err);
			throw err;
		}
	}

	disconnect() {
		return Promise.resolve();
	}

	async find(filters) {
		if (filters)
			this.service.logger.warn(
				'Filters not yet implemented for S3 driver.',
			);
		return new Promise((resolve, reject) => {
			try {
				const items = [];
				const stream = this.client.listObjects(
					this.collection,
					'',
					true,
				);
				stream.on('data', (obj) => {
					items.push(obj);
				});
				stream.on('end', () => {
					resolve(data);
				});
				stream.on('error', (err) => {
					this.service.logger.error(
						'Error during find operation in S3 bucket.',
						err,
					);
					reject(err);
				});
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
		return new Promise((resolve, reject) => {
			this.client.getObject(this.collection, fd, (err, stream) => {
				if (err) {
					if (err.code == 'NoSuchKey') {
						return reject(new MoleculerError(`File \`${fd}\` not found`, 404, "ERR_NOT_FOUND"));
					}
					return reject(err);
				}
				return resolve(stream);
			});
		});
	}

	async count(filters = {}) {
		// To be implemented
		return;
	}

	async save(entity, meta) {
		if (!isStream(entity))
			throw new MoleculerError(
				'Entity is not a stream',
				400,
				'E_BAD_REQUEST',
			)();

		const filename = meta.id || meta.filename || uuidv4();
		return this.client.putObject(
			this.collection,
			filename,
			entity,
			null,
			meta,
		);
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

module.exports = S3Adapter;
