/*
 * moleculer
 * Copyright (c) 2020 MoleculerJS (https://github.com/moleculerjs/moleculer)
 * MIT Licensed
 */

'use strict';

let Redis, Redlock;
const BaseCacher = require('moleculer/src/cachers/base');
const _ = require('lodash');
const utils = require('moleculer/src/utils');
const { METRIC } = require('moleculer/src/metrics');
const { BrokerOptionsError } = require('moleculer/src/errors');
const LRU = require('redis-lru');

/**
 * Cacher factory for Redis
 *
 * @class RedisCacher
 */
class RedisLRUCacher extends BaseCacher {
  /**
	 * Creates an instance of RedisCacher.
	 *
	 * @param {object} opts
	 *
	 * @memberof RedisCacher
	 */
  constructor (opts) {
    if (typeof opts === 'string') opts = { redis: opts };

    super(opts);

    this.opts = _.defaultsDeep(this.opts, {
      prefix: null,
      pingInterval: null,
      max: 1000,
      namespace: 'REDIS-LRU!'
    });

    this.pingIntervalHandle = null;
  }

  /**
	 * Initialize cacher. Connect to Redis server
	 *
	 * @param {any} broker
	 *
	 * @memberof RedisCacher
	 */
  init (broker) {
    super.init(broker);
    try {
      Redis = require('ioredis');
    } catch (err) {
      /* istanbul ignore next */
      this.broker.fatal(
        "The 'ioredis' package is missing. Please install it with 'npm install ioredis --save' command.",
        err,
        true
      );
    }
    /**
		 * ioredis client instance
		 * @memberof RedisCacher
		 */
    if (this.opts.cluster) {
      if (!this.opts.cluster.nodes || this.opts.cluster.nodes.length === 0) {
        throw new BrokerOptionsError('No nodes defined for cluster');
      }

      this.clientRedis = new Redis.Cluster(this.opts.cluster.nodes, this.opts.cluster.options);
    } else {
      this.clientRedis = new Redis(this.opts.redis);
    }

    this.clientRedis.on('connect', () => {
      /* istanbul ignore next */
      this.logger.info('Redis cacher connected.');
    });

    this.clientRedis.on('error', err => {
      /* istanbul ignore next */
      this.logger.error(err);
    });

    // Create redis-lru
    this.client = LRU(this.clientRedis, this.opts);

    try {
      Redlock = require('redlock');
    } catch (err) {
      /* istanbul ignore next */
      this.logger.warn(
        "The 'redlock' package is missing. If you want to enable cache lock, please install it with 'npm install redlock --save' command."
      );
    }
    if (Redlock) {
      let redlockClients = (this.opts.redlock ? this.opts.redlock.clients : null) || [
        this.clientRedis
      ];
      /**
			 * redlock client instance
			 * @memberof RedisCacher
			 */
      this.redlock = new Redlock(redlockClients, _.omit(this.opts.redlock, ['clients']));
      // Non-blocking redlock client, used for tryLock()
      this.redlockNonBlocking = new Redlock(redlockClients, {
        retryCount: 0
      });
    }
    if (this.opts.monitor) {
      /* istanbul ignore next */
      this.clientRedis.monitor((err, monitor) => {
        this.logger.debug('Redis cacher entering monitoring mode...');
        monitor.on('monitor', (time, args /*, source, database */) => {
          this.logger.debug(args);
        });
      });
    }

    // add interval for ping if set
    if (this.opts.pingInterval > 0) {
      this.pingIntervalHandle = setInterval(() => {
        this.clientRedis
          .ping()
          .then(() => {
            this.logger.debug('Sent PING to Redis Server');
          })
          .catch(err => {
            this.logger.error('Failed to send PING to Redis Server', err);
          });
      }, Number(this.opts.pingInterval));
    }

    this.logger.debug('Redis Cacher created. Prefix: ' + this.prefix);
  }

  /**
	 * Close Redis client connection
	 *
	 * @memberof RedisCacher
	 */
  close () {
    if (this.pingIntervalHandle != null) {
      clearInterval(this.pingIntervalHandle);
      this.pingIntervalHandle = null;
    }
    return this.clientRedis != null ? this.clientRedis.quit() : Promise.resolve();
  }

  /**
	 * Get data from cache by key
	 *
	 * @param {any} key
	 * @returns {Promise}
	 *
	 * @memberof Cacher
	 */
  get (key) {
    this.logger.debug(`GET ${key}`);
    this.metrics.increment(METRIC.MOLECULER_CACHER_GET_TOTAL);
    const timeEnd = this.metrics.timer(METRIC.MOLECULER_CACHER_GET_TIME);

    return this.client.get(this.prefix + key).then(data => {
      if (data) {
        this.logger.debug(`FOUND ${key}`);
        this.metrics.increment(METRIC.MOLECULER_CACHER_FOUND_TOTAL);

        timeEnd();
        return data;
      }
      timeEnd();
      return null;
    });
  }

  /**
	 * Save data to cache by key
	 *
	 * @param {String} key
	 * @param {any} data JSON object
	 * @param {Number} ttl Optional Time-to-Live
	 * @returns {Promise}
	 *
	 * @memberof Cacher
	 */
  set (key, data, ttl) {
    this.metrics.increment(METRIC.MOLECULER_CACHER_SET_TOTAL);
    const timeEnd = this.metrics.timer(METRIC.MOLECULER_CACHER_SET_TIME);

    this.logger.debug(`SET ${key}`);

    if (ttl == null) ttl = this.opts.ttl;

    let p;
    if (ttl) {
      p = this.client.set(this.prefix + key, data, ttl);
    } else {
      p = this.client.set(this.prefix + key, data);
    }

    return p
      .then(res => {
        timeEnd();
        return res;
      })
      .catch(err => {
        timeEnd();
        throw err;
      });
  }

  /**
	 * Delete a key from cache
	 *
	 * @param {string|Array<string>} deleteTargets
	 * @returns {Promise}
	 *
	 * @memberof Cacher
	 */
  del (deleteTargets) {
    const self = this;
    this.metrics.increment(METRIC.MOLECULER_CACHER_DEL_TOTAL);
    const timeEnd = this.metrics.timer(METRIC.MOLECULER_CACHER_DEL_TIME);

    deleteTargets = Array.isArray(deleteTargets) ? deleteTargets : [deleteTargets];
    const keysToDelete = deleteTargets.map(key => this.prefix + key);
    this.logger.debug(`DELETE ${keysToDelete}`);
    const p = keysToDelete.map(key => {
      return self.del(key);
    });

    return Promise.all(p)
      .then(res => {
        timeEnd();
        return res;
      })
      .catch(err => {
        timeEnd();
        this.logger.error(`Redis 'del' error. Key: ${keysToDelete}`, err);
        throw err;
      });
  }

  /**
	 * Clean cache. Remove every key by prefix
	 *        http://stackoverflow.com/questions/4006324/how-to-atomically-delete-keys-matching-a-pattern-using-redis
	 * alternative solution:
	 *        https://github.com/cayasso/cacheman-redis/blob/master/lib/index.js#L125
	 * @param {String|Array<String>} match Match string for SCAN. Default is "*"
	 * @returns {Promise}
	 *
	 * @memberof Cacher
	 */
  async clean (match = '**') {
    this.metrics.increment(METRIC.MOLECULER_CACHER_CLEAN_TOTAL);
    const timeEnd = this.metrics.timer(METRIC.MOLECULER_CACHER_CLEAN_TIME);

    const cleaningPatterns = Array.isArray(match) ? match : [match];
    const matches = cleaningPatterns.map(match => this.prefix + match);
    this.logger.debug(`CLEAN ${matches.join(', ')}`);

    const keys = await this.client.keys();
    keys.forEach(async (key) => {
      if (matches.some(match => utils.match(key, match))) {
        this.logger.debug(`REMOVE ${key}`);
        await this.client.del(key);
      }
    });
    timeEnd();

    return this.broker.Promise.resolve();
  }

  /**
	 * Get data and ttl from cache by key.
	 *
	 * @param {string|Array<string>} key
	 * @returns {Promise}
	 *
	 * @memberof RedisCacher
	 */
  getWithTTL (key) {
    // There are no way to get the ttl of LRU cache :(
    return this.client.get(key).then(data => {
      return { data, ttl: null };
    });
  }

  /**
	 * Acquire a lock
	 *
	 * @param {string|Array<string>} key
	 * @param {Number} ttl Optional Time-to-Live
	 * @returns {Promise}
	 *
	 * @memberof RedisCacher
	 */
  lock (key, ttl = 15000) {
    key = this.prefix + key + '-lock';
    return this.redlock.lock(key, ttl).then(lock => {
      return () => lock.unlock();
    });
  }

  /**
	 * Try to acquire a lock
	 *
	 * @param {string|Array<string>} key
	 * @param {Number} ttl Optional Time-to-Live
	 * @returns {Promise}
	 *
	 * @memberof RedisCacher
	 */
  tryLock (key, ttl = 15000) {
    key = this.prefix + key + '-lock';
    return this.redlockNonBlocking.lock(key, ttl).then(lock => {
      return () => lock.unlock();
    });
  }

  /**
	 * Return all cache keys with available properties (ttl, lastUsed, ...etc).
	 *
	 * @returns Promise<Array<Object>>
	 */
  async getCacheKeys () {
    const keys = await this.client.keys();
    return keys.map(key => ({
      key: key.startsWith(this.prefix) ? key.slice(this.prefix.length) : key
    }));
  }
}

module.exports = RedisLRUCacher;
