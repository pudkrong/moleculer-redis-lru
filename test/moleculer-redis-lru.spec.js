const ServiceBroker = require('moleculer/src/service-broker');
const RedisLRUCacher = require('../');

const LRU = require('redis-lru');
jest.mock('redis-lru');

jest.mock('ioredis');
const Redis = require('ioredis');

const lolex = require('@sinonjs/fake-timers');

const protectReject = (err) => {
  if (err && err.stack) {
    console.error(err);
    console.error(err.stack);
  }
  expect(err).toBe(true);
};

describe('Test RedisLRUCacher constructor', () => {
  it('should create an empty options', () => {
    const cacher = new RedisLRUCacher();
    expect(cacher).toBeDefined();
    expect(cacher.opts).toBeDefined();
    expect(cacher.opts.max).toBe(1000);
    expect(cacher.opts.ttl).toBeNull();
  });

  it('should create a timer if set ttl option', () => {
    let opts = { ttl: 500, max: 1024, prefix: 'custom-' };
    let cacher = new RedisLRUCacher(opts);
    expect(cacher).toBeDefined();
    expect(cacher.opts).toEqual(opts);
    expect(cacher.opts.ttl).toBe(500);
    expect(cacher.opts.prefix).toBe('custom-');
    expect(cacher.opts.max).toBe(1024);
  });

  it('should add option for pingInterval', () => {
    let opts = { pingInterval: 5000 };
    let cacher = new RedisLRUCacher(opts);
    expect(cacher).toBeDefined();
    expect(cacher.opts).toEqual(opts);
    expect(cacher.opts.pingInterval).toBe(5000);
  });

  it('should create with redis opts from string', () => {
    let opts = 'redis://localhost:6379';
    let cacher = new RedisLRUCacher(opts);
    expect(cacher).toBeDefined();
    expect(cacher.opts).toEqual({
      keygen: null,
      ttl: null,
      prefix: null,
      pingInterval: null,
      maxParamsLength: null,
      redis: opts,
      max: 1000,
      namespace: 'REDIS-LRU!'
    });
  });
});

describe('Test RedisLRUCacher init', () => {
  const broker = new ServiceBroker({ logger: false });

  beforeEach(() => {
    LRU.mockClear();
    Redis.mockClear();
  });

  it('should create Redis client with default options', () => {
    const cacher = new RedisLRUCacher();
    cacher.init(broker);

    expect(cacher.clientRedis).toBeInstanceOf(Redis);
    expect(LRU).toHaveBeenCalledTimes(1);
    expect(LRU).toHaveBeenCalledWith(cacher.clientRedis, Object.assign({ maxAge: cacher.opts.ttl }, cacher.opts));
  });

  it('should create Redis client with default options', () => {
    const opts = { redis: { host: '1.2.3.4' } };
    const cacher = new RedisLRUCacher(opts);
    cacher.init(broker);

    expect(cacher.clientRedis).toBeInstanceOf(Redis);
    expect(Redis).toHaveBeenCalledTimes(1);
    expect(Redis).toHaveBeenCalledWith(opts.redis);
  });
});

describe('Test RedisCacher cluster', () => {
  it('should create with redis opts', () => {
    let opts = {
      type: 'Redis',
      ttl: 30,
      cluster: {
        nodes: [
          {
            host: 'localhost',
            port: 6379
          }
        ]
      }
    };

    let cacher = new RedisLRUCacher(opts);
    expect(cacher).toBeDefined();
    expect(cacher.opts).toEqual(opts);
  });

  it('should init redis cluster', () => {
    let broker = new ServiceBroker({ logger: false });

    let opts = {
      type: 'Redis',
      ttl: 30,
      cluster: {
        nodes: [
          {
            host: 'localhost',
            port: 6379
          }
        ]
      }
    };

    let cacher = new RedisLRUCacher(opts);
    expect(cacher).toBeDefined();
    expect(cacher.opts).toEqual(opts);
    cacher.init(broker);
    expect(cacher.clientRedis).toBeInstanceOf(Redis.Cluster);
  });

  it('should fail to init redis cluster without nodes', () => {
    let broker = new ServiceBroker({ logger: false });

    let opts = {
      type: 'Redis',
      ttl: 30,
      cluster: {
        nodes: []
      }
    };

    let cacher = new RedisLRUCacher(opts);
    expect(cacher).toBeDefined();
    expect(cacher.opts).toEqual(opts);
    expect(() => {
      cacher.init(broker);
    }).toThrowError('No nodes defined for cluster');
  });

  it('should ping based on numeric interval', () => {
    jest.useFakeTimers();
    let broker = new ServiceBroker({ logger: false });

    let opts = {
      type: 'Redis',
      pingInterval: 25
    };

    let cacher = new RedisLRUCacher(opts);
    expect(cacher).toBeDefined();
    cacher.init(broker);
    cacher.clientRedis.ping = jest.fn().mockResolvedValue(undefined);

    jest.advanceTimersByTime(25);
    expect(cacher.clientRedis.ping).toHaveBeenCalledTimes(1);
    jest.advanceTimersByTime(25);
    expect(cacher.clientRedis.ping).toHaveBeenCalledTimes(2);
    jest.advanceTimersByTime(25);
    expect(cacher.clientRedis.ping).toHaveBeenCalledTimes(3);

    jest.clearAllTimers();
    jest.useRealTimers();
  });

  it('should ping based on numeric string interval', () => {
    jest.useFakeTimers();
    let broker = new ServiceBroker({ logger: false });

    let opts = {
      type: 'Redis',
      pingInterval: '25'
    };

    let cacher = new RedisLRUCacher(opts);
    expect(cacher).toBeDefined();
    cacher.init(broker);
    cacher.clientRedis.ping = jest.fn().mockResolvedValue(undefined);

    jest.advanceTimersByTime(25);
    expect(cacher.clientRedis.ping).toHaveBeenCalledTimes(1);
    jest.advanceTimersByTime(25);
    expect(cacher.clientRedis.ping).toHaveBeenCalledTimes(2);
    jest.advanceTimersByTime(25);
    expect(cacher.clientRedis.ping).toHaveBeenCalledTimes(3);

    jest.clearAllTimers();
    jest.useRealTimers();
  });

  it('should not ping with malformed pingInterval', () => {
    jest.useFakeTimers();
    let broker = new ServiceBroker({ logger: false });

    let opts = {
      type: 'Redis',
      pingInterval: 'foo'
    };

    let cacher = new RedisLRUCacher(opts);
    expect(cacher).toBeDefined();
    cacher.init(broker);
    cacher.clientRedis.ping = jest.fn().mockResolvedValue(undefined);

    jest.advanceTimersByTime(25);
    expect(cacher.clientRedis.ping).toHaveBeenCalledTimes(0);
    jest.advanceTimersByTime(25);
    expect(cacher.clientRedis.ping).toHaveBeenCalledTimes(0);
    jest.advanceTimersByTime(25);
    expect(cacher.clientRedis.ping).toHaveBeenCalledTimes(0);

    jest.clearAllTimers();
    jest.useRealTimers();
  });
});

describe('Test RedisCacher set & get without prefix', () => {
  let broker = new ServiceBroker({ logger: false });
  let cacher = new RedisLRUCacher();
  cacher.init(broker);

  let key = 'tst123';
  let data1 = {
    a: 1,
    b: false,
    c: 'Test',
    d: {
      e: 55
    }
  };

  let key2 = 'posts123';

  let prefix = 'MOL-';

  beforeEach(() => {
    Redis.mockClear();
    LRU.mockClear();

    cacher.client = jest.fn();
    cacher.client.get = jest.fn(() => Promise.resolve(data1));
    cacher.client.set = jest.fn(() => Promise.resolve());
    cacher.client.setex = jest.fn(() => Promise.resolve());
    cacher.client.del = jest.fn(() => Promise.resolve());
    cacher.client.keys = jest.fn(() => Promise.resolve([prefix + key, prefix + key2]));
  });

  it('should call client.set with key & data', () => {
    cacher.set(key, data1);
    expect(cacher.client.set).toHaveBeenCalledTimes(1);
    expect(cacher.client.set).toHaveBeenCalledWith(
      prefix + key,
      data1
    );
    expect(cacher.client.setex).toHaveBeenCalledTimes(0);
  });

  it('should call client.get with key & return with data1', () => {
    let p = cacher.get(key);
    expect(cacher.client.get).toHaveBeenCalledTimes(1);
    expect(cacher.client.get).toHaveBeenCalledWith(prefix + key);
    return p.catch(protectReject).then(d => {
      expect(d).toEqual(data1);
    });
  });

  it('should call client.del with key', () => {
    cacher.del(key);
    expect(cacher.client.del).toHaveBeenCalledTimes(1);
    expect(cacher.client.del).toHaveBeenCalledWith(prefix + key);
  });

  it('should delete an array of keys', () => {
    cacher.del(['key1', 'key2']);
    expect(cacher.client.del).toHaveBeenCalledTimes(2);
    expect(cacher.client.del).toHaveBeenNthCalledWith(1, prefix + 'key1');
    expect(cacher.client.del).toHaveBeenNthCalledWith(2, prefix + 'key2');
  });

  it('should call client.keys & del', () => {
    return cacher
      .clean()
      .catch(protectReject)
      .then(() => {
        expect(cacher.client.keys).toHaveBeenCalledTimes(1);
        expect(cacher.client.del).toHaveBeenCalledTimes(2);
        expect(cacher.client.del).toHaveBeenNthCalledWith(1, prefix + key);
        expect(cacher.client.del).toHaveBeenNthCalledWith(2, prefix + key2);
      });
  });

  it('should clean tst* keys', () => {
    return cacher
      .clean('tst*')
      .catch(protectReject)
      .then(() => {
        expect(cacher.client.keys).toHaveBeenCalledTimes(1);
        expect(cacher.client.del).toHaveBeenCalledTimes(1);
        expect(cacher.client.del).toHaveBeenNthCalledWith(1, prefix + key);
      });
  });

  it('should clean posts* keys', () => {
    return cacher
      .clean('posts*')
      .catch(protectReject)
      .then(() => {
        expect(cacher.client.keys).toHaveBeenCalledTimes(1);
        expect(cacher.client.del).toHaveBeenCalledTimes(1);
        expect(cacher.client.del).toHaveBeenNthCalledWith(1, prefix + key2);
      });
  });

  it('should clean by multiple patterns', () => {
    return cacher
      .clean(['tst*', 'posts*'])
      .catch(protectReject)
      .then(() => {
        expect(cacher.client.keys).toHaveBeenCalledTimes(1);
        expect(cacher.client.del).toHaveBeenCalledTimes(2);
        expect(cacher.client.del).toHaveBeenNthCalledWith(1, prefix + key);
        expect(cacher.client.del).toHaveBeenNthCalledWith(2, prefix + key2);
      });
  });
});

describe('Test RedisLRUCacher set & get with namespace & ttl', () => {
  const broker = new ServiceBroker({ logger: false, namespace: 'uat' });
  let cacher = new RedisLRUCacher({
    ttl: 60
  });
  cacher.init(broker); // for empty logger

  ['fatal', 'error', 'info', 'log', 'debug'].forEach(level => (cacher.logger[level] = jest.fn()));

  let key = 'tst123';
  let data1 = {
    a: 1,
    b: false,
    c: 'Test',
    d: {
      e: 55
    }
  };

  let key2 = 'posts123';

  let prefix = 'MOL-uat-';

  beforeEach(() => {
    Redis.mockClear();
    LRU.mockClear();

    cacher.client = jest.fn();
    cacher.client.get = jest.fn(() => Promise.resolve(data1));
    cacher.client.set = jest.fn(() => Promise.resolve());
    cacher.client.setex = jest.fn(() => Promise.resolve());
    cacher.client.del = jest.fn(() => Promise.resolve());
    cacher.client.keys = jest.fn(() => Promise.resolve([prefix + key, prefix + key2]));

    ['error', 'fatal', 'info', 'log', 'debug'].forEach(level =>
      cacher.logger[level].mockClear()
    );
  });

  it('should call client.set with key, data, and ttl', () => {
    cacher.set(key, data1);
    expect(cacher.client.set).toHaveBeenCalledTimes(1);
    expect(cacher.client.set).toHaveBeenCalledWith(
      prefix + key,
      data1,
      60
    );
  });

  it('should give back the data by key', () => {
    cacher.get(key);
    expect(cacher.client.get).toHaveBeenCalledTimes(1);
    expect(cacher.client.get).toHaveBeenCalledWith(prefix + key);
  });

  it('should call client.del with key', () => {
    return cacher
      .del(key)
      .catch(protectReject)
      .then(() => {
        expect(cacher.client.del).toHaveBeenCalledTimes(1);
        expect(cacher.client.del).toHaveBeenCalledWith(prefix + key);
      });
  });

  it('should call client.del with multiple keys', () => {
    return cacher
      .del(['key1', 'key2'])
      .catch(protectReject)
      .then(() => {
        expect(cacher.client.del).toHaveBeenCalledTimes(2);
        expect(cacher.client.del).toHaveBeenNthCalledWith(1, prefix + 'key1');
        expect(cacher.client.del).toHaveBeenNthCalledWith(2, prefix + 'key2');
      });
  });

  it('should throw error', () => {
    const error = new Error('Redis delete error');
    cacher.client.del = jest.fn(() => Promise.reject(error));
    return cacher
      .del(['key1'])
      .then(protectReject)
      .catch(err => {
        expect(err).toBe(error);
        expect(cacher.client.del).toHaveBeenCalledTimes(1);
        expect(cacher.client.del).toHaveBeenCalledWith(prefix + 'key1');
        expect(cacher.logger.error).toHaveBeenCalledTimes(1);
        expect(cacher.logger.error).toHaveBeenCalledWith(
          "Redis 'del' error. Key: MOL-uat-key1",
          error
        );
      });
  });

  it('should call client.keys & del', () => {
    return cacher
      .clean()
      .catch(protectReject)
      .then(() => {
        expect(cacher.client.keys).toHaveBeenCalledTimes(1);
        expect(cacher.client.del).toHaveBeenCalledTimes(2);
        expect(cacher.client.del).toHaveBeenNthCalledWith(1, prefix + key);
        expect(cacher.client.del).toHaveBeenNthCalledWith(2, prefix + key2);
      });
  });

  it('should clean tst* keys', () => {
    return cacher
      .clean('tst*')
      .catch(protectReject)
      .then(() => {
        expect(cacher.client.keys).toHaveBeenCalledTimes(1);
        expect(cacher.client.del).toHaveBeenCalledTimes(1);
        expect(cacher.client.del).toHaveBeenNthCalledWith(1, prefix + key);
      });
  });
});

describe('Test RedisLRUCacher close', () => {
  it('should call client.quit', () => {
    let broker = new ServiceBroker({ logger: false });
    let cacher = new RedisLRUCacher();
    cacher.init(broker); // for empty logger
    cacher.close();

    expect(cacher.clientRedis.quit).toHaveBeenCalledTimes(1);
  });

  it('should clear interval', () => {
    jest.useFakeTimers();
    let broker = new ServiceBroker({ logger: false });

    let opts = {
      pingInterval: 25
    };

    let cacher = new RedisLRUCacher(opts);
    cacher.init(broker); // for empty logger
    cacher.clientRedis.ping = jest.fn().mockResolvedValue(undefined);

    expect(jest.getTimerCount()).toBe(1);

    cacher.close();
    expect(jest.getTimerCount()).toBe(0);

    jest.clearAllTimers();
    jest.useRealTimers();
  });
});
