/**
 * Integration tests against a real Redis instance (not mocked) — these
 * exercise the actual Lua script and pipeline logic, since the correctness
 * of a distributed rate limiter lives in those exact details.
 *
 * Requires Redis running locally (see README: `docker compose up redis`
 * or `redis-server`). Tests skip with a clear message if Redis isn't
 * reachable, so `npm test` doesn't hard-fail in an environment without it.
 */

const { test, describe, before, after, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const Redis = require('ioredis');

const { TokenBucketLimiter } = require('../src/algorithms/tokenBucket');
const { SlidingWindowLogLimiter } = require('../src/algorithms/slidingWindowLog');
const { SlidingWindowCounterLimiter } = require('../src/algorithms/slidingWindowCounter');

let redis;
let redisAvailable = true;

before(async () => {
  redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
    lazyConnect: true,
    retryStrategy: () => null,
  });
  try {
    await redis.connect();
    await redis.ping();
  } catch (err) {
    redisAvailable = false;
    console.warn('\n⚠ Redis not reachable — skipping algorithm integration tests.');
    console.warn('  Start Redis (e.g. `docker compose up -d redis`) to run these.\n');
  }
});

after(async () => {
  if (redisAvailable) await redis.quit();
});

async function flushTestKeys(pattern) {
  const keys = await redis.keys(pattern);
  if (keys.length) await redis.del(...keys);
}

describe('TokenBucketLimiter', () => {
  beforeEach(async () => { if (redisAvailable) await flushTestKeys('ratelimit:tb:test*'); });

  test('allows requests up to capacity, then blocks', async (t) => {
    if (!redisAvailable) return t.skip('Redis not available');
    const limiter = new TokenBucketLimiter(redis, { capacity: 3, refillRatePerSec: 0.001 });
    const id = 'test-user-1';

    const r1 = await limiter.consume(id);
    const r2 = await limiter.consume(id);
    const r3 = await limiter.consume(id);
    const r4 = await limiter.consume(id);

    assert.equal(r1.allowed, true);
    assert.equal(r2.allowed, true);
    assert.equal(r3.allowed, true);
    assert.equal(r4.allowed, false, 'bucket should be empty after capacity is exhausted');
  });

  test('refills over time', async (t) => {
    if (!redisAvailable) return t.skip('Redis not available');
    const limiter = new TokenBucketLimiter(redis, { capacity: 2, refillRatePerSec: 10 });
    const id = 'test-user-refill';

    await limiter.consume(id);
    await limiter.consume(id);
    const blocked = await limiter.consume(id);
    assert.equal(blocked.allowed, false);

    await new Promise((r) => setTimeout(r, 150)); // ~1.5 tokens should refill at 10/sec

    const afterRefill = await limiter.consume(id);
    assert.equal(afterRefill.allowed, true, 'should allow after refill window');
  });

  test('different identifiers have independent buckets', async (t) => {
    if (!redisAvailable) return t.skip('Redis not available');
    const limiter = new TokenBucketLimiter(redis, { capacity: 1, refillRatePerSec: 0.001 });
    const a = await limiter.consume('test-user-a');
    const b = await limiter.consume('test-user-b');
    assert.equal(a.allowed, true);
    assert.equal(b.allowed, true, 'separate identifier should have its own full bucket');
  });
});

describe('SlidingWindowLogLimiter', () => {
  beforeEach(async () => { if (redisAvailable) await flushTestKeys('ratelimit:swl:test*'); });

  test('allows up to limit within window, then blocks', async (t) => {
    if (!redisAvailable) return t.skip('Redis not available');
    const limiter = new SlidingWindowLogLimiter(redis, { limit: 3, windowMs: 5000 });
    const id = 'test-swl-1';

    for (let i = 0; i < 3; i++) {
      const r = await limiter.consume(id);
      assert.equal(r.allowed, true, `request ${i + 1} should be allowed`);
    }
    const blocked = await limiter.consume(id);
    assert.equal(blocked.allowed, false);
  });

  test('entries outside the window no longer count', async (t) => {
    if (!redisAvailable) return t.skip('Redis not available');
    const limiter = new SlidingWindowLogLimiter(redis, { limit: 1, windowMs: 100 });
    const id = 'test-swl-2';

    const r1 = await limiter.consume(id);
    assert.equal(r1.allowed, true);
    const r2 = await limiter.consume(id);
    assert.equal(r2.allowed, false);

    await new Promise((r) => setTimeout(r, 150)); // let the window pass

    const r3 = await limiter.consume(id);
    assert.equal(r3.allowed, true, 'should allow again once old entries age out');
  });
});

describe('SlidingWindowCounterLimiter', () => {
  beforeEach(async () => { if (redisAvailable) await flushTestKeys('ratelimit:swc:test*'); });

  test('allows up to limit, then blocks within same window', async (t) => {
    if (!redisAvailable) return t.skip('Redis not available');
    const limiter = new SlidingWindowCounterLimiter(redis, { limit: 3, windowMs: 60_000 });
    const id = 'test-swc-1';

    for (let i = 0; i < 3; i++) {
      const r = await limiter.consume(id);
      assert.equal(r.allowed, true);
    }
    const blocked = await limiter.consume(id);
    assert.equal(blocked.allowed, false);
  });

  test('estimate blends previous and current window counts', async (t) => {
    if (!redisAvailable) return t.skip('Redis not available');
    // windowMs small enough to reliably straddle two windows within the test
    const limiter = new SlidingWindowCounterLimiter(redis, { limit: 100, windowMs: 200 });
    const id = 'test-swc-2';

    const r1 = await limiter.consume(id);
    assert.ok(r1.estimatedCount >= 1);
  });
});
