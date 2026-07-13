/**
 * Sliding Window Log Algorithm
 *
 * Stores a timestamp for every request in a Redis sorted set (score = timestamp).
 * On each request: drop timestamps older than the window, count what's left,
 * and allow the request only if the count is under the limit.
 *
 * Why sliding window log: it's the most *accurate* of the three algorithms —
 * no burst-at-boundary problem like fixed windows have (e.g. 100 requests at
 * 11:59:59 and another 100 at 12:00:01 both passing a "100/min" fixed-window
 * limiter because they land in different windows). The tradeoff is memory:
 * one Redis entry per request, per key, until it ages out of the window.
 *
 * This is why the service offers three algorithms rather than one — sliding
 * window log is the right choice for low-to-medium traffic keys where exact
 * accuracy matters (e.g. billing-relevant endpoints), while sliding window
 * counter (see slidingWindowCounter.js) is the better choice at high volume
 * where the memory cost of logging every request becomes real.
 */

class SlidingWindowLogLimiter {
  /**
   * @param {import('ioredis').Redis} redisClient
   * @param {{limit: number, windowMs: number}} config
   */
  constructor(redisClient, { limit = 100, windowMs = 60_000 } = {}) {
    this.redis = redisClient;
    this.limit = limit;
    this.windowMs = windowMs;
  }

  /**
   * @param {string} identifier
   * @returns {Promise<{allowed: boolean, remaining: number, limit: number}>}
   */
  async consume(identifier) {
    const key = `ratelimit:swl:${identifier}`;
    const now = Date.now();
    const windowStart = now - this.windowMs;

    try {
      const pipeline = this.redis.pipeline();
      // 1. Drop entries outside the current window
      pipeline.zremrangebyscore(key, 0, windowStart);
      // 2. Count what's left inside the window
      pipeline.zcard(key);
      const results = await pipeline.exec();
      const currentCount = results[1][1];

      if (currentCount >= this.limit) {
        return { allowed: false, remaining: 0, limit: this.limit };
      }

      // 3. Record this request (member must be unique — timestamp alone can
      //    collide under high concurrency, so we append a random suffix)
      const member = `${now}-${Math.random().toString(36).slice(2, 8)}`;
      const writePipeline = this.redis.pipeline();
      writePipeline.zadd(key, now, member);
      writePipeline.expire(key, Math.ceil(this.windowMs / 1000) + 1);
      await writePipeline.exec();

      return {
        allowed: true,
        remaining: Math.max(0, this.limit - currentCount - 1),
        limit: this.limit,
      };
    } catch (err) {
      return { allowed: true, remaining: this.limit, limit: this.limit, degraded: true };
    }
  }
}

module.exports = { SlidingWindowLogLimiter };
