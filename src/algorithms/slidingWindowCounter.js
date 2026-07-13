/**
 * Sliding Window Counter Algorithm
 *
 * A hybrid of fixed-window and sliding-window-log: keeps a simple integer
 * counter per fixed window (e.g. per-minute buckets), but weights the
 * previous window's count by how much it overlaps the current sliding
 * frame. This gives sliding-window-log-like accuracy without storing a
 * timestamp per request.
 *
 * estimatedCount = currentWindowCount + previousWindowCount * overlapFraction
 *
 * Why this is the production default: O(1) memory per key (two counters,
 * not one entry per request), so it's the algorithm this service uses at
 * the 5,000+ req/sec scale referenced in the README benchmark — sliding
 * window log would work but its memory footprint grows linearly with
 * traffic, which doesn't hold up at that volume.
 */

class SlidingWindowCounterLimiter {
  /**
   * @param {import('ioredis').Redis} redisClient
   * @param {{limit: number, windowMs: number}} config
   */
  constructor(redisClient, { limit = 100, windowMs = 60_000 } = {}) {
    this.redis = redisClient;
    this.limit = limit;
    this.windowMs = windowMs;
  }

  _windowId(timestamp) {
    return Math.floor(timestamp / this.windowMs);
  }

  /**
   * @param {string} identifier
   * @returns {Promise<{allowed: boolean, remaining: number, limit: number, estimatedCount: number}>}
   */
  async consume(identifier) {
    const now = Date.now();
    const currentWindowId = this._windowId(now);
    const previousWindowId = currentWindowId - 1;

    const currentKey = `ratelimit:swc:${identifier}:${currentWindowId}`;
    const previousKey = `ratelimit:swc:${identifier}:${previousWindowId}`;

    try {
      const pipeline = this.redis.pipeline();
      pipeline.get(currentKey);
      pipeline.get(previousKey);
      const results = await pipeline.exec();

      const currentCount = parseInt(results[0][1], 10) || 0;
      const previousCount = parseInt(results[1][1], 10) || 0;

      // How far are we into the current window, as a fraction (0 to 1)?
      const elapsedInCurrentWindow = now % this.windowMs;
      const overlapFraction = 1 - elapsedInCurrentWindow / this.windowMs;

      const estimatedCount = currentCount + previousCount * overlapFraction;

      if (estimatedCount >= this.limit) {
        return {
          allowed: false,
          remaining: 0,
          limit: this.limit,
          estimatedCount: Math.round(estimatedCount),
        };
      }

      const writePipeline = this.redis.pipeline();
      writePipeline.incr(currentKey);
      writePipeline.expire(currentKey, Math.ceil((this.windowMs * 2) / 1000));
      await writePipeline.exec();

      return {
        allowed: true,
        remaining: Math.max(0, Math.floor(this.limit - estimatedCount - 1)),
        limit: this.limit,
        estimatedCount: Math.round(estimatedCount + 1),
      };
    } catch (err) {
      return {
        allowed: true,
        remaining: this.limit,
        limit: this.limit,
        estimatedCount: 0,
        degraded: true,
      };
    }
  }
}

module.exports = { SlidingWindowCounterLimiter };
