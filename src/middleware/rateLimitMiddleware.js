/**
 * Express middleware factory. This is the "pluggable" piece referenced in
 * the project README — drop this into any Express app to rate-limit it,
 * per-user or per-API-key, backed by shared Redis state so it works
 * correctly across multiple horizontally-scaled instances of the app
 * behind a load balancer.
 *
 * Usage:
 *   const { createRateLimiter } = require('distributed-rate-limiter');
 *   app.use(createRateLimiter({ algorithm: 'token-bucket', capacity: 50, refillRatePerSec: 10 }));
 */

const Redis = require('ioredis');
const { TokenBucketLimiter } = require('../algorithms/tokenBucket');
const { SlidingWindowLogLimiter } = require('../algorithms/slidingWindowLog');
const { SlidingWindowCounterLimiter } = require('../algorithms/slidingWindowCounter');
const { CircuitBreaker } = require('./circuitBreaker');
const { metrics } = require('./metrics');

function buildLimiter(algorithm, redisClient, options) {
  switch (algorithm) {
    case 'token-bucket':
      return new TokenBucketLimiter(redisClient, options);
    case 'sliding-window-log':
      return new SlidingWindowLogLimiter(redisClient, options);
    case 'sliding-window-counter':
      return new SlidingWindowCounterLimiter(redisClient, options);
    default:
      throw new Error(
        `Unknown algorithm "${algorithm}". Use "token-bucket", "sliding-window-log", or "sliding-window-counter".`
      );
  }
}

/**
 * @param {object} options
 * @param {'token-bucket'|'sliding-window-log'|'sliding-window-counter'} [options.algorithm]
 * @param {(req: import('express').Request) => string} [options.keyGenerator] - how to identify the caller
 * @param {import('ioredis').Redis} [options.redisClient] - pass an existing client to share a connection pool
 * @param {string} [options.redisUrl]
 * @param {CircuitBreaker} [options.circuitBreaker] - optional, protects a downstream call this middleware guards
 */
function createRateLimiter(options = {}) {
  const {
    algorithm = 'sliding-window-counter',
    keyGenerator = (req) => req.ip,
    redisClient,
    redisUrl = process.env.REDIS_URL || 'redis://localhost:6379',
    circuitBreaker,
    ...algorithmOptions
  } = options;

  const redis = redisClient || new Redis(redisUrl, { lazyConnect: false, maxRetriesPerRequest: 1 });
  const limiter = buildLimiter(algorithm, redis, algorithmOptions);

  return async function rateLimitMiddleware(req, res, next) {
    const start = process.hrtime.bigint();

    if (circuitBreaker && !circuitBreaker.canAttempt()) {
      metrics.recordBlocked(algorithm, 'circuit-open');
      return res.status(503).json({
        error: 'Service temporarily unavailable (circuit breaker open)',
        retryAfterMs: circuitBreaker.getStatus().msUntilRetry,
      });
    }

    const identifier = keyGenerator(req);
    const result = await limiter.consume(identifier);

    const decisionNs = process.hrtime.bigint() - start;
    const decisionMs = Number(decisionNs) / 1_000_000;
    metrics.recordDecision(algorithm, decisionMs, result.allowed);

    res.setHeader('X-RateLimit-Limit', String(result.capacity ?? result.limit ?? ''));
    res.setHeader('X-RateLimit-Remaining', String(result.remaining ?? ''));
    if (result.degraded) res.setHeader('X-RateLimit-Degraded', 'redis-unreachable-fail-open');

    if (!result.allowed) {
      metrics.recordBlocked(algorithm, 'limit-exceeded');
      return res.status(429).json({
        error: 'Too many requests',
        limit: result.capacity ?? result.limit,
        remaining: 0,
      });
    }

    next();
  };
}

module.exports = { createRateLimiter, buildLimiter };
