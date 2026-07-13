/**
 * Token Bucket Algorithm
 *
 * Each key (user/API key) has a bucket that holds up to `capacity` tokens.
 * Tokens refill continuously at `refillRatePerSec`. Every request costs 1 token.
 * If the bucket is empty, the request is rejected.
 *
 * Why token bucket: it allows short bursts up to the bucket capacity while
 * still enforcing a long-run average rate — better UX than a hard cutoff
 * for bursty traffic like page loads that fire several requests at once.
 *
 * Implementation notes:
 * - State (tokens remaining, last refill timestamp) is stored in Redis so
 *   multiple horizontally-scaled API instances share one source of truth.
 * - Refill + consume is done in a single Lua script executed atomically via
 *   EVAL, so concurrent requests from different instances can't race each
 *   other into over-consuming tokens (this is the part that actually makes
 *   it "distributed" rather than just "backed by a shared database").
 */

const REFILL_AND_CONSUME_SCRIPT = `
  local key = KEYS[1]
  local capacity = tonumber(ARGV[1])
  local refillRatePerSec = tonumber(ARGV[2])
  local now = tonumber(ARGV[3])
  local requested = tonumber(ARGV[4])

  local bucket = redis.call('HMGET', key, 'tokens', 'lastRefillMs')
  local tokens = tonumber(bucket[1])
  local lastRefillMs = tonumber(bucket[2])

  if tokens == nil then
    tokens = capacity
    lastRefillMs = now
  end

  local elapsedSec = math.max(0, (now - lastRefillMs) / 1000)
  local refill = elapsedSec * refillRatePerSec
  tokens = math.min(capacity, tokens + refill)

  local allowed = 0
  if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
  end

  redis.call('HMSET', key, 'tokens', tokens, 'lastRefillMs', now)
  -- expire idle buckets after 1 hour of no traffic to avoid unbounded key growth
  redis.call('EXPIRE', key, 3600)

  return { allowed, tostring(tokens) }
`;

class TokenBucketLimiter {
  /**
   * @param {import('ioredis').Redis} redisClient
   * @param {{capacity: number, refillRatePerSec: number}} config
   */
  constructor(redisClient, { capacity = 20, refillRatePerSec = 5 } = {}) {
    this.redis = redisClient;
    this.capacity = capacity;
    this.refillRatePerSec = refillRatePerSec;
    this._scriptSha = null;
  }

  async _ensureScriptLoaded() {
    if (!this._scriptSha) {
      this._scriptSha = await this.redis.script('LOAD', REFILL_AND_CONSUME_SCRIPT);
    }
    return this._scriptSha;
  }

  /**
   * @param {string} identifier - e.g. `user:123` or `apikey:abc`
   * @param {number} cost - tokens this request consumes (default 1)
   * @returns {Promise<{allowed: boolean, remaining: number, capacity: number}>}
   */
  async consume(identifier, cost = 1) {
    const key = `ratelimit:tb:${identifier}`;
    const now = Date.now();

    let sha;
    try {
      sha = await this._ensureScriptLoaded();
    } catch (err) {
      // Redis unreachable — fail open with a warning so a Redis outage doesn't
      // take down the whole API. Circuit breaker (see circuitBreaker.js) handles
      // sustained failures at a higher level.
      return { allowed: true, remaining: this.capacity, capacity: this.capacity, degraded: true };
    }

    try {
      const [allowed, remaining] = await this.redis.evalsha(
        sha,
        1,
        key,
        this.capacity,
        this.refillRatePerSec,
        now,
        cost
      );
      return {
        allowed: allowed === 1,
        remaining: Math.floor(Number(remaining)),
        capacity: this.capacity,
      };
    } catch (err) {
      if (err.message && err.message.includes('NOSCRIPT')) {
        // Redis restarted and lost its script cache — reload once and retry.
        this._scriptSha = null;
        return this.consume(identifier, cost);
      }
      return { allowed: true, remaining: this.capacity, capacity: this.capacity, degraded: true };
    }
  }
}

module.exports = { TokenBucketLimiter };
