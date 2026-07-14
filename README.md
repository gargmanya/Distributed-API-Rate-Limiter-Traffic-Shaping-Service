# Distributed API Rate Limiter & Traffic Shaping Service

A standalone rate-limiting service implementing three algorithms from scratch — token bucket, sliding window log, and sliding window counter — with Redis-backed distributed state, a circuit breaker for downstream failure protection, and a live monitoring dashboard.

Built to work correctly across multiple horizontally-scaled instances of an API sitting behind a load balancer, not just on a single process.

## Why three algorithms

Each makes a different tradeoff, and a real production rate limiter should let you pick per use case:

| Algorithm | Accuracy | Memory per key | Best for |
|---|---|---|---|
| **Token bucket** | Allows controlled bursts | O(1) | User-facing APIs where short bursts are fine but sustained abuse isn't |
| **Sliding window log** | Exact | O(n) — one entry per request in-window | Low/medium traffic, billing-relevant endpoints where exact counts matter |
| **Sliding window counter** | Approximate (good enough in practice) | O(1) | High-volume endpoints — this is the production default here |

## Architecture

```
Client → Express middleware → [Circuit Breaker check] → [Rate Limiter algorithm] → Redis (shared state)
                                                                  ↓
                                                          Metrics store → WebSocket → Live dashboard
```

- **Redis-backed state** so N horizontally-scaled instances of your API share one source of truth for each key's usage — a request hitting instance A and the next hitting instance B still get correctly rate-limited together.
- **Atomic Lua script** for the token bucket algorithm (`EVAL`) so concurrent requests across instances can't race each other into over-consuming tokens. This is the actual mechanism that makes it "distributed" rather than just "using a shared database."
- **Fail-open on Redis outage** — if Redis is unreachable, requests are allowed through with a `X-RateLimit-Degraded` header rather than taking the whole API down. A rate limiter should never become a single point of failure for the thing it's protecting.
- **Circuit breaker** wraps the downstream call this middleware guards — after repeated failures it short-circuits immediately (fast failure) instead of piling retries onto an already-struggling downstream service.

## Benchmarks (measured, not estimated)

Run yourself with `npm run load-test` — numbers below are from this repo's own test run in a resource-constrained single-container sandbox (2 vCPU, no tuning), so treat them as a floor, not a ceiling, on real infrastructure.

**Rate-limiter decision latency** (the algorithm itself — Redis round-trip included, HTTP layer excluded):
```
SlidingWindowCounter:  avg=0.25ms  p50=0.15ms  p99=1.77ms
TokenBucket:            avg=0.14ms  p50=0.09ms  p99=2.17ms
```
This is the number that matters for "how much overhead does adding this middleware add to each request" — sub-2ms at p99.

**End-to-end HTTP throughput** (full request path: Node/Express/middleware/Redis, 50 concurrent connections, distinct API keys per connection to measure raw throughput rather than one key's allowance):
```
Requests/sec (avg): 636
Requests/sec (max): 1078
Latency p50: 63ms   Latency p99: 206ms
Total: 6,359 requests over 10s — 5,200 allowed (200), 1,159 correctly rate-limited (429)
```
The gap between the ~0.2ms decision latency and the ~63ms end-to-end p50 is Node/Express HTTP handling overhead and this sandbox's loopback networking — not the rate limiter. On real infrastructure (dedicated instance, connection pooling tuned, HTTP keep-alive) this end-to-end number improves significantly; the algorithm's own overhead is the part that's architecturally fixed and small.

**Reproduce these numbers yourself:**
```bash
npm start                    # terminal 1
npm run load-test            # terminal 2 — hits /api/resource with 50 connections for 15s
```

## Circuit breaker

Three states — `CLOSED` (normal) → `OPEN` (short-circuiting, fast-fails without calling downstream) → `HALF_OPEN` (cooldown elapsed, testing recovery with one trial request). Configurable failure threshold and cooldown window. See `test/circuitBreaker.test.js` for the full behavioral spec — 8 tests covering every state transition.

Demo the circuit breaker tripping under simulated downstream failure:
```bash
DEMO_FAIL_RATE=0.8 npm start   # 80% of downstream calls simulate failure
npm run load-test              # watch the circuit open in the server logs after 5 failures
```

## Quickstart

```bash
git clone https://github.com/gargmanya/Distributed-API-Rate-Limiter-Traffic-Shaping-Service.git
cd Distributed-API-Rate-Limiter-Traffic-Shaping-Service
npm install

# Redis required — either:
docker run -d -p 6379:6379 redis:7-alpine
# or install locally: apt install redis-server / brew install redis

npm start
# → http://localhost:3000 (live dashboard)
# → http://localhost:3000/api/resource (rate-limited demo endpoint)
```

Change algorithm via env var:
```bash
RATE_LIMIT_ALGORITHM=token-bucket npm start
RATE_LIMIT_ALGORITHM=sliding-window-log npm start
RATE_LIMIT_ALGORITHM=sliding-window-counter npm start   # default
```

## Using it as middleware in your own Express app

```js
const { createRateLimiter } = require('./src/middleware/rateLimitMiddleware');

app.use(createRateLimiter({
  algorithm: 'token-bucket',
  capacity: 50,
  refillRatePerSec: 10,
  keyGenerator: (req) => req.headers['x-api-key'] || req.ip,
}));
```

## Tests

```bash
npm test
```
15 tests: 7 algorithm integration tests against real Redis (not mocked — they exercise the actual Lua script and Redis pipeline logic), 8 circuit breaker unit tests covering every state transition. Redis must be running; tests skip cleanly with a warning if it isn't reachable.

## What I'd add next

- Ship metrics to Prometheus/Grafana instead of the in-memory store here (the in-memory version is intentionally simple — good for a live demo dashboard, not for production observability)
- Sliding window log currently doesn't cap memory growth under sustained high traffic on one key — would add a max-entries safety valve
- Multi-region Redis (currently assumes one Redis instance/cluster reachable by all API nodes — fine for one region, needs replication strategy for multi-region)

## Stack

Node.js, Express, Redis (ioredis), WebSockets (ws), React (dashboard, via CDN — no build step), autocannon (load testing)
