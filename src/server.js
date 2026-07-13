/**
 * Demo server for the distributed rate limiter.
 *
 * Exposes:
 *   GET  /api/resource        - a sample protected endpoint, rate-limited
 *   GET  /health              - basic health check
 *   GET  /metrics             - JSON snapshot of current metrics
 *   WS   /ws/metrics          - live metrics stream for the dashboard
 *   GET  /                    - serves the monitoring dashboard (dashboard/index.html)
 *
 * Run alongside Redis (see README for docker-compose) and optionally
 * `npm run load-test` in another terminal to see live traffic on the
 * dashboard.
 */

require('dotenv').config();
const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');
const Redis = require('ioredis');

const { createRateLimiter } = require('./middleware/rateLimitMiddleware');
const { CircuitBreaker } = require('./middleware/circuitBreaker');
const { metrics } = require('./middleware/metrics');

const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const ALGORITHM = process.env.RATE_LIMIT_ALGORITHM || 'sliding-window-counter';

const app = express();
const redisClient = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });

redisClient.on('error', (err) => {
  console.warn(`[redis] connection issue (service will fail-open): ${err.message}`);
});

const circuitBreaker = new CircuitBreaker({
  failureThreshold: 5,
  openDurationMs: 30_000,
  rollingWindowMs: 10_000,
});
circuitBreaker.onStateChange = (newState, oldState) => {
  console.log(`[circuit-breaker] ${oldState} -> ${newState}`);
};

const limiterMiddleware = createRateLimiter({
  algorithm: ALGORITHM,
  redisClient,
  circuitBreaker,
  keyGenerator: (req) => req.headers['x-api-key'] || req.ip,
  // token-bucket options
  capacity: 50,
  refillRatePerSec: 10,
  // sliding-window options (used if algorithm is set to one of those instead)
  limit: 100,
  windowMs: 60_000,
});

app.use(express.static(path.join(__dirname, 'dashboard')));
app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ status: 'ok', algorithm: ALGORITHM, circuitBreaker: circuitBreaker.getStatus() });
});

app.get('/metrics', (req, res) => {
  res.json(metrics.getSnapshot());
});

// The actual protected resource — this is what the rate limiter guards.
app.get('/api/resource', limiterMiddleware, async (req, res) => {
  try {
    if (!circuitBreaker.canAttempt()) {
      throw new Error('circuit open');
    }
    // Simulate a downstream dependency (DB, third-party API, etc).
    // FAIL_RATE env var lets the load-test/demo script simulate an outage
    // to demonstrate the circuit breaker tripping.
    const failRate = Number(process.env.DEMO_FAIL_RATE || 0);
    if (Math.random() < failRate) {
      throw new Error('simulated downstream failure');
    }
    circuitBreaker.recordSuccess();
    res.json({ ok: true, servedAt: Date.now() });
  } catch (err) {
    circuitBreaker.recordFailure();
    res.status(502).json({ ok: false, error: err.message });
  }
});

const server = http.createServer(app);

// --- WebSocket dashboard feed ---
const wss = new WebSocketServer({ server, path: '/ws/metrics' });
wss.on('connection', (ws) => {
  ws.send(JSON.stringify(metrics.getSnapshot()));
  const unsubscribe = metrics.onUpdate((snapshot) => {
    if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(snapshot));
  });
  ws.on('close', unsubscribe);
});

// Push a heartbeat snapshot every second even if no new requests came in,
// so the dashboard chart doesn't stall.
setInterval(() => {
  const snapshot = metrics.getSnapshot();
  const payload = JSON.stringify(snapshot);
  wss.clients.forEach((client) => {
    if (client.readyState === client.OPEN) client.send(payload);
  });
}, 1000);

server.listen(PORT, () => {
  console.log(`Rate limiter demo server listening on http://localhost:${PORT}`);
  console.log(`Algorithm: ${ALGORITHM}`);
  console.log(`Dashboard: http://localhost:${PORT}`);
});

module.exports = { app, server };
