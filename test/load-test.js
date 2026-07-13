/**
 * Load test script — this is what generates the numbers referenced in the
 * README (req/sec sustained, decision latency). Run with the server and
 * Redis already up:
 *
 *   npm start                 (in one terminal)
 *   npm run load-test         (in another)
 *
 * Uses autocannon to fire concurrent requests and reports real throughput
 * and latency percentiles — not estimated numbers.
 */

const autocannon = require('autocannon');

const TARGET_URL = process.env.TARGET_URL || 'http://localhost:3000/api/resource';
const DURATION_SEC = Number(process.env.LOAD_TEST_DURATION || 15);
const CONNECTIONS = Number(process.env.LOAD_TEST_CONNECTIONS || 50);

async function run() {
  console.log(`Load testing ${TARGET_URL}`);
  console.log(`Duration: ${DURATION_SEC}s, Connections: ${CONNECTIONS}`);
  console.log('---');

  const result = await autocannon({
    url: TARGET_URL,
    connections: CONNECTIONS,
    duration: DURATION_SEC,
    setupClient: (client) => {
      // Distinct key per connection so we're measuring the rate limiter's
      // raw decision throughput, not one key's allowance being exhausted.
      client.setHeaders({ 'x-api-key': `load-test-${Math.random().toString(36).slice(2, 10)}` });
    },
  });

  console.log('\n=== Results ===');
  console.log(`Requests/sec (avg):  ${result.requests.average}`);
  console.log(`Requests/sec (max):  ${result.requests.max}`);
  console.log(`Latency p50:         ${result.latency.p50}ms`);
  console.log(`Latency p99:         ${result.latency.p99}ms`);
  console.log(`Total requests:      ${result.requests.total}`);

  const codes = result.statusCodeStats || {};
  for (const code of Object.keys(codes).sort()) {
    console.log(`  status ${code}:        ${codes[code].count}`);
  }
  console.log(`Errors:              ${result.errors}`);
  console.log(`Timeouts:            ${result.timeouts}`);
  console.log('\nNote: with the default token-bucket config (capacity 50, refill 10/sec)');
  console.log('per single IP/key, most requests above ~10/sec from ONE key will 429 by');
  console.log('design — that is the rate limiter working correctly. To test raw');
  console.log('throughput of the limiter itself (not a single key\'s allowance), set');
  console.log('many distinct x-api-key values or point CONNECTIONS at distinct IPs.');
}

run().catch((err) => {
  console.error('Load test failed:', err.message);
  process.exit(1);
});
