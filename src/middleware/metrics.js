/**
 * Rolling in-process metrics store. Feeds the WebSocket dashboard with
 * live request rate, block rate, and decision-latency numbers.
 *
 * Kept intentionally simple (ring buffer, no external dependency) since
 * its only job is powering a real-time chart, not long-term analytics —
 * for that you'd ship these events to Prometheus/Datadog instead, which
 * is a natural next step called out in the README.
 */

const WINDOW_SECONDS = 60;

class MetricsStore {
  constructor() {
    this._buckets = new Map(); // secondTimestamp -> { allowed, blocked, latencies: [] }
    this._listeners = new Set();
  }

  _bucketFor(tsMs) {
    const sec = Math.floor(tsMs / 1000);
    if (!this._buckets.has(sec)) {
      this._buckets.set(sec, { allowed: 0, blocked: 0, latencies: [], byAlgorithm: {} });
      this._prune();
    }
    return this._buckets.get(sec);
  }

  _prune() {
    const cutoff = Math.floor(Date.now() / 1000) - WINDOW_SECONDS;
    for (const sec of this._buckets.keys()) {
      if (sec < cutoff) this._buckets.delete(sec);
    }
  }

  recordDecision(algorithm, latencyMs, allowed) {
    const bucket = this._bucketFor(Date.now());
    if (allowed) bucket.allowed += 1;
    else bucket.blocked += 1;
    bucket.latencies.push(latencyMs);
    bucket.byAlgorithm[algorithm] = (bucket.byAlgorithm[algorithm] || 0) + 1;
    this._emit();
  }

  recordBlocked(algorithm, reason) {
    // circuit-breaker blocks bypass the normal decision path but should
    // still show up on the dashboard's blocked-traffic line
    const bucket = this._bucketFor(Date.now());
    bucket.blocked += 1;
    this._emit();
  }

  onUpdate(fn) {
    this._listeners.add(fn);
    return () => this._listeners.delete(fn);
  }

  _emit() {
    const snapshot = this.getSnapshot();
    for (const fn of this._listeners) fn(snapshot);
  }

  getSnapshot() {
    this._prune();
    const now = Math.floor(Date.now() / 1000);
    const series = [];
    let totalAllowed = 0;
    let totalBlocked = 0;
    let allLatencies = [];

    for (let i = WINDOW_SECONDS - 1; i >= 0; i--) {
      const sec = now - i;
      const bucket = this._buckets.get(sec) || { allowed: 0, blocked: 0, latencies: [] };
      series.push({ t: sec, allowed: bucket.allowed, blocked: bucket.blocked });
      totalAllowed += bucket.allowed;
      totalBlocked += bucket.blocked;
      allLatencies = allLatencies.concat(bucket.latencies);
    }

    allLatencies.sort((a, b) => a - b);
    const p50 = percentile(allLatencies, 0.5);
    const p99 = percentile(allLatencies, 0.99);

    return {
      series,
      totalAllowed,
      totalBlocked,
      reqPerSec: series.length ? Math.round((totalAllowed + totalBlocked) / series.length) : 0,
      p50LatencyMs: round2(p50),
      p99LatencyMs: round2(p99),
      updatedAt: Date.now(),
    };
  }
}

function percentile(sortedArr, p) {
  if (!sortedArr.length) return 0;
  const idx = Math.min(sortedArr.length - 1, Math.floor(p * sortedArr.length));
  return sortedArr[idx];
}

function round2(n) {
  return Math.round(n * 100) / 100;
}

const metrics = new MetricsStore();

module.exports = { metrics, MetricsStore };
