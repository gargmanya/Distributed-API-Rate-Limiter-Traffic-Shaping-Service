/**
 * Circuit Breaker
 *
 * Wraps calls to a downstream service. Tracks failures in a rolling window;
 * once failures cross `failureThreshold`, the circuit "opens" and further
 * calls are rejected immediately (without hitting the downstream service)
 * for `openDurationMs`. After that cooldown, it moves to "half-open" and
 * allows a single trial request through — success closes the circuit again,
 * failure re-opens it.
 *
 * States:
 *   CLOSED    — normal operation, requests pass through
 *   OPEN      — downstream is considered down, requests short-circuit and fail fast
 *   HALF_OPEN — cooldown elapsed, testing with one request to see if it recovered
 *
 * Why this matters for a rate limiter specifically: if a downstream service
 * starts failing, retrying/queueing every rejected request against it makes
 * the outage worse (thundering herd on recovery). Short-circuiting protects
 * the downstream service and gives the caller a fast, predictable failure
 * instead of hanging on timeouts.
 */

const STATES = { CLOSED: 'CLOSED', OPEN: 'OPEN', HALF_OPEN: 'HALF_OPEN' };

class CircuitBreaker {
  /**
   * @param {{failureThreshold: number, openDurationMs: number, rollingWindowMs: number}} config
   */
  constructor({
    failureThreshold = 5,
    openDurationMs = 30_000,
    rollingWindowMs = 10_000,
  } = {}) {
    this.failureThreshold = failureThreshold;
    this.openDurationMs = openDurationMs;
    this.rollingWindowMs = rollingWindowMs;

    this.state = STATES.CLOSED;
    this.failureTimestamps = [];
    this.openedAt = null;
    this.onStateChange = null; // optional callback(newState, oldState) for dashboard/metrics
  }

  _setState(newState) {
    if (newState === this.state) return;
    const old = this.state;
    this.state = newState;
    if (typeof this.onStateChange === 'function') {
      this.onStateChange(newState, old);
    }
  }

  _pruneOldFailures() {
    const cutoff = Date.now() - this.rollingWindowMs;
    this.failureTimestamps = this.failureTimestamps.filter((t) => t > cutoff);
  }

  /**
   * Call before attempting the downstream request.
   * @returns {boolean} true if the call should proceed, false if it should short-circuit
   */
  canAttempt() {
    if (this.state === STATES.CLOSED) return true;

    if (this.state === STATES.OPEN) {
      const elapsed = Date.now() - this.openedAt;
      if (elapsed >= this.openDurationMs) {
        this._setState(STATES.HALF_OPEN);
        return true; // allow exactly one trial request through
      }
      return false;
    }

    // HALF_OPEN: only one trial in flight at a time
    return this._halfOpenTrialInFlight !== true;
  }

  recordSuccess() {
    if (this.state === STATES.HALF_OPEN) {
      this.failureTimestamps = [];
      this._halfOpenTrialInFlight = false;
      this._setState(STATES.CLOSED);
    }
  }

  recordFailure() {
    this._pruneOldFailures();
    this.failureTimestamps.push(Date.now());

    if (this.state === STATES.HALF_OPEN) {
      this._halfOpenTrialInFlight = false;
      this.openedAt = Date.now();
      this._setState(STATES.OPEN);
      return;
    }

    if (this.state === STATES.CLOSED && this.failureTimestamps.length >= this.failureThreshold) {
      this.openedAt = Date.now();
      this._setState(STATES.OPEN);
    }
  }

  getStatus() {
    this._pruneOldFailures();
    return {
      state: this.state,
      recentFailures: this.failureTimestamps.length,
      failureThreshold: this.failureThreshold,
      msUntilRetry:
        this.state === STATES.OPEN
          ? Math.max(0, this.openDurationMs - (Date.now() - this.openedAt))
          : 0,
    };
  }
}

module.exports = { CircuitBreaker, STATES };
