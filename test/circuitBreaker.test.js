const { test, describe } = require('node:test');
const assert = require('node:assert/strict');
const { CircuitBreaker, STATES } = require('../src/middleware/circuitBreaker');

describe('CircuitBreaker', () => {
  test('starts CLOSED and allows attempts', () => {
    const cb = new CircuitBreaker();
    assert.equal(cb.state, STATES.CLOSED);
    assert.equal(cb.canAttempt(), true);
  });

  test('opens after failureThreshold failures within rolling window', () => {
    const cb = new CircuitBreaker({ failureThreshold: 3, rollingWindowMs: 10_000 });
    cb.recordFailure();
    cb.recordFailure();
    assert.equal(cb.state, STATES.CLOSED, 'should still be closed before hitting threshold');
    cb.recordFailure();
    assert.equal(cb.state, STATES.OPEN);
    assert.equal(cb.canAttempt(), false);
  });

  test('does not open if failures are below threshold', () => {
    const cb = new CircuitBreaker({ failureThreshold: 5 });
    cb.recordFailure();
    cb.recordFailure();
    assert.equal(cb.state, STATES.CLOSED);
    assert.equal(cb.canAttempt(), true);
  });

  test('moves to HALF_OPEN after openDurationMs elapses', async () => {
    const cb = new CircuitBreaker({ failureThreshold: 1, openDurationMs: 50 });
    cb.recordFailure();
    assert.equal(cb.state, STATES.OPEN);
    assert.equal(cb.canAttempt(), false);

    await new Promise((r) => setTimeout(r, 60));

    assert.equal(cb.canAttempt(), true, 'should allow trial request after cooldown');
    assert.equal(cb.state, STATES.HALF_OPEN);
  });

  test('HALF_OPEN success closes the circuit and clears failure history', async () => {
    const cb = new CircuitBreaker({ failureThreshold: 1, openDurationMs: 30 });
    cb.recordFailure();
    await new Promise((r) => setTimeout(r, 40));
    cb.canAttempt(); // transitions to HALF_OPEN
    cb.recordSuccess();
    assert.equal(cb.state, STATES.CLOSED);
    assert.equal(cb.getStatus().recentFailures, 0);
  });

  test('HALF_OPEN failure re-opens the circuit', async () => {
    const cb = new CircuitBreaker({ failureThreshold: 1, openDurationMs: 30 });
    cb.recordFailure();
    await new Promise((r) => setTimeout(r, 40));
    cb.canAttempt(); // transitions to HALF_OPEN
    cb.recordFailure();
    assert.equal(cb.state, STATES.OPEN);
  });

  test('onStateChange callback fires with correct arguments', () => {
    const cb = new CircuitBreaker({ failureThreshold: 1 });
    const transitions = [];
    cb.onStateChange = (newState, oldState) => transitions.push([oldState, newState]);
    cb.recordFailure();
    assert.deepEqual(transitions, [[STATES.CLOSED, STATES.OPEN]]);
  });

  test('getStatus reports msUntilRetry while OPEN', () => {
    const cb = new CircuitBreaker({ failureThreshold: 1, openDurationMs: 10_000 });
    cb.recordFailure();
    const status = cb.getStatus();
    assert.equal(status.state, STATES.OPEN);
    assert.ok(status.msUntilRetry > 9000 && status.msUntilRetry <= 10000);
  });
});
