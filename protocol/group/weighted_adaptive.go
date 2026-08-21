package group

import "sync/atomic"

// adaptiveLimiter is a self-tuning per-member concurrency limit, so a
// weighted group member's real sustainable throughput doesn't need to be
// guessed or hand-configured (no fixed weight, no fixed max_connections):
// it discovers it live from what actually happens on the wire, the same way
// TCP congestion control discovers a path's usable bandwidth rather than
// having it configured.
//
//   - On a success that happened while the member was fully using its
//     current limit (inFlight >= limit), grow the limit by one - the member
//     has just proven it can carry one more concurrent connection than we'd
//     been giving it credit for.
//   - On any failure, halve the limit immediately - a sharp, TCP-style
//     multiplicative backoff, since a member that just failed shouldn't keep
//     being handed the same amount of concurrent work a moment later.
//
// This runs underneath - not instead of - the existing circuit breaker
// (memberHealth/circuitBreakerThreshold): the limiter reacts to every single
// result and keeps concurrency throttled down long before three consecutive
// failures would trip the breaker outright, so in practice a merely
// struggling member gets gently starved rather than repeatedly hard-tripped.
type adaptiveLimiter struct {
	limit           atomic.Int64
	inFlight        atomic.Int32
	ceiling         int64 // optional operator-configured cap on how high limit may grow; 0 = none beyond adaptiveMaxLimit
	saturatedStreak atomic.Int32
}

const (
	// adaptiveInitialLimit was 4 - tuned around a download manager's
	// sustained, parallel-segment overshoot scenario. In practice, an
	// ordinary browser page load doesn't look like that at all: it opens
	// dozens of short-lived connections across many hosts in one burst,
	// each closing well before 3 consecutive *saturated* successes (see
	// adaptiveGrowthHoldDown below) can accumulate to grow the limit even
	// once. Confirmed live: a real Load Balance session sitting at
	// inFlight=31 against limit=1-4 for its entire duration (vload log,
	// "picked slot N ... active=31, limit=1") - i.e. the limiter was
	// permanently pinned near its floor while genuinely carrying 30+
	// connections, throttling every pick() decision toward whichever
	// member's headroom (limit-inFlight) happened to be least negative
	// rather than actually balancing load, and making Load Balance feel
	// slow next to a direct profile's unlimited concurrency for no real
	// capacity reason. Start high enough to cover realistic browser
	// concurrency from the first connection instead of relying on a ramp
	// that bursty traffic rarely completes - the halving-on-failure and
	// circuit breaker below are what actually protect against a member
	// that can't sustain this, unchanged.
	adaptiveInitialLimit = 32
	adaptiveMinLimit     = 1 // never throttle a member to zero - it must always get an occasional probe to recover
	adaptiveMaxLimit     = 256 // safety ceiling against unbounded growth
	// adaptiveGrowthHoldDown is how many consecutive saturated successes are
	// required before the limit grows by one, instead of growing on the very
	// first one. A member's real capacity ceiling is only ever discovered by
	// overshooting it and causing an actual failure - growing on every single
	// saturated success let a burst of near-simultaneous completions (a real
	// download's parallel connections all landing around the same time) push
	// the limit far past a network's true sustainable concurrency before the
	// first failure had any chance to correct it. That deep an overshoot then
	// failed several connections at once, tripping the circuit breaker's full
	// exclusion instead of the limiter's own gentler halving handling it.
	// Requiring a short streak of corroborating saturated successes first
	// slows the climb enough to keep overshoots small, without meaningfully
	// slowing how fast a member reaches a genuinely high sustainable
	// capacity - every still-saturated success keeps counting toward the
	// next step either way.
	adaptiveGrowthHoldDown = 3
)

func newAdaptiveLimiter(ceiling int) *adaptiveLimiter {
	l := &adaptiveLimiter{ceiling: int64(ceiling)}
	l.limit.Store(adaptiveInitialLimit)
	return l
}

func (l *adaptiveLimiter) effectiveMax() int64 {
	if l.ceiling > 0 && l.ceiling < adaptiveMaxLimit {
		return l.ceiling
	}
	return adaptiveMaxLimit
}

// headroom is how much spare proven capacity this member currently has.
// Callers pick the member with the greatest headroom - i.e. whichever one
// has the most room left within what it's already shown it can sustain.
func (l *adaptiveLimiter) headroom() int64 {
	return l.limit.Load() - int64(l.inFlight.Load())
}

func (l *adaptiveLimiter) acquire() {
	l.inFlight.Add(1)
}

func (l *adaptiveLimiter) release() {
	l.inFlight.Add(-1)
}

// onSuccess grows the limit by one if inFlightAtCompletion (the in-flight
// count read before this connection's own release) shows the member was
// fully saturated - i.e. this success is actual evidence of spare capacity,
// not just a lightly-loaded member behaving fine.
func (l *adaptiveLimiter) onSuccess(inFlightAtCompletion int32) {
	if int64(inFlightAtCompletion) < l.limit.Load() {
		l.saturatedStreak.Store(0)
		return
	}
	if l.saturatedStreak.Add(1) < adaptiveGrowthHoldDown {
		return
	}
	l.saturatedStreak.Store(0)
	max := l.effectiveMax()
	for {
		cur := l.limit.Load()
		if cur >= max {
			return
		}
		next := cur + 1
		if next > max {
			next = max
		}
		if l.limit.CompareAndSwap(cur, next) {
			return
		}
	}
}

func (l *adaptiveLimiter) onFailure() {
	l.saturatedStreak.Store(0)
	for {
		cur := l.limit.Load()
		next := cur / 2
		if next < adaptiveMinLimit {
			next = adaptiveMinLimit
		}
		if cur == next {
			return
		}
		if l.limit.CompareAndSwap(cur, next) {
			return
		}
	}
}
