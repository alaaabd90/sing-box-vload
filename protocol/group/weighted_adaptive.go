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
	limit    atomic.Int64
	inFlight atomic.Int32
	ceiling  int64 // optional operator-configured cap on how high limit may grow; 0 = none beyond adaptiveMaxLimit
}

const (
	adaptiveInitialLimit = 4   // conservative starting point; grows from here under real load
	adaptiveMinLimit     = 1   // never throttle a member to zero - it must always get an occasional probe to recover
	adaptiveMaxLimit     = 256 // safety ceiling against unbounded growth
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
		return
	}
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
