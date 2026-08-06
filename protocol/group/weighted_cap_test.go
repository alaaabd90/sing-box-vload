package group

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

// fakeOutbound is a minimal adapter.Outbound whose DialContext/ListenPacket
// hand back a real, closable net.Conn/net.PacketConn without touching the
// network, so tests can drive Weighted's pick()/release()/recordResult()
// bookkeeping end-to-end.
type fakeOutbound struct {
	tag string
}

func (f *fakeOutbound) Type() string           { return "fake" }
func (f *fakeOutbound) Tag() string            { return f.tag }
func (f *fakeOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (f *fakeOutbound) Dependencies() []string { return nil }

func (f *fakeOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	client, server := net.Pipe()
	go func() { _ = server.Close() }() // nobody reads/writes; just let Close() on our end succeed
	return client, nil
}

func (f *fakeOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return net.ListenPacket("udp", "127.0.0.1:0")
}

// newAdaptiveTestWeighted builds a real Weighted (not just its picker) with
// fake members, so pick()/release()/recordResult() - and therefore the
// adaptive concurrency limiter driving admission control in pick() - all
// actually run, not just the limiter in isolation. ceilings mirrors the
// optional operator-configured MaxConnections override (0 = none).
func newAdaptiveTestWeighted(weights []int, ceilings []int) *Weighted {
	w := &Weighted{
		tags:   make([]string, len(weights)),
		picker: newPCCPicker(weights),
		health: make([]memberHealth, len(weights)),
		logger: testNopLogger{},
	}
	w.limiters = make([]*adaptiveLimiter, len(weights))
	w.members = make([]weightedMember, len(weights))
	for i := range weights {
		w.limiters[i] = newAdaptiveLimiter(ceilings[i])
		w.tags[i] = "member"
		w.members[i] = weightedMember{tag: "member", outbound: &fakeOutbound{tag: "member"}}
	}
	return w
}

func TestPickPrefersMemberWithMoreHeadroom(t *testing.T) {
	w := newAdaptiveTestWeighted([]int{50, 50}, []int{0, 0})
	// Manually starve member 0's headroom by acquiring most of its initial
	// limit, without touching member 1 - the next pick must go to member 1,
	// the one with more spare capacity, regardless of weight/cycle position.
	w.limiters[0].acquire()
	w.limiters[0].acquire()
	w.limiters[0].acquire() // member 0: limit=4, inFlight=3, headroom=1
	// member 1: limit=4, inFlight=0, headroom=4 - should win every time now
	for i := 0; i < 5; i++ {
		index, _, err := w.pick()
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		if index != 1 {
			t.Fatalf("expected member 1 (more headroom), got %d", index)
		}
		w.release(index)
	}
}

func TestAdaptiveLimiterGrowsOnSaturatedSuccess(t *testing.T) {
	l := newAdaptiveLimiter(0)
	if l.limit.Load() != adaptiveInitialLimit {
		t.Fatalf("expected initial limit %d, got %d", adaptiveInitialLimit, l.limit.Load())
	}
	// A success where inFlightAtCompletion == limit proves the member can
	// sustain one more - the limit should grow.
	l.onSuccess(int32(adaptiveInitialLimit))
	if l.limit.Load() != adaptiveInitialLimit+1 {
		t.Fatalf("expected limit to grow to %d, got %d", adaptiveInitialLimit+1, l.limit.Load())
	}
}

func TestAdaptiveLimiterDoesNotGrowOnUnsaturatedSuccess(t *testing.T) {
	l := newAdaptiveLimiter(0)
	// inFlightAtCompletion well below the limit - no evidence of spare
	// capacity being proven, so the limit must stay put.
	l.onSuccess(1)
	if l.limit.Load() != adaptiveInitialLimit {
		t.Fatalf("expected limit to stay at %d, got %d", adaptiveInitialLimit, l.limit.Load())
	}
}

func TestAdaptiveLimiterHalvesOnFailure(t *testing.T) {
	l := newAdaptiveLimiter(0)
	l.limit.Store(20)
	l.onFailure()
	if got := l.limit.Load(); got != 10 {
		t.Fatalf("expected limit to halve to 10, got %d", got)
	}
	l.onFailure()
	if got := l.limit.Load(); got != 5 {
		t.Fatalf("expected limit to halve to 5, got %d", got)
	}
}

func TestAdaptiveLimiterNeverBelowMinimum(t *testing.T) {
	l := newAdaptiveLimiter(0)
	l.limit.Store(1)
	for i := 0; i < 5; i++ {
		l.onFailure()
	}
	if got := l.limit.Load(); got != adaptiveMinLimit {
		t.Fatalf("expected limit floored at %d, got %d", adaptiveMinLimit, got)
	}
}

func TestAdaptiveLimiterRespectsConfiguredCeiling(t *testing.T) {
	l := newAdaptiveLimiter(5)
	l.limit.Store(5)
	l.onSuccess(5) // saturated success, would normally grow further
	if got := l.limit.Load(); got != 5 {
		t.Fatalf("expected growth to stop at configured ceiling 5, got %d", got)
	}
}

func TestWeightedEndToEndGrowsHealthyMemberAndShrinksFailingOne(t *testing.T) {
	w := newAdaptiveTestWeighted([]int{50, 50}, []int{0, 0})
	ctx := context.Background()
	dest := M.ParseSocksaddrHostPort("example.com", 443)

	// Member 0 "succeeds" repeatedly while kept fully saturated - its limit
	// should grow past the initial value each time.
	for i := 0; i < 10; i++ {
		limit := int(w.limiters[0].limit.Load())
		for j := 0; j < limit; j++ {
			w.limiters[0].acquire()
		}
		w.limiters[0].onSuccess(int32(limit))
		for j := 0; j < limit; j++ {
			w.limiters[0].release()
		}
	}
	if got := w.limiters[0].limit.Load(); got <= adaptiveInitialLimit {
		t.Fatalf("expected member 0's limit to have grown above initial %d after repeated saturated successes, got %d", adaptiveInitialLimit, got)
	}

	// Member 1 fails via the real DialContext path - its limit should shrink.
	w.members[1].outbound = &erroringOutbound{tag: "member"}
	_, err := w.dialSpecific(ctx, dest, 1)
	if err == nil {
		t.Fatalf("expected dial error from member 1")
	}
	if got := w.limiters[1].limit.Load(); got >= adaptiveInitialLimit {
		t.Fatalf("expected member 1's limit to shrink below initial %d after a failure, got %d", adaptiveInitialLimit, got)
	}
}

// dialSpecific exercises the exact DialContext/recordResult/release path for
// a chosen member index, bypassing pick() - used only to drive a controlled
// failure through the real code path in the test above.
func (w *Weighted) dialSpecific(ctx context.Context, dest M.Socksaddr, index int) (net.Conn, error) {
	w.limiters[index].acquire()
	conn, dialErr := w.members[index].outbound.DialContext(ctx, "tcp", dest)
	w.recordResult(index, dialErr)
	if dialErr != nil {
		w.release(index)
		return nil, dialErr
	}
	return conn, nil
}

// TestConnectionAdmissionNeverExceedsLimitUnderConcurrentBurstAtCapacity
// reproduces the exact pattern a segmented downloader produces - many new
// connections arriving in the same instant - sized so total demand exactly
// equals total available capacity (4+4=8), so a correct, race-free
// implementation should saturate both members exactly at their limit and no
// further. A racy check-then-acquire (checking headroom, then incrementing
// inFlight afterwards, each under separate/no locking) would let some
// goroutines observe stale headroom and over-admit past a member's limit
// even though enough spare capacity existed elsewhere - this test is the
// regression guard for that failure mode. (Oversubscription beyond total
// capacity is a separate, expected case - see the doc comment on pick - and
// deliberately not what this test is checking.)
func TestConnectionAdmissionNeverExceedsLimitUnderConcurrentBurstAtCapacity(t *testing.T) {
	const iterations = 20

	var maxObserved [2]atomic.Int32
	for iter := 0; iter < iterations; iter++ {
		w := newAdaptiveTestWeighted([]int{50, 50}, []int{0, 0})
		limit0 := w.limiters[0].limit.Load()
		limit1 := w.limiters[1].limit.Load()
		burst := int(limit0 + limit1)

		var ready sync.WaitGroup
		var wg sync.WaitGroup
		start := make(chan struct{})
		release := make(chan struct{})
		ready.Add(burst)
		for i := 0; i < burst; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ready.Done()
				<-start // all goroutines fire pick() at the same instant
				index, _, err := w.pick()
				if err != nil {
					return
				}
				for {
					cur := maxObserved[index].Load()
					v := int32(w.limiters[index].inFlight.Load())
					if v <= cur || maxObserved[index].CompareAndSwap(cur, v) {
						break
					}
				}
				<-release
				w.release(index)
			}()
		}
		ready.Wait() // every goroutine is parked on <-start, primed to race
		close(start)
		time.Sleep(2 * time.Millisecond)
		close(release)
		wg.Wait()

		if got := maxObserved[0].Load(); int64(got) > limit0 {
			t.Fatalf("iteration %d: member 0's in-flight count exceeded its limit of %d under concurrent load: peaked at %d", iter, limit0, got)
		}
		if got := maxObserved[1].Load(); int64(got) > limit1 {
			t.Fatalf("iteration %d: member 1's in-flight count exceeded its limit of %d under concurrent load: peaked at %d", iter, limit1, got)
		}
		maxObserved[0].Store(0)
		maxObserved[1].Store(0)
	}
}

// TestPickKeepsAdmittingSoleAvailableMemberEvenFarOverItsLimit documents a
// deliberate choice: with only two members, once one trips its circuit
// breaker the other becomes the *only* available choice regardless of how
// deep its own headroom has gone negative. An earlier version of pick()
// hard-refused new connections once a member was carrying well beyond its
// proven-safe limit, reasoning that failing fast was safer than piling on -
// but measured directly against the real backends this moved strictly
// *fewer* total bytes than admitting everything, since a refused connection
// contributes nothing at all while even a degraded/slow admitted one still
// moves some data. pick() must keep admitting to the sole available member
// no matter how negative its headroom gets.
func TestPickKeepsAdmittingSoleAvailableMemberEvenFarOverItsLimit(t *testing.T) {
	w := newAdaptiveTestWeighted([]int{50, 50}, []int{0, 0})
	w.limiters[0].limit.Store(1)     // shrunk to the minimum, as if it's been failing repeatedly
	w.picker.SetAvailable(1, false) // member 1 is the only one breaker-tripped away

	for i := 0; i < 20; i++ {
		index, _, err := w.pick()
		if err != nil {
			t.Fatalf("expected pick %d to still admit to the sole available member, got error: %v", i, err)
		}
		if index != 0 {
			t.Fatalf("expected member 0, got %d", index)
		}
	}
	if got := w.limiters[0].inFlight.Load(); got != 20 {
		t.Fatalf("expected all 20 picks to have been admitted, inFlight=%d", got)
	}
}

func TestConnectionReleasedOnDialError(t *testing.T) {
	w := newAdaptiveTestWeighted([]int{50}, []int{0})
	w.members[0].outbound = &erroringOutbound{tag: "member"}

	ctx := context.Background()
	dest := M.ParseSocksaddrHostPort("example.com", 443)

	for i := 0; i < 5; i++ {
		_, err := w.DialContext(ctx, "tcp", dest)
		if err == nil {
			t.Fatalf("expected dial error")
		}
	}
	if got := w.limiters[0].inFlight.Load(); got != 0 {
		t.Fatalf("expected inFlight=0 after failed dials (each must release its slot), got %d", got)
	}
}

type erroringOutbound struct {
	tag string
}

func (f *erroringOutbound) Type() string           { return "fake-error" }
func (f *erroringOutbound) Tag() string            { return f.tag }
func (f *erroringOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (f *erroringOutbound) Dependencies() []string { return nil }
func (f *erroringOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return nil, errDial
}
func (f *erroringOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, errDial
}

var errDial = &dialError{}

type dialError struct{}

func (*dialError) Error() string { return "fake dial error" }

var _ adapter.Outbound = (*fakeOutbound)(nil)
var _ adapter.Outbound = (*erroringOutbound)(nil)
