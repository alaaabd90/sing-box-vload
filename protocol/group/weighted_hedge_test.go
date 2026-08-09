package group

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

// scriptedOutbound is a controllable fakeOutbound-alike for hedge tests: it
// can be told to wait before responding and to fail, and it counts how many
// times DialContext was actually invoked so a test can assert whether a
// hedge race started a second attempt at all.
type scriptedOutbound struct {
	tag   string
	delay time.Duration
	err   error
	calls atomic.Int32
}

func (f *scriptedOutbound) Type() string           { return "scripted" }
func (f *scriptedOutbound) Tag() string            { return f.tag }
func (f *scriptedOutbound) Network() []string      { return []string{"tcp", "udp"} }
func (f *scriptedOutbound) Dependencies() []string { return nil }

func (f *scriptedOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	f.calls.Add(1)
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	client, server := net.Pipe()
	go func() { _ = server.Close() }()
	return client, nil
}

func (f *scriptedOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return net.ListenPacket("udp", "127.0.0.1:0")
}

// newHedgeTestWeighted builds a real two-member Weighted, like
// newAdaptiveTestWeighted, but with caller-supplied outbounds per member so
// each side's timing/success can be scripted independently.
func newHedgeTestWeighted(member0, member1 *scriptedOutbound) *Weighted {
	w := &Weighted{
		tags:   []string{"member0", "member1"},
		picker: newPCCPicker([]int{50, 50}),
		health: make([]memberHealth, 2),
		logger: testNopLogger{},
	}
	w.limiters = []*adaptiveLimiter{newAdaptiveLimiter(0), newAdaptiveLimiter(0)}
	w.members = []weightedMember{
		{tag: "member0", outbound: member0},
		{tag: "member1", outbound: member1},
	}
	return w
}

func TestHedgeFailsOverToOtherMemberWhenPrimaryErrorsImmediately(t *testing.T) {
	bad := &scriptedOutbound{tag: "member0", err: errDial}
	good := &scriptedOutbound{tag: "member1"}
	w := newHedgeTestWeighted(bad, good)
	// Force member 0 to be picked first regardless of tie-break rotation.
	w.limiters[1].acquire()

	ctx := context.Background()
	dest := M.ParseSocksaddrHostPort("example.com", 443)
	conn, err := w.DialContext(ctx, "tcp", dest)
	if err != nil {
		t.Fatalf("expected the connection to fail over to the healthy member, got error: %v", err)
	}
	defer conn.Close()

	if bad.calls.Load() != 1 {
		t.Errorf("expected the failing member to have been tried exactly once, got %d", bad.calls.Load())
	}
	if good.calls.Load() != 1 {
		t.Errorf("expected the healthy member to have been raced in, got %d calls", good.calls.Load())
	}
}

func TestHedgeReturnsErrorOnlyWhenAllMembersFail(t *testing.T) {
	bad0 := &scriptedOutbound{tag: "member0", err: errDial}
	bad1 := &scriptedOutbound{tag: "member1", err: errDial}
	w := newHedgeTestWeighted(bad0, bad1)

	ctx := context.Background()
	dest := M.ParseSocksaddrHostPort("example.com", 443)
	_, err := w.DialContext(ctx, "tcp", dest)
	if err == nil {
		t.Fatalf("expected an error when every member fails")
	}
	if got := w.limiters[0].inFlight.Load(); got != 0 {
		t.Errorf("member 0's slot must be released after a genuine failure, inFlight=%d", got)
	}
	if got := w.limiters[1].inFlight.Load(); got != 0 {
		t.Errorf("member 1's slot must be released after a genuine failure, inFlight=%d", got)
	}
}

func TestHedgeSkipsSecondaryWhenPrimarySucceedsQuickly(t *testing.T) {
	fast := &scriptedOutbound{tag: "member0"}
	other := &scriptedOutbound{tag: "member1"}
	w := newHedgeTestWeighted(fast, other)
	w.limiters[1].acquire() // bias pick() toward member 0

	ctx := context.Background()
	dest := M.ParseSocksaddrHostPort("example.com", 443)
	conn, err := w.DialContext(ctx, "tcp", dest)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer conn.Close()

	// Give any wrongly-started hedge goroutine time to have made its call.
	time.Sleep(hedgeDelay * 2)
	if other.calls.Load() != 0 {
		t.Errorf("a fast, healthy primary must not trigger a hedge dial on the other member, got %d calls", other.calls.Load())
	}
}

func TestHedgeRacesSecondaryWhenPrimaryIsSlowAndDoesNotPenalizeAbandonedLoser(t *testing.T) {
	slow := &scriptedOutbound{tag: "member0", delay: hedgeDelay * 8}
	fast := &scriptedOutbound{tag: "member1", delay: hedgeDelay / 4}
	w := newHedgeTestWeighted(slow, fast)
	w.limiters[1].acquire() // bias pick() toward member 0 (the slow one) first

	ctx := context.Background()
	dest := M.ParseSocksaddrHostPort("example.com", 443)
	start := time.Now()
	conn, err := w.DialContext(ctx, "tcp", dest)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer conn.Close()

	if fast.calls.Load() != 1 {
		t.Fatalf("expected the hedge race to have started the second member, got %d calls", fast.calls.Load())
	}
	if elapsed >= slow.delay {
		t.Fatalf("expected the fast hedge winner to return well before the slow member's %s dial, took %s", slow.delay, elapsed)
	}

	// The slow primary is still in flight in the background, abandoned once
	// the hedge race picked a winner. It must not be counted as a failure
	// against member 0 just because we lost interest in it.
	h := &w.health[0]
	if h.consecutiveFailures.Load() != 0 {
		t.Errorf("an abandoned race loser must not be recorded as a failure, consecutiveFailures=%d", h.consecutiveFailures.Load())
	}

	// Once the slow primary actually finishes in the background, its
	// concurrency slot must still be released - otherwise a hedge loser
	// would permanently leak capacity every time it happens to lose.
	deadline := time.Now().Add(slow.delay * 2)
	for time.Now().Before(deadline) {
		if w.limiters[0].inFlight.Load() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("expected member 0's slot to be released once its abandoned dial finished, inFlight=%d", w.limiters[0].inFlight.Load())
}
