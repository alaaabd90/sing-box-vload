package group

import (
	"context"
	"testing"

	M "github.com/sagernet/sing/common/metadata"
)

// newPriorityTestWeighted is newHedgeTestWeighted with ModePriority set -
// same fake two-member setup, but strict-order/failover-only selection.
func newPriorityTestWeighted(member0, member1 *scriptedOutbound) *Weighted {
	w := newHedgeTestWeighted(member0, member1)
	w.priority = true
	return w
}

func TestPriorityAlwaysPrefersFirstMemberWhenHealthy(t *testing.T) {
	primary := &scriptedOutbound{tag: "member0"}
	secondary := &scriptedOutbound{tag: "member1"}
	w := newPriorityTestWeighted(primary, secondary)

	// Starve member 0's headroom hard - an adaptive-mode pick would prefer
	// member 1 here (see TestPickPrefersMemberWithMoreHeadroom); priority
	// mode must ignore that entirely and keep choosing member 0.
	w.limiters[0].limit.Store(1)
	w.limiters[0].inFlight.Store(10)

	for i := 0; i < 5; i++ {
		index, _, err := w.pick()
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		if index != 0 {
			t.Fatalf("expected priority mode to keep preferring member 0 regardless of headroom, got %d", index)
		}
		w.release(index)
	}
}

func TestPriorityDoesNotHedgeWhenPrimaryIsSlow(t *testing.T) {
	slowButHealthy := &scriptedOutbound{tag: "member0", delay: hedgeDelay * 4}
	other := &scriptedOutbound{tag: "member1"}
	w := newPriorityTestWeighted(slowButHealthy, other)

	ctx := context.Background()
	dest := M.ParseSocksaddrHostPort("example.com", 443)
	conn, err := w.DialContext(ctx, "tcp", dest)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer conn.Close()

	// Adaptive mode would have started member 1 after hedgeDelay even
	// though member 0 was healthy, just slow. Priority mode must not - it
	// only fails over on an actual failure, never on a timeout.
	if other.calls.Load() != 0 {
		t.Errorf("priority mode must not hedge a merely-slow-but-healthy primary, got %d calls to the other member", other.calls.Load())
	}
	if slowButHealthy.calls.Load() != 1 {
		t.Errorf("expected exactly one call to the primary, got %d", slowButHealthy.calls.Load())
	}
}

func TestPriorityFailsOverOnPrimaryFailure(t *testing.T) {
	bad := &scriptedOutbound{tag: "member0", err: errDial}
	good := &scriptedOutbound{tag: "member1"}
	w := newPriorityTestWeighted(bad, good)

	ctx := context.Background()
	dest := M.ParseSocksaddrHostPort("example.com", 443)
	conn, err := w.DialContext(ctx, "tcp", dest)
	if err != nil {
		t.Fatalf("expected failover to the healthy member, got error: %v", err)
	}
	defer conn.Close()

	if good.calls.Load() != 1 {
		t.Errorf("expected failover to member 1 after member 0 failed, got %d calls", good.calls.Load())
	}
}

func TestPriorityStaysOnSecondMemberUntilFirstRecovers(t *testing.T) {
	primary := &scriptedOutbound{tag: "member0", err: errDial}
	secondary := &scriptedOutbound{tag: "member1"}
	w := newPriorityTestWeighted(primary, secondary)

	ctx := context.Background()
	dest := M.ParseSocksaddrHostPort("example.com", 443)

	// Trip member 0's circuit breaker via repeated real failures, exactly
	// as live traffic would.
	for i := 0; i < circuitBreakerThreshold; i++ {
		_, _ = w.DialContext(ctx, "tcp", dest)
	}
	if w.picker.IsAvailable(0) {
		t.Fatalf("expected member 0's circuit breaker to have tripped after %d failures", circuitBreakerThreshold)
	}

	// While tripped, every pick must go to member 1 - no attempt at member
	// 0 at all, not even a losing race.
	callsBefore := primary.calls.Load()
	index, _, err := w.pick()
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	if index != 1 {
		t.Fatalf("expected member 1 while member 0 is breaker-tripped, got %d", index)
	}
	w.release(index)
	if primary.calls.Load() != callsBefore {
		t.Errorf("a breaker-tripped primary must not be dialed at all, got %d new calls", primary.calls.Load()-callsBefore)
	}

	// An explicit platform-confirmed recovery (what the Kotlin network
	// callback path calls on a real handoff/recovery signal) must bring it
	// back as the preferred member immediately.
	w.UpdateAvailability(0, true)
	index, _, err = w.pick()
	if err != nil {
		t.Fatalf("pick after recovery: %v", err)
	}
	if index != 0 {
		t.Fatalf("expected member 0 to be preferred again after recovery, got %d", index)
	}
	w.release(index)
}
