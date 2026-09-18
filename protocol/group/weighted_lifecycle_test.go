package group

import (
	"context"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

type contextProbeOutbound struct {
	*scriptedOutbound
	dialContext context.Context
}

func (o *contextProbeOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	o.dialContext = ctx
	return o.scriptedOutbound.DialContext(ctx, network, destination)
}

func (o *contextProbeOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	o.dialContext = ctx
	return o.scriptedOutbound.ListenPacket(ctx, destination)
}

func TestWinningDialContextLivesUntilConnectionClose(t *testing.T) {
	for _, secondary := range []bool{false, true} {
		for _, packet := range []bool{false, true} {
			w := newPriorityTestWeighted(&scriptedOutbound{tag: "a", err: errDial}, &scriptedOutbound{tag: "b"})
			probe := &contextProbeOutbound{scriptedOutbound: &scriptedOutbound{tag: "probe"}}
			if secondary {
				w.members[1].outbound = probe
			} else {
				w.members[0].outbound = probe
			}
			if packet && secondary {
				w.UpdateAvailability(0, false)
			}
			var closeConn func() error
			if packet {
				conn, err := w.ListenPacket(context.Background(), M.ParseSocksaddr("127.0.0.1:443"))
				if err != nil {
					t.Fatal(err)
				}
				closeConn = conn.Close
			} else {
				conn, err := w.DialContext(context.Background(), "tcp", M.ParseSocksaddr("127.0.0.1:443"))
				if err != nil {
					t.Fatal(err)
				}
				closeConn = conn.Close
			}
			if err := probe.dialContext.Err(); err != nil {
				closeConn()
				t.Fatalf("winner canceled before first write: secondary=%v packet=%v: %v", secondary, packet, err)
			}
			closeConn()
			if probe.dialContext.Err() != context.Canceled {
				t.Fatal("winner context leaked after close")
			}
		}
	}
}

func TestPlatformUnavailableMembersAreNeverFallbackCandidates(t *testing.T) {
	w := newPriorityTestWeighted(&scriptedOutbound{tag: "a"}, &scriptedOutbound{tag: "b"})
	w.UpdateAvailability(0, false)
	w.UpdateAvailability(1, false)
	if index, _, err := w.pick(); err == nil {
		w.release(index)
		t.Fatal("picked a physically unavailable network")
	}
	// A stale breaker timer must not override Android's network loss event.
	w.picker.SetAvailable(0, true)
	if index, _, err := w.pick(); err == nil {
		w.release(index)
		t.Fatal("breaker recovery revived an unavailable network")
	}
	w.UpdateAvailability(1, true)
	index, _, err := w.pick()
	if err != nil || index != 1 {
		t.Fatalf("recovered slot: index=%d err=%v", index, err)
	}
	w.release(index)
}

func TestFailedPrimarySuccessfulSecondaryDoesNotLeakCleanup(t *testing.T) {
	count := func() int {
		stack := make([]byte, 1<<20)
		n := runtime.Stack(stack, true)
		return strings.Count(string(stack[:n]), "abandonHedgeLoser[...].func1()")
	}
	before := count()
	for i := 0; i < 12; i++ {
		w := newPriorityTestWeighted(&scriptedOutbound{tag: "a", err: errDial}, &scriptedOutbound{tag: "b"})
		conn, err := w.DialContext(context.Background(), "tcp", M.ParseSocksaddr("example.com:443"))
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		runtime.Gosched()
		if count() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("failover leaked %d cleanup goroutines", count()-before)
}
