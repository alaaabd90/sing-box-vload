//go:build linux

package route

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	tun "github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common/control"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service/pause"
)

type platformStateTestMonitor struct {
	tun.DefaultInterfaceMonitor
	external bool
}

func (m *platformStateTestMonitor) PlatformManagesNetworkState() bool    { return m.external }
func (m *platformStateTestMonitor) DefaultInterface() *control.Interface { return nil }
func (m *platformStateTestMonitor) RegisterCallback(tun.DefaultInterfaceUpdateCallback) *list.Element[tun.DefaultInterfaceUpdateCallback] {
	return nil
}

func TestPlatformOwnedSnapshotDoesNotPauseButRealMissingInterfaceDoes(t *testing.T) {
	for _, external := range []bool{true, false} {
		ctx := pause.ContextWithDefaultManager(context.Background())
		manager := pause.ManagerFromContext(ctx)
		r := &NetworkManager{ctx: ctx, logger: logger.NOP(), pauseManager: manager, interfaceMonitor: &platformStateTestMonitor{external: external}}
		if err := r.Start(adapter.StartStatePostStart); err != nil {
			t.Fatal(err)
		}
		r.startedCancel()
		if manager.IsNetworkPaused() == external {
			t.Fatalf("incorrect network pause: platform-owned=%v paused=%v", external, manager.IsNetworkPaused())
		}
	}
}
