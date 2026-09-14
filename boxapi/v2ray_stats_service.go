package boxapi

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	N "github.com/sagernet/sing/common/network"
)

// vload: surfaces every connection routed to the "direct" outbound in the
// app's own log viewer/logcat (stdlib log is redirected there by
// libneko/neko_log), tagged distinctly from normal sing-box routing logs.
// Requested for on-device testing so a connection that silently falls back
// to direct (e.g. a new protocol's outbound failing to build/connect) is
// visible immediately instead of looking like a working proxied connection.
func logIfDirect(inbound string, outbound string, destination fmt.Stringer, kind string) {
	if outbound != "direct" {
		return
	}
	log.Printf(
		"[Debug] [vload-direct-check] %s connection went DIRECT (not through proxy): inbound=%s destination=%s",
		kind, inbound, destination.String(),
	)
}

type SbStatsService struct {
	createdAt time.Time
	inbounds  map[string]bool
	outbounds map[string]bool
	users     map[string]bool
	access    sync.Mutex
	counters  map[string]*atomic.Int64
}

func NewSbStatsService(options option.V2RayStatsServiceOptions) *SbStatsService {
	if !options.Enabled {
		return nil
	}
	inbounds := make(map[string]bool)
	outbounds := make(map[string]bool)
	users := make(map[string]bool)
	for _, inbound := range options.Inbounds {
		inbounds[inbound] = true
	}
	for _, outbound := range options.Outbounds {
		outbounds[outbound] = true
	}
	for _, user := range options.Users {
		users[user] = true
	}
	return &SbStatsService{
		createdAt: time.Now(),
		inbounds:  inbounds,
		outbounds: outbounds,
		users:     users,
		counters:  make(map[string]*atomic.Int64),
	}
}

func (s *SbStatsService) RoutedConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, matchedRule adapter.Rule, matchOutbound adapter.Outbound) net.Conn {
	inbound := metadata.Inbound
	user := metadata.User
	outbound := matchOutbound.Tag()
	logIfDirect(inbound, outbound, metadata.Destination, "TCP")
	return s.RoutedConnectionInternal(inbound, outbound, user, conn, true)
}

func (s *SbStatsService) RoutedConnectionInternal(inbound string, outbound string, user string, conn net.Conn, directIn bool) net.Conn {
	var readCounter []*atomic.Int64
	var writeCounter []*atomic.Int64
	countInbound := inbound != "" && s.inbounds[inbound]
	countOutbound := outbound != "" && s.outbounds[outbound]
	countUser := user != "" && s.users[user]
	if !countInbound && !countOutbound && !countUser {
		return conn
	}
	s.access.Lock()
	if countInbound {
		readCounter = append(readCounter, s.loadOrCreateCounter("inbound>>>"+inbound+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("inbound>>>"+inbound+">>>traffic>>>downlink"))
	}
	if countOutbound {
		readCounter = append(readCounter, s.loadOrCreateCounter("outbound>>>"+outbound+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("outbound>>>"+outbound+">>>traffic>>>downlink"))
	}
	if countUser {
		readCounter = append(readCounter, s.loadOrCreateCounter("user>>>"+user+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("user>>>"+user+">>>traffic>>>downlink"))
	}
	s.access.Unlock()
	if directIn {
		conn = bufio.NewInt64CounterConn(conn, readCounter, writeCounter)
	} else {
		conn = bufio.NewInt64CounterConn(conn, writeCounter, readCounter)
	}
	return conn
}

func (s *SbStatsService) RoutedPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, matchedRule adapter.Rule, matchOutbound adapter.Outbound) N.PacketConn {
	inbound := metadata.Inbound
	user := metadata.User
	outbound := matchOutbound.Tag()
	logIfDirect(inbound, outbound, metadata.Destination, "UDP")
	var readCounter []*atomic.Int64
	var writeCounter []*atomic.Int64
	countInbound := inbound != "" && s.inbounds[inbound]
	countOutbound := outbound != "" && s.outbounds[outbound]
	countUser := user != "" && s.users[user]
	if !countInbound && !countOutbound && !countUser {
		return conn
	}
	s.access.Lock()
	if countInbound {
		readCounter = append(readCounter, s.loadOrCreateCounter("inbound>>>"+inbound+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("inbound>>>"+inbound+">>>traffic>>>downlink"))
	}
	if countOutbound {
		readCounter = append(readCounter, s.loadOrCreateCounter("outbound>>>"+outbound+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("outbound>>>"+outbound+">>>traffic>>>downlink"))
	}
	if countUser {
		readCounter = append(readCounter, s.loadOrCreateCounter("user>>>"+user+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("user>>>"+user+">>>traffic>>>downlink"))
	}
	s.access.Unlock()
	return bufio.NewInt64CounterPacketConn(conn, readCounter, nil, writeCounter, nil)
}

// RoutedFlow tracks L3 flows (TUN/WireGuard/Tailscale endpoints forwarded
// directly at L3, added in sing-box 1.14.0). Not needed for this service's
// mobile-UI traffic stats scope - every proxied connection here already
// goes through RoutedConnection/RoutedPacketConnection instead - so this is
// a no-op, matching how the upstream StatsService returns nil when nothing
// needs counting.
func (s *SbStatsService) RoutedFlow(ctx context.Context, metadata adapter.InboundContext, matchedRule adapter.Rule, matchOutbound adapter.Outbound) tun.FlowTracker {
	return nil
}

func (s *SbStatsService) GetStats(ctx context.Context, name string, reset bool) (int64, error) {
	s.access.Lock()
	counter, loaded := s.counters[name]
	s.access.Unlock()
	if !loaded {
		return 0, E.New(name, " not found.")
	}
	var value int64
	if reset {
		value = counter.Swap(0)
	} else {
		value = counter.Load()
	}
	return value, nil
}

// QueryStats

// GetSysStats

//nolint:staticcheck
func (s *SbStatsService) loadOrCreateCounter(name string) *atomic.Int64 {
	counter, loaded := s.counters[name]
	if loaded {
		return counter
	}
	counter = &atomic.Int64{}
	s.counters[name] = counter
	return counter
}
