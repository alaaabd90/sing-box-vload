package group

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterWeighted(registry *outbound.Registry) {
	outbound.Register[option.WeightedOutboundOptions](registry, C.TypeWeighted, NewWeighted)
}

var (
	_ adapter.OutboundGroup             = (*Weighted)(nil)
	_ adapter.ConnectionHandlerEx       = (*Weighted)(nil)
	_ adapter.PacketConnectionHandlerEx = (*Weighted)(nil)
)

type weightedMember struct {
	tag      string
	outbound adapter.Outbound
}

// Weighted distributes new connections across its members using smooth
// weighted round robin (see weightedSWRR), picking a member per new
// connection/packet-connection rather than sticking to a single selection
// like Selector does. It's intended for splitting traffic across two
// physical networks bound via per-member protect_path dialer options, with
// runtime availability toggling for failover.
type Weighted struct {
	outbound.Adapter
	ctx        context.Context
	outboundMg adapter.OutboundManager
	connection adapter.ConnectionManager
	logger     logger.ContextLogger
	tags       []string
	members    []weightedMember
	picker     *weightedSWRR
	lastPicked atomic.Int64
}

func NewWeighted(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.WeightedOutboundOptions) (adapter.Outbound, error) {
	if len(options.Outbounds) < 2 {
		return nil, E.New("weighted outbound requires at least 2 members")
	}
	tags := make([]string, len(options.Outbounds))
	weights := make([]int, len(options.Outbounds))
	for i, member := range options.Outbounds {
		if member.Outbound == "" {
			return nil, E.New("member ", i, " missing outbound tag")
		}
		weight := member.Weight
		if weight == 0 {
			weight = 1
		}
		tags[i] = member.Outbound
		weights[i] = int(weight)
	}
	return &Weighted{
		Adapter:    outbound.NewAdapter(C.TypeWeighted, tag, nil, tags),
		ctx:        ctx,
		outboundMg: service.FromContext[adapter.OutboundManager](ctx),
		connection: service.FromContext[adapter.ConnectionManager](ctx),
		logger:     logger,
		tags:       tags,
		picker:     newWeightedSWRR(weights),
	}, nil
}

func (w *Weighted) Start() error {
	members := make([]weightedMember, len(w.tags))
	for i, tag := range w.tags {
		detour, loaded := w.outboundMg.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		members[i] = weightedMember{tag: tag, outbound: detour}
	}
	w.members = members
	return nil
}

func (w *Weighted) Network() []string {
	return []string{N.NetworkTCP, N.NetworkUDP}
}

// Now and All satisfy adapter.OutboundGroup. Weighted has no single
// "current" selection since it picks per connection - Now reports whichever
// member was picked most recently, purely for display/tracking purposes
// (e.g. the ClashAPI connection tracker, which walks Now() until it reaches
// a non-group outbound; returning this group's own tag here would resolve
// back to itself and loop forever).
func (w *Weighted) Now() string {
	index := int(w.lastPicked.Load())
	if index < 0 || index >= len(w.tags) {
		return w.tags[0]
	}
	return w.tags[index]
}

func (w *Weighted) All() []string {
	return w.tags
}

// UpdateAvailability marks member index (0-based, matching the order given
// in the outbounds option) as available or unavailable, for failover when
// the underlying network it's bound to drops or recovers.
func (w *Weighted) UpdateAvailability(index int, available bool) {
	w.logger.Info("vload: slot ", index, " availability -> ", available)
	w.picker.SetAvailable(index, available)
}

func (w *Weighted) pick() (weightedMember, error) {
	index := w.picker.Next()
	if index < 0 || index >= len(w.members) {
		return weightedMember{}, E.New("no members available")
	}
	w.lastPicked.Store(int64(index))
	w.logger.Info("vload: picked slot ", index, " (", w.members[index].tag, ")")
	return w.members[index], nil
}

func (w *Weighted) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	member, err := w.pick()
	if err != nil {
		return nil, err
	}
	return member.outbound.DialContext(ctx, network, destination)
}

func (w *Weighted) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	member, err := w.pick()
	if err != nil {
		return nil, err
	}
	return member.outbound.ListenPacket(ctx, destination)
}

func (w *Weighted) NewConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	member, err := w.pick()
	if err != nil {
		N.CloseOnHandshakeFailure(conn, onClose, err)
		w.logger.ErrorContext(ctx, err)
		return
	}
	if outboundHandler, isHandler := member.outbound.(adapter.ConnectionHandlerEx); isHandler {
		outboundHandler.NewConnectionEx(ctx, conn, metadata, onClose)
	} else {
		w.connection.NewConnection(ctx, member.outbound, conn, metadata, onClose)
	}
}

func (w *Weighted) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	member, err := w.pick()
	if err != nil {
		N.CloseOnHandshakeFailure(conn, onClose, err)
		w.logger.ErrorContext(ctx, err)
		return
	}
	if outboundHandler, isHandler := member.outbound.(adapter.PacketConnectionHandlerEx); isHandler {
		outboundHandler.NewPacketConnectionEx(ctx, conn, metadata, onClose)
	} else {
		w.connection.NewPacketConnection(ctx, member.outbound, conn, metadata, onClose)
	}
}
