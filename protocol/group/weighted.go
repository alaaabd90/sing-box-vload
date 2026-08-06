package group

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

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

// circuitBreakerThreshold consecutive connection failures on a member trips
// its breaker. Cooldown follows Envoy's outlier-detection design
// (base_ejection_time * 2^(consecutive_ejections-1), capped): a member
// tripping for the first time gets circuitBreakerBaseCooldown, but a
// chronically-flaky member - one that trips again shortly after its
// previous recovery, which is exactly what a real download manager's burst
// of concurrent new connections will do to a member that's only
// marginally too slow to keep up - gets progressively longer exclusions
// instead of flapping in and out of rotation every circuitBreakerBaseCooldown
// seconds. A member only resets back to the base cooldown after
// circuitBreakerRecoverySuccesses consecutive successes following a
// recovery, i.e. it has to actually prove it's healthy again, not just
// survive one lucky connection.
const (
	circuitBreakerThreshold         = 3
	circuitBreakerBaseCooldown      = 20 * time.Second
	circuitBreakerMaxCooldown       = 5 * time.Minute
	circuitBreakerRecoverySuccesses = 3
)

// memberHealth tracks a member's recent connection outcomes for the
// automatic circuit breaker. generation is bumped on every trip (and on any
// explicit UpdateAvailability call) so a recovery timer from an earlier
// trip can tell it's been superseded and skip re-enabling.
type memberHealth struct {
	consecutiveFailures  atomic.Int32
	consecutiveSuccesses atomic.Int32
	ejectionCount        atomic.Int32 // consecutive trips without a full (circuitBreakerRecoverySuccesses-success) recovery in between
	generation           atomic.Int64
}

// Weighted distributes new connections across its members by picking
// whichever member currently has the most spare proven capacity (see
// adaptiveLimiter), picking a member per new connection/packet-connection
// rather than sticking to a single selection like Selector does. It's
// intended for splitting traffic across two physical networks bound via
// per-member bind_interface dialer options. Nothing needs to be
// hand-configured for this to work correctly: each member's real
// concurrency ceiling is discovered live from its own connection outcomes,
// not from a fixed weight or connection-count guess.
//
// Members are excluded from picking in two independent ways: explicitly via
// UpdateAvailability (for a platform-side network-change signal), and
// automatically via the circuit breaker below, which reacts to what the
// member's connections actually do - repeated failures/timeouts trip it
// into a cooldown regardless of whether its network interface still
// reports "up", since a network can be technically connected but unable to
// carry real traffic (an overloaded upstream proxy, a throttled link, and
// so on aren't visible as a link-state change at all).
type Weighted struct {
	outbound.Adapter
	ctx        context.Context
	outboundMg adapter.OutboundManager
	connection adapter.ConnectionManager
	logger     logger.ContextLogger
	tags       []string
	members    []weightedMember
	picker     *pccPicker // still used for availability tracking and the all-members-tripped fallback
	health     []memberHealth
	limiters   []*adaptiveLimiter
	tieCursor  atomic.Int64
	// pickMu serializes pick()'s headroom-check-then-acquire as a single
	// unit. Without it, a burst of concurrent picks (exactly what a
	// segmented downloader produces) can all read stale headroom before any
	// of them commits its own acquire, letting the whole burst pile onto one
	// member anyway - checking and acquiring under the same lock closes that
	// gap.
	pickMu     sync.Mutex
	lastPicked atomic.Int64
}

func NewWeighted(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.WeightedOutboundOptions) (adapter.Outbound, error) {
	if len(options.Outbounds) < 2 {
		return nil, E.New("weighted outbound requires at least 2 members")
	}
	tags := make([]string, len(options.Outbounds))
	weights := make([]int, len(options.Outbounds))
	limiters := make([]*adaptiveLimiter, len(options.Outbounds))
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
		limiters[i] = newAdaptiveLimiter(int(member.MaxConnections))
	}
	return &Weighted{
		Adapter:    outbound.NewAdapter(C.TypeWeighted, tag, nil, tags),
		ctx:        ctx,
		outboundMg: service.FromContext[adapter.OutboundManager](ctx),
		connection: service.FromContext[adapter.ConnectionManager](ctx),
		logger:     logger,
		tags:       tags,
		picker:     newPCCPicker(weights),
		health:     make([]memberHealth, len(tags)),
		limiters:   limiters,
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
// in the outbounds option) as available or unavailable, for faster failover
// when the underlying network it's bound to drops or recovers than waiting
// on that member's own dial timeouts. Members are available by default;
// this is an optional accelerator, not a requirement for correct operation.
// An explicit call also resets that member's circuit breaker state, so a
// platform-confirmed "network is up" always overrides a stale trip.
func (w *Weighted) UpdateAvailability(index int, available bool) {
	w.logger.Info("vload: slot ", index, " availability -> ", available)
	w.picker.SetAvailable(index, available)
	if index >= 0 && index < len(w.health) {
		h := &w.health[index]
		h.generation.Add(1)
		h.consecutiveFailures.Store(0)
		h.consecutiveSuccesses.Store(0)
		if available {
			// an explicit, platform-confirmed "network is up" is stronger
			// evidence than our own connection outcomes: trust it fully and
			// let the next trip (if any) start back at the base cooldown.
			h.ejectionCount.Store(0)
		}
	}
}

// recordResult feeds one connection's outcome into index's circuit breaker.
// nil means it closed cleanly; any other value counts as a failure. Tripping
// the breaker excludes the member via the same SetAvailable path an
// external monitor would use, and schedules its own recovery - this needs
// no external signal at all to eventually retry a member that's recovered.
func (w *Weighted) recordResult(index int, err error) {
	if index < 0 || index >= len(w.health) {
		return
	}
	h := &w.health[index]
	// Read inFlight before this connection's own release() runs, so a
	// success/failure here reflects how loaded the member actually was.
	inFlightNow := w.limiters[index].inFlight.Load()
	if err == nil {
		w.limiters[index].onSuccess(inFlightNow)
		h.consecutiveFailures.Store(0)
		if h.consecutiveSuccesses.Add(1) >= circuitBreakerRecoverySuccesses {
			h.ejectionCount.Store(0)
		}
		return
	}
	w.limiters[index].onFailure()
	h.consecutiveSuccesses.Store(0)
	failures := h.consecutiveFailures.Add(1)
	if failures != circuitBreakerThreshold {
		return
	}
	// A member that's already excluded already has a recovery timer pending
	// from its current trip. A failure landing here for it can only have
	// come through the picker's fallback-when-everything's-unavailable path
	// (see pccPicker.nextLocked) - i.e. it was forced back into service
	// with nowhere better to send the connection, not because it looked
	// healthy again. Letting that re-trip and escalate the backoff here
	// would ratchet the cooldown towards its cap almost instantly whenever
	// two members happen to be down at once, and would keep resetting the
	// member's own recovery clock before it ever gets a chance to elapse.
	// Reset the streak so it doesn't just sit at threshold and hit this
	// branch again on the very next failure, but leave the existing trip
	// (and its scheduled recovery) alone.
	if !w.picker.IsAvailable(index) {
		h.consecutiveFailures.Store(0)
		return
	}
	ejections := h.ejectionCount.Add(1)
	exp := ejections - 1
	if exp > 10 { // guard against shift overflow; circuitBreakerMaxCooldown binds well before this
		exp = 10
	}
	cooldown := circuitBreakerBaseCooldown * time.Duration(uint64(1)<<uint(exp))
	if cooldown > circuitBreakerMaxCooldown || cooldown <= 0 {
		cooldown = circuitBreakerMaxCooldown
	}
	gen := h.generation.Add(1)
	w.logger.Info("vload: slot ", index, " tripped circuit breaker after ", failures, " consecutive connection failures (ejection #", ejections, "), excluding for ", cooldown)
	w.picker.SetAvailable(index, false)
	time.AfterFunc(cooldown, func() {
		if h.generation.Load() != gen {
			return // superseded by a newer trip or an explicit UpdateAvailability call
		}
		h.consecutiveFailures.Store(0)
		w.logger.Info("vload: slot ", index, " circuit breaker cooldown elapsed, re-enabling")
		w.picker.SetAvailable(index, true)
	})
}

// pick chooses the available member with the most spare proven capacity
// (adaptiveLimiter.headroom) - i.e. whichever member currently has the most
// room left within what it's already shown it can sustain, so traffic
// naturally flows toward whichever network is actually performing better
// right now rather than a fixed, hand-configured ratio. Ties are rotated
// round-robin for fairness. If every member's circuit breaker is currently
// open, falls back to the picker's plain rotation so a connection still
// gets placed somewhere instead of being refused outright.
func (w *Weighted) pick() (int, weightedMember, error) {
	w.pickMu.Lock()
	best := -1
	var bestHeadroom int64
	tied := 0
	for i := range w.limiters {
		if !w.picker.IsAvailable(i) {
			continue
		}
		h := w.limiters[i].headroom()
		switch {
		case best == -1 || h > bestHeadroom:
			best = i
			bestHeadroom = h
			tied = 1
		case h == bestHeadroom:
			tied++
			// Reservoir-style rotation across tied candidates so repeated
			// ties don't always resolve to the lowest index.
			if int(w.tieCursor.Add(1))%tied == 0 {
				best = i
			}
		}
	}
	if best == -1 {
		// Every member is breaker-tripped - fall back to plain rotation
		// (ignoring headroom entirely) so we keep probing rather than
		// stalling every new connection outright.
		best = w.picker.Next()
	}
	// Deliberately no hard admission ceiling here even when a member's own
	// headroom has gone deeply negative (e.g. it's the sole breaker-available
	// member and already carrying far more than its proven-safe limit):
	// measured directly against the real backends, refusing new connections
	// once over some ceiling moved strictly *fewer* total bytes than letting
	// them all through, because a refused connection contributes nothing at
	// all while even a degraded/slow admitted one still moves some data - a
	// download manager's retry-on-refusal behavior doesn't make up the
	// difference in practice. Piling on is genuinely the better outcome
	// given no way to queue a connection for later.
	if best >= 0 && best < len(w.members) {
		w.limiters[best].acquire()
	}
	w.pickMu.Unlock()

	if best < 0 || best >= len(w.members) {
		return -1, weightedMember{}, E.New("no members available")
	}
	w.lastPicked.Store(int64(best))
	w.logger.Info("vload: picked slot ", best, " (", w.members[best].tag, "), active=", w.limiters[best].inFlight.Load(), ", limit=", w.limiters[best].limit.Load())
	return best, w.members[best], nil
}

// release returns index's connection slot, undoing the acquire pick() made.
// Must be called exactly once per successful pick() - callers wrap whatever
// signals "this connection is over" (a close callback, or the dial/listen
// error path when no connection was ever established) to do so.
func (w *Weighted) release(index int) {
	if index < 0 || index >= len(w.limiters) {
		return
	}
	w.limiters[index].release()
}

func (w *Weighted) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	index, member, err := w.pick()
	if err != nil {
		return nil, err
	}
	conn, dialErr := member.outbound.DialContext(ctx, network, destination)
	w.recordResult(index, dialErr)
	if dialErr != nil {
		w.release(index)
		return nil, dialErr
	}
	return &weightedCountedConn{Conn: conn, release: func() { w.release(index) }}, nil
}

func (w *Weighted) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	index, member, err := w.pick()
	if err != nil {
		return nil, err
	}
	conn, dialErr := member.outbound.ListenPacket(ctx, destination)
	w.recordResult(index, dialErr)
	if dialErr != nil {
		w.release(index)
		return nil, dialErr
	}
	return &weightedCountedPacketConn{PacketConn: conn, release: func() { w.release(index) }}, nil
}

func (w *Weighted) NewConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	index, member, err := w.pick()
	if err != nil {
		N.CloseOnHandshakeFailure(conn, onClose, err)
		w.logger.ErrorContext(ctx, err)
		return
	}
	wrappedClose := func(closeErr error) {
		w.recordResult(index, closeErr)
		w.release(index)
		if onClose != nil {
			onClose(closeErr)
		}
	}
	if outboundHandler, isHandler := member.outbound.(adapter.ConnectionHandlerEx); isHandler {
		outboundHandler.NewConnectionEx(ctx, conn, metadata, wrappedClose)
	} else {
		w.connection.NewConnection(ctx, member.outbound, conn, metadata, wrappedClose)
	}
}

func (w *Weighted) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	index, member, err := w.pick()
	if err != nil {
		N.CloseOnHandshakeFailure(conn, onClose, err)
		w.logger.ErrorContext(ctx, err)
		return
	}
	wrappedClose := func(closeErr error) {
		w.recordResult(index, closeErr)
		w.release(index)
		if onClose != nil {
			onClose(closeErr)
		}
	}
	if outboundHandler, isHandler := member.outbound.(adapter.PacketConnectionHandlerEx); isHandler {
		outboundHandler.NewPacketConnectionEx(ctx, conn, metadata, wrappedClose)
	} else {
		w.connection.NewPacketConnection(ctx, member.outbound, conn, metadata, wrappedClose)
	}
}

// weightedCountedConn/weightedCountedPacketConn release their member's
// connection-cap slot exactly once, on Close - DialContext/ListenPacket
// return these instead of the raw conn since sing-box gives the group no
// other signal for when a connection handed back through those two methods
// actually ends (unlike NewConnectionEx/NewPacketConnectionEx, which come
// with their own close callback).
type weightedCountedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *weightedCountedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

type weightedCountedPacketConn struct {
	net.PacketConn
	once    sync.Once
	release func()
}

func (c *weightedCountedPacketConn) Close() error {
	err := c.PacketConn.Close()
	c.once.Do(c.release)
	return err
}
