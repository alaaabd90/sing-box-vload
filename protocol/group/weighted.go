package group

import (
	"context"
	"io"
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
	return w.pickExcept(-1)
}

// pickExcept behaves like pick but never returns member index exclude
// (pass -1 to consider every member). Used to find a genuinely different
// second member for a hedge race - racing a member against itself would be
// pointless.
func (w *Weighted) pickExcept(exclude int) (int, weightedMember, error) {
	w.pickMu.Lock()
	best := -1
	var bestHeadroom int64
	tied := 0
	for i := range w.limiters {
		if i == exclude {
			continue
		}
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
		// Every other member is breaker-tripped - fall back to plain
		// rotation (ignoring headroom entirely) so we keep probing rather
		// than stalling every new connection outright.
		fallback := w.picker.Next()
		if fallback != exclude {
			best = fallback
		}
		// fallback == exclude means the only member the picker could offer
		// is the one we're specifically avoiding - genuinely nothing else
		// to try right now, best stays -1.
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

// hedgeDelay bounds how long a hedge race waits on the preferred member
// before also starting a second attempt on a different member. Short enough
// that a struggling member's full dial timeout is never on the critical
// path for a new connection (previously a single bad pick surfaced straight
// to the caller as a connection error, needing a manual refresh/retry to
// land on the other, healthy network), while long enough that a normal dial
// on a healthy member essentially never triggers a second attempt at all.
const hedgeDelay = 250 * time.Millisecond

type hedgeResult[T io.Closer] struct {
	index int
	conn  T
	err   error
}

// hedgedPick races dial against the pick()-preferred member and, if that
// attempt is still outstanding after hedgeDelay or fails outright, against a
// second, different member in parallel - whichever succeeds first wins. A
// connection only actually fails once every available member has failed it,
// not just the one pick() happened to prefer at that instant.
//
// A race loser is never counted against its member's health (see
// abandonHedgeLoser): canceling mid-dial because the other side already won
// is our choice, not evidence the member is unhealthy.
func hedgedPick[T io.Closer](w *Weighted, ctx context.Context, dial func(context.Context, adapter.Outbound) (T, error)) (int, T, error) {
	var zero T
	primaryIdx, primaryMember, err := w.pick()
	if err != nil {
		return -1, zero, err
	}

	start := func(idx int, member weightedMember) (<-chan hedgeResult[T], context.CancelFunc) {
		dialCtx, cancel := context.WithCancel(ctx)
		ch := make(chan hedgeResult[T], 1)
		go func() {
			conn, dialErr := dial(dialCtx, member.outbound)
			ch <- hedgeResult[T]{idx, conn, dialErr}
		}()
		return ch, cancel
	}

	primaryCh, primaryCancel := start(primaryIdx, primaryMember)
	defer primaryCancel()

	var secondaryCh <-chan hedgeResult[T]
	var secondaryCancel context.CancelFunc
	defer func() {
		if secondaryCancel != nil {
			secondaryCancel()
		}
	}()

	trySecondary := func() {
		if secondaryCh != nil {
			return
		}
		idx, member, pickErr := w.pickExcept(primaryIdx)
		if pickErr != nil {
			return // nothing else available right now; keep waiting on primary
		}
		secondaryCh, secondaryCancel = start(idx, member)
	}

	timer := time.NewTimer(hedgeDelay)
	defer timer.Stop()

	var primaryFailed, secondaryFailed bool
	for {
		select {
		case res := <-primaryCh:
			primaryCh = nil
			w.recordResult(res.index, res.err)
			if res.err == nil {
				if secondaryCh != nil {
					secondaryCancel()
					abandonHedgeLoser(w, secondaryCh)
				}
				return res.index, res.conn, nil
			}
			// A genuine failure, not a cancellation - this attempt is done
			// either way (we're either racing a different secondary or
			// giving up entirely), so its slot must be freed now rather
			// than left to a Close() that will never come.
			w.release(res.index)
			primaryFailed = true
			if secondaryFailed {
				return -1, zero, res.err
			}
			trySecondary()
			if secondaryCh == nil {
				return -1, zero, res.err
			}
		case res := <-secondaryCh:
			secondaryCh = nil
			w.recordResult(res.index, res.err)
			if res.err == nil {
				primaryCancel()
				abandonHedgeLoser(w, primaryCh)
				return res.index, res.conn, nil
			}
			w.release(res.index)
			secondaryFailed = true
			if primaryFailed {
				return -1, zero, res.err
			}
		case <-timer.C:
			trySecondary()
		case <-ctx.Done():
			primaryCancel()
			if primaryCh != nil {
				abandonHedgeLoser(w, primaryCh)
			}
			if secondaryCh != nil {
				secondaryCancel()
				abandonHedgeLoser(w, secondaryCh)
			}
			return -1, zero, ctx.Err()
		}
	}
}

// abandonHedgeLoser waits for a hedge race participant that lost (or that
// we're giving up on entirely) to actually finish, then releases its
// concurrency slot and closes it if it turned out to succeed anyway despite
// no longer being wanted. Deliberately never calls recordResult - see
// hedgedPick's doc comment.
func abandonHedgeLoser[T io.Closer](w *Weighted, ch <-chan hedgeResult[T]) {
	go func() {
		res := <-ch
		w.release(res.index)
		if res.err == nil {
			res.conn.Close()
		}
	}()
}

func (w *Weighted) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	index, conn, err := hedgedPick(w, ctx, func(dialCtx context.Context, ob adapter.Outbound) (net.Conn, error) {
		return ob.DialContext(dialCtx, network, destination)
	})
	if err != nil {
		return nil, err
	}
	return &weightedCountedConn{Conn: conn, release: func() { w.release(index) }}, nil
}

func (w *Weighted) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	index, conn, err := hedgedPick(w, ctx, func(dialCtx context.Context, ob adapter.Outbound) (net.PacketConn, error) {
		return ob.ListenPacket(dialCtx, destination)
	})
	if err != nil {
		return nil, err
	}
	return &weightedCountedPacketConn{PacketConn: conn, release: func() { w.release(index) }}, nil
}

// NewConnectionEx and NewPacketConnectionEx hand off to the connection
// manager with the group itself as the dialer, rather than picking a member
// and delegating to its own NewConnectionEx: that would mean handing the
// same inbound conn to two members at once to hedge it, which isn't safe
// (only one side may ever read/write it). Routing through DialContext/
// ListenPacket instead means real TUN-routed traffic - not just direct
// DialContext callers - gets the same hedge race. No real vload member
// (VLESS/VMess/Trojan/Hysteria2/...) implements the ConnectionHandlerEx
// fast path the old code special-cased, so this changes nothing for any of
// them; it only affects the same connection setup path they were already
// going through.
func (w *Weighted) NewConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	w.connection.NewConnection(ctx, w, conn, metadata, onClose)
}

func (w *Weighted) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	w.connection.NewPacketConnection(ctx, w, conn, metadata, onClose)
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
