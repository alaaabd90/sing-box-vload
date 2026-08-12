package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	Outbounds                 []string `json:"outbounds"`
	Default                   string   `json:"default,omitempty"`
	InterruptExistConnections bool     `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	Outbounds                 []string           `json:"outbounds"`
	URL                       string             `json:"url,omitempty"`
	Interval                  badoption.Duration `json:"interval,omitempty"`
	Tolerance                 uint16             `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool               `json:"interrupt_exist_connections,omitempty"`
}

// WeightedOutboundMember is one member of a WeightedOutboundOptions group: a
// reference to another outbound's tag. Neither field needs to be set for
// correct operation - each member's real sustainable concurrency is
// discovered automatically at runtime (see adaptiveLimiter), not from a
// fixed ratio or connection-count guess.
//
// Weight only seeds the rare fallback rotation used when every member's
// circuit breaker is simultaneously open; it plays no part in normal traffic
// distribution, which is driven entirely by each member's live, adaptively
// discovered spare capacity. Treated as 1 when omitted/zero.
//
// MaxConnections, if set, is an optional hard ceiling that the adaptive
// limit is never allowed to grow past - useful if an operator already knows
// a backend's absolute upper bound and wants to guard against transient
// over-growth. Zero (the default) means no extra ceiling beyond the
// built-in safety maximum, so existing configs are unaffected and no
// configuration is required for the adaptive behavior to work.
type WeightedOutboundMember struct {
	Outbound       string `json:"outbound"`
	Weight         uint   `json:"weight,omitempty"`
	MaxConnections uint   `json:"max_connections,omitempty"`
}

// WeightedOutboundOptions distributes new connections across its member
// outbounds. Mode selects how:
//
//   - "" / "adaptive" (default): picks, per new connection, whichever member
//     currently has the most spare proven capacity, and additionally hedges
//     (races a second member in parallel) if the preferred one hasn't
//     finished within a short delay or fails outright. Intended for bulk
//     traffic that should actually use both members' combined throughput
//     (e.g. a download manager splitting load across two networks).
//   - "priority": always prefers Outbounds[0] and only ever uses a later
//     member when an earlier one is circuit-broken (see the package-level
//     circuit breaker docs); never hedges/races, so traffic doesn't
//     casually spread across members just because the preferred one was a
//     little slow this instant. Intended for things that want a single
//     consistent path with automatic failover rather than combined
//     throughput - DNS resolution is the motivating case: answers from two
//     different networks can differ (split-horizon DNS, different
//     upstream results), so bouncing between them per-query undermines the
//     "stable browsing" this mode exists for, even though the same two
//     networks are still fine to combine for raw throughput elsewhere.
type WeightedOutboundOptions struct {
	Outbounds []WeightedOutboundMember `json:"outbounds"`
	Mode      string                   `json:"mode,omitempty"`
}
