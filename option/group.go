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

// WeightedOutboundMember is one member of a WeightedOutboundOptions group:
// a reference to another outbound's tag plus its relative share of new
// connections. Weight is treated as 1 when omitted/zero.
type WeightedOutboundMember struct {
	Outbound string `json:"outbound"`
	Weight   uint   `json:"weight,omitempty"`
}

// WeightedOutboundOptions distributes new connections across its member
// outbounds using smooth weighted round robin, picking a (possibly
// different) member per new connection rather than sticking to one
// selection like SelectorOutboundOptions does.
type WeightedOutboundOptions struct {
	Outbounds []WeightedOutboundMember `json:"outbounds"`
}
