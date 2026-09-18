package route

import (
	"context"
	"net"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/sniff"
	R "github.com/sagernet/sing-box/route/rule"
	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// A browser may retain a synthetic address from before this app's cache existed.
// Recover only this flow from its own hostname, never publish that unverified
// association into the shared DNS cache or send the synthetic IP to a proxy.
// Healthy mapped flows never enter this path.
func (r *Router) recoverFakeIPConnection(ctx context.Context, metadata *adapter.InboundContext, conn net.Conn, packetConn N.PacketConn) ([]*buf.Buffer, []*N.PacketBuffer, error) {
	original := metadata.Destination
	missing := E.Cause(errMissingFakeIP, original.Addr)
	if conn == nil && (packetConn == nil || original.Port != 443) {
		return nil, nil, missing
	}
	action := &R.RuleActionSniff{
		SnifferNames:   []string{"fakeip-recovery"},
		StreamSniffers: []sniff.StreamSniffer{sniff.TLSClientHello, sniff.HTTPHost},
		PacketSniffers: []sniff.PacketSniffer{sniff.QUICClientHello},
		Timeout:        200 * time.Millisecond,
	}
	buffer, packets, err := r.actionSniff(ctx, metadata, action, conn, packetConn, nil, nil)
	var buffers []*buf.Buffer
	if buffer != nil {
		buffers = append(buffers, buffer)
	}
	if err != nil {
		return buffers, packets, err
	}
	if metadata.SniffError != nil || !M.IsDomainName(metadata.Domain) {
		return buffers, packets, missing
	}
	metadata.OriginDestination = original
	metadata.Destination = M.Socksaddr{Fqdn: metadata.Domain, Port: original.Port}
	metadata.FakeIP = true
	r.logger.DebugContext(ctx, "recovered stale fakeip connection using ", metadata.Protocol, " hostname")
	return buffers, packets, nil
}
