package route

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/quic-go"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestRecoverStaleFakeIPHTTPPreservesPayload(t *testing.T) {
	r := &Router{logger: logger.NOP()}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	payload := []byte("GET /video HTTP/1.1\r\nHost: example.com\r\n\r\n")
	go func() { _, _ = client.Write(payload) }()
	m := adapter.InboundContext{Destination: M.ParseSocksaddr("198.18.1.2:80"), Network: "tcp"}
	buffers, _, err := r.recoverFakeIPConnection(context.Background(), &m, server, nil)
	defer buf.ReleaseMulti(buffers)
	if err != nil {
		t.Fatal(err)
	}
	if !m.FakeIP || m.Destination.Fqdn != "example.com" || m.OriginDestination.String() != "198.18.1.2:80" {
		t.Fatalf("incorrect recovery: %+v", m)
	}
	if len(buffers) != 1 || !bytes.Equal(buffers[0].Bytes(), payload) {
		t.Fatal("first request bytes were consumed or changed")
	}
}

func TestRecoverStaleFakeIPQUICPreservesDatagrams(t *testing.T) {
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, _ := quic.DialAddr(ctx, listener.LocalAddr().String(), &tls.Config{ServerName: "example.com", NextProtos: []string{"h3"}}, nil)
		if conn != nil {
			conn.CloseWithError(0, "")
		}
	}()
	r := &Router{logger: logger.NOP()}
	m := adapter.InboundContext{Destination: M.ParseSocksaddr("198.18.1.2:443"), Network: "udp"}
	_, packets, err := r.recoverFakeIPConnection(ctx, &m, nil, bufio.NewPacketConn(listener))
	defer N.ReleaseMultiPacketBuffer(packets)
	cancel()
	<-done
	if err != nil || m.Destination.Fqdn != "example.com" || !m.FakeIP {
		t.Fatalf("QUIC hostname recovery failed: %v", err)
	}
	if len(packets) == 0 || packets[0].Buffer.Len() == 0 {
		t.Fatal("QUIC datagrams consumed instead of preserved")
	}
}

func TestRecoverStaleFakeIPTLS(t *testing.T) {
	r := &Router{logger: logger.NOP()}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() { c := tls.Client(client, &tls.Config{ServerName: "example.com"}); _ = c.Handshake() }()
	m := adapter.InboundContext{Destination: M.ParseSocksaddr("198.18.1.2:443"), Network: "tcp"}
	buffers, _, err := r.recoverFakeIPConnection(context.Background(), &m, server, nil)
	defer buf.ReleaseMulti(buffers)
	if err != nil || m.Destination.Fqdn != "example.com" || len(buffers) == 0 {
		t.Fatalf("TLS recovery failed: %v", err)
	}
}

func TestUnknownFakeIPCannotBeDialedAndRecoveryIsBounded(t *testing.T) {
	r := &Router{logger: logger.NOP()}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	m := adapter.InboundContext{Destination: M.ParseSocksaddr("198.18.1.2:443"), Network: "tcp"}
	start := time.Now()
	buffers, _, err := r.recoverFakeIPConnection(context.Background(), &m, server, nil)
	buf.ReleaseMulti(buffers)
	if !errors.Is(err, errMissingFakeIP) {
		t.Fatalf("missing hostname accepted: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("unbounded stale-address recovery")
	}
	if m.FakeIP || m.Destination.IsDomain() {
		t.Fatal("invented a hostname")
	}
	// Reader remains usable after the peek timeout resets its deadline.
	go func() { _, _ = client.Write([]byte{1}) }()
	var b [1]byte
	if _, err = io.ReadFull(server, b[:]); err != nil {
		t.Fatal(err)
	}
}
