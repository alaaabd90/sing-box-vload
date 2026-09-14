package group

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

func TestWeightedOptionsJSONRoundTrip(t *testing.T) {
	registry := outbound.NewRegistry()
	RegisterWeighted(registry)

	raw, loaded := registry.CreateOptions("weighted")
	if !loaded {
		t.Fatalf("weighted outbound type not registered")
	}

	sample := `{"outbounds":[{"outbound":"wifi-out","weight":70},{"outbound":"sim-out","weight":30}]}`
	if err := json.Unmarshal([]byte(sample), raw); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	options, ok := raw.(*option.WeightedOutboundOptions)
	if !ok {
		t.Fatalf("expected *option.WeightedOutboundOptions, got %T", raw)
	}
	if len(options.Outbounds) != 2 {
		t.Fatalf("expected 2 members, got %d", len(options.Outbounds))
	}
	if options.Outbounds[0].Outbound != "wifi-out" || options.Outbounds[0].Weight != 70 {
		t.Errorf("member 0 mismatch: %+v", options.Outbounds[0])
	}
	if options.Outbounds[1].Outbound != "sim-out" || options.Outbounds[1].Weight != 30 {
		t.Errorf("member 1 mismatch: %+v", options.Outbounds[1])
	}
}

func TestNewWeightedValidation(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNOPFactory().Logger()

	if _, err := NewWeighted(ctx, nil, logger, "test", option.WeightedOutboundOptions{
		Outbounds: []option.WeightedOutboundMember{{Outbound: "only-one"}},
	}); err == nil {
		t.Fatalf("expected error for fewer than 2 members")
	}

	if _, err := NewWeighted(ctx, nil, logger, "test", option.WeightedOutboundOptions{
		Outbounds: []option.WeightedOutboundMember{{Outbound: "a"}, {Outbound: ""}},
	}); err == nil {
		t.Fatalf("expected error for empty outbound tag")
	}

	w, err := NewWeighted(ctx, nil, logger, "test", option.WeightedOutboundOptions{
		Outbounds: []option.WeightedOutboundMember{{Outbound: "a", Weight: 70}, {Outbound: "b", Weight: 30}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.Tag() != "test" || w.Type() != "weighted" {
		t.Errorf("unexpected tag/type: %s/%s", w.Tag(), w.Type())
	}
}
