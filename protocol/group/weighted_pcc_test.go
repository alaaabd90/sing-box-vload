package group

import "testing"

func countPCCPicks(w *pccPicker, n int) []int {
	counts := make([]int, len(w.avail))
	for i := 0; i < n; i++ {
		idx := w.Next()
		if idx >= 0 {
			counts[idx]++
		}
	}
	return counts
}

func TestPCCRatioExactOverOneCycle(t *testing.T) {
	cases := []struct {
		name        string
		weights     []int
		wantRatio   []int // reduced ratio, e.g. weights [10,20] -> [1,2]
	}{
		{"10-20", []int{10, 20}, []int{1, 2}},
		{"50-50", []int{50, 50}, []int{1, 1}},
		{"70-30", []int{70, 30}, []int{7, 3}},
		{"1-99", []int{1, 99}, []int{1, 99}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newPCCPicker(tc.weights)
			cycleLen := 0
			for _, r := range tc.wantRatio {
				cycleLen += r
			}
			if len(w.cycle) != cycleLen {
				t.Fatalf("expected cycle length %d, got %d (%v)", cycleLen, len(w.cycle), w.cycle)
			}
			// exactly one full cycle must match the exact ratio, not just approximately
			counts := countPCCPicks(w, cycleLen)
			for i, want := range tc.wantRatio {
				if counts[i] != want {
					t.Errorf("member %d: got %d picks over one cycle, want exactly %d (weights=%v)", i, counts[i], want, tc.weights)
				}
			}
		})
	}
}

func TestPCCInterleavesEvenly(t *testing.T) {
	// weights [1,1,2] -> ratio [1,1,2], cycle should interleave as
	// [0,2,1,2] (member 2 spread out), not cluster as [0,1,2,2].
	w := newPCCPicker([]int{1, 1, 2})
	if len(w.cycle) != 4 {
		t.Fatalf("expected cycle length 4, got %d (%v)", len(w.cycle), w.cycle)
	}
	// member 2 (the weight-2 one) must not appear twice in a row
	for i := 0; i < len(w.cycle)-1; i++ {
		if w.cycle[i] == 2 && w.cycle[i+1] == 2 {
			t.Fatalf("expected member 2 to be spread out, got consecutive picks in cycle %v", w.cycle)
		}
	}
}

func TestPCCAvailableByDefault(t *testing.T) {
	w := newPCCPicker([]int{50, 50})
	counts := countPCCPicks(w, 20)
	if counts[0] == 0 || counts[1] == 0 {
		t.Fatalf("expected both members to be picked with no SetAvailable calls at all, got %v", counts)
	}
}

func TestPCCUnavailableSkipped(t *testing.T) {
	w := newPCCPicker([]int{50, 50})
	w.SetAvailable(1, false)
	counts := countPCCPicks(w, 20)
	if counts[1] != 0 {
		t.Fatalf("expected member 1 to never be picked while unavailable, got %d picks", counts[1])
	}
	if counts[0] != 20 {
		t.Fatalf("expected member 0 to get all 20 picks, got %d", counts[0])
	}
}

func TestPCCAllUnavailableFallsBack(t *testing.T) {
	w := newPCCPicker([]int{50, 50})
	w.SetAvailable(0, false)
	w.SetAvailable(1, false)
	idx := w.Next()
	if idx == -1 {
		t.Fatalf("expected a pick even when all members are marked unavailable, got -1")
	}
}

func TestPCCRecoveryIsImmediate(t *testing.T) {
	// Unlike a load-aware scheme, recovery needs no special-casing at all:
	// the cycle position just keeps advancing and skipping unavailable
	// slots; making a member available again means its very next scheduled
	// slot in the cycle is honored right away.
	w := newPCCPicker([]int{50, 50})
	w.SetAvailable(1, false)
	countPCCPicks(w, 3)
	w.SetAvailable(1, true)
	counts := countPCCPicks(w, 2)
	if counts[0] != 1 || counts[1] != 1 {
		t.Fatalf("expected an even 1/1 split immediately after recovery, got %v", counts)
	}
}

func TestPCCSingleMemberAlwaysPicked(t *testing.T) {
	w := newPCCPicker([]int{1})
	for i := 0; i < 5; i++ {
		if idx := w.Next(); idx != 0 {
			t.Fatalf("expected index 0, got %d", idx)
		}
	}
}

func TestGCDInt(t *testing.T) {
	cases := []struct{ a, b, want int }{
		{10, 20, 10},
		{70, 30, 10},
		{1, 99, 1},
		{50, 50, 50},
		{0, 5, 5},
	}
	for _, tc := range cases {
		if got := gcdInt(tc.a, tc.b); got != tc.want {
			t.Errorf("gcdInt(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}
