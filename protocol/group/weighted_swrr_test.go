package group

import "testing"

func countPicks(w *weightedSWRR, n int) []int {
	counts := make([]int, len(w.weights))
	for i := 0; i < n; i++ {
		idx := w.Next()
		if idx >= 0 {
			counts[idx]++
		}
	}
	return counts
}

func TestWeightedSWRRRatio(t *testing.T) {
	cases := []struct {
		name    string
		weights []int
		picks   int
	}{
		{"70-30", []int{70, 30}, 100},
		{"50-50", []int{50, 50}, 100},
		{"1-99", []int{1, 99}, 1000},
		{"small-8-picks-70-30", []int{7, 3}, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWeightedSWRR(tc.weights)
			counts := countPicks(w, tc.picks)
			total := 0
			for _, c := range counts {
				total += c
			}
			if total != tc.picks {
				t.Fatalf("expected %d total picks, got %d", tc.picks, total)
			}
			totalWeight := 0
			for _, wt := range tc.weights {
				totalWeight += wt
			}
			for i, c := range counts {
				expected := tc.picks * tc.weights[i] / totalWeight
				diff := c - expected
				if diff < 0 {
					diff = -diff
				}
				// SWRR should track the exact ratio to within 1 pick at any point.
				if diff > 1 {
					t.Errorf("member %d: got %d picks, expected ~%d (weights=%v)", i, c, expected, tc.weights)
				}
			}
		})
	}
}

func TestWeightedSWRRUnavailableSkipped(t *testing.T) {
	w := newWeightedSWRR([]int{50, 50})
	// Members start unavailable by default; member 0 must be explicitly
	// confirmed available (mirroring a real onAvailable callback) or the
	// all-unavailable fallback would let member 1 through too.
	w.SetAvailable(0, true)
	counts := countPicks(w, 20)
	if counts[1] != 0 {
		t.Fatalf("expected member 1 to never be picked while unavailable, got %d picks", counts[1])
	}
	if counts[0] != 20 {
		t.Fatalf("expected member 0 to get all 20 picks, got %d", counts[0])
	}
}

func TestWeightedSWRRAllUnavailableFallsBack(t *testing.T) {
	w := newWeightedSWRR([]int{50, 50})
	w.SetAvailable(0, false)
	w.SetAvailable(1, false)
	idx := w.Next()
	if idx == -1 {
		t.Fatalf("expected a pick even when all members are marked unavailable, got -1")
	}
}

func TestWeightedSWRRRecoveryResetsAccumulators(t *testing.T) {
	w := newWeightedSWRR([]int{50, 50})
	w.SetAvailable(0, true)
	countPicks(w, 5) // all go to member 0, building up member 0's history
	w.SetAvailable(1, true)
	// Immediately after recovery, neither member should have a stale
	// advantage that causes a run of consecutive picks for one side.
	counts := countPicks(w, 2)
	if counts[0] != 1 || counts[1] != 1 {
		t.Fatalf("expected an even 1/1 split immediately after recovery, got %v", counts)
	}
}

func TestWeightedSWRRSingleMemberAlwaysPicked(t *testing.T) {
	w := newWeightedSWRR([]int{1})
	for i := 0; i < 5; i++ {
		if idx := w.Next(); idx != 0 {
			t.Fatalf("expected index 0, got %d", idx)
		}
	}
}
