package group

import "sync"

// weightedSWRR selects among a fixed set of indices using smooth weighted
// round robin (the same algorithm Nginx uses for weighted upstream
// selection): each pick advances every member's accumulator by its own
// weight, chooses the largest accumulator, then subtracts the total weight
// from the winner. Over any run of picks this converges to the exact
// configured ratio, including for small pick counts, which plain weighted
// random selection does not guarantee.
//
// Members can be marked unavailable at runtime (e.g. their underlying
// network dropped); unavailable members are excluded from selection unless
// every member is unavailable, in which case all members are treated as
// available so a pick is still produced. Toggling availability resets all
// accumulators to avoid a transient burst toward a just-recovered member.
type weightedSWRR struct {
	mu      sync.Mutex
	weights []int
	current []int
	avail   []bool
}

func newWeightedSWRR(weights []int) *weightedSWRR {
	// Members start unavailable, not available: availability is only ever
	// flipped by an explicit SetAvailable call from the network-callback
	// side (see VpnService.kt's onSlotChanged), which only fires on an
	// available<->unavailable transition. If a member's network was never
	// up in the first place (e.g. the session is started while Wi-Fi is
	// already off), no transition ever happens - defaulting to "available"
	// would let the picker keep routing connections into a network that
	// never existed, where they'd just fail every time. The all-unavailable
	// fallback below still covers the brief window before either member's
	// first onAvailable callback has landed.
	return &weightedSWRR{
		weights: weights,
		current: make([]int, len(weights)),
		avail:   make([]bool, len(weights)),
	}
}

// SetAvailable marks the member at index as available/unavailable. Indexes
// out of range are ignored.
func (w *weightedSWRR) SetAvailable(index int, available bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if index < 0 || index >= len(w.avail) {
		return
	}
	if w.avail[index] == available {
		return
	}
	w.avail[index] = available
	for i := range w.current {
		w.current[i] = 0
	}
}

// Next returns the index of the next member to use, or -1 if there are no
// members at all.
func (w *weightedSWRR) Next() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	fallbackAll := !w.anyAvailableLocked()

	total := 0
	best := -1
	for i, weight := range w.weights {
		if !fallbackAll && !w.avail[i] {
			continue
		}
		w.current[i] += weight
		total += weight
		if best == -1 || w.current[i] > w.current[best] {
			best = i
		}
	}
	if best == -1 {
		return -1
	}
	w.current[best] -= total
	return best
}

func (w *weightedSWRR) anyAvailableLocked() bool {
	for _, a := range w.avail {
		if a {
			return true
		}
	}
	return false
}
