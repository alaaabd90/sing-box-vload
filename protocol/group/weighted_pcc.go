package group

import "sync"

// pccPicker implements a deterministic, ratio-based round robin - the same
// idea as "PCC" (Per Connection Classifier) load balancing on multi-WAN
// routers: reduce the configured weights to their smallest integer ratio
// (e.g. 10 and 20 reduce to 1:2), lay that ratio out as a fixed, evenly
// interleaved repeating cycle of member indices (1:2 -> [0,1,1], not
// [0,0,1,1,1,1,...]), and hand out the next index in that cycle on every
// pick.
//
// This is deliberately simpler than a load-aware scheme (smooth weighted
// round robin, least connections, etc.): which member gets picked next
// depends on nothing but "which pick number is this", never on how long
// previous connections on either member take to finish, how many are still
// open, or any timing/availability signal from outside. That makes it
// trivial to reason about and impossible for a bad external signal (a
// missed network-state callback, for instance) to leave it silently stuck
// favoring one member - the only way to exclude a member at all is the
// explicit SetAvailable call below, and even then Next() just skips its
// slots in the cycle rather than needing any separate state to recover.
type pccPicker struct {
	mu    sync.Mutex
	cycle []int
	pos   int
	avail []bool
}

func newPCCPicker(weights []int) *pccPicker {
	n := len(weights)
	avail := make([]bool, n)
	for i := range avail {
		avail[i] = true
	}

	ratios := make([]int, n)
	g := 0
	for _, w := range weights {
		if w <= 0 {
			w = 1
		}
		g = gcdInt(g, w)
	}
	if g <= 0 {
		g = 1
	}
	total := 0
	for i, w := range weights {
		if w <= 0 {
			w = 1
		}
		r := w / g
		if r <= 0 {
			r = 1
		}
		ratios[i] = r
		total += r
	}

	// Evenly interleave the cycle instead of laying out each member's whole
	// share back-to-back (e.g. weights [1,1,2] should produce a cycle like
	// [0,2,1,2], not cluster as [0,1,2,2]): run one full period of the
	// standard smooth-weighted-round-robin accumulator over the reduced
	// ratio to lay out the cycle once, then play that fixed sequence back
	// on every future call. This borrows SWRR purely as a cycle-generator -
	// the picker itself has no per-pick state beyond its position in the
	// resulting fixed array, unlike a live SWRR picker.
	current := make([]int, n)
	cycle := make([]int, 0, total)
	for len(cycle) < total {
		best := -1
		for i, r := range ratios {
			current[i] += r
			if best == -1 || current[i] > current[best] {
				best = i
			}
		}
		current[best] -= total
		cycle = append(cycle, best)
	}

	return &pccPicker{cycle: cycle, avail: avail}
}

func gcdInt(a, b int) int {
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// SetAvailable marks the member at index as available/unavailable. Indexes
// out of range are ignored.
func (p *pccPicker) SetAvailable(index int, available bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if index < 0 || index >= len(p.avail) {
		return
	}
	p.avail[index] = available
}

// IsAvailable reports whether index is currently marked available. Out of
// range indexes report true (fail open).
func (p *pccPicker) IsAvailable(index int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if index < 0 || index >= len(p.avail) {
		return true
	}
	return p.avail[index]
}

// Next advances the cycle and returns the next member index to use,
// skipping unavailable members, or -1 if there are no members at all. If
// every member is unavailable, all are treated as available so a pick is
// still produced rather than stalling entirely.
func (p *pccPicker) Next() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nextLocked()
}

// nextLocked scans at most one full cycle for a member that is available
// (or fallbackAll, if none are).
func (p *pccPicker) nextLocked() int {
	if len(p.cycle) == 0 {
		return -1
	}

	fallbackAll := !p.anyAvailableLocked()

	for i := 0; i < len(p.cycle); i++ {
		idx := p.cycle[p.pos]
		p.pos = (p.pos + 1) % len(p.cycle)
		if fallbackAll || p.avail[idx] {
			return idx
		}
	}
	return -1
}

func (p *pccPicker) anyAvailableLocked() bool {
	for _, a := range p.avail {
		if a {
			return true
		}
	}
	return false
}
