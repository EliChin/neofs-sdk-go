package eacl

import (
	"github.com/nspcc-dev/neofs-api-go/v2/acl"
)

// Targets represents set of Target's.
type Targets struct {
	elems []Target
}

// Len returns number of elements in the Targets.
func (x Targets) Len() int {
	return len(x.elems)
}

// SetLen sets number of elements in the Targets.
// Does not modify already existing elements.
func (x *Targets) SetLen(num int) {
	if len(x.elems) < num {
		x.elems = append(x.elems, make([]Target, num)...)
	}

	x.elems = x.elems[:num]
}

// Iterate is a read-only iterator over all elements of the Targets. Passes each element to the handler.
// Breaks iterating on true handler's return.
func (x Targets) Iterate(f func(Target) bool) {
	for i := range x.elems {
		if f(x.elems[i]) {
			return
		}
	}
}

// IterateP is a read-write iterator over all elements of the Targets. Passes pointer to each element to the handler.
// Breaks iterating on true handler's return.
func (x Targets) IterateP(f func(*Target) bool) {
	for i := range x.elems {
		if f(&x.elems[i]) {
			return
		}
	}
}

// AppendTargets appends elements to the Targets.
//
// Targets must not be nil.
func AppendTargets(ts *Targets, elems ...Target) {
	lnNew := len(elems)
	if lnNew == 0 {
		return
	}

	lnPrev := ts.Len()

	ts.SetLen(lnPrev + lnNew)

	var indFull, indPos int

	ts.IterateP(func(t *Target) bool {
		if indFull < lnPrev {
			indFull++
			return false
		}

		*t = elems[indPos]

		indPos++

		return false
	})
}

// targetsFromV2 reads Filters from acl.Target slice.
//
// All slice elements must not be nil.
func targetsFromV2(ts *Targets, tsv2 []*acl.Target) {
	ln := len(tsv2)
	ts.SetLen(ln)

	ind := 0

	ts.IterateP(func(t *Target) bool {
		t.FromV2(*tsv2[ind])

		ind++

		return false
	})
}

// targetsToV2 writes Targets to acl.Target slice.
//
// Slice length must be at least Len(). Items can be nil.
func targetsToV2(tsv2 []*acl.Target, ts Targets) {
	ind := 0

	ts.Iterate(func(f Target) bool {
		if tsv2[ind] == nil {
			tsv2[ind] = new(acl.Target)
		}

		f.WriteToV2(tsv2[ind])

		ind++

		return false
	})
}
