package oid

import (
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
)

// IDs represents set of ID's.
type IDs struct {
	elems []ID
}

// Len returns number of elements in the IDs.
func (x IDs) Len() int {
	return len(x.elems)
}

// SetLen sets number of elements in the IDs.
// Does not modify already existing elements.
func (x *IDs) SetLen(num int) {
	if cap(x.elems) < num {
		x.elems = make([]ID, 0, num)
	}

	x.elems = x.elems[:num]
}

// Iterate is a read-only iterator over all elements of the IDs. Passes each element to the handler.
// Breaks iterating on true handler's return.
func (x IDs) Iterate(f func(ID) bool) {
	for i := range x.elems {
		if f(x.elems[i]) {
			return
		}
	}
}

// Iterate is a read-write iterator over all elements of the IDs. Passes pointer to each element to the handler.
// Breaks iterating on true handler's return.
func (x IDs) IterateP(f func(*ID) bool) {
	for i := range x.elems {
		if f(&x.elems[i]) {
			return
		}
	}
}

// AppendIDs appends elements to the IDs.
//
// IDs must not be nil.
func AppendIDs(ids *IDs, elems ...ID) {
	lnNew := len(elems)
	if lnNew == 0 {
		return
	}

	lnPrev := ids.Len()

	ids.SetLen(lnPrev + lnNew)

	var indFull, indPos int

	ids.IterateP(func(a *ID) bool {
		if indFull < lnPrev {
			indFull++
			return false
		}

		*a = elems[indPos]

		indPos++

		return false
	})
}

// IDsFromV2 reads IDs from refs.ObjectID slice.
//
// All slice elements must not be nil.
func IDsFromV2(ids *IDs, idsv2 []*refs.ObjectID) {
	ln := len(idsv2)
	ids.SetLen(ln)

	ind := 0

	ids.IterateP(func(id *ID) bool {
		id.FromV2(*idsv2[ind])

		ind++

		return false
	})
}

// IDsToV2 writes IDs to refs.ObjectID slice.
//
// Slice length must be at least Len(). Items can be nil.
func IDsToV2(idsv2 []*refs.ObjectID, ids IDs) {
	ind := 0

	ids.Iterate(func(id ID) bool {
		if idsv2[ind] == nil {
			idsv2[ind] = new(refs.ObjectID)
		}

		IDToV2(idsv2[ind], id)

		ind++

		return false
	})
}
