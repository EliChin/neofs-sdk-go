package eacl

import (
	"github.com/nspcc-dev/neofs-api-go/v2/acl"
)

// Records represents set of Record's.
type Records struct {
	elems []Record
}

// Len returns number of elements in the Records.
func (x Records) Len() int {
	return len(x.elems)
}

// SetLen sets number of elements in the Records.
// Does not modify already existing elements.
func (x *Records) SetLen(num int) {
	if len(x.elems) < num {
		x.elems = append(x.elems, make([]Record, num)...)
	}

	x.elems = x.elems[:num]
}

// Iterate is a read-only iterator over all elements of the Records. Passes each element to the handler.
// Breaks iterating on true handler's return.
func (x Records) Iterate(f func(Record) bool) {
	for i := range x.elems {
		if f(x.elems[i]) {
			return
		}
	}
}

// IterateP is a read-write iterator over all elements of the Records. Passes pointer to each element to the handler.
// Breaks iterating on true handler's return.
func (x Records) IterateP(f func(*Record) bool) {
	for i := range x.elems {
		if f(&x.elems[i]) {
			return
		}
	}
}

// AppendRecords appends elements to the Records.
//
// Records must not be nil.
func AppendRecords(fs *Records, elems ...Record) {
	lnNew := len(elems)
	if lnNew == 0 {
		return
	}

	lnPrev := fs.Len()

	fs.SetLen(lnPrev + lnNew)

	var indFull, indPos int

	fs.IterateP(func(r *Record) bool {
		if indFull < lnPrev {
			indFull++
			return false
		}

		*r = elems[indPos]

		indPos++

		return false
	})
}

// recordsFromV2 reads Records from acl.Record slice.
//
// All slice elements must not be nil.
func recordsFromV2(rs *Records, rsv2 []*acl.Record) {
	ln := len(rsv2)
	rs.SetLen(ln)

	ind := 0

	rs.IterateP(func(r *Record) bool {
		r.FromV2(*rsv2[ind])

		ind++

		return false
	})
}

// recordsToV2 writes Records to acl.Record slice.
//
// Slice length must be at least Len(). Items can be nil.
func recordsToV2(rsv2 []*acl.Record, rs Records) {
	ind := 0

	rs.Iterate(func(r Record) bool {
		if rsv2[ind] == nil {
			rsv2[ind] = new(acl.Record)
		}

		r.WriteToV2(rsv2[ind])

		ind++

		return false
	})
}
