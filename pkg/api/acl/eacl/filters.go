package eacl

import (
	"github.com/nspcc-dev/neofs-api-go/v2/acl"
)

// Filters represents set of Filter's.
type Filters struct {
	elems []Filter
}

// Len returns number of elements in the Filters.
func (x Filters) Len() int {
	return len(x.elems)
}

// SetLen sets number of elements in the Filters.
// Does not modify already existing elements.
func (x *Filters) SetLen(num int) {
	if len(x.elems) < num {
		x.elems = append(x.elems, make([]Filter, num)...)
	}

	x.elems = x.elems[:num]
}

// Iterate is a read-only iterator over all elements of the Filters. Passes each element to the handler.
// Breaks iterating on true handler's return.
func (x Filters) Iterate(f func(Filter) bool) {
	for i := range x.elems {
		if f(x.elems[i]) {
			return
		}
	}
}

// IterateP is a read-write iterator over all elements of the Filters. Passes pointer to each element to the handler.
// Breaks iterating on true handler's return.
func (x Filters) IterateP(f func(*Filter) bool) {
	for i := range x.elems {
		if f(&x.elems[i]) {
			return
		}
	}
}

// AppendFilters appends elements to the Filters.
//
// Filters must not be nil.
func AppendFilters(fs *Filters, elems ...Filter) {
	lnNew := len(elems)
	if lnNew == 0 {
		return
	}

	lnPrev := fs.Len()

	fs.SetLen(lnPrev + lnNew)

	var indFull, indPos int

	fs.IterateP(func(a *Filter) bool {
		if indFull < lnPrev {
			indFull++
			return false
		}

		*a = elems[indPos]

		indPos++

		return false
	})
}

// filtersFromV2 reads Filters from acl.HeaderFilter slice.
//
// All slice elements must not be nil.
func filtersFromV2(fs *Filters, fsv2 []*acl.HeaderFilter) {
	ln := len(fsv2)
	fs.SetLen(ln)

	ind := 0

	fs.IterateP(func(f *Filter) bool {
		f.FromV2(*fsv2[ind])

		ind++

		return false
	})
}

// filtersToV2 writes Filters to acl.HeaderFilter slice.
//
// Slice length must be at least Len(). Items can be nil.
func filtersToV2(fsv2 []*acl.HeaderFilter, fs Filters) {
	ind := 0

	fs.Iterate(func(f Filter) bool {
		if fsv2[ind] == nil {
			fsv2[ind] = new(acl.HeaderFilter)
		}

		f.WriteToV2(fsv2[ind])

		ind++

		return false
	})
}
