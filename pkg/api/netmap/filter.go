package netmap

import (
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

// Filter represents NeoFS API V2-compatible netmap filter.
type Filter struct {
	operation Operation

	name string

	key string

	value string

	innerFilters Filters
}

// fromV2 restores Filter from netmap.Filter message.
func (x *Filter) fromV2(fv2 netmap.Filter) {
	filtersFromV2(&x.innerFilters, fv2.GetFilters())
	x.operation.fromV2(fv2.GetOp())
	x.SetName(fv2.GetName())
	x.SetKey(fv2.GetKey())
	x.SetValue(fv2.GetValue())
}

// writeToV2 writes Filter to netmap.Filter message.
//
// Message must not be nil.
func (x Filter) writeToV2(fv2 *netmap.Filter) {
	{ // inner filters
		var fsv2 []*netmap.Filter

		if ln := x.innerFilters.Len(); ln > 0 {
			fsv2 = fv2.GetFilters()

			if cap(fsv2) < ln {
				fsv2 = make([]*netmap.Filter, 0, ln)
			}

			fsv2 = fsv2[:ln]

			filtersToV2(fsv2, x.innerFilters)
		}

		fv2.SetFilters(fsv2)
	}

	{ // operation
		var opv2 netmap.Operation

		x.operation.writeToV2(&opv2)

		fv2.SetOp(opv2)
	}

	fv2.SetName(x.name)
	fv2.SetKey(x.key)
	fv2.SetValue(x.value)
}

// Key returns key to filter.
func (x Filter) Key() string {
	return x.key
}

// SetKey sets key to filter.
func (x *Filter) SetKey(key string) {
	x.key = key
}

// Value returns value to match.
func (x Filter) Value() string {
	return x.value
}

// SetValue sets value to match.
func (x *Filter) SetValue(val string) {
	x.value = val
}

// Name returns filter name.
func (x Filter) Name() string {
	return x.name
}

// SetName sets filter name.
func (x *Filter) SetName(name string) {
	x.name = name
}

// Operation returns filtering operation.
func (x Filter) Operation() Operation {
	return x.operation
}

// SetOperation sets filtering operation.
func (x *Filter) SetOperation(operation Operation) {
	x.operation = operation
}

// InnerFilters returns set of the inner filters.
//
// Result mutation affects the Filter.
func (x Filter) InnerFilters() Filters {
	return x.innerFilters
}

// SetInnerFilters sets set of the inner filters.
//
// Parameter mutation affects the Filter.
func (x *Filter) SetInnerFilters(innerFilters Filters) {
	x.innerFilters = innerFilters
}

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
	if cap(x.elems) < num {
		x.elems = make([]Filter, 0, num)
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

	fs.IterateP(func(f *Filter) bool {
		if indFull < lnPrev {
			indFull++
			return false
		}

		*f = elems[indPos]

		indPos++

		return false
	})
}

// filtersFromV2 restores Filters from netmap.Filter slice.
//
// All slice elements must not be nil.
func filtersFromV2(fs *Filters, fsv2 []*netmap.Filter) {
	ln := len(fsv2)
	fs.SetLen(ln)

	ind := 0

	fs.IterateP(func(f *Filter) bool {
		f.fromV2(*fsv2[ind])

		ind++

		return false
	})
}

// filtersToV2 writes Filters to netmap.Filter slice.
//
// Slice length must be at least Len(). Items can be nil.
func filtersToV2(fsv2 []*netmap.Filter, fs Filters) {
	ind := 0

	fs.Iterate(func(f Filter) bool {
		if fsv2[ind] == nil {
			fsv2[ind] = new(netmap.Filter)
		}

		f.writeToV2(fsv2[ind])

		ind++

		return false
	})
}
