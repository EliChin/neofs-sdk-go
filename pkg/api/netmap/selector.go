package netmap

import (
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

// Selector represents NeoFS API V2-compatible netmap selector.
type Selector struct {
	amount uint32

	clause Clause

	name string

	attribute string

	filter string
}

// fromV2 reads Selector from netmap.Selector message.
func (x *Selector) fromV2(sv2 netmap.Selector) {
	x.amount = sv2.GetCount()
	x.clause.fromV2(sv2.GetClause())
	x.name = sv2.GetName()
	x.attribute = sv2.GetAttribute()
	x.filter = sv2.GetFilter()
}

// writeToV2 writes Selector to netmap.Selector message.
//
// Message must not be nil.
func (x Selector) writeToV2(sv2 *netmap.Selector) {
	{ // clause
		var cv2 netmap.Clause

		x.clause.writeToV2(&cv2)

		sv2.SetClause(cv2)
	}

	sv2.SetName(x.name)
	sv2.SetAttribute(x.attribute)
	sv2.SetFilter(x.filter)
	sv2.SetCount(x.amount)
}

// Amount returns number of nodes to select from bucket.
func (x Selector) Amount() uint32 {
	return x.amount
}

// SetAmount sets number of nodes to select from bucket.
func (x *Selector) SetAmount(amount uint32) {
	x.amount = amount
}

// Name returns selector name.
func (x Selector) Name() string {
	return x.name
}

// SetName sets selector name.
func (x *Selector) SetName(name string) {
	x.name = name
}

// Clause returns modifier showing how to form a bucket.
func (x Selector) Clause() Clause {
	return x.clause
}

// SetClause sets modifier showing how to form a bucket.
func (x *Selector) SetClause(clause Clause) {
	x.clause = clause
}

// Attribute returns attribute bucket to select from.
func (x Selector) Attribute() string {
	return x.attribute
}

// SetAttribute sets attribute bucket to select from.
func (x *Selector) SetAttribute(attribute string) {
	x.attribute = attribute
}

// Filter returns filter reference to select from.
func (x Selector) Filter() string {
	return x.filter
}

// SetFilter sets filter reference to select from.
func (x *Selector) SetFilter(filter string) {
	x.filter = filter
}

// Selectors represents set of Selector's.
type Selectors struct {
	elems []Selector
}

// Len returns number of elements in the Selectors.
func (x Selectors) Len() int {
	return len(x.elems)
}

// SetLen sets number of elements in the Selectors.
// Does not modify already existing elements.
func (x *Selectors) SetLen(num int) {
	if cap(x.elems) < num {
		x.elems = make([]Selector, 0, num)
	}

	x.elems = x.elems[:num]
}

// Iterate is a read-only iterator over all elements of the Selectors. Passes each element to the handler.
// Breaks iterating on true handler's return.
func (x Selectors) Iterate(f func(Selector) bool) {
	for i := range x.elems {
		if f(x.elems[i]) {
			return
		}
	}
}

// IterateP is a read-write iterator over all elements of the Selectors. Passes pointer to each element to the handler.
// Breaks iterating on true handler's return.
func (x Selectors) IterateP(f func(*Selector) bool) {
	for i := range x.elems {
		if f(&x.elems[i]) {
			return
		}
	}
}

// AppendSelectors appends elements to the Selectors.
//
// Selectors must not be nil.
func AppendSelectors(ss *Selectors, elems ...Selector) {
	lnNew := len(elems)
	if lnNew == 0 {
		return
	}

	lnPrev := ss.Len()

	ss.SetLen(lnPrev + lnNew)

	var indFull, indPos int

	ss.IterateP(func(s *Selector) bool {
		if indFull < lnPrev {
			indFull++
			return false
		}

		*s = elems[indPos]

		indPos++

		return false
	})
}

// selectorsFromV2 restores Selectors from netmap.Selector slice.
//
// All slice elements must not be nil.
func selectorsFromV2(ss *Selectors, ssv2 []*netmap.Selector) {
	ln := len(ssv2)
	ss.SetLen(ln)

	ind := 0

	ss.IterateP(func(s *Selector) bool {
		s.fromV2(*ssv2[ind])

		ind++

		return false
	})
}

// selectorsToV2 writes Selectors to netmap.Selector slice.
//
// Slice length must be at least Len(). Items can be nil.
func selectorsToV2(ssv2 []*netmap.Selector, ss Selectors) {
	ind := 0

	ss.Iterate(func(s Selector) bool {
		if ssv2[ind] == nil {
			ssv2[ind] = new(netmap.Selector)
		}

		s.writeToV2(ssv2[ind])

		ind++

		return false
	})
}
