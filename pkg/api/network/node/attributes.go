package neofsnode

import (
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

// Attributes represents set of Attribute's.
type Attributes struct {
	elems []Attribute
}

// Len returns number of elements in the Attributes.
func (x Attributes) Len() int {
	return len(x.elems)
}

// SetLen sets number of elements in the Attributes.
// Does not modify already existing elements.
func (x *Attributes) SetLen(num int) {
	if len(x.elems) < num {
		x.elems = append(x.elems, make([]Attribute, num)...)
	}

	x.elems = x.elems[:num]
}

// Iterate is a read-only iterator over all elements of the Attributes. Passes each element to the handler.
// Breaks iterating on true handler's return.
func (x Attributes) Iterate(f func(Attribute) bool) {
	for i := range x.elems {
		if f(x.elems[i]) {
			return
		}
	}
}

// Iterate is a read-write iterator over all elements of the Attributes. Passes pointer to each element to the handler.
// Breaks iterating on true handler's return.
func (x Attributes) IterateP(f func(*Attribute) bool) {
	for i := range x.elems {
		if f(&x.elems[i]) {
			return
		}
	}
}

// AppendAttributes appends elements to the Attributes.
//
// Attributes must not be nil.
func AppendAttributes(as *Attributes, elems ...Attribute) {
	lnNew := len(elems)
	if lnNew == 0 {
		return
	}

	lnPrev := as.Len()

	as.SetLen(lnPrev + lnNew)

	var indFull, indPos int

	as.IterateP(func(a *Attribute) bool {
		if indFull < lnPrev {
			indFull++
			return false
		}

		*a = elems[indPos]

		indPos++

		return false
	})
}

// attributesFromV2 restores Attributes from netmap.Attribute slice.
//
// All slice elements must not be nil.
func attributesFromV2(as *Attributes, asv2 []*netmap.Attribute) {
	ln := len(asv2)
	as.SetLen(ln)

	ind := 0

	as.IterateP(func(a *Attribute) bool {
		attributeFromV2(a, *asv2[ind])

		ind++

		return false
	})
}

// attributesWriteToV2 writes Attributes to netmap.Attribute slice.
//
// Slice length must be at least Len(). Items can be nil.
func attributesToV2(asv2 []*netmap.Attribute, as Attributes) {
	ind := 0

	as.Iterate(func(a Attribute) bool {
		if asv2[ind] == nil {
			asv2[ind] = new(netmap.Attribute)
		}

		attributeToV2(asv2[ind], a)

		ind++

		return false
	})
}
