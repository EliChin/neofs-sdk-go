package netmap

import (
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

// Replica represents NeoFS API V2-compatible descriptor of object replicas.
type Replica struct {
	amount uint32

	selector string
}

// Amount returns number of object replicas.
func (x Replica) Amount() uint32 {
	return x.amount
}

// SetAmount sets number of object replicas.
func (x *Replica) SetAmount(amount uint32) {
	x.amount = amount
}

// Selector returns name of selector bucket to put replicas.
func (x Replica) Selector() string {
	return x.selector
}

// SetSelector sets name of selector bucket to put replicas.
func (x *Replica) SetSelector(selector string) {
	x.selector = selector
}

// replicaFromV2 restores Replica from netmap.Replica message.
func replicaFromV2(r *Replica, rv2 netmap.Replica) {
	r.SetAmount(rv2.GetCount())
	r.SetSelector(rv2.GetSelector())
}

// replicaToV2 writes Replica to netmap.Replica message.
//
// Message must not be nil.
func replicaToV2(rv2 *netmap.Replica, r Replica) {
	rv2.SetCount(r.Amount())
	rv2.SetSelector(r.Selector())
}

// Replicas represents set of Replica's.
type Replicas struct {
	elems []Replica
}

// Len returns number of elements in the Replicas.
func (x Replicas) Len() int {
	return len(x.elems)
}

// SetLen sets number of elements in the Replicas.
// Does not modify already existing elements.
func (x *Replicas) SetLen(num int) {
	if cap(x.elems) < num {
		x.elems = make([]Replica, 0, num)
	}

	x.elems = x.elems[:num]
}

// Iterate is a read-only iterator over all elements of the Replicas. Passes each element to the handler.
// Breaks iterating on true handler's return.
func (x Replicas) Iterate(f func(Replica) bool) {
	for i := range x.elems {
		if f(x.elems[i]) {
			return
		}
	}
}

// IterateP is a read-write iterator over all elements of the Replicas. Passes pointer to each element to the handler.
// Breaks iterating on true handler's return.
func (x Replicas) IterateP(f func(*Replica) bool) {
	for i := range x.elems {
		if f(&x.elems[i]) {
			return
		}
	}
}

// AppendReplicas appends elements to the Replicas.
//
// Replicas must not be nil.
func AppendReplicas(rs *Replicas, elems ...Replica) {
	lnNew := len(elems)
	if lnNew == 0 {
		return
	}

	lnPrev := rs.Len()

	rs.SetLen(lnPrev + lnNew)

	var indFull, indPos int

	rs.IterateP(func(r *Replica) bool {
		if indFull < lnPrev {
			indFull++
			return false
		}

		*r = elems[indPos]

		indPos++

		return false
	})
}

// replicasFromV2 restores Replicas from netmap.Replica slice.
//
// All slice elements must not be nil.
func replicasFromV2(rs *Replicas, rsv2 []*netmap.Replica) {
	ln := len(rsv2)
	rs.SetLen(ln)

	ind := 0

	rs.IterateP(func(r *Replica) bool {
		replicaFromV2(r, *rsv2[ind])

		ind++

		return false
	})
}

// replicasToV2 writes Replicas to netmap.Replica slice.
//
// Slice length must be at least Len(). Items can be nil.
func replicasToV2(rsv2 []*netmap.Replica, rs Replicas) {
	ind := 0

	rs.Iterate(func(r Replica) bool {
		if rsv2[ind] == nil {
			rsv2[ind] = new(netmap.Replica)
		}

		replicaToV2(rsv2[ind], r)

		ind++

		return false
	})
}
