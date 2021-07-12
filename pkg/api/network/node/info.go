package neofsnode

import (
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

// Info represents NeoFS API V2-compatible information about NeoFS node.
type Info struct {
	state State

	key []byte

	addresses []string

	attributes Attributes
}

// FromV2 reads Info from netmap.NodeInfo message.
func (x *Info) FromV2(iv2 netmap.NodeInfo) {
	{ // addresses
		ln := iv2.NumberOfAddresses()

		if cap(x.addresses) < ln {
			x.addresses = make([]string, 0, ln)
		}

		x.addresses = x.addresses[:0]

		iv2.IterateAddresses(func(a string) bool {
			x.addresses = append(x.addresses, a)
			return false
		})
	}

	x.state.fromV2(iv2.GetState())
	attributesFromV2(&x.attributes, iv2.GetAttributes())
	x.key = iv2.GetPublicKey()
}

// WriteToV2 writes Info to netmap.NodeInfo message.
//
// Message must not be nil.
func (x Info) WriteToV2(iv2 *netmap.NodeInfo) {
	{ // state
		var sv2 netmap.NodeState

		x.state.writeToV2(&sv2)

		iv2.SetState(sv2)
	}

	{ // attributes
		var asv2 []*netmap.Attribute

		if ln := x.attributes.Len(); ln > 0 {
			asv2 = iv2.GetAttributes()

			if cap(asv2) < ln {
				asv2 = make([]*netmap.Attribute, 0, ln)
				iv2.SetAttributes(asv2)
			}

			asv2 = asv2[:ln]
		}

		iv2.SetAttributes(asv2)
	}

	iv2.SetPublicKey(x.key)
	iv2.SetAddresses(x.addresses...)
}

// State returns node state.
func (x Info) State() State {
	return x.state
}

// SetState sets node state.
func (x *Info) SetState(state State) {
	x.state = state
}

// PublicKey returns public key of the node in a binary format.
func (x Info) PublicKey() []byte {
	return x.key
}

// SetPublicKey sets public key of the node in a binary format.
func (x Info) SetPublicKey(key []byte) {
	x.key = key
}

// Attributes returns node attributes.
//
// Result must not be mutated.
func (x Info) Attributes() Attributes {
	return x.attributes
}

// SetAttributes sets node attributes.
//
// Result must not be mutated.
func (x *Info) SetAttributes(attributes Attributes) {
	x.attributes = attributes
}

// LenAddresses returns number of network addresses of the node.
func (x Info) LenAddresses() int {
	return len(x.addresses)
}

// IterateAddresses iterates over network addresses of the node.
// Breaks iterating on f's true return.
//
// Handler should not be nil.
func (x Info) IterateAddresses(f func(string) bool) {
	for i := range x.addresses {
		if f(x.addresses[i]) {
			break
		}
	}
}

// SetAddresses sets set of the network addresses of the node.
func (x *Info) SetAddresses(as ...string) {
	x.addresses = as
}
