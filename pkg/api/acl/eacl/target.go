package eacl

import (
	v2acl "github.com/nspcc-dev/neofs-api-go/v2/acl"
)

// Target represents NeoFS API V2-compatible group of request senders to match eACL.
type Target struct {
	role Role

	keys [][]byte
}

// Keys returns list of public keys to identify target subject in a binary format.
//
// Result mutation affects the Target.
func (x Target) Keys() [][]byte {
	return x.keys
}

// SetKeys sets list of binary public keys to identify target subject.
//
// Parameter mutation affects the Target.
func (x *Target) SetKeys(keys [][]byte) {
	x.keys = keys
}

// Role returns target subject's role class.
func (x Target) Role() Role {
	return x.role
}

// SetRole sets target subject's role class.
func (x *Target) SetRole(r Role) {
	x.role = r
}

// FromV2 reads Target from acl.Target message.
func (x *Target) FromV2(tv2 v2acl.Target) {
	x.role.fromV2(tv2.GetRole())
	x.keys = tv2.GetKeys()
}

// WriteToV2 writes Target to v2acl.Target message.
//
// Message must not be nil.
func (x Target) WriteToV2(cv2 *v2acl.Target) {
	{ // role
		var rv2 v2acl.Role

		x.role.writeToV2(&rv2)

		cv2.SetRole(rv2)
	}

	cv2.SetKeys(x.keys)
}
