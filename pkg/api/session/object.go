package session

import (
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	oid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/object/id"
)

// ObjectContext represents NeoFS API V2-compatible context of the object session.
type ObjectContext struct {
	verb session.ObjectSessionVerb

	withAddress bool
	address     oid.Address
}

// FromV2 reads ObjectContext from session.ObjectSessionContext message.
func (x *ObjectContext) FromV2(ov2 session.ObjectSessionContext) {
	{ // address
		av2 := ov2.GetAddress()

		x.withAddress = av2 != nil

		if x.withAddress {
			x.address.FromV2(*av2)
		}
	}

	x.verb = ov2.GetVerb()
}

// WriteToV2 writes ObjectContext to session.ObjectSessionContext message.
//
// Message must not be nil.
func (x ObjectContext) WriteToV2(ov2 *session.ObjectSessionContext) {
	{ // address
		var av2 *refs.Address

		if x.withAddress {
			av2 = ov2.GetAddress()
			if av2 == nil {
				av2 = new(refs.Address)
			}

			x.address.WriteToV2(av2)
		}

		ov2.SetAddress(av2)
	}

	ov2.SetVerb(x.verb)
}

// WithAddress checks if object address was specified.
func (x ObjectContext) WithAddress() bool {
	return x.withAddress
}

// Object returns identifier of the object to which the ContainerContext applies.
//
// Makes sense only if WithAddress returns true.
func (x ObjectContext) Object() oid.Address {
	return x.address
}

// SetObject specifies which object the ObjectContext applies to.
func (x *ObjectContext) SetObject(a oid.Address) {
	x.address = a
	x.withAddress = true
}

// ForPut binds the ObjectContext to PUT operation.
func (x *ObjectContext) ForPut() {
	x.verb = session.ObjectVerbPut
}

// IsForPut checks if ObjectContext is bound to PUT operation.
func (x ObjectContext) IsForPut() bool {
	return x.verb == session.ObjectVerbPut
}

// ForGet binds the ObjectContext to GET operation.
func (x *ObjectContext) ForGet() {
	x.verb = session.ObjectVerbGet
}

// IsForGet checks if ObjectContext is bound to GET operation.
func (x ObjectContext) IsForGet() bool {
	return x.verb == session.ObjectVerbGet
}

// ForDelete binds the ObjectContext to DELETE operation.
func (x *ObjectContext) ForDelete() {
	x.verb = session.ObjectVerbDelete
}

// IsForDelete checks if ObjectContext is bound to DELETE operation.
func (x ObjectContext) IsForDelete() bool {
	return x.verb == session.ObjectVerbDelete
}

// ForHead binds the ObjectContext to HEAD operation.
func (x *ObjectContext) ForHead() {
	x.verb = session.ObjectVerbHead
}

// IsForHead checks if ObjectContext is bound to HEAD operation.
func (x ObjectContext) IsForHead() bool {
	return x.verb == session.ObjectVerbHead
}

// ForSearch binds the ObjectContext to SEARCH operation.
func (x *ObjectContext) ForSearch() {
	x.verb = session.ObjectVerbSearch
}

// IsForSearch checks if ObjectContext is bound to SEARCH operation.
func (x ObjectContext) IsForSearch() bool {
	return x.verb == session.ObjectVerbSearch
}

// ForRange binds the ObjectContext to GETRANGE operation.
func (x *ObjectContext) ForRange() {
	x.verb = session.ObjectVerbRange
}

// IsForRange checks if ObjectContext is bound to GETRANGE operation.
func (x ObjectContext) IsForRange() bool {
	return x.verb == session.ObjectVerbRange
}

// ForRangeHash binds the ObjectContext to GETRANGEHASH operation.
func (x *ObjectContext) ForRangeHash() {
	x.verb = session.ObjectVerbRangeHash
}

// IsForRangeHash checks if ObjectContext is bound to GETRANGEHASH operation.
func (x ObjectContext) IsForRangeHash() bool {
	return x.verb == session.ObjectVerbRangeHash
}
