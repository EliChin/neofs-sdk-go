package session

import (
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	cid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/container/id"
)

// ContainerContext represents NeoFS API V2-compatible context of the container session.
type ContainerContext struct {
	noWildcard bool

	verb session.ContainerSessionVerb

	withID bool
	id     cid.ID
}

// FromV2 reads ContainerContext from session.ContainerSessionContext message.
func (x *ContainerContext) FromV2(cv2 session.ContainerSessionContext) {
	x.noWildcard = !cv2.Wildcard()

	{ // id
		idv2 := cv2.ContainerID()

		x.withID = idv2 != nil

		if x.withID {
			x.id.FromV2(*idv2)
		}
	}

	x.verb = cv2.Verb()
}

// WriteToV2 writes ContainerContext to session.ContainerSessionContext message.
//
// Message must not be nil.
func (x ContainerContext) WriteToV2(cv2 *session.ContainerSessionContext) {
	{ // id
		var idv2 *refs.ContainerID

		if x.withID {
			idv2 = cv2.ContainerID()
			if idv2 == nil {
				idv2 = new(refs.ContainerID)
			}

			cid.IDToV2(idv2, x.id)
		}

		cv2.SetContainerID(idv2)
	}

	cv2.SetWildcard(!x.noWildcard)
	cv2.SetVerb(x.verb)
}

// ApplyToAll applies ContainerContext to all containers.
func (x *ContainerContext) ApplyToAll() {
	x.noWildcard = false
	x.withID = false
}

// AppliedToAll checks if ContainerContext is applied to all containers.
func (x ContainerContext) AppliedToAll() bool {
	return !x.noWildcard
}

// ApplyTo specifies which container the ContainerContext applies to.
func (x *ContainerContext) ApplyTo(id cid.ID) {
	x.noWildcard = true
	x.withID = true
	x.id = id
}

// WithContainer checks if container was specified.
func (x ContainerContext) WithContainer() bool {
	return x.withID
}

// Container returns identifier of the container to which the ContainerContext applies.
//
// Makes sense only if AppliedToAll returns false and WithContainer returns true.
func (x ContainerContext) Container() cid.ID {
	return x.id
}

// ForPut binds the ContainerContext to PUT operation.
func (x *ContainerContext) ForPut() {
	x.verb = session.ContainerVerbPut
}

// IsForPut checks if ContainerContext is bound to PUT operation.
func (x ContainerContext) IsForPut() bool {
	return x.verb == session.ContainerVerbPut
}

// ForDelete binds the ContainerContext to DELETE operation.
func (x *ContainerContext) ForDelete() {
	x.verb = session.ContainerVerbDelete
}

// IsForDelete checks if ContainerContext is bound to DELETE operation.
func (x ContainerContext) IsForDelete() bool {
	return x.verb == session.ContainerVerbDelete
}

// ForSetEACL binds the ContainerContext to SETEACL operation.
func (x *ContainerContext) ForSetEACL() {
	x.verb = session.ContainerVerbSetEACL
}

// IsForSetEACL checks if ContainerContext is bound to SETEACL operation.
func (x ContainerContext) IsForSetEACL() bool {
	return x.verb == session.ContainerVerbSetEACL
}

// ContainerContextMarshalProtoJSON encodes ContainerContext to protobuf JSON format.
func ContainerContextMarshalProtoJSON(c ContainerContext) ([]byte, error) {
	var cv2 session.ContainerSessionContext

	c.WriteToV2(&cv2)

	return cv2.MarshalJSON()
}

// ContainerContextUnmarshalProtoJSON decodes ContainerContext from protobuf JSON format.
func ContainerContextUnmarshalProtoJSON(c *ContainerContext, data []byte) error {
	var cv2 session.ContainerSessionContext

	if err := cv2.UnmarshalJSON(data); err != nil {
		return err
	}

	c.FromV2(cv2)

	return nil
}
