package container

import (
	"crypto/sha256"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-api-go/v2/container"
	v2netmap "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	cid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/owner"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/refs"
)

// Container represents NeoFS API V2-compatible container.
type Container struct {
	basicACL uint32

	withVersion bool
	version     refs.Version

	withOwner bool
	owner     owner.ID

	withPolicy bool
	policy     netmap.PlacementPolicy

	attributes Attributes

	nonce []byte
}

// FromV2 restores Container from container.Container message.
func (x *Container) FromV2(cv2 container.Container) {
	{ // version
		vv2 := cv2.GetVersion()

		x.withVersion = vv2 != nil
		if x.withVersion {
			x.version.FromV2(*vv2)
		}
	}

	{ // owner
		idv2 := cv2.GetOwnerID()

		x.withOwner = idv2 != nil
		if x.withOwner {
			x.owner.FromV2(*idv2)
		}
	}

	{ // policy
		pv2 := cv2.GetPlacementPolicy()

		x.withPolicy = pv2 != nil
		if x.withPolicy {
			x.policy.FromV2(*pv2)
		}
	}

	x.nonce = cv2.GetNonce()
	x.basicACL = cv2.GetBasicACL()
	AttributesFromV2(&x.attributes, cv2.GetAttributes())
}

// WriteToV2 writes Container to container.Container message.
//
// Message must not be nil.
func (x Container) WriteToV2(cv2 *container.Container) {
	{ // version
		var vv2 *v2refs.Version

		if x.withVersion {
			vv2 = cv2.GetVersion()
			if vv2 == nil {
				vv2 = new(v2refs.Version)
			}

			x.version.WriteToV2(vv2)
		}

		cv2.SetVersion(vv2)
	}

	{ // owner
		var idv2 *v2refs.OwnerID

		if x.withOwner {
			idv2 = cv2.GetOwnerID()
			if idv2 == nil {
				idv2 = new(v2refs.OwnerID)
			}

			owner.IDToV2(idv2, x.owner)
		}

		cv2.SetOwnerID(idv2)
	}

	{ // policy
		var pv2 *v2netmap.PlacementPolicy

		if x.withPolicy {
			pv2 = cv2.GetPlacementPolicy()
			if pv2 == nil {
				pv2 = new(v2netmap.PlacementPolicy)
			}

			x.policy.WriteToV2(pv2)
		}

		cv2.SetPlacementPolicy(pv2)
	}

	{ // attributes
		var asv2 []*container.Attribute

		if ln := x.attributes.Len(); ln > 0 {
			asv2 = cv2.GetAttributes()
			if cap(asv2) < ln {
				asv2 = make([]*container.Attribute, 0, ln)
			}

			asv2 = asv2[:ln]

			AttributesToV2(asv2, x.attributes)
		}

		cv2.SetAttributes(asv2)
	}

	cv2.SetBasicACL(x.basicACL)
	cv2.SetNonce(x.nonce)
}

// Attributes returns Container attributes.
//
// Result mutation affects the Container.
func (x Container) Attributes() Attributes {
	return x.attributes
}

// SetAttributes sets Container attributes.
//
// Parameter mutation affects the Container.
func (x *Container) SetAttributes(attributes Attributes) {
	x.attributes = attributes
}

// WithPolicy checks if Container policy was specified.
func (x Container) WithPolicy() bool {
	return x.withPolicy
}

// Policy returns Container placement policy.
//
// Makes sense only if WithPolicy returns true.
func (x Container) Policy() netmap.PlacementPolicy {
	return x.policy
}

// SetPolicy sets Container placement policy.
func (x *Container) SetPolicy(policy netmap.PlacementPolicy) {
	x.policy = policy
	x.withPolicy = true
}

// BasicACL returns Container basic ACL bits.
func (x Container) BasicACL() uint32 {
	return x.basicACL
}

// SetBasicACL sets Container basic ACL bits.
func (x *Container) SetBasicACL(basicACL uint32) {
	x.basicACL = basicACL
}

// Nonce returns Container nonce.
//
// Result mutation affects the Container.
func (x Container) Nonce() []byte {
	return x.nonce
}

// SetNonce sets Container nonce in uuid.UUID format.
func (x *Container) SetNonce(nonce uuid.UUID) {
	data, err := nonce.MarshalBinary()
	if err != nil {
		panic(err) // no method to get slice, [:] isn't compatible
	}

	x.nonce = data
}

// WithOwner checks if Container owner was specified.
func (x Container) WithOwner() bool {
	return x.withOwner
}

// Owner returns Container owner identifier.
//
// Makes sense only if WithOwner returns true.
//
// Result mutation affects the Container.
func (x Container) Owner() owner.ID {
	return x.owner
}

// SetOwner sets Container owner identifier.
//
// Parameter mutation affects the Container.
func (x *Container) SetOwner(id owner.ID) {
	x.owner = id
	x.withOwner = true
}

// WithVersion checks if Container protocol version was specified.
func (x Container) WithVersion() bool {
	return x.withVersion
}

// Version returns version of the protocol within which container is created.
//
// Makes sense only if WithVersion returns true.
func (x Container) Version() refs.Version {
	return x.version
}

// SetVersion sets version of the protocol within which container is created.
func (x *Container) SetVersion(version refs.Version) {
	x.version = version
	x.withVersion = true
}

// ContainerMarshalProto marshals Container into a protobuf binary form.
func ContainerMarshalProto(c Container) ([]byte, error) {
	var cv2 container.Container

	c.WriteToV2(&cv2)

	return cv2.StableMarshal(nil)
}

// ContainerUnmarshalProto unmarshals protobuf binary representation of Container.
func ContainerUnmarshalProto(c *Container, data []byte) error {
	var cv2 container.Container

	err := cv2.Unmarshal(data)
	if err == nil {
		c.FromV2(cv2)
	}

	return err
}

// SetID calculates container identifier/ based on its structure and writes it to id.
func SetID(c Container, id *cid.ID) error {
	data, err := ContainerMarshalProto(c)
	if err != nil {
		return err
	}

	id.SetBytes(sha256.Sum256(data))

	return nil
}
