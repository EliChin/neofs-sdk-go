package container

import (
	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	cid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/container/id"
	neofsnetwork "github.com/nspcc-dev/neofs-sdk-go/pkg/api/network"
)

// UsedSpace represents estimation of container size.
type UsedSpace struct {
	epoch neofsnetwork.Epoch

	value uint64

	withID bool
	id     cid.ID
}

// FromV2 restores UsedSpace from container.UsedSpaceAnnouncement message.
func (x *UsedSpace) FromV2(uv2 container.UsedSpaceAnnouncement) {
	{ // id
		idv2 := uv2.GetContainerID()

		x.withID = idv2 != nil
		if x.withID {
			x.id.FromV2(*idv2)
		}
	}

	x.epoch.FromUint64(uv2.GetEpoch())
	x.value = uv2.GetUsedSpace()
}

// WriteToV2 writes UsedSpace to container.UsedSpaceAnnouncement message.
//
// Message must not be nil.
func (x UsedSpace) WriteToV2(uv2 *container.UsedSpaceAnnouncement) {
	{ // id
		var idv2 *refs.ContainerID

		if x.withID {
			idv2 = uv2.GetContainerID()
			if idv2 == nil {
				idv2 = new(refs.ContainerID)
			}

			cid.IDToV2(idv2, x.id)
		}

		uv2.SetContainerID(idv2)
	}

	{ // epoch
		var u64 uint64

		x.epoch.WriteToUint64(&u64)

		uv2.SetEpoch(u64)
	}

	uv2.SetUsedSpace(x.value)
}

// WithID checks if ID was specified.
func (x UsedSpace) WithID() bool {
	return x.withID
}

// ID returns identifier of the container under estimate.
//
// Makes sense only if WithID returns true.
func (x UsedSpace) ID() cid.ID {
	return x.id
}

// SetID sets identifier of the container under estimate.
func (x *UsedSpace) SetID(id cid.ID) {
	x.id = id
	x.withID = true
}

// Value returns used space value.
func (x UsedSpace) Value() uint64 {
	return x.value
}

// SetValue sets used space value.
func (x *UsedSpace) SetValue(value uint64) {
	x.value = value
}

// Epoch returns number of the epoch when was the estimate.
func (x UsedSpace) Epoch() neofsnetwork.Epoch {
	return x.epoch
}

// SetEpoch sets number of the epoch when was the estimate.
func (x *UsedSpace) SetEpoch(epoch neofsnetwork.Epoch) {
	x.epoch = epoch
}

// UsedSpaceMarshalProto marshals UsedSpace into a protobuf binary form.
func UsedSpaceMarshalProto(us UsedSpace) ([]byte, error) {
	var uv2 container.UsedSpaceAnnouncement

	us.WriteToV2(&uv2)

	return uv2.StableMarshal(nil)
}

// UsedSpaceUnmarshalProto unmarshals protobuf binary representation of UsedSpace.
func UsedSpaceUnmarshalProto(us *UsedSpace, data []byte) error {
	var uv2 container.UsedSpaceAnnouncement

	err := uv2.Unmarshal(data)
	if err == nil {
		us.FromV2(uv2)
	}

	return err
}
