package storagegroup

import (
	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/storagegroup"
	neofsnetwork "github.com/nspcc-dev/neofs-sdk-go/pkg/api/network"
	oid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/refs"
)

// StorageGroup represents NeoFS API V2-compatible descriptor of object storage group.
type StorageGroup struct {
	size uint64

	exp neofsnetwork.Epoch

	withChecksum bool
	checksum     refs.Checksum

	members oid.IDs
}

// FromV2 restores StorageGroup from storagegroup.StorageGroup message.
func (x *StorageGroup) FromV2(sgv2 storagegroup.StorageGroup) {
	{ // checksum
		csv2 := sgv2.GetValidationHash()

		x.withChecksum = csv2 != nil
		if x.withChecksum {
			x.checksum.FromV2(*csv2)
		}
	}

	x.exp.FromUint64(sgv2.GetExpirationEpoch())
	oid.IDsFromV2(&x.members, sgv2.GetMembers())
	x.size = sgv2.GetValidationDataSize()
}

// WriteToV2 writes StorageGroup to storagegroup.StorageGroup message.
//
// Message must not be nil.
func (x StorageGroup) WriteToV2(sgv2 *storagegroup.StorageGroup) {
	{ // checksum
		var csv2 *v2refs.Checksum

		if x.withChecksum {
			csv2 = sgv2.GetValidationHash()
			if csv2 == nil {
				csv2 = new(v2refs.Checksum)
			}

			x.checksum.WriteToV2(csv2)
		}

		sgv2.SetValidationHash(csv2)
	}

	{ // members
		var idsv2 []*v2refs.ObjectID

		if ln := x.members.Len(); ln > 0 {
			idsv2 = sgv2.GetMembers()

			if cap(idsv2) < ln {
				idsv2 = make([]*v2refs.ObjectID, 0, ln)
			}

			idsv2 = idsv2[:ln]

			oid.IDsToV2(idsv2, x.members)
		}

		sgv2.SetMembers(idsv2)
	}

	{ // exp
		var u64 uint64

		x.exp.WriteToUint64(&u64)

		sgv2.SetExpirationEpoch(u64)
	}

	sgv2.SetValidationDataSize(x.size)
}

// Size returns total payload size of the all members.
func (x StorageGroup) Size() uint64 {
	return x.size
}

// SetSize sets total payload size of the all members.
func (x *StorageGroup) SetSize(sz uint64) {
	x.size = sz
}

// WithChecksum checks if checksum was specified.
func (x StorageGroup) WithChecksum() bool {
	return x.withChecksum
}

// Checksum returns checksum of payload concatenation of the all members.
//
// Makes sense only if WithChecksum returns true.
func (x StorageGroup) Checksum() refs.Checksum {
	return x.checksum
}

// SetChecksum sets checksum of payload concatenation of the all members.
func (x *StorageGroup) SetChecksum(checksum refs.Checksum) {
	x.checksum = checksum
	x.withChecksum = true
}

// Exp returns epoch number of the token expiration.
func (x StorageGroup) Exp() neofsnetwork.Epoch {
	return x.exp
}

// SetExp sets epoch number of the token expiration.
func (x *StorageGroup) SetExp(exp neofsnetwork.Epoch) {
	x.exp = exp
}

// Members returns strictly ordered list of storage group member objects.
//
// Result mutation affects the StorageGroup.
func (x StorageGroup) Members() oid.IDs {
	return x.members
}

// SetMembers sets strictly ordered list of storage group member objects.
//
// Parameter mutation affects the StorageGroup.
func (x *StorageGroup) SetMembers(members oid.IDs) {
	x.members = members
}

// MarshalProto marshals StorageGroup into a protobuf binary form.
func MarshalProto(sg StorageGroup) ([]byte, error) {
	var sgv storagegroup.StorageGroup

	sg.WriteToV2(&sgv)

	return sgv.StableMarshal(nil)
}

// UnmarshalProto unmarshals protobuf binary representation of StorageGroup.
func UnmarshalProto(sg *StorageGroup, data []byte) error {
	var sgv2 storagegroup.StorageGroup

	err := sgv2.Unmarshal(data)
	if err == nil {
		sg.FromV2(sgv2)
	}

	return err
}
