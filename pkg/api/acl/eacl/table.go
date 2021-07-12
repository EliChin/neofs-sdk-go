package eacl

import (
	v2acl "github.com/nspcc-dev/neofs-api-go/v2/acl"
	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	cid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/refs"
)

// Table represents NeoFS API V2-compatible group of EACL records for single container.
type Table struct {
	withVersion bool
	version     refs.Version

	withContainer bool
	container     cid.ID

	records Records
}

// FromV2 restores Table from acl.Table message.
func (x *Table) FromV2(tv2 v2acl.Table) {
	{ // version
		vv2 := tv2.GetVersion()

		x.withVersion = vv2 != nil
		if x.withVersion {
			x.version.FromV2(*vv2)
		}
	}

	{ // owner
		idv2 := tv2.GetContainerID()

		x.withContainer = idv2 != nil
		if x.withContainer {
			x.container.FromV2(*idv2)
		}
	}

	recordsFromV2(&x.records, tv2.GetRecords())
}

// WriteToV2 writes Table to acl.Table message.
//
// Message must not be nil.
func (x Table) WriteToV2(tv2 *v2acl.Table) {
	{ // version
		var vv2 *v2refs.Version

		if x.withVersion {
			vv2 = tv2.GetVersion()
			if vv2 == nil {
				vv2 = new(v2refs.Version)
			}

			x.version.WriteToV2(vv2)
		}

		tv2.SetVersion(vv2)
	}

	{ // container
		var idv2 *v2refs.ContainerID

		if x.withContainer {
			idv2 = tv2.GetContainerID()
			if idv2 == nil {
				idv2 = new(v2refs.ContainerID)
			}

			cid.IDToV2(idv2, x.container)
		}

		tv2.SetContainerID(idv2)
	}

	{ // records
		var rsv2 []*v2acl.Record

		if ln := x.records.Len(); ln > 0 {
			rsv2 = tv2.GetRecords()
			if cap(rsv2) < ln {
				rsv2 = make([]*v2acl.Record, 0, ln)
			}

			rsv2 = rsv2[:ln]

			recordsToV2(rsv2, x.records)
		}

		tv2.SetRecords(rsv2)
	}
}

// WithContainer checks if Table container was specified.
func (x Table) WithContainer() bool {
	return x.withContainer
}

// Container returns identifier of the container that should use given access control rules.
//
// Makes sense only if WithContainer returns true.
//
// Result mutation affects the Table.
func (x Table) Container() cid.ID {
	return x.container
}

// SetContainer sets identifier of the container that should use given access control rules.
//
// Parameter mutation affects the Table.
func (x *Table) SetContainer(container cid.ID) {
	x.container = container
	x.withContainer = true
}

// WithVersion checks if Table protocol version was specified.
func (x Table) WithVersion() bool {
	return x.withVersion
}

// Version returns version of eACL format.
//
// Makes sense only if WithVersion returns true.
func (x Table) Version() refs.Version {
	return x.version
}

// SetVersion sets version of eACL format.
func (x *Table) SetVersion(version refs.Version) {
	x.version = version
	x.withVersion = true
}

// Records returns list of extended ACL rules.
//
// Result mutation affects the Table.
func (x Table) Records() Records {
	return x.records
}

// SetRecords sets list of extended ACL rules.
//
// Parameter mutation affects the Table.
func (x Table) SetRecords(records Records) {
	x.records = records
}

// Marshal marshals Table into a protobuf binary form.
func TableMarshalProto(t Table) ([]byte, error) {
	var cv2 v2acl.Table

	t.WriteToV2(&cv2)

	return cv2.StableMarshal(nil)
}

// TableUnmarshalProto unmarshals protobuf binary representation of Table.
func TableUnmarshalProto(t *Table, data []byte) error {
	var tv2 v2acl.Table

	err := tv2.Unmarshal(data)
	if err == nil {
		t.FromV2(tv2)
	}

	return err
}
