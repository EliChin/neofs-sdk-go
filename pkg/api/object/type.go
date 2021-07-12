package object

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
)

// Type represents NeoFS API V2-compatible type of the object payload content.
type Type struct {
	t object.Type
}

// fromV2 reads Type from object.Type enum value.
func (x *Type) fromV2(tv2 object.Type) {
	x.t = tv2
}

// writeToV2 writes Type to object.Type enum value.
//
// Enum value must not be nil.
func (x Type) writeToV2(cv2 *object.Type) {
	*cv2 = x.t
}

// String implements fmt.Stringer.
//
// To get the canonical string MarshalText should be used.
func (x Type) String() string {
	switch x.t {
	default:
		return "UNDEFINED"
	case object.TypeRegular:
		return "REGULAR"
	case object.TypeTombstone:
		return "TOMBSTONE"
	case object.TypeStorageGroup:
		return "STORAGE_GROUP"
	}
}

// MarshalText implements encoding.TextMarshaler.
// Returns canonical Type string according to NeoFS API V2 spec.
//
// Returns an error if Type is not supported.
func (x Type) MarshalText() ([]byte, error) {
	switch x.t {
	default:
		return nil, fmt.Errorf("unsupported type: %d", x.t)
	case
		object.TypeRegular,
		object.TypeTombstone,
		object.TypeStorageGroup:
		return []byte(x.t.String()), nil
	}
}

// UnmarshalText implements encoding.TextUnmarshaler.
//
// Returns an error if d is not a canonical Type string according to NeoFS API V2 spec
// or if the value is not supported. In these cases Type remains untouched.
func (x *Type) UnmarshalText(txt []byte) error {
	if ok := x.t.FromString(string(txt)); !ok {
		return fmt.Errorf("unsupported type text: %s", txt)
	}

	return nil
}

// Regular checks if Type represents regular object.
func (x Type) Regular() bool {
	return x.t == object.TypeRegular
}

// SetRegular makes Type to represent regular object.
func (x *Type) SetRegular() {
	x.t = object.TypeRegular
}

// Tombstone checks if Type represents tombstone object.
func (x Type) Tombstone() bool {
	return x.t == object.TypeTombstone
}

// SetTombstone makes Type to represent tombstone object.
func (x *Type) SetTombstone() {
	x.t = object.TypeTombstone
}

// StorageGroup checks if Type represents storage group object.
func (x Type) StorageGroup() bool {
	return x.t == object.TypeStorageGroup
}

// SetStorageGroup makes Type to represent storage group object.
func (x *Type) SetStorageGroup() {
	x.t = object.TypeStorageGroup
}
