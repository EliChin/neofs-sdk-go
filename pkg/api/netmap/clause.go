package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

// Clause represents NeoFS API V2-compatible selector modifiers
// that show how the node set will be formed.
type Clause struct {
	c netmap.Clause
}

// fromV2 restores Clause from netmap.Clause enum value.
func (x *Clause) fromV2(cv2 netmap.Clause) {
	x.c = cv2
}

// writeToV2 writes Clause to netmap.Clause enum value.
//
// Enum value must not be nil.
func (x Clause) writeToV2(cv2 *netmap.Clause) {
	*cv2 = x.c
}

// String implements fmt.Stringer.
//
// To get the canonical string MarshalText should be used.
func (x Clause) String() string {
	switch x.c {
	default:
		return "UNDEFINED"
	case netmap.UnspecifiedClause:
		return "UNSPECIFIED"
	case netmap.Same:
		return "SAME"
	case netmap.Distinct:
		return "DISTINCT"
	}
}

// MarshalText implements encoding.TextMarshaler.
// Returns canonical Clause string according to NeoFS API V2 spec.
//
// Returns an error if Clause is not supported.
func (x Clause) MarshalText() ([]byte, error) {
	switch x.c {
	default:
		return nil, fmt.Errorf("unsupported clause: %d", x.c)
	case
		netmap.UnspecifiedClause,
		netmap.Same,
		netmap.Distinct:
		return []byte(x.c.String()), nil
	}
}

// UnmarshalText implements encoding.TextUnmarshaler.
//
// Returns an error if d is not a canonical Clause string according to NeoFS API V2 spec
// or if the value is not supported. In these cases Clause remains untouched.
func (x *Clause) UnmarshalText(txt []byte) error {
	if ok := x.c.FromString(string(txt)); !ok {
		return fmt.Errorf("unsupported clause text: %s", txt)
	}

	return nil
}

// Unspecified checks if State is set to select nodes from bucket randomly.
func (x Clause) Unspecified() bool {
	return x.c == netmap.UnspecifiedClause
}

// Same checks if Clause is set to select only nodes having the same value of bucket attribute.
func (x Clause) Same() bool {
	return x.c == netmap.Same
}

// SetSame sets Clause to select only nodes having the same value of bucket attribute.
func (x *Clause) SetSame() {
	x.c = netmap.Same
}

// Distinct checks if Clause is set to select nodes having different values of bucket attribute.
func (x Clause) Distinct() bool {
	return x.c == netmap.Distinct
}

// SetDistinct sets Clause to select nodes having different values of bucket attribute.
func (x *Clause) SetDistinct() {
	x.c = netmap.Distinct
}
