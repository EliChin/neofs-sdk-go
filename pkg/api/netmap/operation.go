package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

// Operation represents NeoFS API V2-compatible filtering operations.
type Operation struct {
	o netmap.Operation
}

// fromV2 restores Operation from netmap.Operation enum value.
func (x *Operation) fromV2(opv2 netmap.Operation) {
	x.o = opv2
}

// writeToV2 writes Operation to netmap.Operation enum value.
//
// Enum value must not be nil.
func (x Operation) writeToV2(opv2 *netmap.Operation) {
	*opv2 = x.o
}

// String implements fmt.Stringer.
//
// To get the canonical string MarshalText should be used.
func (x Operation) String() string {
	switch x.o {
	default:
		return "UNDEFINED"
	case netmap.UnspecifiedOperation:
		return "UNSPECIFIED"
	case netmap.OR:
		return "OR"
	case netmap.AND:
		return "AND"
	case netmap.GE:
		return "GE"
	case netmap.GT:
		return "GT"
	case netmap.LE:
		return "LE"
	case netmap.LT:
		return "LT"
	case netmap.EQ:
		return "EQ"
	case netmap.NE:
		return "NE"
	}
}

// MarshalText implements encoding.TextMarshaler.
// Returns canonical Operation string according to NeoFS API V2 spec.
//
// Returns an error if Operation is not supported.
func (x Operation) MarshalText() ([]byte, error) {
	switch x.o {
	default:
		return nil, fmt.Errorf("unsupported Operation: %d", x.o)
	case
		netmap.UnspecifiedOperation,
		netmap.OR,
		netmap.AND,
		netmap.GE,
		netmap.GT,
		netmap.LE,
		netmap.LT,
		netmap.EQ,
		netmap.NE:
		return []byte(x.o.String()), nil
	}
}

// UnmarshalText implements encoding.TextUnmarshaler.
//
// Returns an error if d is not a canonical Operation string according to NeoFS API V2 spec
// or if the value is not supported. In these cases Operation remains untouched.
func (x *Operation) UnmarshalText(txt []byte) error {
	if ok := x.o.FromString(string(txt)); !ok {
		return fmt.Errorf("unsupported operation text: %s", txt)
	}

	return nil
}

// Unspecified checks if Operation is not specified.
func (x Operation) Unspecified() bool {
	return x.o == netmap.UnspecifiedOperation
}

// EQ checks if Operation is set string "Equal".
func (x Operation) EQ() bool {
	return x.o == netmap.EQ
}

// SetEQ sets Operation to string "Equal".
func (x *Operation) SetEQ() {
	x.o = netmap.EQ
}

// NE checks if Operation is set to string "Not equal".
func (x Operation) NE() bool {
	return x.o == netmap.NE
}

// SetNE sets Operation to string "Not equal".
func (x *Operation) SetNE() {
	x.o = netmap.NE
}

// GT checks if Operation is set to numerical "Greater than".
func (x Operation) GT() bool {
	return x.o == netmap.GT
}

// SetGT sets Operation to numerical "Greater than".
func (x *Operation) SetGT() {
	x.o = netmap.GT
}

// GE checks if Operation is set to numerical "Greater than or equal to".
func (x Operation) GE() bool {
	return x.o == netmap.GE
}

// SetGE sets Operation to numerical "Greater than or equal to".
func (x *Operation) SetGE() {
	x.o = netmap.GE
}

// LT checks if Operation is set to numerical "Less than".
func (x Operation) LT() bool {
	return x.o == netmap.LT
}

// SetLT sets Operation to numerical "Less than".
func (x *Operation) SetLT() {
	x.o = netmap.LT
}

// LE checks if Operation is set to numerical "Less than or equal to".
func (x Operation) LE() bool {
	return x.o == netmap.LE
}

// SetLE sets Operation to numerical "Less than or equal to".
func (x *Operation) SetLE() {
	x.o = netmap.LE
}

// OR checks if Operation is set to logical "OR".
func (x Operation) OR() bool {
	return x.o == netmap.OR
}

// SetOR sets Operation to logical "OR".
func (x *Operation) SetOR() {
	x.o = netmap.OR
}

// AND checks if Operation is set to logical "AND".
func (x Operation) AND() bool {
	return x.o == netmap.AND
}

// SetAND sets Operation to logical "AND".
func (x *Operation) SetAND() {
	x.o = netmap.AND
}
