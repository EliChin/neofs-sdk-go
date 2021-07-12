package neofsnode

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

// State represents NeoFS API V2-compatible network state of the NeoFS node.
type State struct {
	s netmap.NodeState
}

// fromV2 reads State from netmap.NodeState enum value.
func (x *State) fromV2(sv2 netmap.NodeState) {
	x.s = sv2
}

// writeToV2 writes State to netmap.Clause enum value.
//
// Enum value must not be nil.
func (x State) writeToV2(sv2 *netmap.NodeState) {
	*sv2 = x.s
}

// String implements fmt.Stringer.
//
// To get the canonical string MarshalText should be used.
func (x State) String() string {
	switch x.s {
	default:
		return "UNDEFINED"
	case netmap.UnspecifiedState:
		return "UNSPECIFIED"
	case netmap.Online:
		return "ONLINE"
	case netmap.Offline:
		return "OFFLINE"
	}
}

// MarshalText implements encoding.TextMarshaler.
// Returns canonical State string according to NeoFS API V2 spec.
//
// Returns an error if State is not supported.
func (x State) MarshalText() ([]byte, error) {
	switch x.s {
	default:
		return nil, fmt.Errorf("unsupported state: %d", x)
	case
		netmap.UnspecifiedState,
		netmap.Online,
		netmap.Offline:
		return []byte(x.s.String()), nil
	}
}

// UnmarshalText implements encoding.TextUnmarshaler.
//
// Returns an error if d is not a canonical State string according to NeoFS API V2 spec
// or if the value is not supported. In these cases State remains untouched.
func (x *State) UnmarshalText(d []byte) error {
	var sv2 netmap.NodeState

	if ok := sv2.FromString(string(d)); !ok {
		return fmt.Errorf("unsupported state text: %s", d)
	}

	x.fromV2(sv2)

	return nil
}

// Unspecified checks if State is not specified.
func (x State) Unspecified() bool {
	return x.s == netmap.UnspecifiedState
}

// Online checks if State is set to online.
func (x State) Online() bool {
	return x.s == netmap.Online
}

// SetOnline sets State to online.
func (x *State) SetOnline() {
	x.s = netmap.Online
}

// Offline checks if State is set to offline.
func (x State) Offline() bool {
	return x.s == netmap.Offline
}

// SetOffline sets State to offline.
func (x *State) SetOffline() {
	x.s = netmap.Offline
}
