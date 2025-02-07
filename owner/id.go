package owner

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
)

// ID represents v2-compatible owner identifier.
type ID refs.OwnerID

var errInvalidIDString = errors.New("incorrect format of the string owner ID")

// NewIDFromV2 wraps v2 OwnerID message to ID.
//
// Nil refs.OwnerID converts to nil.
func NewIDFromV2(idV2 *refs.OwnerID) *ID {
	return (*ID)(idV2)
}

// NewID creates and initializes blank ID.
//
// Works similar as NewIDFromV2(new(OwnerID)).
//
// Defaults:
//  - value: nil.
func NewID() *ID {
	return NewIDFromV2(new(refs.OwnerID))
}

// SetNeo3Wallet sets owner identifier value to NEO3 wallet address.
func (id *ID) SetNeo3Wallet(v *NEO3Wallet) {
	(*refs.OwnerID)(id).SetValue(v.Bytes())
}

// ToV2 returns the v2 owner ID message.
//
// Nil ID converts to nil.
func (id *ID) ToV2() *refs.OwnerID {
	return (*refs.OwnerID)(id)
}

// String implements fmt.Stringer.
func (id *ID) String() string {
	return base58.Encode((*refs.OwnerID)(id).GetValue())
}

// Equal defines a comparison relation on ID's.
//
// ID's are equal if they have the same binary representation.
func (id *ID) Equal(id2 *ID) bool {
	return bytes.Equal(
		(*refs.ObjectID)(id).GetValue(),
		(*refs.ObjectID)(id2).GetValue(),
	)
}

// NewIDFromNeo3Wallet creates new owner identity from 25-byte neo wallet.
func NewIDFromNeo3Wallet(v *NEO3Wallet) *ID {
	id := NewID()
	id.SetNeo3Wallet(v)

	return id
}

// Parse converts base58 string representation into ID.
func (id *ID) Parse(s string) error {
	data, err := base58.Decode(s)
	if err != nil {
		return fmt.Errorf("could not parse owner.ID from string: %w", err)
	} else if !valid(data) {
		return errInvalidIDString
	}

	(*refs.OwnerID)(id).SetValue(data)

	return nil
}

// Valid returns true if id is a valid owner id.
// The rules for v2 are the following:
// 1. Must be 25 bytes in length.
// 2. Must have N3 address prefix-byte.
// 3. Last 4 bytes must contain the address checksum.
func (id *ID) Valid() bool {
	rawID := id.ToV2().GetValue()
	return valid(rawID)
}

func valid(rawID []byte) bool {
	if len(rawID) != NEO3WalletSize {
		return false
	}
	if rawID[0] != address.NEO3Prefix {
		return false
	}

	const boundIndex = NEO3WalletSize - 4
	return bytes.Equal(rawID[boundIndex:], hash.Checksum(rawID[:boundIndex]))
}

// Marshal marshals ID into a protobuf binary form.
func (id *ID) Marshal() ([]byte, error) {
	return (*refs.OwnerID)(id).StableMarshal(nil)
}

// Unmarshal unmarshals protobuf binary representation of ID.
func (id *ID) Unmarshal(data []byte) error {
	return (*refs.OwnerID)(id).Unmarshal(data)
}

// MarshalJSON encodes ID to protobuf JSON format.
func (id *ID) MarshalJSON() ([]byte, error) {
	return (*refs.OwnerID)(id).MarshalJSON()
}

// UnmarshalJSON decodes ID from protobuf JSON format.
func (id *ID) UnmarshalJSON(data []byte) error {
	return (*refs.OwnerID)(id).UnmarshalJSON(data)
}
