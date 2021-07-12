package cid

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
)

// IDLength is a byte length of ID according to NeoFS V2 spec.
const IDLength = sha256.Size

// ID represents NeoFS API V2-compatible container identifier.
type ID struct {
	b []byte
}

// FromV2 reads ID from refs.ContainerID message.
func (x *ID) FromV2(idv2 refs.ContainerID) {
	x.b = idv2.GetValue()
}

// Bytes returns slice of ID bytes.
//
// Slice mutation affects the ID.
func (x ID) Bytes() []byte {
	return x.b
}

// SetBytes sets ID bytes.
func (x *ID) SetBytes(b [IDLength]byte) {
	x.b = b[:]
}

// String implements fmt.Stringer through Hex encoding.
//
// To get the canonical string MarshalText should be used.
func (x ID) String() string {
	// using hex encoding has better perfomance than the base58 one
	return hex.EncodeToString(x.b)
}

// MarshalText implements encoding.TextMarshaler through Base58 encoding.
// Returns canonical ID string according to NeoFS API V2 spec.
func (x ID) MarshalText() ([]byte, error) {
	return []byte(base58.Encode(x.b)), nil
}

// UnmarshalText implements encoding.TextUnmarshaler through Base58 decoding.
//
// Returns an error if txt is not a canonical ID string according to NeoFS API V2 spec.
// In this case ID remains untouched.
func (x *ID) UnmarshalText(txt []byte) error {
	data, err := base58.Decode(string(txt))
	if err != nil {
		return fmt.Errorf("incorrect encoding: %w", err)
	}

	x.b = data

	return nil
}

// Equal defines a comparison relation on ID's.
//
// ID's are equal if they have the same binary representation.
func Equal(id1, id2 ID) bool {
	return bytes.Equal(id1.Bytes(), id2.Bytes())
}

// IDToV2 writes ID to refs.ContainerID message.
//
// Message must not be nil.
func IDToV2(idv2 *refs.ContainerID, id ID) {
	idv2.SetValue(id.Bytes())
}
