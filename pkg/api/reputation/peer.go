package reputation

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/v2/reputation"
)

// IDLength is a byte length of PeerID according to NeoFS V2 spec.
const IDLength = 33

// PeerID represents NeoFS API V2-compatible ID of the participant of the reputation system.
type PeerID struct {
	b []byte
}

// FromV2 reads PeerID reputation.PeerID message.
func (x *PeerID) FromV2(idv2 reputation.PeerID) {
	x.b = idv2.GetPublicKey()
}

// Bytes returns slice of PeerID bytes.
//
// Slice mutation affects the ID.
func (x PeerID) Bytes() []byte {
	return x.b
}

// SetBytes sets PeerID bytes.
func (x *PeerID) SetBytes(b [IDLength]byte) {
	x.b = b[:]
}

// String implements fmt.Stringer through Hex encoding.
//
// To get the canonical string MarshalText should be used.
func (x PeerID) String() string {
	// using hex encoding has better perfomance than the base58 one
	return hex.EncodeToString(x.b)
}

// MarshalText implements encoding.TextMarshaler through Base58 encoding.
// Returns canonical PeerID string according to NeoFS API V2 spec.
func (x PeerID) MarshalText() ([]byte, error) {
	return []byte(base58.Encode(x.b)), nil
}

// UnmarshalText implements encoding.TextUnmarshaler through Base58 decoding.
//
// Returns an error if d is not a canonical PeerID string according to NeoFS API V2 spec.
// In this case ID remains untouched.
func (x *PeerID) UnmarshalText(txt []byte) error {
	data, err := base58.Decode(string(txt))
	if err != nil {
		return fmt.Errorf("incorrect encoding: %w", err)
	}

	x.b = data

	return nil
}

// Equal defines a comparison relation on PeerID's.
//
// PeerID's are equal if they have the same binary representation.
func Equal(id1, id2 PeerID) bool {
	return bytes.Equal(id1.Bytes(), id2.Bytes())
}

// PeerIDToV2 writes ID to reputation.PeerID message.
//
// Message must not be nil.
func PeerIDToV2(idv2 *reputation.PeerID, id PeerID) {
	idv2.SetPublicKey(id.Bytes())
}
