package object

import (
	"bytes"
	"encoding/hex"

	"github.com/google/uuid"
)

// SplitID represents NeoFS API V2-compatible split identifier.
type SplitID struct {
	b []byte
}

// AccessBytes returns slice of SplitID bytes.
//
// Slice mutation affects the ID.
func (x SplitID) AccessBytes() []byte {
	return x.b
}

// SetUUID sets SplitID in uuid.UUID format.
func (x *SplitID) SetUUID(uid uuid.UUID) {
	data, err := uid.MarshalBinary()
	if err != nil {
		panic(err) // never returns an error, direct [:] isn't compatible
	}

	x.b = data
}

// String implements fmt.Stringer through Hex encoding.
//
// To get the canonical string MarshalText should be used.
func (x SplitID) String() string {
	// using hex encoding has better perfomance than the base58 one
	return hex.EncodeToString(x.b)
}

// Equal defines a comparison relation on SplitID's.
//
// SplitID's are equal if they have the same binary representation.
func Equal(id1, id2 SplitID) bool {
	return bytes.Equal(id1.AccessBytes(), id2.AccessBytes())
}

// FromV2 reads SplitID from []byte.
//
// Parameter mutation affects the SplitID.
func (x *SplitID) FromV2(idv2 []byte) {
	x.b = idv2
}
