package refs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"

	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/tzhash/tz"
)

// TZLength is a length of Tillich-Zemor (TZ) hashing function result.
//
// See docs https://github.com/nspcc-dev/tzhash.
const TZLength = 64

// Checksum represents NeoFS API V2-compatible binary checksum.
type Checksum struct {
	typ refs.ChecksumType

	val []byte
}

// SetSHA256 makes Checksum to represent the SHA256 checksum.
//
// Slice length should be sha256.Size.
func (x *Checksum) SetSHA256(v [sha256.Size]byte) {
	x.typ = refs.SHA256
	x.val = v[:]
}

// SetTZ makes Checksum to represent the TZ checksum.
func (x *Checksum) SetTZ(v [TZLength]byte) {
	x.typ = refs.TillichZemor
	x.val = v[:]
}

// VerificationHash returns hash.Hash instance to verify the checksum using Verify.
//
// Returns nil if hash algorithm is not supported.
func (x Checksum) VerificationHash() hash.Hash {
	switch x.typ {
	default:
		return nil
	case refs.SHA256:
		return sha256.New()
	case refs.TillichZemor:
		return tz.New()
	}
}

// HomomorphicVerificationHash returns hash.Hash instance to verify homomorphic checksum using Verify.
//
// Returns nil if hash algorithm is not homomorphic.
func (x Checksum) HomomorphicVerificationHash() hash.Hash {
	switch x.typ {
	default:
		return nil
	case refs.TillichZemor:
		return tz.New()
	}
}

// Verify checks if Checksum represents hash.Hash. VerificationHash method result should be used.
//
// Hash must not be nil.
func (x Checksum) Verify(h hash.Hash) bool {
	return bytes.Equal(x.val, h.Sum(nil))
}

// String implements fmt.Stringer.
//
// Format: <type>:<hex>.
func (x Checksum) String() string {
	const checksumStrFmt = "%s:%s"

	return fmt.Sprintf(checksumStrFmt,
		x.typ,
		hex.EncodeToString(x.val),
	)
}

// FromV2 reads Checksum from refs.Checksum message.
func (x *Checksum) FromV2(csv2 refs.Checksum) {
	x.typ = csv2.GetType()
	x.val = csv2.GetSum()
}

// WriteToV2 writes Checksum to refs.Checksum message.
//
// Message must not be nil.
func (x Checksum) WriteToV2(csv2 *refs.Checksum) {
	csv2.SetType(x.typ)
	csv2.SetSum(x.val)
}
