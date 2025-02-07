package cidtest

import (
	"crypto/sha256"
	"math/rand"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// ID returns random cid.ID.
func ID() *cid.ID {
	checksum := [sha256.Size]byte{}

	rand.Read(checksum[:])

	return IDWithChecksum(checksum)
}

// IDWithChecksum returns cid.ID initialized
// with specified checksum.
func IDWithChecksum(cs [sha256.Size]byte) *cid.ID {
	id := cid.New()
	id.SetSHA256(cs)

	return id
}
