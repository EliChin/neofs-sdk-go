package cidtest

import (
	"math/rand"

	cid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/container/id"
)

// ID returns random cid.ID.
func ID() cid.ID {
	var b [cid.IDLength]byte

	rand.Read(b[:])

	return IDFromBytes(b)
}

// IDFromBytes returns cid.ID initialized
// with specified bytes.
func IDFromBytes(b [cid.IDLength]byte) (id cid.ID) {
	id.SetBytes(b)
	return id
}
