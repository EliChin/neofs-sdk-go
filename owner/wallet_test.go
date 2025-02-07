package owner

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNEO3WalletFromPublicKey(t *testing.T) {
	rawPub, _ := hex.DecodeString("0369b7b6c49fb937f3de52af189b91069767679c2739798d85f2ed69c079940680")
	x, y := elliptic.UnmarshalCompressed(elliptic.P256(), rawPub)
	require.True(t, x != nil && y != nil)

	expected := "35ee628f21922d7308f1bd71f03a0d8ba89c4e7372fca1442c"
	w, err := NEO3WalletFromPublicKey(&ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y})
	require.NoError(t, err)
	require.Equal(t, expected, hex.EncodeToString(w[:]))
}
