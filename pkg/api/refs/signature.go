package refs

import (
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
)

// Signature represents NeoFS API V2-compatible key-signature pair.
type Signature struct {
	key, val []byte
}

// Key returns signer's public key in a binary representation.
//
// Slice mutation affects the Signature.
func (x Signature) Key() []byte {
	return x.key
}

// SetKey sets binary representation of signer's public key.
//
// Slice mutation affects the Signature.
func (x *Signature) SetKey(v []byte) {
	x.key = v
}

// Value returns signature value.
//
// Slice mutation affects the Signature.
func (x Signature) Value() []byte {
	return x.val
}

// SetValue sets signature value.
//
// Slice mutation affects the Signature.
func (x *Signature) SetValue(v []byte) {
	x.val = v
}

// SignatureFromV2 reads Signature from refs.Signature message.
func SignatureFromV2(s *Signature, sv2 refs.Signature) {
	s.SetKey(sv2.GetKey())
	s.SetValue(sv2.GetSign())
}

// SignatureToV2 writes Signature to refs.Signature message.
//
// Message must not be nil.
func SignatureToV2(sv2 *refs.Signature, s Signature) {
	sv2.SetKey(s.Key())
	sv2.SetSign(s.Value())
}
