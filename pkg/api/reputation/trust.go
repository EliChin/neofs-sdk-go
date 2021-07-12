package reputation

import (
	"github.com/nspcc-dev/neofs-api-go/v2/reputation"
)

// Trust represents NeoFS API V2-compatible peer trust.
type Trust struct {
	value float64

	withPeer    bool
	trustedPeer PeerID
}

// FromV2 reads Trust from reputation.Trust message.
func (x *Trust) FromV2(tv2 reputation.Trust) {
	{ // trusted peer
		idv2 := tv2.GetPeer()

		x.withPeer = idv2 != nil
		if x.withPeer {
			x.trustedPeer.FromV2(*idv2)
		}
	}

	x.value = tv2.GetValue()
}

// WriteToV2 writes Trust to reputation.Trust message.
//
// Message must not be nil.
func (x Trust) WriteToV2(tv2 *reputation.Trust) {
	{ // trusted peer
		var idv2 *reputation.PeerID

		if x.withPeer {
			idv2 = tv2.GetPeer()
			if idv2 == nil {
				idv2 = new(reputation.PeerID)
			}

			PeerIDToV2(idv2, x.trustedPeer)
		}

		tv2.SetPeer(idv2)
	}

	tv2.SetValue(x.value)
}

// Value returns trust value.
func (x Trust) Value() float64 {
	return x.value
}

// SetValue sets trust value.
func (x *Trust) SetValue(value float64) {
	x.value = value
}

// WithTrustedPeer checks if trusted peer was specified.
func (x Trust) WithTrustedPeer() bool {
	return x.withPeer
}

// TrustedPeer returns trusted peer ID.
//
// Makes sense only if WithTrustedPeer returns true.
//
// Result mutation affects the Trust.
func (x Trust) TrustedPeer() PeerID {
	return x.trustedPeer
}

// SetTrustedPeer sets trusted peer ID.
//
// Parameter mutation affects the Trust.
func (x *Trust) SetTrustedPeer(trustedPeer PeerID) {
	x.trustedPeer = trustedPeer
	x.withPeer = true
}
