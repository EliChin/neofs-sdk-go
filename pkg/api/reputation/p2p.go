package reputation

import (
	"github.com/nspcc-dev/neofs-api-go/v2/reputation"
)

// PeerToPeerTrust represents NeoFS API V2-compatible peer-to-peer trust.
type PeerToPeerTrust struct {
	withPeer     bool
	trustingPeer PeerID

	withTrust bool
	trust     Trust
}

// FromV2 restores PeerToPeerTrust from reputation.PeerToPeerTrust message.
func (x *PeerToPeerTrust) FromV2(ptpv2 reputation.PeerToPeerTrust) {
	{ // trusting peer
		idv2 := ptpv2.GetTrustingPeer()

		x.withPeer = idv2 != nil
		if x.withPeer {
			x.trustingPeer.FromV2(*idv2)
		}

		ptpv2.SetTrustingPeer(idv2)
	}

	{ // trust
		tv2 := ptpv2.GetTrust()

		x.withTrust = tv2 != nil
		if x.withTrust {
			x.trust.FromV2(*tv2)
		}

		ptpv2.SetTrust(tv2)
	}
}

// WriteToV2 writes PeerToPeerTrust to reputation.PeerToPeerTrust message.
//
// Message must not be nil.
func (x PeerToPeerTrust) WriteToV2(ptpv2 *reputation.PeerToPeerTrust) {
	{ // trusting peer
		var idv2 *reputation.PeerID

		if x.withPeer {
			idv2 = ptpv2.GetTrustingPeer()
			if idv2 == nil {
				idv2 = new(reputation.PeerID)
			}

			PeerIDToV2(idv2, x.trustingPeer)
		}

		ptpv2.SetTrustingPeer(idv2)
	}

	{ // trust
		var tv2 *reputation.Trust

		if x.withTrust {
			tv2 = ptpv2.GetTrust()
			if tv2 == nil {
				tv2 = new(reputation.Trust)
			}

			x.trust.WriteToV2(tv2)
		}

		ptpv2.SetTrust(tv2)
	}
}

// WithTrustingPeer checks if trusting peer was specified.
func (x PeerToPeerTrust) WithTrustingPeer() bool {
	return x.withPeer
}

// TrustingPeer returns trusting peer ID.
//
// Makes sense only if WithTrustingPeer returns true.
//
// Result mutation affects the PeerToPeerTrust.
func (x PeerToPeerTrust) TrustingPeer() PeerID {
	return x.trustingPeer
}

// SetTrustingPeer sets trusting peer ID.
//
// Parameter mutation affects the PeerToPeerTrust.
func (x *PeerToPeerTrust) SetTrustingPeer(trustingPeer PeerID) {
	x.trustingPeer = trustingPeer
	x.withPeer = true
}

// WithTrust returns true if trust value was set.
func (x PeerToPeerTrust) WithTrust() bool {
	return x.withTrust
}

// Trust returns trust value of the trusting peer to the trusted one.
//
// Makes sense only if WithTrust returns true.
func (x PeerToPeerTrust) Trust() Trust {
	return x.trust
}

// SetTrust sets trust value of the trusting peer to the trusted one.
func (x *PeerToPeerTrust) SetTrust(trust Trust) {
	x.trust = trust
	x.withTrust = true
}
