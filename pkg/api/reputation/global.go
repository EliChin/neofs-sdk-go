package reputation

import (
	"crypto/ecdsa"
	"errors"

	cryptoalgo "github.com/nspcc-dev/neofs-api-go/crypto/algo"
	neofsecdsa "github.com/nspcc-dev/neofs-api-go/crypto/ecdsa"
	apicrypto "github.com/nspcc-dev/neofs-api-go/v2/crypto"
	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/reputation"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/refs"
)

// GlobalTrust represents NeoFS API V2-compatible global trust to peer.
type GlobalTrust struct {
	withBody bool

	withVersion bool
	version     refs.Version

	withManager bool
	manager     PeerID

	withTrust bool
	trust     Trust

	withSignature bool
	signature     v2refs.Signature
}

// FromV2 restores GlobalTrust from reputation.GlobalTrust message.
func (x *GlobalTrust) FromV2(gtv2 reputation.GlobalTrust) {
	{ // version
		vv2 := gtv2.GetVersion()

		x.withVersion = vv2 != nil
		if x.withVersion {
			x.version.FromV2(*vv2)
		}
	}

	{ // signature
		sv2 := gtv2.GetSignature()

		x.withSignature = sv2 != nil
		if x.withSignature {
			x.signature = *sv2
		}
	}

	body := gtv2.GetBody()

	x.withBody = body != nil
	if !x.withBody {
		return
	}

	{ // manager
		mv2 := body.GetManager()

		x.withManager = mv2 != nil
		if x.withManager {
			x.manager.FromV2(*mv2)
		}
	}

	{ // trust
		tv2 := body.GetTrust()

		x.withTrust = true
		if x.withTrust {
			x.trust.FromV2(*tv2)
		}
	}
}

func (x GlobalTrust) writeToBody(body *reputation.GlobalTrustBody) {
	{ // manager
		var mv2 *reputation.PeerID

		if x.withManager {
			mv2 = body.GetManager()
			if mv2 == nil {
				mv2 = new(reputation.PeerID)
			}

			PeerIDToV2(mv2, x.manager)
		}

		body.SetManager(mv2)

	}

	{ // trust
		var tv2 *reputation.Trust

		if x.withTrust {
			tv2 = body.GetTrust()
			if tv2 == nil {
				tv2 = new(reputation.Trust)
			}

			x.trust.WriteToV2(tv2)
		}

		body.SetTrust(tv2)

	}
}

// WriteToV2 writes GlobalTrust to reputation.Trust message.
//
// Message must not be nil.
func (x GlobalTrust) WriteToV2(tv2 *reputation.GlobalTrust) {
	{ // version
		var vv2 *v2refs.Version

		if x.withVersion {
			vv2 = tv2.GetVersion()
			if vv2 == nil {
				vv2 = new(v2refs.Version)
			}

			x.version.WriteToV2(vv2)
		}

		tv2.SetVersion(vv2)
	}

	{ // signature
		var sv2 *v2refs.Signature

		if x.withSignature {
			sv2 = &x.signature
		}

		tv2.SetSignature(sv2)
	}

	{ // body
		var body *reputation.GlobalTrustBody

		if x.withBody {
			body = tv2.GetBody()
			if body == nil {
				body = new(reputation.GlobalTrustBody)
			}

			x.writeToBody(body)
		}

		tv2.SetBody(body)
	}
}

// WithVersion checks if GlobalTrust version was specified.
func (x GlobalTrust) WithVersion() bool {
	return x.withVersion
}

// Version returns protocol version within which GlobalTrust is formed.
//
// Makes sense only if WithVersion returns true.
func (x GlobalTrust) Version() refs.Version {
	return x.version
}

// SetVersion sets protocol version within which GlobalTrust is formed.
func (x *GlobalTrust) SetVersion(version refs.Version) {
	x.version = version
	x.withVersion = true
}

func (x *GlobalTrust) setBodyData(f func(*GlobalTrust)) {
	x.withBody = true
	f(x)
}

// WithManager checks if manager peer was specified.
func (x GlobalTrust) WithManager() bool {
	return x.withManager
}

// Manager returns trusted peer's manager ID.
//
// Makes sense only if Manager returns true.
//
// Result mutation affects the GlobalTrust.
func (x GlobalTrust) Manager() PeerID {
	return x.manager
}

// SetManager sets trusted peer's manager ID.
//
// Parameter mutation affects the GlobalTrust.
func (x *GlobalTrust) SetManager(manager PeerID) {
	x.setBodyData(func(x *GlobalTrust) {
		x.manager = manager
	})

	x.withManager = true
}

// WithTrust checks if trust value was specified.
func (x GlobalTrust) WithTrust() bool {
	return x.withTrust
}

// Trust returns peer's global trust.
//
// Makes sense only if WithTrust returns true.
func (x GlobalTrust) Trust() Trust {
	return x.trust
}

// SetTrust sets peer's global trust.
func (x *GlobalTrust) SetTrust(trust Trust) {
	x.setBodyData(func(x *GlobalTrust) {
		x.trust = trust
	})

	x.withTrust = true
}

// SignECDSA calculates and writes ECDSA signature of the GlobalTrust data.
//
// Key must not be nil.
func (x *GlobalTrust) SignECDSA(key ecdsa.PrivateKey) error {
	var body *reputation.GlobalTrustBody

	if x.withBody {
		body = new(reputation.GlobalTrustBody)

		x.writeToBody(body)
	}

	var prm apicrypto.SignPrm

	prm.SetProtoMarshaler(signature.StableMarshalerCrypto(body))

	prm.SetTargetSignature(&x.signature)

	if err := apicrypto.Sign(neofsecdsa.Signer(key), prm); err != nil {
		return err
	}

	x.withSignature = true

	return nil
}

// VerifySignature checks if GlobalTrust signature is presented and valid.
//
// Returns nil if signature is valid.
func (x GlobalTrust) VerifySignature() error {
	if !x.withSignature {
		return errors.New("missing signature")
	}

	key, err := cryptoalgo.UnmarshalKey(cryptoalgo.ECDSA, x.signature.GetKey())
	if err != nil {
		return err
	}

	var body *reputation.GlobalTrustBody

	if x.withBody {
		body = new(reputation.GlobalTrustBody)

		x.writeToBody(body)
	}

	var prm apicrypto.VerifyPrm

	prm.SetProtoMarshaler(signature.StableMarshalerCrypto(body))

	prm.SetSignature(x.signature.GetSign())

	if !apicrypto.Verify(key, prm) {
		return errors.New("invalid signature")
	}

	return nil
}

// GlobalTrustMarshalProto marshals GlobalTrust into a protobuf binary form.
func GlobalTrustMarshalProto(gt GlobalTrust) ([]byte, error) {
	var gtv2 reputation.GlobalTrust

	gt.WriteToV2(&gtv2)

	return gtv2.StableMarshal(nil)
}

// GlobalTrustUnmarshalProto unmarshals protobuf binary representation of GlobalTrust.
func GlobalTrustUnmarshalProto(gt *GlobalTrust, data []byte) error {
	var gtv2 reputation.GlobalTrust

	err := gtv2.Unmarshal(data)
	if err == nil {
		gt.FromV2(gtv2)
	}

	return err
}
