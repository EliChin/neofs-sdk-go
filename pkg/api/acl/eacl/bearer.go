package eacl

import (
	"crypto/ecdsa"
	"errors"

	cryptoalgo "github.com/nspcc-dev/neofs-api-go/crypto/algo"
	neofsecdsa "github.com/nspcc-dev/neofs-api-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	apicrypto "github.com/nspcc-dev/neofs-api-go/v2/crypto"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	neofsnetwork "github.com/nspcc-dev/neofs-sdk-go/pkg/api/network"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/owner"
)

// BearerToken represents NeoFS API V2-compatible bearer token.
type BearerToken struct {
	withBody bool

	withLifetime  bool
	exp, iat, nbf neofsnetwork.Epoch

	withOwner bool
	owner     owner.ID

	withTable bool
	table     Table

	withSignature bool
	signature     refs.Signature
}

// FromV2 reads BearerToken from acl.BearerToken message.
func (x *BearerToken) FromV2(tv2 acl.BearerToken) {
	{ // signature
		sv2 := tv2.GetSignature()

		x.withSignature = sv2 != nil
		if x.withSignature {
			x.signature = *sv2
		}
	}

	body := tv2.GetBody()

	x.withBody = body != nil

	if !x.withBody {
		return
	}

	{ // owner
		idv2 := body.GetOwnerID()

		x.withOwner = idv2 != nil
		if x.withOwner {
			x.owner.FromV2(*idv2)
		}
	}

	{ // lifetime
		lt := body.GetLifetime()

		x.withLifetime = lt != nil
		if x.withLifetime {
			x.iat.FromUint64(lt.GetIat())
			x.exp.FromUint64(lt.GetExp())
			x.nbf.FromUint64(lt.GetNbf())
		}
	}

	{ // table
		tv2 := body.GetEACL()

		x.withTable = tv2 != nil
		if x.withTable {
			x.table.FromV2(*tv2)
		}
	}
}

// writeToBody writes BearerToken data to acl.BearerTokenBody message.
func (x BearerToken) writeToBody(body *acl.BearerTokenBody) {
	{ // owner
		var idv2 *refs.OwnerID

		if x.withOwner {
			idv2 = body.GetOwnerID()
			if idv2 == nil {
				idv2 = new(refs.OwnerID)
			}

			owner.IDToV2(idv2, x.owner)
		}

		body.SetOwnerID(idv2)
	}

	{ // lifetime
		var lt *acl.TokenLifetime

		if x.withLifetime {
			lt = body.GetLifetime()
			if lt == nil {
				lt = new(acl.TokenLifetime)
			}

			var u64 uint64

			x.iat.WriteToUint64(&u64)
			lt.SetIat(u64)

			x.exp.WriteToUint64(&u64)
			lt.SetExp(u64)

			x.nbf.WriteToUint64(&u64)
			lt.SetNbf(u64)
		}

		body.SetLifetime(lt)
	}

	{ // table
		var tv2 *acl.Table

		if x.withTable {
			tv2 = body.GetEACL()
			if tv2 == nil {
				tv2 = new(acl.Table)
			}

			x.table.WriteToV2(tv2)
		}

		body.SetEACL(tv2)
	}
}

// writeToV2 writes BearerToken to acl.BearerToken message.
//
// Message must not be nil.
func (x BearerToken) WriteToV2(tv2 *acl.BearerToken) {
	{ // body
		var body *acl.BearerTokenBody

		if x.withBody {
			body = tv2.GetBody()
			if body == nil {
				body = new(acl.BearerTokenBody)
			}

			x.writeToBody(body)
		}

		tv2.SetBody(nil)
	}

	{ // signature
		var sv2 *refs.Signature

		if x.withSignature {
			sv2 = &x.signature
		}

		tv2.SetSignature(sv2)
	}
}

func (x *BearerToken) setBodyData(f func(*BearerToken)) {
	x.withBody = true
	f(x)
}

// WithOwner checks if owner identifier was specified.
func (x BearerToken) WithOwner() bool {
	return x.withOwner
}

// Owner returns BearerToken's owner identifier.
//
// Makes sense only if WithOwner returns true.
//
// Result mutation affects the Token.
func (x BearerToken) Owner() owner.ID {
	return x.owner
}

// SetOwnerID sets BearerToken's owner identifier.
//
// Parameter mutation affects the Token.
func (x *BearerToken) SetOwnerID(id owner.ID) {
	x.setBodyData(func(x *BearerToken) {
		x.owner = id
	})
}

func (x *BearerToken) setLifetimeData(f func(*BearerToken)) {
	x.withLifetime = true
	x.setBodyData(f)
}

// WithLifetime checks if BearerToken lifetime was specified.
func (x BearerToken) WithLifetime() bool {
	return x.withBody && x.withLifetime
}

// Exp returns epoch of the BearerToken expiration.
//
// Makes sens only if WithLifetime returns true.
func (x BearerToken) Exp() neofsnetwork.Epoch {
	return x.exp
}

// SetExp sets epoch number of the BearerToken expiration.
func (x *BearerToken) SetExp(exp neofsnetwork.Epoch) {
	x.setLifetimeData(func(x *BearerToken) {
		x.exp = exp
	})
}

// Nbf returns starting epoch of the BearerToken.
//
// Makes sens only if WithLifetime returns true.
func (x BearerToken) Nbf() neofsnetwork.Epoch {
	return x.nbf
}

// SetNbf sets starting epoch number of the BearerToken.
func (x *BearerToken) SetNbf(nbf neofsnetwork.Epoch) {
	x.setLifetimeData(func(x *BearerToken) {
		x.nbf = nbf
	})
}

// ReadIat reads starting epoch of the BearerToken.
//
// Makes sens only if WithLifetime returns true.
func (x BearerToken) Iat() neofsnetwork.Epoch {
	return x.iat
}

// SetIat sets the number of the epoch in which the BearerToken was issued.
func (x *BearerToken) SetIat(iat neofsnetwork.Epoch) {
	x.setLifetimeData(func(x *BearerToken) {
		x.iat = iat
	})
}

// SignECDSA calculates and writes ECDSA signature of the BearerToken data.
//
// Returns signature calculation errors.
func (x *BearerToken) SignECDSA(key ecdsa.PrivateKey) error {
	var body *acl.BearerTokenBody

	if x.withBody {
		body = new(acl.BearerTokenBody)

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

// VerifySignature checks if BearerToken signature is presented and valid.
//
// Returns nil if signature is valid.
func (x *BearerToken) VerifySignature() error {
	if !x.withSignature {
		return errors.New("missing signature")
	}

	key, err := cryptoalgo.UnmarshalKey(cryptoalgo.ECDSA, x.signature.GetKey())
	if err != nil {
		return err
	}

	var body *acl.BearerTokenBody

	if x.withBody {
		body = new(acl.BearerTokenBody)

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

// BearerTokenMarshalProto marshals BearerToken into a protobuf binary form.
func BearerTokenMarshalProto(t BearerToken) ([]byte, error) {
	var tv2 acl.BearerToken

	t.WriteToV2(&tv2)

	return tv2.StableMarshal(nil)
}

// BearerTokenUnmarshalProto unmarshals protobuf binary representation of BearerToken.
func BearerTokenUnmarshalProto(t *BearerToken, data []byte) error {
	var tv2 acl.BearerToken

	err := tv2.Unmarshal(data)
	if err == nil {
		t.FromV2(tv2)
	}

	return err
}
