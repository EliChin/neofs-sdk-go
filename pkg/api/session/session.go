package session

import (
	"crypto/ecdsa"
	"errors"

	"github.com/google/uuid"
	cryptoalgo "github.com/nspcc-dev/neofs-api-go/crypto/algo"
	neofsecdsa "github.com/nspcc-dev/neofs-api-go/crypto/ecdsa"
	apicrypto "github.com/nspcc-dev/neofs-api-go/v2/crypto"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	neofsnetwork "github.com/nspcc-dev/neofs-sdk-go/pkg/api/network"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/owner"
)

// Token represents NeoFS API V2-compatible session token.
type Token struct {
	withBody bool

	withLifetime  bool
	exp, iat, nbf neofsnetwork.Epoch

	withOwner bool
	owner     owner.ID

	ctxType uint8

	withCtxContainer bool
	ctxContainer     ContainerContext

	withCtxObject bool
	ctxObject     ObjectContext

	sessionKey []byte

	id []byte

	withSignature bool
	signature     refs.Signature
}

const (
	_ uint8 = iota
	ctxContainer
	ctxObject
)

// FromV2 reads Token from session.SessionToken message.
func (x *Token) FromV2(tv2 session.SessionToken) {
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

	{ // context
		switch v := body.GetContext().(type) {
		default:
			x.ctxType = 0
		case *session.ContainerSessionContext:
			x.ctxType = ctxContainer

			x.withCtxContainer = v != nil
			if x.withCtxContainer {
				x.ctxContainer.FromV2(*v)
			}
		case *session.ObjectSessionContext:
			x.ctxType = ctxObject

			x.withCtxObject = v != nil
			if x.withCtxObject {
				x.ctxObject.FromV2(*v)
			}
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

	x.id = body.GetID()
	x.sessionKey = body.GetSessionKey()
}

// writeToBody writes Token data to session.SessionTokenBody.
func (x Token) writeToBody(body *session.SessionTokenBody) {
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

	{ // context
		switch x.ctxType {
		default:
			body.SetContext(nil)
		case ctxContainer:
			var cctx *session.ContainerSessionContext

			if x.withCtxContainer {
				var ok bool

				cctx, ok = body.GetContext().(*session.ContainerSessionContext)
				if !ok {
					cctx = new(session.ContainerSessionContext)
				}

				x.ctxContainer.WriteToV2(cctx)
			}

			body.SetContext(cctx)
		case ctxObject:
			var octx *session.ObjectSessionContext

			if x.withCtxObject {
				var ok bool

				octx, ok = body.GetContext().(*session.ObjectSessionContext)
				if !ok {
					octx = new(session.ObjectSessionContext)
				}

				x.ctxObject.WriteToV2(octx)
			}

			body.SetContext(octx)
		}
	}

	{ // lifetime
		var lt *session.TokenLifetime

		if x.withLifetime {
			lt = body.GetLifetime()
			if lt == nil {
				lt = new(session.TokenLifetime)
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

	body.SetID(x.id)
	body.SetSessionKey(x.sessionKey)
}

// writeToV2 writes Token to session.SessionToken message.
//
// Message must not be nil.
func (x Token) WriteToV2(tv2 *session.SessionToken) {
	{ // body
		var body *session.SessionTokenBody

		if x.withBody {
			body = tv2.GetBody()
			if body == nil {
				body = new(session.SessionTokenBody)
			}

			x.writeToBody(body)
		}

		tv2.SetBody(body)
	}

	{ // signature
		var sv2 *refs.Signature

		if x.withSignature {
			sv2 = &x.signature
		}

		tv2.SetSignature(sv2)
	}
}

func (x *Token) setBodyData(f func(*Token)) {
	x.withBody = true
	f(x)
}

// WithID checks if Token ID was specified.
func (x Token) WithID() bool {
	return x.withBody
}

// ID returns Token identifier.
//
// Makes sense only if WithID returns true.
//
// Result mutation affects the Token.
func (x Token) ID() []byte {
	return x.id
}

// SetID sets Token identifier in a binary format.
func (x *Token) SetID(id []byte) {
	x.setBodyData(func(x *Token) {
		x.id = id
	})
}

// SetTokenUUID sets Token identifier in a uuid.UUID format.
func SetTokenUUID(t *Token, uid uuid.UUID) {
	data, err := uid.MarshalBinary()
	if err != nil {
		panic(err) // never returns an error, direct [:] isn't compatible
	}

	t.SetID(data)
}

// WithOwner checks if owner identifier was specified.
func (x Token) WithOwner() bool {
	return x.withOwner
}

// Owner returns Token's owner identifier.
//
// Makes sense only if WithOwner returns true.
//
// Result mutation affects the Token.
func (x Token) Owner() owner.ID {
	return x.owner
}

// SetOwnerID sets Token's owner identifier.
//
// Parameter mutation affects the Token.
func (x *Token) SetOwnerID(id owner.ID) {
	x.setBodyData(func(x *Token) {
		x.owner = id
	})

	x.withOwner = true
}

// WithSessionKey checks if session key was specified.
func (x Token) WithSessionKey() bool {
	return x.withBody
}

// SessionKey returns public key of the session in a binary format.
//
// Makes sense only if WithSessionKey returns true.
//
// Result mutation affects the Token.
func (x Token) SessionKey() []byte {
	return x.sessionKey
}

// SetSessionKey sets public key of the session in a binary format.
//
// Parameter mutation affects the Token.
func (x *Token) SetSessionKey(key []byte) {
	x.setBodyData(func(x *Token) {
		x.sessionKey = key
	})
}

func (x *Token) setLifetimeData(f func(*Token)) {
	x.withLifetime = true
	x.setBodyData(f)
}

// WithLifetime checks if Token lifetime was specified.
func (x Token) WithLifetime() bool {
	return x.withBody && x.withLifetime
}

// Exp returns epoch of the Token expiration.
//
// Makes sens only if WithLifetime returns true.
func (x Token) Exp() neofsnetwork.Epoch {
	return x.exp
}

// SetExp sets epoch number of the token expiration.
func (x *Token) SetExp(exp neofsnetwork.Epoch) {
	x.setLifetimeData(func(x *Token) {
		x.exp = exp
	})
}

// Nbf returns starting epoch of the Token.
//
// Makes sens only if WithLifetime returns true.
func (x Token) Nbf() neofsnetwork.Epoch {
	return x.nbf
}

// SetNbf sets starting epoch number of the Token.
func (x *Token) SetNbf(nbf neofsnetwork.Epoch) {
	x.setLifetimeData(func(x *Token) {
		x.nbf = nbf
	})
}

// ReadIat reads starting epoch of the Token.
//
// Makes sens only if WithLifetime returns true.
func (x Token) Iat() neofsnetwork.Epoch {
	return x.iat
}

// SetIat sets the number of the epoch in which the Token was issued.
func (x *Token) SetIat(iat neofsnetwork.Epoch) {
	x.setLifetimeData(func(x *Token) {
		x.iat = iat
	})
}

// SignECDSA calculates and writes ECDSA signature of the Token data.
//
// Returns signature calculation errors.
func (x *Token) SignECDSA(key ecdsa.PrivateKey) error {
	var body *session.SessionTokenBody

	if x.withBody {
		body = new(session.SessionTokenBody)

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

// VerifySignature checks if Token signature is presented and valid.
//
// Returns nil if signature is valid.
func (x *Token) VerifySignature() error {
	if !x.withSignature {
		return errors.New("missing signature")
	}

	key, err := cryptoalgo.UnmarshalKey(cryptoalgo.ECDSA, x.signature.GetKey())
	if err != nil {
		return err
	}

	var body *session.SessionTokenBody

	if x.withBody {
		body = new(session.SessionTokenBody)

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

// ForContainer propagates ContainerContext to the Token.
func (x *Token) ForContainer(ctx ContainerContext) {
	x.setBodyData(func(x *Token) {
		x.ctxType = ctxContainer
		x.ctxContainer = ctx
		x.withCtxContainer = true
	})
}

// IsForContainer checks if Token's context is ContainerContext.
func (x Token) IsForContainer() bool {
	return x.ctxType == ctxContainer
}

// ContainerContext returns ContainerContext of the Token.
//
// Makes sense only if IsForContainer returns true.
func (x Token) ContainerContext() ContainerContext {
	return x.ctxContainer
}

// ForObject propagates ObjectContext to the Token.
func (x *Token) ForObject(ctx ObjectContext) {
	x.setBodyData(func(x *Token) {
		x.ctxType = ctxObject
		x.ctxObject = ctx
		x.withCtxObject = true
	})
}

// IsForObject checks if Token's context is ObjectContext.
func (x Token) IsForObject() bool {
	return x.ctxType == ctxObject
}

// ObjectContext returns ObjectContext of the Token.
//
// Makes sense only if IsForObject returns true.
func (x Token) ObjectContext() ObjectContext {
	return x.ctxObject
}

// TokenMarshalProto marshals Token into a protobuf binary form.
func TokenMarshalProto(t Token) ([]byte, error) {
	var sv2 session.SessionToken

	t.WriteToV2(&sv2)

	return sv2.StableMarshal(nil)
}

// TokenUnmarshalProto unmarshals protobuf binary representation of Token.
func TokenUnmarshalProto(t *Token, data []byte) error {
	var sv2 session.SessionToken

	err := sv2.Unmarshal(data)
	if err == nil {
		t.FromV2(sv2)
	}

	return err
}
