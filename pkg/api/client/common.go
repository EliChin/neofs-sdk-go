package neofsapiclient

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	neofsecdsa "github.com/nspcc-dev/neofs-api-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	v2session "github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/acl/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/refs"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/session"
)

// requestMetaHeaderWriter is a common interface of parameter types.
type requestMetaHeaderWriter interface {
	writeToRequestMetaHeader(*v2session.RequestMetaHeader)
	privateKey() ecdsa.PrivateKey
}

// check checks if all required parameters are set and panics if not.
func (x commonPrm) checkInputs(ctx context.Context) {
	switch {
	case ctx == nil:
		panic("nil context")
	case !x.keySet:
		panic("no private key provided")
	}
}

// SetECDSAPrivateKey sets ecdsa.PrivateKey to sign the request(s).
//
// Required parameter.
func (x *commonPrm) SetECDSAPrivateKey(key ecdsa.PrivateKey) {
	x.keySet = true
	x.key = key
}

// privateKey returns ecdsa.PrivateKey to sign the request(s).
func (x commonPrm) privateKey() ecdsa.PrivateKey {
	return x.key
}

// writeToRequestMetaHeader writes the parameters that should be reflected
// in request to RequestMetaHeader.
func (x commonPrm) writeToRequestMetaHeader(h *v2session.RequestMetaHeader) {}

// commonPrm groups the parameters inherited by all Client operations.
type commonPrm struct {
	keySet bool
	key    ecdsa.PrivateKey
}

// commonPrmWithSession groups commonPrm with session token.
type commonPrmWithSession struct {
	commonPrm

	withSes bool
	sesTok  session.Token
}

// SetSessionToken sets token of the session within which the operation should be carried out.
//
// Token should be signed by its owner.
func (x *commonPrmWithSession) SetSessionToken(sesTok session.Token) {
	x.withSes = true
	x.sesTok = sesTok
}

// writeToRequestMetaHeader writes the parameters that should be reflected
// in request to RequestMetaHeader.
func (x commonPrmWithSession) writeToRequestMetaHeader(h *v2session.RequestMetaHeader) {
	x.commonPrm.writeToRequestMetaHeader(h)

	{ // session token
		var tv2 *v2session.SessionToken

		if x.withSes {
			tv2 = h.GetSessionToken()
			if tv2 == nil {
				tv2 = new(v2session.SessionToken)
			}

			x.sesTok.WriteToV2(tv2)
		}

		h.SetSessionToken(tv2)
	}
}

// commonPrmWithTokens groups commonPrmWithSession with bearer token.
type commonPrmWithTokens struct {
	commonPrmWithSession

	withBrr bool
	brrTok  eacl.BearerToken
}

// SetBearerToken sets bearer token that will be attached to all requests within the operation.
//
// Token should be signed by its owner.
func (x *commonPrmWithTokens) SetBearerToken(sesTok session.Token) {
	x.withBrr = true
	x.sesTok = sesTok
}

// writeToRequestMetaHeader writes the parameters that should be reflected
// in request to RequestMetaHeader.
func (x commonPrmWithTokens) writeToRequestMetaHeader(h *v2session.RequestMetaHeader) {
	x.commonPrmWithSession.writeToRequestMetaHeader(h)

	{ // bearer token
		var tv2 *acl.BearerToken

		if x.withBrr {
			tv2 = h.GetBearerToken()
			if tv2 == nil {
				tv2 = new(acl.BearerToken)
			}

			x.brrTok.WriteToV2(tv2)
		}

		h.SetBearerToken(tv2)
	}
}

type requestInterface interface {
	GetMetaHeader() *v2session.RequestMetaHeader
	SetMetaHeader(*v2session.RequestMetaHeader)
	GetVerificationHeader() *v2session.RequestVerificationHeader
	SetVerificationHeader(*v2session.RequestVerificationHeader)
}

// prepareRequest forms meta header, sets body using callback and signs the request with private key.
func prepareRequest(r requestInterface, wMeta requestMetaHeaderWriter, bodyCallback func(requestInterface)) error {
	// construct the request meta header
	var hMeta v2session.RequestMetaHeader

	var vv2 v2refs.Version

	refs.CurrentVersion().WriteToV2(&vv2)

	wMeta.writeToRequestMetaHeader(&hMeta)
	hMeta.SetVersion(&vv2)
	hMeta.SetTTL(2)

	bodyCallback(r)

	r.SetMetaHeader(&hMeta)

	key := wMeta.privateKey()

	// arg interface is copy-pasted from signature pkg, may it is worth to share
	err := signature.SignServiceMessage(neofsecdsa.Signer(key), r)
	if err != nil {
		return fmt.Errorf("could not sign the request: %w", err)
	}

	return nil
}

func verifyResponseSignature(r interface {
	GetMetaHeader() *v2session.ResponseMetaHeader
	GetVerificationHeader() *v2session.ResponseVerificationHeader
	SetVerificationHeader(*v2session.ResponseVerificationHeader)
}) error {
	err := signature.VerifyServiceMessage(r)
	if err != nil {
		return fmt.Errorf("invalid response signature: %w", err)
	}

	return err
}
