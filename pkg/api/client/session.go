package neofsapiclient

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	rpcapi "github.com/nspcc-dev/neofs-api-go/v2/rpc"
	v2session "github.com/nspcc-dev/neofs-api-go/v2/session"
	neofsnetwork "github.com/nspcc-dev/neofs-sdk-go/pkg/api/network"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/owner"
)

// CreateSessionKeyPrm groups the parameters of Client.CreateSessionKey operation.
type CreateSessionKeyPrm struct {
	commonPrm

	ownerSet bool
	owner    owner.ID

	exp neofsnetwork.Epoch
}

// CreateSessionKeyRes groups the results of Client.CreateSessionKey operation.
type CreateSessionKeyRes struct {
	pubkey []byte

	id []byte
}

// CreateSessionKey requests remote node to create private session key. The key later can be used for signing of
// trusted transactions. To conduct a trusted transaction, you will need to attach the public key to the session token,
// which appears in the parameters of some operations.
//
// All required parameters must be set. Result must not be nil.
//
// Context is used for network communication. To set the timeout, use context.WithTimeout or context.WithDeadline.
// It must not be nil.
func (x Client) CreateSessionKey(ctx context.Context, prm CreateSessionKeyPrm, res *CreateSessionKeyRes) error {
	// prelim checks
	prm.checkInputs(ctx, res)

	var reqBody v2session.CreateRequestBody

	{ // construct the request body
		{ // owner
			var idv2 refs.OwnerID

			owner.IDToV2(&idv2, prm.owner)

			reqBody.SetOwnerID(&idv2)
		}

		{ // expiration epoch
			var expU64 uint64

			prm.exp.WriteToUint64(&expU64)

			reqBody.SetExpiration(expU64)
		}
	}

	var (
		err error
		req v2session.CreateRequest
	)

	{ // construct the request
		if err = prepareRequest(&req, prm, func(r requestInterface) {
			r.(*v2session.CreateRequest).SetBody(&reqBody)
		}); err != nil {
			return err
		}
	}

	var rpcRes rpcapi.CreateSessionRes

	{ // exec RPC
		var rpcPrm rpcapi.CreateSessionPrm

		rpcPrm.SetRequest(req)

		err = rpcapi.CreateSession(ctx, x.c, rpcPrm, &rpcRes)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}
	}

	var (
		resp = rpcRes.Response()
		body *v2session.CreateResponseBody
	)

	{ // verify the response
		if body = resp.GetBody(); body == nil {
			// some sort of selfishness because NeoFS API does not tell "MUST NOT be null" and perhaps it would be worth
			return errMalformedResponse
		}

		if err = verifyResponseSignature(&resp); err != nil {
			return err
		}
	}

	{ // set results
		res.pubkey = body.GetSessionKey()
		res.id = body.GetID()
	}

	return nil
}

// check checks if all required parameters are set and panics if not.
func (x CreateSessionKeyPrm) checkInputs(ctx context.Context, res *CreateSessionKeyRes) {
	x.commonPrm.checkInputs(ctx)

	switch {
	case res == nil:
		panic("nil result")
	case !x.ownerSet:
		panic("account ID is required")
	}
}

// SetOwner sets account identifier to bind the session.
//
// Required parameter.
func (x *CreateSessionKeyPrm) SetOwner(id owner.ID) {
	x.ownerSet = true
	x.owner = id
}

// SetExp sets last epoch of session key lifetime. Node should store the private session key up to the limit.
func (x *CreateSessionKeyPrm) SetExp(exp neofsnetwork.Epoch) {
	x.exp = exp
}

// PublicKey returns public session key in a binary format.
//
// Result is free to be mutated.
func (x CreateSessionKeyRes) PublicKey() []byte {
	return x.pubkey
}

// ID returns opened session identifier. It can be used as token ID to link the token to the session.
//
// Result is free to be mutated.
func (x CreateSessionKeyRes) ID() []byte {
	return x.id
}
