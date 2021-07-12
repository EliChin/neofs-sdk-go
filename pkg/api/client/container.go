package neofsapiclient

import (
	"context"
	"fmt"

	v2container "github.com/nspcc-dev/neofs-api-go/v2/container"
	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	rpcapi "github.com/nspcc-dev/neofs-api-go/v2/rpc"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/refs"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/session"
)

// PutContainerPrm groups the parameters of Client.PutContainer operation.
type PutContainerPrm struct {
	commonPrmWithSession

	cnrSet    bool
	container container.Container

	sigSet    bool
	signature refs.Signature
}

// PutContainerRes groups the results of Client.PutContainer operation.
type PutContainerRes struct {
	id cid.ID
}

// PutContainer requests to store the container in NeoFS network. The container is written asynchronously, the absence
// of errors does not guarantee that the container will be saved. You can check the appearance using read operations.
//
// All required parameters must be set. Result must not be nil.
//
// Context is used for network communication. To set the timeout, use context.WithTimeout or context.WithDeadline.
// It must not be nil.
func (x Client) PutContainer(ctx context.Context, prm PutContainerPrm, res *PutContainerRes) error {
	// prelim checks
	prm.checkInputs(ctx, res)

	var reqBody v2container.PutRequestBody

	{ // construct the request body
		{ // container
			var cnrv2 v2container.Container

			prm.container.WriteToV2(&cnrv2)

			reqBody.SetContainer(&cnrv2)
		}

		{ // signature
			var sigv2 v2refs.Signature

			refs.SignatureToV2(&sigv2, prm.signature)

			reqBody.SetSignature(&sigv2)
		}
	}

	var (
		err error
		req v2container.PutRequest
	)

	{ // construct the request
		if err = prepareRequest(&req, prm, func(r requestInterface) {
			r.(*v2container.PutRequest).SetBody(&reqBody)
		}); err != nil {
			return err
		}
	}

	var rpcRes rpcapi.PutContainerRes

	{ // exec RPC
		var rpcPrm rpcapi.PutContainerPrm

		rpcPrm.SetRequest(req)

		err = rpcapi.PutContainer(ctx, x.c, rpcPrm, &rpcRes)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}
	}

	var (
		resp = rpcRes.Response()
		body *v2container.PutResponseBody
		idv2 *v2refs.ContainerID
	)

	{ // verify the response
		if body = resp.GetBody(); body == nil {
			// some sort of selfishness because NeoFS API does not tell "MUST NOT be null" and perhaps it would be worth
			return errMalformedResponse
		}

		if idv2 = body.GetContainerID(); idv2 == nil {
			// some sort of selfishness because NeoFS API does not tell "MUST NOT be null" and perhaps it would be worth
			return errMalformedResponse
		}

		if err = verifyResponseSignature(&resp); err != nil {
			return err
		}
	}

	{ // set results
		res.id.FromV2(*idv2)
	}

	return nil
}

// check checks if all required parameters are set and panics if not.
func (x PutContainerPrm) checkInputs(ctx context.Context, res *PutContainerRes) {
	x.commonPrm.checkInputs(ctx)

	switch {
	case res == nil:
		panic("nil result")
	case !x.cnrSet:
		panic("container is required")
	case !x.sigSet:
		panic("container signature is required")
	}
}

// SetContainer sets structured information about new container.
//
// Required parameter.
func (x *PutContainerPrm) SetContainer(cnr container.Container) {
	// thought we could instead provide
	//
	// func (x *PutContainerPrm) AccessContainer(f func(*container.Container)) {
	//   f(&x.container)
	// }
	//
	// It would be more convenient if we want to compose the container from the pieces or from the message.
	// With this approach current function can be implemented:
	//
	// func SetContainer(p *PutContainerPrm, cnr container.Container) {
	//   p.AccessContainer(func(cnrp *container.Container) { *cnrp = cnr })
	// }
	x.cnrSet = true
	x.container = cnr
}

// TODO: need a function to calculate the signature.

// SetSignature sets signature of the container structure in a protobuf binary format.
//
// Required parameter.
func (x *PutContainerPrm) SetSignature(sig refs.Signature) {
	x.sigSet = true
	x.signature = sig
}

// ID returns identifier of the processing container. It can be used to observe the appearance of a container.
//
// Result is free to be mutated.
func (x PutContainerRes) ID() cid.ID {
	return x.id
}

// GetContainerPrm groups the parameters of Client.GetContainer operation.
type GetContainerPrm struct {
	commonPrm

	idSet bool
	id    cid.ID
}

// GetContainerRes groups the results of Client.GetContainer operation.
type GetContainerRes struct {
	cnr container.Container

	withToken bool
	token     session.Token

	withSignature bool
	signature     refs.Signature
}

// GetContainer reads the container from the NeoFS network.
//
// All required parameters must be set. Result must not be nil.
//
// Context is used for network communication. To set the timeout, use context.WithTimeout or context.WithDeadline.
// It must not be nil.
func (x Client) GetContainer(ctx context.Context, prm GetContainerPrm, res *GetContainerRes) error {
	// prelim checks
	prm.checkInputs(ctx, res)

	var reqBody v2container.GetRequestBody

	{ // construct the request body
		{ // container ID
			var idv2 v2refs.ContainerID

			cid.IDToV2(&idv2, prm.id)

			reqBody.SetContainerID(&idv2)
		}
	}

	var (
		err error
		req v2container.GetRequest
	)

	{ // construct the request
		if err = prepareRequest(&req, prm, func(r requestInterface) {
			r.(*v2container.GetRequest).SetBody(&reqBody)
		}); err != nil {
			return err
		}
	}

	var rpcRes rpcapi.GetContainerRes

	{ // exec RPC
		var rpcPrm rpcapi.GetContainerPrm

		rpcPrm.SetRequest(req)

		err = rpcapi.GetContainer(ctx, x.c, rpcPrm, &rpcRes)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}
	}

	var (
		resp = rpcRes.Response()
		body *v2container.GetResponseBody
		cnr  *v2container.Container
	)

	{ // verify the response
		body = resp.GetBody()
		if body == nil {
			// some sort of selfishness because NeoFS API does not tell "MUST NOT be null"
			return errMalformedResponse
		}

		cnr = body.GetContainer()
		if cnr == nil {
			// some sort of selfishness because NeoFS API does not tell "MUST NOT be null"
			return errMalformedResponse
		}

		if err = verifyResponseSignature(&resp); err != nil {
			return err
		}
	}

	{ // set results
		{ // container
			res.cnr.FromV2(*cnr)
		}

		{ // session token
			tokv2 := body.GetSessionToken()

			res.withToken = tokv2 != nil
			if res.withToken {
				res.token.FromV2(*tokv2)
			}
		}

		{ // signature
			sigv2 := body.GetSignature()

			res.withSignature = sigv2 != nil
			if res.withSignature {
				refs.SignatureFromV2(&res.signature, *sigv2)
			}
		}
	}

	return nil
}

// check checks if all required parameters are set and panics if not.
func (x GetContainerPrm) checkInputs(ctx context.Context, res *GetContainerRes) {
	x.commonPrm.checkInputs(ctx)

	switch {
	case res == nil:
		panic("nil result")
	case !x.idSet:
		panic("container ID is required")
	}
}

// SetID sets identifier of the container to be read.
//
// Required parameter. Parameter must not be mutated before completing the operation.
func (x *GetContainerPrm) SetID(id cid.ID) {
	x.id = id
	x.idSet = true
}

// Container returns structured information about requested container.
func (x GetContainerRes) Container() container.Container {
	return x.cnr
}

// WithSession checks whether the server returned token of the session within which the container was created.
func (x GetContainerRes) WithSession() bool {
	return x.withToken
}

// SessionToken returns session token.
//
// Makes sense only if WithSession returns true.
func (x GetContainerRes) SessionToken() session.Token {
	return x.token
}

// WithSignature checks whether the server returned signature of the container in a protobuf binary format.
func (x GetContainerRes) WithSignature() bool {
	return x.withToken
}

// TODO: need a function to verify the signature.

// Signature returns signature of the container in a protobuf binary format.
//
// Makes sense only if WithSignature returns true.
func (x GetContainerRes) Signature() refs.Signature {
	return x.signature
}

// DeleteContainerPrm groups the parameters of Client.DeleteContainer operation.
type DeleteContainerPrm struct {
	commonPrmWithSession

	idSet bool
	id    cid.ID

	sigSet    bool
	signature refs.Signature
}

// DeleteContainerRes groups the results of Client.DeleteContainer operation.
type DeleteContainerRes struct{}

// DeleteContainer requests to remove the container from the NeoFS network. The container is removed asynchronously, the
// absence of errors does not guarantee that the container will be removed. You can check the absence using read
// operations.
//
// All required parameters must be set. Result is ignored and can be nil.
//
// Context is used for network communication. To set the timeout, use context.WithTimeout or context.WithDeadline.
// It must not be nil.
func (x Client) DeleteContainer(ctx context.Context, prm DeleteContainerPrm, res *DeleteContainerRes) error {
	// prelim checks
	prm.checkInputs(ctx, res)

	var reqBody v2container.DeleteRequestBody

	{ // construct the request body
		{ // container ID
			var idv2 v2refs.ContainerID

			cid.IDToV2(&idv2, prm.id)

			reqBody.SetContainerID(&idv2)
		}

		{ // signature
			var sigv2 v2refs.Signature

			refs.SignatureToV2(&sigv2, prm.signature)

			reqBody.SetSignature(&sigv2)
		}
	}

	var (
		err error
		req v2container.DeleteRequest
	)

	{ // construct the request
		if err = prepareRequest(&req, prm, func(r requestInterface) {
			r.(*v2container.DeleteRequest).SetBody(&reqBody)
		}); err != nil {
			return err
		}
	}

	var rpcRes rpcapi.DeleteContainerRes

	{ // exec RPC
		var rpcPrm rpcapi.DeleteContainerPrm

		rpcPrm.SetRequest(req)

		err = rpcapi.DeleteContainer(ctx, x.c, rpcPrm, &rpcRes)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}
	}

	var resp = rpcRes.Response()

	{ // verify the response
		if body := resp.GetBody(); body == nil {
			// some sort of selfishness because NeoFS API does not tell "MUST NOT be null" and perhaps it would be worth
			return errMalformedResponse
		}

		if err = verifyResponseSignature(&resp); err != nil {
			return err
		}
	}

	{ // set results
	}

	return nil
}

// check checks if all required parameters are set and panics if not.
func (x DeleteContainerPrm) checkInputs(ctx context.Context, _ *DeleteContainerRes) {
	x.commonPrm.checkInputs(ctx)

	switch {
	case !x.idSet:
		panic("container ID is required")
	case !x.sigSet:
		panic("container ID signature is required")
	}
}

// SetID sets identifier of the container to be removed.
//
// Required parameter. Parameter must not be mutated before completing the operation.
func (x *DeleteContainerPrm) SetID(id cid.ID) {
	x.id = id
	x.idSet = true
}

// TODO: need a function to calculate the signature.

// SetSignature sets signature of the container ID bytes.
//
// Required parameter.
func (x *DeleteContainerPrm) SetSignature(sig refs.Signature) {
	x.sigSet = true
	x.signature = sig
}
