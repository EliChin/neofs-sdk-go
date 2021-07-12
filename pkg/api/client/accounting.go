package neofsapiclient

import (
	"context"
	"fmt"

	v2accounting "github.com/nspcc-dev/neofs-api-go/v2/accounting"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	rpcapi "github.com/nspcc-dev/neofs-api-go/v2/rpc"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/accounting"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/owner"
)

// AccountBalancePrm groups the parameters of Client.AccountBalance operation.
type AccountBalancePrm struct {
	commonPrm

	ownerSet bool
	owner    owner.ID
}

// AccountBalanceRes groups the results of Client.AccountBalance operation.
type AccountBalanceRes struct {
	fundNum accounting.Decimal
}

// AccountBalance requests the current balance of the NeoFS account.
//
// All required parameters must be set. Result must not be nil.
//
// Context is used for network communication. To set the timeout, use context.WithTimeout or context.WithDeadline.
// It must not be nil.
func (x Client) AccountBalance(ctx context.Context, prm AccountBalancePrm, res *AccountBalanceRes) error {
	// prelim checks
	prm.checkInputs(ctx, res)

	var reqBody v2accounting.BalanceRequestBody

	{ // construct the request body
		{ // account ID
			var idv2 refs.OwnerID

			owner.IDToV2(&idv2, prm.owner)

			reqBody.SetOwnerID(&idv2)
		}
	}

	var (
		err error
		req v2accounting.BalanceRequest
	)

	{ // construct the request
		if err = prepareRequest(&req, prm, func(r requestInterface) {
			r.(*v2accounting.BalanceRequest).SetBody(&reqBody)
		}); err != nil {
			return err
		}
	}

	var rpcRes rpcapi.BalanceRes

	{ // exec RPC
		var rpcPrm rpcapi.BalancePrm

		rpcPrm.SetRequest(req)

		err = rpcapi.Balance(ctx, x.c, rpcPrm, &rpcRes)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}
	}

	var (
		resp = rpcRes.Response()
		body *v2accounting.BalanceResponseBody
		bal  *v2accounting.Decimal
	)

	{ // verify the response
		if body = resp.GetBody(); body == nil {
			// some sort of selfishness because NeoFS API does not tell "MUST NOT be null" and perhaps it would be worth
			return errMalformedResponse
		}

		if bal = body.GetBalance(); bal == nil {
			// some sort of selfishness because NeoFS API does not tell "MUST NOT be null" and perhaps it would be worth
			return errMalformedResponse
		}

		if err = verifyResponseSignature(&resp); err != nil {
			return err
		}
	}

	{ // set results
		accounting.DecimalFromV2(&res.fundNum, *bal)
	}

	return nil
}

// checkInputs checks if all inputs are correctly set and panics if not.
func (x AccountBalancePrm) checkInputs(ctx context.Context, res *AccountBalanceRes) {
	x.commonPrm.checkInputs(ctx)

	switch {
	case res == nil:
		panic("nil result")
	case !x.ownerSet:
		panic("account ID is required")
	}
}

// SetOwner sets account identifier to get the balance.
//
// Required parameter.
func (x *AccountBalancePrm) SetOwner(id owner.ID) {
	x.ownerSet = true
	x.owner = id
}

// NumberOfFunds returns current number of funds on the account.
func (x AccountBalanceRes) NumberOfFunds() accounting.Decimal {
	return x.fundNum
}
