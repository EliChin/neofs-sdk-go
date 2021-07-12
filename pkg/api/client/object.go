package neofsapiclient

import (
	"context"
	"errors"
	"fmt"
	"io"

	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	rpcapi "github.com/nspcc-dev/neofs-api-go/v2/rpc"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/object/id"
)

// PutObjectPrm groups the parameters of Client.PutObject operation.
type PutObjectPrm struct {
	commonPrmWithTokens
}

// PutObjectRes groups the results of Client.PutObject operation.
type PutObjectRes struct {
	stream PutObjectStream
}

// PutObjectStream is a tool to stream the object to the NeoFS network.
type PutObjectStream struct {
	called bool

	prm commonPrmWithTokens

	v2stream rpcapi.PutObjectStream

	reqInit v2object.PutObjectPartInit
}

// WriteObjectHeaderPrm groups the parameters of PutObjectStream.WriteHeader operation.
type WriteObjectHeaderPrm struct {
	hdrSet bool
	hdr    object.HeaderWithIDAndSignature
}

// SetHeader sets object information except payload.
//
// Required parameter.
func (x *WriteObjectHeaderPrm) SetHeader(h object.HeaderWithIDAndSignature) {
	x.hdr = h
	x.hdrSet = true
}

// WriteObjectHeaderRes groups the results of PutObjectStream.WriteHeader operation.
type WriteObjectHeaderRes struct {
	payloadStream WritePayloadStream
}

// WritePayloadStream returns payload stream instance.
func (x WriteObjectHeaderRes) PayloadStream() WritePayloadStream {
	return x.payloadStream
}

// WriteHeader initializes stream with writing the object header. Must be called once.
// To write the payload res.PayloadStream() can be used.
//
// All required parameters must be set. Result must not be nil.
func (x *PutObjectStream) WriteHeader(prm WriteObjectHeaderPrm, res *WriteObjectHeaderRes) error {
	switch {
	case !prm.hdrSet:
		panic("header required")
	case x.called:
		panic("re-call detected")
	}

	x.called = true

	{ // construct init part
		var (
			idv2 *v2refs.ObjectID
			hv2  *v2object.Header
			sv2  *v2refs.Signature
		)

		if prm.hdr.WithID() {
			idv2 = new(v2refs.ObjectID)
		}

		if prm.hdr.WithHeader() {
			hv2 = new(v2object.Header)
		}

		if prm.hdr.WithSignature() {
			sv2 = new(v2refs.Signature)
		}

		prm.hdr.WriteToV2(hv2, sv2, idv2)

		x.reqInit.SetObjectID(idv2)
		x.reqInit.SetHeader(hv2)
		x.reqInit.SetSignature(sv2)
	}

	err := sendObjectPutRequest(&x.reqInit, x.prm, x.v2stream)
	if err != nil {
		return err
	}

	res.payloadStream.prm = x.prm
	res.payloadStream.v2stream = x.v2stream

	return nil
}

// WritePayloadStream provides the interface to stream the payload chunks.
//
// Implements io.WriterCloser.
type WritePayloadStream struct {
	prm commonPrmWithTokens

	v2stream rpcapi.PutObjectStream

	reqChunk v2object.PutObjectPartChunk

	id oid.ID
}

func (x *WritePayloadStream) Write(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	var (
		err error

		written, chunkLen int
	)

	for {
		const maxChunkLen = 3 * 1 << 20 // 3MB

		if chunkLen = len(buf); chunkLen > maxChunkLen {
			chunkLen = maxChunkLen
		}

		{ // construct chunk part
			x.reqChunk.SetChunk(buf[:chunkLen])
		}

		err = sendObjectPutRequest(&x.reqChunk, x.prm, x.v2stream)
		if err != nil {
			return written, err
		}

		written += chunkLen

		// update the buffer
		buf = buf[chunkLen:]
		if len(buf) == 0 {
			break
		}
	}

	return written, nil
}

// Close closes the stream. Should be called once after which there should be no write operations.
// ID of the saved object can be received using ID().
func (x *WritePayloadStream) Close() error {
	if err := x.v2stream.CloseSend(); err != nil {
		return err
	}

	resp := x.v2stream.Response()

	body := resp.GetBody()
	if body == nil {
		// some sort of selfishness because NeoFS API does not tell "MUST NOT be null" and perhaps it would be worth
		return errMalformedResponse
	}

	idv2 := body.GetObjectID()
	if idv2 == nil {
		// some sort of selfishness because NeoFS API does not tell "MUST NOT be null" and perhaps it would be worth
		return errMalformedResponse
	}

	x.id.FromV2(*idv2)

	return nil
}

// ID returns identifier of the saved object. Should be called after the successful Close.
func (x WritePayloadStream) ID() oid.ID {
	return x.id
}

func sendObjectPutRequest(
	prt v2object.PutObjectPart,
	prm commonPrmWithTokens,
	v2stream rpcapi.PutObjectStream,
) error {
	var reqBody v2object.PutRequestBody

	reqBody.SetObjectPart(prt)

	var (
		err error
		req v2object.PutRequest
	)

	{ // construct the request
		if err = prepareRequest(&req, prm, func(r requestInterface) {
			r.(*v2object.PutRequest).SetBody(&reqBody)
		}); err != nil {
			return err
		}
	}

	{ // send the request
		if err = v2stream.Write(req); err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}
	}

	return nil
}

// PutObject requests to store the object in NeoFS network. After the successful call object stream is opened
// and can be used to save the object in parts.
//
// All required parameters must be set. Result must not be nil.
//
// Context is used for network communication. To set the timeout, use context.WithTimeout or context.WithDeadline.
// It must not be nil.
func (x Client) PutObject(ctx context.Context, prm PutObjectPrm, res *PutObjectRes) error {
	// prelim checks
	prm.checkInputs(ctx, res)

	var (
		err    error
		rpcRes rpcapi.PutObjectRes
	)

	{ // exec RPC
		var rpcPrm rpcapi.PutObjectPrm

		err = rpcapi.PutObject(ctx, x.c, rpcPrm, &rpcRes)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}
	}

	{ // set results
		res.stream.v2stream = rpcRes.Stream()
		res.stream.prm = prm.commonPrmWithTokens
	}

	return nil
}

// check checks if all required parameters are set and panics if not.
func (x PutObjectPrm) checkInputs(ctx context.Context, res *PutObjectRes) {
	x.commonPrm.checkInputs(ctx)

	if res == nil {
		panic("nil result")
	}
}

// Stream returns initialized PutObjectStream.
func (x PutObjectRes) Stream() PutObjectStream {
	return x.stream
}

// GetObjectPrm groups the parameters of Client.GetObject operation.
type GetObjectPrm struct {
	commonPrmWithTokens

	withAddr bool
	addr     oid.Address
}

// GetObjectRes groups the results of Client.GetObject operation.
type GetObjectRes struct {
	stream GetObjectStream
}

// GetObjectStream is a tool to read the object from to the NeoFS network.
type GetObjectStream struct {
	called bool

	v2stream *rpcapi.GetObjectStream

	respInit v2object.GetObjectPartInit
	resp     v2object.GetResponse
}

// ReadObjectHeaderPrm groups the parameters of GetObjectStream.ReadHeader operation.
type ReadObjectHeaderPrm struct{}

// ReadObjectHeaderRes groups the results of GetObjectStream.ReadObject operation.
type ReadObjectHeaderRes struct {
	hdr object.HeaderWithIDAndSignature

	payloadStream ReadPayloadStream
}

// Header returns sets object information except payload.
func (x ReadObjectHeaderRes) Header() object.HeaderWithIDAndSignature {
	return x.hdr
}

// PayloadStream returns payload stream instance.
func (x ReadObjectHeaderRes) PayloadStream() ReadPayloadStream {
	return x.payloadStream
}

// ReadHeader initializes stream with reading the object header. Must be called once.
// To read the payload res.PayloadStream() can be used.
//
// All required parameters must be set. Result must not be nil.
func (x *GetObjectStream) ReadHeader(_ ReadObjectHeaderPrm, res *ReadObjectHeaderRes) error {
	if x.called {
		panic("re-call detected")
	}

	x.called = true

	{ // read init part
		if fin, err := receiveObjectGetResponse(&x.resp, x.v2stream); err != nil {
			return err
		} else if fin {
			return io.ErrUnexpectedEOF
		}
	}

	res.hdr.FromV2(x.respInit.GetHeader(), x.respInit.GetSignature(), x.respInit.GetObjectID())

	res.payloadStream.expectedLen = res.hdr.PayloadLength()
	res.payloadStream.v2stream = x.v2stream

	var (
		respChunk v2object.GetObjectPartChunk
		respBody  v2object.GetResponseBody
	)

	respBody.SetObjectPart(&respChunk)

	res.payloadStream.resp.SetBody(&respBody)

	return nil
}

// ReadPayloadStream provides the interface to read the payload chunks.
//
// Implements io.Reader.
type ReadPayloadStream struct {
	expectedLen, read uint64

	v2stream *rpcapi.GetObjectStream

	resp v2object.GetResponse

	tail []byte
}

func (x *ReadPayloadStream) Read(p []byte) (read int, err error) {
	defer func() {
		x.read += uint64(read)

		if err == io.EOF && x.read < x.expectedLen {
			err = io.ErrUnexpectedEOF
		}
	}()

	// read remaining tail
	read = copy(p, x.tail)

	x.tail = x.tail[read:]

	if len(p)-read == 0 {
		return
	}

	var fin bool
	if fin, err = receiveObjectGetResponse(&x.resp, x.v2stream); err != nil {
		return
	} else if fin {
		err = io.EOF
		return
	}

	// receive message from server stream
	err = x.v2stream.Read(&x.resp)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.EOF
			return
		}

		err = fmt.Errorf("reading the response failed: %w", err)

		return
	}

	// get chunk part message
	part := x.resp.GetBody().GetObjectPart()

	chunkPart, ok := part.(*v2object.GetObjectPartChunk)
	if !ok {
		err = errors.New("wrong message sequence")
		return
	}

	// read new chunk
	chunk := chunkPart.GetChunk()

	tailOffset := copy(p[read:], chunk)

	read += tailOffset

	// save the tail
	x.tail = append(x.tail, chunk[tailOffset:]...)

	return
}

// returns true if stream is finished.
func receiveObjectGetResponse(resp *v2object.GetResponse, v2stream *rpcapi.GetObjectStream) (bool, error) {
	var err error

	if err = v2stream.Read(resp); err != nil {
		if errors.Is(err, io.EOF) {
			return true, nil
		}

		return false, fmt.Errorf("rpc error: %w", err)
	}

	{ // verify the response
		if err = verifyResponseSignature(resp); err != nil {
			return false, err
		}
	}

	return false, nil
}

// GetObject requests to read the object from NeoFS network. After the successful call object stream is opened
// and can be used to receive the object in parts.
//
// All required parameters must be set. Result must not be nil.
//
// Context is used for network communication. To set the timeout, use context.WithTimeout or context.WithDeadline.
// It must not be nil.
func (x Client) GetObject(ctx context.Context, prm GetObjectPrm, res *GetObjectRes) error {
	// prelim checks
	prm.checkInputs(ctx, res)

	var reqBody v2object.GetRequestBody

	{ // construct the request body
		{ // address
			var idv2 v2refs.Address

			prm.addr.WriteToV2(&idv2)

			reqBody.SetAddress(&idv2)
		}
	}

	var (
		err error
		req v2object.GetRequest
	)

	{ // construct the request
		if err = prepareRequest(&req, prm, func(r requestInterface) {
			r.(*v2object.GetRequest).SetBody(&reqBody)
		}); err != nil {
			return err
		}
	}

	var rpcRes rpcapi.GetObjectRes

	{ // exec RPC
		var rpcPrm rpcapi.GetObjectPrm

		rpcPrm.SetRequest(req)

		err = rpcapi.GetObject(ctx, x.c, rpcPrm, &rpcRes)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}
	}

	{ // set results
		objStream := rpcRes.Stream()
		res.stream.v2stream = &objStream

		var (
			respInit v2object.GetObjectPartInit
			respBody v2object.GetResponseBody
		)

		respBody.SetObjectPart(&respInit)

		res.stream.resp.SetBody(&respBody)
	}

	return nil
}

// check checks if all required parameters are set and panics if not.
func (x GetObjectPrm) checkInputs(ctx context.Context, res *GetObjectRes) {
	x.commonPrm.checkInputs(ctx)

	switch {
	case res == nil:
		panic("nil result")
	case !x.withAddr:
		panic("object address required")
	}
}

// SetObject sets object identifier to be read.
//
// Required parameter.
func (x *GetObjectPrm) SetObject(addr oid.Address) {
	x.addr = addr
	x.withAddr = true
}

// Stream returns initialized GetObjectStream.
func (x GetObjectRes) Stream() GetObjectStream {
	return x.stream
}
