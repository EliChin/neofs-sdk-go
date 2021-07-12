package object

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"

	cryptoalgo "github.com/nspcc-dev/neofs-api-go/crypto/algo"
	neofsecdsa "github.com/nspcc-dev/neofs-api-go/crypto/ecdsa"
	apicrypto "github.com/nspcc-dev/neofs-api-go/v2/crypto"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	v2session "github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	cid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/container/id"
	neofsnetwork "github.com/nspcc-dev/neofs-sdk-go/pkg/api/network"
	oid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/owner"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/refs"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/session"
)

// allows to share fields between MainHeader and Header.
type mainHeader struct {
	withVersion bool
	version     refs.Version

	creatEpoch neofsnetwork.Epoch

	withOwner bool
	owner     owner.ID

	typ Type

	payloadLen uint64

	withPayloadChecksum bool
	payloadChecksum     refs.Checksum

	withPayloadHomo bool
	payloadHomo     refs.Checksum
}

// common interface of refs.ShortHeader and refs.Header messages.
type shortHeaderFields interface {
	GetVersion() *v2refs.Version
	SetVersion(*v2refs.Version)

	GetCreationEpoch() uint64
	SetCreationEpoch(uint64)

	GetOwnerID() *v2refs.OwnerID
	SetOwnerID(*v2refs.OwnerID)

	GetObjectType() object.Type
	SetObjectType(object.Type)

	GetPayloadLength() uint64
	SetPayloadLength(uint64)

	GetPayloadHash() *v2refs.Checksum
	SetPayloadHash(*v2refs.Checksum)

	GetHomomorphicHash() *v2refs.Checksum
	SetHomomorphicHash(*v2refs.Checksum)
}

func (x *mainHeader) fromV2(shf shortHeaderFields) {
	{ // version
		vv2 := shf.GetVersion()

		x.withVersion = vv2 != nil
		if x.withVersion {
			x.version.FromV2(*vv2)
		}
	}

	{ // owner
		idv2 := shf.GetOwnerID()

		x.withOwner = idv2 != nil
		if x.withOwner {
			x.owner.FromV2(*idv2)
		}
	}

	{ // payload checksum
		csv2 := shf.GetPayloadHash()

		x.withPayloadChecksum = csv2 != nil
		if x.withPayloadChecksum {
			x.payloadChecksum.FromV2(*csv2)
		}
	}

	{ // payload homomorphic checksum
		csv2 := shf.GetHomomorphicHash()

		x.withPayloadHomo = csv2 != nil
		if x.withPayloadHomo {
			x.payloadHomo.FromV2(*csv2)
		}
	}

	x.creatEpoch.FromUint64(shf.GetCreationEpoch())
	x.typ.fromV2(shf.GetObjectType())
	x.payloadLen = shf.GetPayloadLength()
}

func (x *mainHeader) writeToV2(shf shortHeaderFields) {
	{ // version
		var vv2 *v2refs.Version

		if x.withVersion {
			vv2 = shf.GetVersion()
			if vv2 == nil {
				vv2 = new(v2refs.Version)
			}

			x.version.WriteToV2(vv2)
		}

		shf.SetVersion(vv2)
	}

	{ // creation epoch
		var u64 uint64

		x.creatEpoch.WriteToUint64(&u64)

		shf.SetCreationEpoch(u64)
	}

	{ // container
		var idv2 *v2refs.OwnerID

		if x.withOwner {
			idv2 = shf.GetOwnerID()
			if idv2 == nil {
				idv2 = new(v2refs.OwnerID)
			}

			owner.IDToV2(idv2, x.owner)
		}

		shf.SetOwnerID(idv2)
	}

	{ // type
		var tv2 object.Type

		x.typ.writeToV2(&tv2)

		shf.SetObjectType(tv2)
	}

	{ // payload checksum
		var csv2 *v2refs.Checksum

		if x.withPayloadChecksum {
			csv2 = shf.GetPayloadHash()
			if csv2 == nil {
				csv2 = new(v2refs.Checksum)
			}

			x.payloadChecksum.WriteToV2(csv2)
		}

		shf.SetPayloadHash(csv2)
	}

	{ // payload homomorphic checksum
		var csv2 *v2refs.Checksum

		if x.withPayloadHomo {
			csv2 = shf.GetHomomorphicHash()
			if csv2 == nil {
				csv2 = new(v2refs.Checksum)
			}

			x.payloadHomo.WriteToV2(csv2)
		}

		shf.SetHomomorphicHash(csv2)
	}

	shf.SetPayloadLength(x.payloadLen)
}

// WithVersion checks if object protocol version was specified.
func (x mainHeader) WithVersion() bool {
	return x.withVersion
}

// Version returns protocol version within which object was formed.
//
// Makes sense only if WithVersion returns true.
func (x mainHeader) Version() refs.Version {
	return x.version
}

// SetVersion sets protocol version within which object was formed.
func (x *mainHeader) SetVersion(version refs.Version) {
	x.version = version
	x.withVersion = true
}

// CreationEpoch returns epoch when object was formed.
func (x mainHeader) CreationEpoch() neofsnetwork.Epoch {
	return x.creatEpoch
}

// SetCreationEpoch sets epoch when object was formed.
func (x *mainHeader) SetCreationEpoch(epoch neofsnetwork.Epoch) {
	x.creatEpoch = epoch
}

// WithOwner checks if object protocol version was specified.
func (x mainHeader) WithOwner() bool {
	return x.withOwner
}

// Owner returns object owner's ID.
//
// Makes sense only if WithOwner returns true.
//
// Result mutation affects the header.
func (x mainHeader) Owner() owner.ID {
	return x.owner
}

// SetOwner sets object owner's ID.
//
// Parameter mutation affects the header.
func (x *mainHeader) SetOwner(id owner.ID) {
	x.owner = id
	x.withOwner = true
}

// Type returns object type.
func (x mainHeader) Type() Type {
	return x.typ
}

// Type sets object type.
func (x *mainHeader) SetType(t Type) {
	x.typ = t
}

// PayloadLength returns object payload length.
func (x mainHeader) PayloadLength() uint64 {
	return x.payloadLen
}

// SetPayloadLength sets object payload length.
func (x *mainHeader) SetPayloadLength(ln uint64) {
	x.payloadLen = ln
}

// WithPayloadChecksum checkis if object payload checksum was specified.
func (x mainHeader) WithPayloadChecksum() bool {
	return x.withPayloadChecksum
}

// PayloadChecksum returns object payload checksum.
//
// Makes sense only if WithPayloadChecksum returns true.
func (x mainHeader) PayloadChecksum() refs.Checksum {
	return x.payloadChecksum
}

// SetPayloadChecksum sets object payload checksum.
func (x *mainHeader) SetPayloadChecksum(cs refs.Checksum) {
	x.payloadChecksum = cs
	x.withPayloadChecksum = true
}

// WithHomomorphicPayloadChecksum checkis if homomorphic object payload checksum was specified.
func (x mainHeader) WithHomomorphicPayloadChecksum() bool {
	return x.withPayloadHomo
}

// HomomorphicPayloadChecksum returns homomorphic object payload checksum.
//
// Makes sense only if WithHomomorphicPayloadChecksum returns true.
func (x mainHeader) HomomorphicPayloadChecksum() refs.Checksum {
	return x.payloadHomo
}

// SetHomomorphicPayloadChecksum sets homomorphic object payload checksum.
func (x *mainHeader) SetHomomorphicPayloadChecksum(cs refs.Checksum) {
	x.payloadHomo = cs
	x.withPayloadHomo = true
}

// MainHeader represents NeoFS API V2-compatible abbreviated field object header.
type MainHeader struct {
	mainHeader
}

// FromShortV2 reads MainHeader from object.ShortHeader message.
func (x *MainHeader) FromV2(shv2 object.ShortHeader) {
	x.mainHeader.fromV2(&shv2)
}

// WriteToV2 writes MainHeader data to object.Header message.
//
// Message must not be nil.
func (x MainHeader) WriteToV2(shv2 *object.ShortHeader) {
	x.mainHeader.writeToV2(shv2)
}

// Header represents NeoFS API V2-compatible object full header.
type Header struct {
	header
}

// allows to share fields between Header, HeaderWithSignature and HeaderWithIDAndSignature.
type header struct {
	mainHeader

	withContainer bool
	container     cid.ID

	withToken bool
	token     session.Token

	attributes Attributes

	withSplit bool

	splitID SplitID

	withPrev bool
	prev     oid.ID

	children oid.IDs

	parent *HeaderWithIDAndSignature
}

// fromV2 reads header from object.ShortHeader message.
func (x *header) fromV2(hv2 object.Header) {
	x.mainHeader.fromV2(&hv2)

	{ // container
		idv2 := hv2.GetContainerID()

		x.withContainer = idv2 != nil
		if x.withContainer {
			x.container.FromV2(*idv2)
		}
	}

	{ // session token
		tv2 := hv2.GetSessionToken()

		x.withToken = tv2 != nil
		if x.withToken {
			x.token.FromV2(*tv2)
		}
	}

	attributesFromV2(&x.attributes, hv2.GetAttributes())

	{ // split
		shv2 := hv2.GetSplit()

		x.withSplit = shv2 != nil
		if x.withSplit {
			{ // previous object
				prv2 := shv2.GetPrevious()

				x.withPrev = prv2 != nil
				if x.withPrev {
					x.prev.FromV2(*prv2)
				}
			}

			{ // parent
				pidv2 := shv2.GetParent()
				psv2 := shv2.GetParentSignature()
				phv2 := shv2.GetParentHeader()

				if pidv2 != nil || psv2 != nil || phv2 != nil {
					if x.parent == nil {
						x.parent = new(HeaderWithIDAndSignature)
					}

					x.parent.FromV2(phv2, psv2, pidv2)
				} else {
					x.parent = nil
				}
			}

			x.splitID.FromV2(shv2.GetSplitID())
			oid.IDsFromV2(&x.children, shv2.GetChildren())
		}
	}
}

// writeToV2 writes header data to object.Header message.
//
// Message must not be nil.
func (x header) writeToV2(hv2 *object.Header) {
	x.mainHeader.writeToV2(hv2)

	{ // container
		var idv2 *v2refs.ContainerID

		if x.withContainer {
			idv2 = hv2.GetContainerID()
			if idv2 == nil {
				idv2 = new(v2refs.ContainerID)
			}

			cid.IDToV2(idv2, x.container)
		}

		hv2.SetContainerID(idv2)
	}

	{ // session token
		var tv2 *v2session.SessionToken

		if x.withToken {
			tv2 = hv2.GetSessionToken()
			if tv2 == nil {
				tv2 = new(v2session.SessionToken)
			}

			x.token.WriteToV2(tv2)
		}

		hv2.SetSessionToken(tv2)
	}

	{ // attributes
		var asv2 []*object.Attribute

		if ln := x.attributes.Len(); ln > 0 {
			asv2 = hv2.GetAttributes()
			if cap(asv2) < ln {
				asv2 = make([]*object.Attribute, 0, ln)
			}

			asv2 = asv2[:ln]

			attributesToV2(asv2, x.attributes)
		}

		hv2.SetAttributes(asv2)
	}

	{ // split
		var shv2 *object.SplitHeader

		if x.withSplit {
			shv2 = hv2.GetSplit()
			if shv2 == nil {
				shv2 = new(object.SplitHeader)
			}

			shv2.SetSplitID(x.splitID.AccessBytes())

			{ // previous object
				var prv2 *v2refs.ObjectID

				if x.withPrev {
					prv2 = shv2.GetPrevious()
					if prv2 == nil {
						prv2 = new(v2refs.ObjectID)
					}

					oid.IDToV2(prv2, x.prev)
				}

				shv2.SetPrevious(prv2)
			}

			{ // children
				var idsv2 []*v2refs.ObjectID

				if ln := x.children.Len(); ln > 0 {
					idsv2 = shv2.GetChildren()

					if cap(idsv2) < ln {
						idsv2 = make([]*v2refs.ObjectID, 0, ln)
					}

					idsv2 = idsv2[:ln]

					oid.IDsToV2(idsv2, x.children)
				}

				shv2.SetChildren(idsv2)
			}

			{ // parent
				var (
					pidv2 *v2refs.ObjectID
					phv2  *object.Header
					psv2  *v2refs.Signature
				)

				if x.parent != nil {
					if x.parent.WithID() {
						if pidv2 = shv2.GetParent(); pidv2 == nil {
							pidv2 = new(v2refs.ObjectID)
						}
					}

					if x.parent.WithSignature() {
						if psv2 = shv2.GetParentSignature(); psv2 == nil {
							psv2 = new(v2refs.Signature)
						}
					}

					if x.parent.WithHeader() {
						if phv2 = shv2.GetParentHeader(); phv2 == nil {
							phv2 = new(object.Header)
						}
					}

					x.parent.WriteToV2(phv2, psv2, pidv2)
				}

				shv2.SetParent(pidv2)
				shv2.SetParentHeader(phv2)
				shv2.SetParentSignature(psv2)
			}
		}

		hv2.SetSplit(shv2)
	}
}

// WithContainer checks if related container was speficied.
func (x header) WithContainer() bool {
	return x.withContainer
}

// Container returns related container identifier.
//
// Makes sense only if WithContainer returns true.
//
// Result mutation affects the header.
func (x header) Container() cid.ID {
	return x.container
}

// SetContainer sets related container identifier.
//
// Parameter mutation affects the header.
func (x *header) SetContainer(id cid.ID) {
	x.container = id
	x.withContainer = true
}

// WithToken checks if session token was specified.
func (x header) WithToken() bool {
	return x.withToken
}

// SessionToken returns token of the session within which object was created.
//
// Makes sense only if WithToken returns true.
func (x header) SessionToken() session.Token {
	return x.token
}

// SetSessionToken sets token of the session within which object was created.
func (x *header) SetSessionToken(token session.Token) {
	x.token = token
	x.withToken = true
}

// Attributes returns returns object attributes.
//
// Result mutation affects the header.
func (x header) Attributes() Attributes {
	return x.attributes
}

// SetAttributes sets returns object attributes.
//
// Parameter mutation affects the header.
func (x *header) SetAttributes(as Attributes) {
	x.attributes = as
}

// WithSplit checks if split fields were specified.
func (x header) WithSplit() bool {
	return x.withSplit
}

func (x *header) setSplitField(f func(*header)) {
	x.withSplit = true
	f(x)
}

// SplitID returns split chain identifier.
//
// Makes sense only if WithSplit returns true.
//
// Result mutation affects the header.
func (x header) SplitID() SplitID {
	return x.splitID
}

// SetSplitID sets split chain identifier.
//
// Parameter mutation affects the header.
func (x *header) SetSplitID(id SplitID) {
	x.setSplitField(func(x *header) {
		x.splitID = id
	})
}

// WithPrevious checks if previous object ID was specified.
func (x header) WithPrevious() bool {
	return x.withPrev
}

// Previous returns identifier of the previous object in split chain.
//
// Makes sense only if WithPrevious returns true.
//
// Result mutation affects the header.
func (x header) Previous() oid.ID {
	return x.prev
}

// SetPrevious sets identifier of the previous object in split chain.
//
// Parameter mutation affects the header.
func (x *header) SetPrevious(id oid.ID) {
	x.setSplitField(func(x *header) {
		x.prev = id
	})

	x.withPrev = true
}

// Children returns identifiers of the child objects in split chain.
//
// Makes sense only if WithSplit returns true.
//
// Result mutation affects the header.
func (x header) Children() oid.IDs {
	return x.children
}

// SetChildren sets identifier of the child objects in split chain.
//
// Parameter mutation affects the header.
func (x *header) SetChildren(ids oid.IDs) {
	x.setSplitField(func(x *header) {
		x.children = ids
	})
}

// WithParent checks if parent object header was specified.
func (x header) WithParent() bool {
	return x.parent != nil
}

// Parent returns parent object header.
//
// Must be called only if WithParent returns true.
func (x header) Parent() HeaderWithIDAndSignature {
	return *x.parent
}

// SetParent sets parent object header.
func (x *header) SetParent(p HeaderWithIDAndSignature) {
	x.setSplitField(func(x *header) {
		x.parent = &p
	})
}

// HeaderWithSignature represents NeoFS API V2-compatible object header and object ID's signature pair.
type HeaderWithSignature struct {
	headerWithSignature
}

// allows to share fields between HeaderWithSignature and headerWithIDAndSignature.
type headerWithSignature struct {
	withHeader bool
	header

	withSignature bool
	signature     v2refs.Signature
}

// FromV2 reads HeaderWithSignature from object.Header and refs.Signature messages.
func (x *HeaderWithSignature) FromV2(hv2 *object.Header, sv2 *v2refs.Signature) {
	x.fromV2(hv2, sv2)
}

func (x *headerWithSignature) fromV2(hv2 *object.Header, sv2 *v2refs.Signature) {
	x.withHeader = hv2 != nil
	if x.withHeader {
		x.header.fromV2(*hv2)
	}

	x.withSignature = sv2 != nil
	if x.withSignature {
		x.signature = *sv2
	}
}

// WriteToV2 writes HeaderWithSignature to object.Header and refs.Signature messages.
//
// Header message is ignored if WithHeader returns false. Otherwise it must not be nil.
// Signature message is ignored if WithSignature returns false. Otherwise it must not be nil.
func (x HeaderWithSignature) WriteToV2(hv2 *object.Header, sv2 *v2refs.Signature) {
	x.writeToV2(hv2, sv2)
}

func (x headerWithSignature) writeToV2(hv2 *object.Header, sv2 *v2refs.Signature) {
	if x.withHeader {
		x.header.writeToV2(hv2)
	}

	if x.withSignature {
		*sv2 = x.signature
	}
}

// WithHeader checks if header fields were specified.
func (x headerWithSignature) WithHeader() bool {
	return x.withHeader
}

// WithSignature checks if signature was specified.
func (x headerWithSignature) WithSignature() bool {
	return x.withSignature
}

func (x *headerWithSignature) setHeaderField(f func(*headerWithSignature)) {
	x.withHeader = true
	f(x)
}

// SetVersion sets protocol version within which object was formed.
func (x *headerWithSignature) SetVersion(version refs.Version) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetVersion(version)
	})
}

// SetCreationEpoch sets epoch when object was formed.
func (x *headerWithSignature) SetCreationEpoch(epoch neofsnetwork.Epoch) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetCreationEpoch(epoch)
	})
}

// SetOwner sets object owner's ID.
//
// Parameter mutation affects the header.
func (x *headerWithSignature) SetOwner(id owner.ID) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetOwner(id)
	})

	x.withOwner = true
}

// Type sets object type.
func (x *headerWithSignature) SetType(t Type) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetType(t)
	})
}

// SetPayloadLength sets object payload length.
func (x *headerWithSignature) SetPayloadLength(ln uint64) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetPayloadLength(ln)
	})
}

// SetPayloadChecksum sets object payload checksum.
func (x *headerWithSignature) SetPayloadChecksum(cs refs.Checksum) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetPayloadChecksum(cs)
	})
}

// SetHomomorphicPayloadChecksum sets homomorphic object payload checksum.
func (x *headerWithSignature) SetHomomorphicPayloadChecksum(cs refs.Checksum) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetHomomorphicPayloadChecksum(cs)
	})
}

// SetContainer sets related container identifier.
//
// Parameter mutation affects the header.
func (x *headerWithSignature) SetContainer(id cid.ID) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetContainer(id)
	})
}

// SetSessionToken sets token of the session within which object was created.
func (x *headerWithSignature) SetSessionToken(token session.Token) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetSessionToken(token)
	})
}

// Attributes returns returns object attributes.
//
// Result mutation affects the header.
func (x headerWithSignature) Attributes() Attributes {
	return x.attributes
}

// SetAttributes sets returns object attributes.
//
// Parameter mutation affects the header.
func (x *headerWithSignature) SetAttributes(as Attributes) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetAttributes(as)
	})
}

// SetSplitID sets split chain identifier.
//
// Parameter mutation affects the header.
func (x *headerWithSignature) SetSplitID(id SplitID) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetSplitID(id)
	})
}

// SetPrevious sets identifier of the previous object in split chain.
//
// Parameter mutation affects the header.
func (x *headerWithSignature) SetPrevious(id oid.ID) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetPrevious(id)
	})
}

// SetChildren sets identifier of the child objects in split chain.
//
// Parameter mutation affects the header.
func (x *headerWithSignature) SetChildren(ids oid.IDs) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetChildren(ids)
	})
}

// SetParent sets parent object header.
func (x *headerWithSignature) SetParent(p HeaderWithIDAndSignature) {
	x.setHeaderField(func(x *headerWithSignature) {
		x.header.SetParent(p)
	})
}

// HeaderWithIDAndSignature represents NeoFS API V2-compatible object header, object ID and ID's signature.
type HeaderWithIDAndSignature struct {
	headerWithIDAndSignature
}

type headerWithIDAndSignature struct {
	headerWithSignature

	withID bool
	id     oid.ID
}

// FromV2 restores HeaderWithIDAndSignature from object.Header, refs.Signature and refs.ObjectID messages.
func (x *HeaderWithIDAndSignature) FromV2(hv2 *object.Header, sv2 *v2refs.Signature, idv2 *v2refs.ObjectID) {
	x.fromV2(hv2, sv2, idv2)
}

func (x *headerWithIDAndSignature) fromV2(hv2 *object.Header, sv2 *v2refs.Signature, idv2 *v2refs.ObjectID) {
	x.headerWithSignature.fromV2(hv2, sv2)

	{ // id
		x.withID = idv2 != nil
		if x.withID {
			x.id.FromV2(*idv2)
		}
	}
}

// WriteToV2 writes HeaderWithIDAndSignature to object.Header and refs.Signature messages.
//
// Header message is ignored if WithHeader returns false. Otherwise it must not be nil.
// Signature message is ignored if WithSignature returns false. Otherwise it must not be nil.
// ID message is ignored if WithID returns false. Otherwise it must not be nil.
func (x HeaderWithIDAndSignature) WriteToV2(hv2 *object.Header, sv2 *v2refs.Signature, idv2 *v2refs.ObjectID) {
	x.writeToV2(hv2, sv2, idv2)
}

func (x headerWithIDAndSignature) writeToV2(hv2 *object.Header, sv2 *v2refs.Signature, idv2 *v2refs.ObjectID) {
	x.headerWithSignature.writeToV2(hv2, sv2)

	if x.withID {
		oid.IDToV2(idv2, x.id)
	}
}

// WithID checks if object ID was specified.
func (x HeaderWithIDAndSignature) WithID() bool {
	return x.withID
}

// ID returns object identifier.
//
// Makes sense only if WithID returns true.
//
// Result mutation affects the header.
func (x headerWithIDAndSignature) ID() oid.ID {
	return x.id
}

// CalculateID calculates and sets object identifier.
//
// Returns an error if hash calculation fails. In this case ID remains untouched.
func (x *headerWithIDAndSignature) CalculateID() error {
	var data []byte

	if x.withHeader {
		var hv2 object.Header

		x.header.writeToV2(&hv2)

		var err error

		data, err = hv2.StableMarshal(nil)
		if err != nil {
			return err
		}
	}

	x.withID = true
	x.id.SetBytes(sha256.Sum256(data))

	return nil
}

// VerifyID checks if object identifier is correctly set.
func (x headerWithIDAndSignature) VerifyID() error {
	if !x.withID {
		return errors.New("missing ID")
	}

	var data []byte

	if x.withHeader {
		var hv2 object.Header

		x.header.writeToV2(&hv2)

		var err error

		data, err = hv2.StableMarshal(nil)
		if err != nil {
			return err
		}
	}

	h := sha256.Sum256(data)

	if !bytes.Equal(x.id.Bytes(), h[:]) {
		return errors.New("incorrect identifier")
	}

	return nil
}

// SignECDSA calculates and writes ECDSA signature of the object identifier. ID should be pre-calculated
// using CalculateID.
//
// Returns signature calculation errors.
func (x *headerWithIDAndSignature) SignECDSA(key ecdsa.PrivateKey) error {
	var idv2 *v2refs.ObjectID

	if x.withID {
		idv2 = new(v2refs.ObjectID)

		oid.IDToV2(idv2, x.id)
	}

	var prm apicrypto.SignPrm

	prm.SetProtoMarshaler(signature.StableMarshalerCrypto(idv2))

	prm.SetTargetSignature(&x.signature)

	if err := apicrypto.Sign(neofsecdsa.Signer(key), prm); err != nil {
		return err
	}

	x.withSignature = true

	return nil
}

// VerifySignature checks if object ID signature is presented and valid.
//
// Returns nil if signature is valid.
func (x *headerWithIDAndSignature) VerifySignature() error {
	if !x.withSignature {
		return errors.New("missing signature")
	}

	key, err := cryptoalgo.UnmarshalKey(cryptoalgo.ECDSA, x.signature.GetKey())
	if err != nil {
		return err
	}

	var idv2 *v2refs.ObjectID

	if x.withID {
		idv2 = new(v2refs.ObjectID)

		oid.IDToV2(idv2, x.id)
	}

	var prm apicrypto.VerifyPrm

	prm.SetProtoMarshaler(signature.StableMarshalerCrypto(idv2))

	prm.SetSignature(x.signature.GetSign())

	if !apicrypto.Verify(key, prm) {
		return errors.New("invalid signature")
	}

	return nil
}

// Object represents NeoFS API V2-compatible object.
type Object struct {
	headerWithIDAndSignature

	payload []byte
}

// FromV2 reads Object from object.Object message.
func (x *Object) FromV2(ov2 object.Object) {
	x.fromV2(ov2.GetHeader(), ov2.GetSignature(), ov2.GetObjectID())
	x.payload = ov2.GetPayload()
}

// WriteToV2 writes Object to object.Object message.
//
// Message must not be nil.
func (x Object) WriteToV2(ov2 *object.Object) {
	{ // header
		var (
			hv2  *object.Header
			sv2  *v2refs.Signature
			idv2 *v2refs.ObjectID
		)

		if x.withHeader {
			hv2 = ov2.GetHeader()
			if hv2 == nil {
				hv2 = new(object.Header)
			}
		}

		if x.withSignature {
			sv2 = ov2.GetSignature()
			if sv2 == nil {
				sv2 = new(v2refs.Signature)
			}
		}

		if x.withID {
			idv2 = ov2.GetObjectID()
			if idv2 == nil {
				idv2 = new(v2refs.ObjectID)
			}
		}

		x.writeToV2(hv2, sv2, idv2)

		ov2.SetHeader(hv2)
		ov2.SetSignature(sv2)
		ov2.SetObjectID(idv2)
	}

	ov2.SetPayload(x.payload)
}

// Payload returns object payload bytes.
//
// Result mutation affects the Object.
func (x Object) Payload() []byte {
	return x.payload
}

// SetPayload sets object payload bytes.
//
// Parameter mutation affects the Object.
func (x *Object) SetPayload(payload []byte) {
	x.payload = payload
}
