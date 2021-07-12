package oid

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	cid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/container/id"
)

// Address represents NeoFS API V2-compatible object address.
type Address struct {
	withContainer bool
	container     cid.ID

	withObject bool
	object     ID
}

// FromV2 reads Address from refs.Address message.
func (x *Address) FromV2(av2 refs.Address) {
	{ // container
		idv2 := av2.GetContainerID()

		x.withContainer = idv2 != nil
		if x.withContainer {
			x.container.FromV2(*idv2)
		}
	}

	{ // object
		idv2 := av2.GetObjectID()

		x.withObject = idv2 != nil
		if x.withObject {
			x.object.FromV2(*idv2)
		}
	}
}

// WriteToV2 writes Address to refs.Address message.
//
// Message must no be nil.
func (x Address) WriteToV2(av2 *refs.Address) {
	{ // container
		var idv2 *refs.ContainerID

		if x.withContainer {
			idv2 = av2.GetContainerID()
			if idv2 == nil {
				idv2 = new(refs.ContainerID)
			}

			cid.IDToV2(idv2, x.container)
		}

		av2.SetContainerID(idv2)
	}

	{ // object
		var idv2 *refs.ObjectID

		if x.withObject {
			idv2 = av2.GetObjectID()
			if idv2 == nil {
				idv2 = new(refs.ObjectID)
			}

			IDToV2(idv2, x.object)
		}

		av2.SetObjectID(idv2)
	}
}

// WithContainer checks if container ID was specified.
func (x Address) WithContainer() bool {
	return x.withContainer
}

// Container returns container identifier.
//
// Makes sense only if WithContainer returns true.
//
// Result mutation affects the Address.
func (x Address) Container() cid.ID {
	return x.container
}

// SetContainer sets container identifier.
//
// Parameter mutation affects the Address.
func (x *Address) SetContainer(id cid.ID) {
	x.container = id
	x.withContainer = true
}

// WithObject checks if object ID was specified.
func (x Address) WithObject() bool {
	return x.withObject
}

// Object returns object identifier.
//
// Makes sense only if WithObject returns true.
//
// Result mutation affects the Address.
func (x Address) Object() ID {
	return x.object
}

// SetObject sets object identifier.
//
// Parameter mutation affects the Address.
func (x *Address) SetObject(id ID) {
	x.object = id
	x.withObject = true
}

// String implements fmt.Stringer through Hex encoding.
//
// To get the canonical string MarshalText should be used.
func (x Address) String() string {
	const addrStrFmt = "%s/%s"

	return fmt.Sprintf(addrStrFmt, x.container, x.object)
}

const addrStrSep = "/"

// MarshalText implements encoding.TextMarshaler through Base58 encoding.
// Returns canonical Address string according to NeoFS API V2 spec.
func (x Address) MarshalText() ([]byte, error) {
	c, err := x.container.MarshalText()
	if err != nil {
		return nil, err
	}

	o, err := x.object.MarshalText()
	if err != nil {
		return nil, err
	}

	const addrCanonStrFmt = "%s" + addrStrSep + "%s"

	return []byte(fmt.Sprintf(addrCanonStrFmt, c, o)), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
//
// Returns an error if txt is not a canonical Address string according to NeoFS API V2 spec.
// In this case Address remains untouched.
func (x *Address) UnmarshalText(txt []byte) error {
	sepInd := strings.Index(string(txt), addrStrSep)
	if sepInd < 0 {
		return errors.New("missing separator")
	}

	var err error

	if err = x.container.UnmarshalText(txt[:sepInd]); err != nil {
		return fmt.Errorf("invalid container ID text: %w", err)
	}

	if err = x.object.UnmarshalText(txt[sepInd+len(addrStrSep):]); err != nil {
		return fmt.Errorf("invalid object ID text: %w", err)
	}

	return nil
}
