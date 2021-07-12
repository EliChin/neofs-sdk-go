package object

import (
	"github.com/nspcc-dev/neofs-api-go/v2/object"
)

// Attribute represents NeoFS object attribute.
type Attribute struct {
	key, val string
}

// Key returns key to Attribute.
func (x Attribute) Key() string {
	return x.key
}

// SetKey sets key to Attribute.
func (x *Attribute) SetKey(v string) {
	x.key = v
}

// Value returns value of Attribute.
func (x Attribute) Value() string {
	return x.val
}

// SetValue sets value of Attribute.
func (x *Attribute) SetValue(v string) {
	x.val = v
}

// AttributeFromV2 restores Attribute from object.Attribute message.
func AttributeFromV2(a *Attribute, av2 object.Attribute) {
	a.SetKey(av2.GetKey())
	a.SetValue(av2.GetValue())
}

// AttributeToV2 writes Attribute to object.Attribute message.
//
// Message must not be nil.
func AttributeToV2(av2 *object.Attribute, a Attribute) {
	av2.SetKey(a.Key())
	av2.SetValue(a.Value())
}
