package neofsnode

import (
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

// Attribute represents NeoFS API V2-compatible node attribute.
type Attribute struct {
	key, val string

	parents []string
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

// Parents returns set of parent keys.
//
// Result mutation affects the Attribute.
func (x Attribute) Parents() []string {
	return x.parents
}

// SetParents returns set of parent keys.
//
// Parameter mutation affects the Attribute.
func (x *Attribute) SetParents(parents []string) {
	x.parents = parents
}

// attributeFromV2 reads Attribute from netmap.Attribute message.
func attributeFromV2(a *Attribute, av2 netmap.Attribute) {
	a.SetKey(av2.GetKey())
	a.SetValue(av2.GetValue())
	a.SetParents(av2.GetParents())
}

// attributeToV2 writes Attribute to netmap.Attribute message.
//
// Message must not be nil.
func attributeToV2(av2 *netmap.Attribute, a Attribute) {
	av2.SetKey(a.Key())
	av2.SetValue(a.Value())
}
