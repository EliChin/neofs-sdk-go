package eacl

import (
	v2acl "github.com/nspcc-dev/neofs-api-go/v2/acl"
)

// Filter represents NeoFS API V2-compatible entity which defines check conditions
// if request header is matched or not.
type Filter struct {
	hTyp HeaderType

	mTyp MatchType

	key, val string
}

// FromV2 restores Filter from acl.HeaderFilter message.
func (x *Filter) FromV2(fv2 v2acl.HeaderFilter) {
	x.hTyp.fromV2(fv2.GetHeaderType())
	x.mTyp.fromV2(fv2.GetMatchType())
	x.key = fv2.GetKey()
	x.val = fv2.GetValue()
}

// WriteToV2 writes Filter to acl.HeaderFilter message.
//
// Message must not be nil.
func (x Filter) WriteToV2(fv2 *v2acl.HeaderFilter) {
	{ // header type
		var hv2 v2acl.HeaderType

		x.hTyp.writeToV2(&hv2)

		fv2.SetHeaderType(hv2)
	}

	{ // match type
		var mv2 v2acl.MatchType

		x.mTyp.writeToV2(&mv2)

		fv2.SetMatchType(mv2)
	}

	fv2.SetKey(x.key)
	fv2.SetValue(x.val)
}

// HeaderType returns type of header source.
func (x Filter) HeaderType() HeaderType {
	return x.hTyp
}

// SetHeaderType sets type of header source.
func (x *Filter) SetHeaderType(hTyp HeaderType) {
	x.hTyp = hTyp
}

// MatchType returns match type.
func (x Filter) MatchType() MatchType {
	return x.mTyp
}

// SetMatchType sets match type.
func (x *Filter) SetMatchType(mTyp MatchType) {
	x.mTyp = mTyp
}

// Key returns filtered header name.
func (x Filter) Key() string {
	return x.val
}

// SetKey returns filtered header name.
func (x *Filter) SetKey(key string) {
	x.key = key
}

// Value returns filtered header value.
func (x Filter) Value() string {
	return x.val
}

// SetValue sets filtered header value.
func (x *Filter) SetValue(val string) {
	x.val = val
}
