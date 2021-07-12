package refs

import (
	"github.com/nspcc-dev/neofs-api-go/v2/session"
)

// XHeader represents NeoFS API V2-compatible X-header.
type XHeader struct {
	key, val string
}

// Key returns key to XHeader.
func (x XHeader) Key() string {
	return x.key
}

// SetKey sets key to XHeader.
func (x *XHeader) SetKey(k string) {
	x.key = k
}

// Value returns value of XHeader.
func (x XHeader) Value() string {
	return x.val
}

// SetValue sets value of XHeader.
func (x *XHeader) SetValue(v string) {
	x.val = v
}

// XHeaderFromV2 reads XHeader from session.XHeader message.
func XHeaderFromV2(xh *XHeader, xhv2 session.XHeader) {
	xh.SetKey(xhv2.GetKey())
	xh.SetValue(xhv2.GetValue())
}

// WriteToV2 writes XHeader to session.XHeader message.
//
// Message must not be nil.
func XHeaderWriteToV2(xhv2 *session.XHeader, xh XHeader) {
	xhv2.SetKey(xh.Key())
	xhv2.SetValue(xh.Value())
}
