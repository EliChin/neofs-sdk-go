package accounting

import (
	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
)

// Decimal represents NeoFS API V2-compatible decimal number.
type Decimal struct {
	val int64

	prec uint32
}

// Value returns value of the decimal number.
func (x Decimal) Value() int64 {
	return x.val
}

// SetValue sets value of the decimal number.
func (x *Decimal) SetValue(v int64) {
	x.val = v
}

// Precision returns precision of the decimal number.
func (x Decimal) Precision() uint32 {
	return x.prec
}

// SetPrecision sets precision of the decimal number.
func (x *Decimal) SetPrecision(p uint32) {
	x.prec = p
}

// DecimalFromV2 reads Decimal from accounting.Decimal message.
func DecimalFromV2(d *Decimal, dv2 accounting.Decimal) {
	d.SetValue(dv2.GetValue())
	d.SetPrecision(dv2.GetPrecision())
}

// DecimalToV2 writes Decimal to accounting.Decimal message.
//
// Message must not be nil.
func DecimalToV2(dv2 *accounting.Decimal, d Decimal) {
	dv2.SetValue(d.Value())
	dv2.SetPrecision(d.Precision())
}
