package neofsnetwork

import "fmt"

// Epoch represents NeoFS epoch.
type Epoch uint64

// FromUint64 restores Epoch from uint64.
func (x *Epoch) FromUint64(u64 uint64) {
	*x = Epoch(u64)
}

// WriteToUint64 writes Epoch to uint64.
//
// Argument must not be nil.
func (x Epoch) WriteToUint64(u64 *uint64) {
	*u64 = uint64(x)
}

// String implements fmt.Stringer.
func (x Epoch) String() string {
	return fmt.Sprintf("E#%d", x)
}
