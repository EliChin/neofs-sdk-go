package neofsnetwork

import "github.com/nspcc-dev/neofs-api-go/v2/netmap"

// Info represents NeoFS API V2-compatible information about the NeoFS network.
type Info struct {
	currentEpoch Epoch

	magicNumber uint64
}

// MagicNumber returns magic number of the NeoFS sidechain.
func (x Info) MagicNumber() uint64 {
	return x.magicNumber
}

// SetMagicNumber sets magic number of the NeoFS sidechain.
func (x *Info) SetMagicNumber(magicNumber uint64) {
	x.magicNumber = magicNumber
}

// CurrentEpoch returns current Epoch.
func (x Info) CurrentEpoch() Epoch {
	return x.currentEpoch
}

// SetCurrentEpoch sets current Epoch.
func (x *Info) SetCurrentEpoch(currentEpoch Epoch) {
	x.currentEpoch = currentEpoch
}

// InfoFromV2 reads Info from netmap.NetworkInfo message.
func InfoFromV2(i *Info, iv2 netmap.NetworkInfo) {
	var e Epoch

	e.FromUint64(iv2.GetCurrentEpoch())

	i.SetCurrentEpoch(e)
	i.SetMagicNumber(iv2.GetMagicNumber())
}

// InfoToV2 writes Info to netmap.NetworkInfo message.
//
// Message must not be nil.
func InfoToV2(iv2 *netmap.NetworkInfo, i Info) {
	var e uint64

	i.CurrentEpoch().WriteToUint64(&e)

	iv2.SetCurrentEpoch(e)
	iv2.SetMagicNumber(i.MagicNumber())
}
