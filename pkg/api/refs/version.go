package refs

import (
	"fmt"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/v2/refs"
)

// Version represents NeoFS API V2-compatible protocol version.
type Version struct {
	mjr, mnr uint32
}

var (
	cur  Version
	once sync.Once
)

func initCurrentVersion() {
	const curMjr, curMnr = 2, 8

	cur.mjr, cur.mnr = curMjr, curMnr
}

// CurrentVersion returns current protocol version supported by the library.
func CurrentVersion() Version {
	once.Do(initCurrentVersion)
	return cur
}

// String implements fmt.Stringer.
//
// Format: v<major>.<minor>.
func (x Version) String() string {
	return fmt.Sprintf("v%d.%d", x.mjr, x.mnr)
}

// Exists checks if version is not less than genesis NeoFS version 2.7.
func (x Version) Exists() bool {
	const genMjr, genMnr = 2, 7
	return x.mjr > genMjr || x.mjr == genMjr && x.mnr >= genMnr
}

// FromV2 reads Version from refs.Version message.
func (x *Version) FromV2(vv2 refs.Version) {
	x.mjr = vv2.GetMajor()
	x.mnr = vv2.GetMinor()
}

// WriteToV2 writes Version to refs.Version message.
//
// Message must not be nil.
func (x Version) WriteToV2(vv2 *refs.Version) {
	vv2.SetMajor(x.mjr)
	vv2.SetMinor(x.mnr)
}
