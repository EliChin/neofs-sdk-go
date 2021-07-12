package netmap

import (
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

// PlacementPolicy represents NeoFS API V2-compatible placement policy.
type PlacementPolicy struct {
	containerBackupFactor uint32

	replicas Replicas

	filters Filters

	selectors Selectors
}

// FromV2 restores PlacementPolicy from netmap.PlacementPolicy message.
func (x *PlacementPolicy) FromV2(pv2 netmap.PlacementPolicy) {
	filtersFromV2(&x.filters, pv2.GetFilters())
	selectorsFromV2(&x.selectors, pv2.GetSelectors())
	replicasFromV2(&x.replicas, pv2.GetReplicas())
	x.containerBackupFactor = pv2.GetContainerBackupFactor()
}

// WriteToV2 writes PlacementPolicy to netmap.PlacementPolicy message.
//
// Message must not be nil.
func (x PlacementPolicy) WriteToV2(pv2 *netmap.PlacementPolicy) {
	{ // replicas
		var rsv2 []*netmap.Replica

		if ln := x.replicas.Len(); ln > 0 {
			rsv2 = pv2.GetReplicas()

			if cap(rsv2) < ln {
				rsv2 = make([]*netmap.Replica, 0, ln)
			}

			rsv2 = rsv2[:ln]

			replicasToV2(rsv2, x.replicas)
		}

		pv2.SetReplicas(rsv2)
	}

	{ // filters
		var fsv2 []*netmap.Filter

		if ln := x.filters.Len(); ln > 0 {
			fsv2 = pv2.GetFilters()

			if cap(fsv2) < ln {
				fsv2 = make([]*netmap.Filter, 0, ln)
			}

			fsv2 = fsv2[:ln]

			filtersToV2(fsv2, x.filters)
		}

		pv2.SetFilters(fsv2)
	}

	{ // selectors
		var ssv2 []*netmap.Selector

		if ln := x.selectors.Len(); ln > 0 {
			ssv2 = pv2.GetSelectors()

			if cap(ssv2) < ln {
				ssv2 = make([]*netmap.Selector, 0, ln)
			}

			ssv2 = ssv2[:ln]

			selectorsToV2(ssv2, x.selectors)
		}

		pv2.SetSelectors(ssv2)
	}

	pv2.SetContainerBackupFactor(x.containerBackupFactor)
}

// Replicas returns set of object replica descriptors.
//
// Result mutation affects the PlacementPolicy.
func (x PlacementPolicy) Replicas() Replicas {
	return x.replicas
}

// SetReplicas sets set of object replica descriptors.
//
// Parameter mutation affects the PlacementPolicy.
func (x *PlacementPolicy) SetReplicas(replicas Replicas) {
	x.replicas = replicas
}

// ContainerBackupFactor returns container backup factor.
func (x PlacementPolicy) ContainerBackupFactor() uint32 {
	return x.containerBackupFactor
}

// SetContainerBackupFactor sets container backup factor.
func (x *PlacementPolicy) SetContainerBackupFactor(cbf uint32) {
	x.containerBackupFactor = cbf
}

// Selectors returns set of selectors to form the container's nodes subset.
//
// Result mutation affects the PlacementPolicy.
func (x PlacementPolicy) Selectors() Selectors {
	return x.selectors
}

// SetSelectors sets set of selectors to form the container's nodes subset.
//
// Parameter mutation affects the PlacementPolicy.
func (x *PlacementPolicy) SetSelectors(selectors Selectors) {
	x.selectors = selectors
}

// Filters returns set of named filters to reference in selectors.
//
// Filters must not be mutated after the call.
func (x PlacementPolicy) Filters() Filters {
	return x.filters
}

// SetFilters sets list of named filters to reference in selectors.
//
// Filters must not be mutated after the call.
func (x *PlacementPolicy) SetFilters(filters Filters) {
	x.filters = filters
}
