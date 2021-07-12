package eacl

import (
	v2acl "github.com/nspcc-dev/neofs-api-go/v2/acl"
)

// Record represents NeoFS API V2-compatible eACL rule record.
type Record struct {
	action Action

	operation Operation

	filters Filters

	targets Targets
}

// FromV2 reads Record from acl.Record message.
func (x *Record) FromV2(fv2 v2acl.Record) {
	x.action.fromV2(fv2.GetAction())
	x.operation.fromV2(fv2.GetOperation())
	filtersFromV2(&x.filters, fv2.GetFilters())
	targetsFromV2(&x.targets, fv2.GetTargets())
}

// WriteToV2 writes Record to acl.Record message.
//
// Message must not be nil.
func (x Record) WriteToV2(fv2 *v2acl.Record) {
	{ // header type
		var hv2 v2acl.Action

		x.action.writeToV2(&hv2)

		fv2.SetAction(hv2)
	}

	{ // match type
		var mv2 v2acl.Operation

		x.operation.writeToV2(&mv2)

		fv2.SetOperation(mv2)
	}

	{ // filters
		var fsv2 []*v2acl.HeaderFilter

		if ln := x.filters.Len(); ln > 0 {
			fsv2 = fv2.GetFilters()

			if cap(fsv2) < ln {
				fsv2 = make([]*v2acl.HeaderFilter, 0, ln)
			}

			fsv2 = fsv2[:ln]

			filtersToV2(fsv2, x.filters)
		}

		fv2.SetFilters(fsv2)
	}

	{ // targets
		var tsv2 []*v2acl.Target

		if ln := x.filters.Len(); ln > 0 {
			tsv2 = fv2.GetTargets()

			if cap(tsv2) < ln {
				tsv2 = make([]*v2acl.Target, 0, ln)
			}

			tsv2 = tsv2[:ln]

			targetsToV2(tsv2, x.targets)
		}

		fv2.SetTargets(tsv2)
	}
}

// Targets returns list of target subjects to apply ACL rule to.
//
// Result mutation affects the Record.
func (x Record) Targets() Targets {
	return x.targets
}

// SetTargets sets list of target subjects to apply ACL rule to.
//
// Parameter mutation affects the Record.
func (x *Record) SetTargets(targets Targets) {
	x.targets = targets
}

// Filters returns list of filters to match and see if rule is applicable.
//
// Result mutation affects the Record.
func (x Record) Filters() Filters {
	return x.filters
}

// SetFilters sets list of filters to match and see if rule is applicable.
//
// Parameter mutation affects the Record.
func (x *Record) SetFilters(filters Filters) {
	x.filters = filters
}

// Operation returns NeoFS request verb to match.
func (x Record) Operation() Operation {
	return x.operation
}

// SetOperation sets NeoFS request verb to match.
func (x *Record) SetOperation(operation Operation) {
	x.operation = operation
}

// Action returns rule execution result.
func (x Record) Action() Action {
	return x.action
}

// SetAction sets rule execution result.
func (x *Record) SetAction(action Action) {
	x.action = action
}
