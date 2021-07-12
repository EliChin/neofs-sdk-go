package audit

import (
	"github.com/nspcc-dev/neofs-api-go/v2/audit"
	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	cid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/container/id"
	neofsnetwork "github.com/nspcc-dev/neofs-sdk-go/pkg/api/network"
	oid "github.com/nspcc-dev/neofs-sdk-go/pkg/api/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/api/refs"
)

// Result represents v2-compatible data audit result.
type Result struct {
	complete bool

	epoch neofsnetwork.Epoch

	withVersion bool
	version     refs.Version

	withContainer bool
	container     cid.ID

	requests, retries uint32

	hit, miss, fail uint32

	key []byte

	passSG, failSG oid.IDs

	failNodes, passNodes [][]byte
}

// FromV2 restores Result from audit.DataAuditResult message.
func (x *Result) FromV2(arv2 audit.DataAuditResult) {
	{ // version
		vv2 := arv2.GetVersion()

		x.withVersion = vv2 != nil
		if x.withVersion {
			x.version.FromV2(*vv2)
		}
	}

	{ // container
		idv2 := arv2.GetContainerID()

		x.withContainer = idv2 != nil
		if x.withContainer {
			x.container.FromV2(*idv2)
		}
	}

	x.epoch.FromUint64(arv2.GetAuditEpoch())
	x.complete = arv2.GetComplete()
	x.requests = arv2.GetRequests()
	x.retries = arv2.GetRetries()
	x.hit = arv2.GetHit()
	x.miss = arv2.GetMiss()
	x.fail = arv2.GetFail()
	x.key = arv2.GetPublicKey()
	x.passNodes = arv2.GetPassNodes()
	x.failNodes = arv2.GetFailNodes()
	oid.IDsFromV2(&x.passSG, arv2.GetPassSG())
	oid.IDsFromV2(&x.failSG, arv2.GetFailSG())
}

// WriteToV2 writes Result to audit.DataAuditResult message.
//
// Message must not be nil.
func (x Result) WriteToV2(arv2 *audit.DataAuditResult) {
	{ // version
		var vv2 *v2refs.Version

		if x.withVersion {
			vv2 = arv2.GetVersion()
			if vv2 == nil {
				vv2 = new(v2refs.Version)
			}

			x.version.WriteToV2(vv2)
		}

		arv2.SetVersion(vv2)
	}

	{ // container
		var idv2 *v2refs.ContainerID

		if x.withContainer {
			idv2 = arv2.GetContainerID()
			if idv2 == nil {
				idv2 = new(v2refs.ContainerID)
			}

			cid.IDToV2(idv2, x.container)
		}

		arv2.SetContainerID(idv2)
	}

	{ // epoch
		var u64 uint64

		x.epoch.WriteToUint64(&u64)

		arv2.SetAuditEpoch(u64)
	}

	{ // pass/fail SG
		fn := func(ids oid.IDs, r func() []*v2refs.ObjectID, w func([]*v2refs.ObjectID)) {
			var idsv2 []*v2refs.ObjectID

			if ln := ids.Len(); ln > 0 {
				idsv2 = r()

				if cap(idsv2) < ln {
					idsv2 = make([]*v2refs.ObjectID, 0, ln)
				}

				idsv2 = idsv2[:ln]

				oid.IDsToV2(idsv2, ids)
			}

			w(idsv2)
		}

		fn(x.passSG, arv2.GetPassSG, arv2.SetPassSG)
		fn(x.failSG, arv2.GetFailSG, arv2.SetFailSG)
	}

	arv2.SetComplete(x.complete)
	arv2.SetRequests(x.requests)
	arv2.SetRetries(x.retries)
	x.SetHit(x.hit)
	x.SetMiss(x.miss)
	x.SetFail(x.fail)
	x.SetPublicKey(x.key)
	x.SetPassNodes(x.passNodes)
	x.SetFailNodes(x.failNodes)
}

// ResultMarshalProto marshals Result into a protobuf binary form.
func ResultMarshalProto(r Result) ([]byte, error) {
	var av2 audit.DataAuditResult

	r.WriteToV2(&av2)

	return av2.StableMarshal(nil)
}

// ResultUnmarshalProto unmarshals protobuf binary representation of Result.
func ResultUnmarshalProto(r *Result, data []byte) error {
	var av2 audit.DataAuditResult

	err := av2.Unmarshal(data)
	if err == nil {
		r.FromV2(av2)
	}

	return err
}

// WithVersion checks if Result protocol version was specified.
func (x Result) WithVersion() bool {
	return x.withVersion
}

// Version returns protocol version within which Result is formed.
//
// Makes sense only if WithVersion returns true.
func (x Result) Version() refs.Version {
	return x.version
}

// SetVersion sets protocol version within which Result is formed.
func (x *Result) SetVersion(version refs.Version) {
	x.version = version
	x.withVersion = true
}

// Epoch returns epoch when the Result was formed.
func (x Result) Epoch() neofsnetwork.Epoch {
	return x.epoch
}

// SetEpoch sets epoch when the Result was formed.
func (x *Result) SetEpoch(epoch neofsnetwork.Epoch) {
	x.epoch = epoch
}

// WithContainer checks if Result container was specified.
func (x Result) WithContainer() bool {
	return x.withContainer
}

// Container returns identifier of the container under audit.
//
// Result must not be mutated.
func (x Result) Container() cid.ID {
	return x.container
}

// SetContainer sets idenfitier of the container under audit.
//
// Parameter must not be mutated.
func (x *Result) SetContainer(id cid.ID) {
	x.container = id
	x.withContainer = true
}

// PublicKey returns public key of the auditing Inner Ring node in a binary format.
//
// Result must not be mutated.
func (x Result) PublicKey() []byte {
	return x.key
}

// SetPublicKey sets public key of the auditing Inner Ring node in a binary format.
//
// Parameter must not be mutated.
func (x *Result) SetPublicKey(key []byte) {
	x.key = key
}

// Complete returns completion state of audit result.
func (x Result) Complete() bool {
	return x.complete
}

// SetComplete sets completion state of audit result.
func (x *Result) SetComplete(v bool) {
	x.complete = v
}

// Requests returns number of requests made by PoR audit check to get
// all headers of the objects inside storage groups.
func (x Result) Requests() uint32 {
	return x.requests
}

// SetRequests sets number of requests made by PoR audit check to get
// all headers of the objects inside storage groups.
func (x *Result) SetRequests(requests uint32) {
	x.requests = requests
}

// Retries returns number of retries made by PoR audit check to get
// all headers of the objects inside storage groups.
func (x Result) Retries() uint32 {
	return x.retries
}

// SetRetries sets number of retries made by PoR audit check to get
// all headers of the objects inside storage groups.
func (x *Result) SetRetries(retries uint32) {
	x.retries = retries
}

// PassSG returns list of Storage Groups that passed audit PoR stage.
//
// Result must not be mutated.
func (x Result) PassSG() oid.IDs {
	return x.passSG
}

// SetPassSG sets list of Storage Groups that passed audit PoR stage.
//
// Parameter must not be mutated.
func (x *Result) SetPassSG(ids oid.IDs) {
	x.passSG = ids
}

// FailSG returns list of Storage Groups that failed audit PoR stage.
//
// Result must not be mutated.
func (x Result) FailSG() oid.IDs {
	return x.failSG
}

// SetFailSG sets list of Storage Groups that failed audit PoR stage.
//
// Parameter must not be mutated.
func (x *Result) SetFailSG(ids oid.IDs) {
	x.failSG = ids
}

// Hit returns number of sampled objects under audit placed
// in an optimal way according to the containers placement policy
// when checking PoP.
func (x Result) Hit() uint32 {
	return x.hit
}

// SetHit sets number of sampled objects under audit placed
// in an optimal way according to the containers placement policy
// when checking PoP.
func (x *Result) SetHit(hit uint32) {
	x.hit = hit
}

// Miss returns number of sampled objects under audit placed
// in suboptimal way according to the containers placement policy,
// but still at a satisfactory level when checking PoP.
func (x Result) Miss() uint32 {
	return x.miss
}

// SetMiss sets number of sampled objects under audit placed
// in suboptimal way according to the containers placement policy,
// but still at a satisfactory level when checking PoP.
func (x *Result) SetMiss(miss uint32) {
	x.miss = miss
}

// Fail returns number of sampled objects under audit stored
// in a way not confirming placement policy or not found at all
// when checking PoP.
func (x Result) Fail() uint32 {
	return x.fail
}

// SetFail sets number of sampled objects under audit stored
// in a way not confirming placement policy or not found at all
// when checking PoP.
func (x *Result) SetFail(fail uint32) {
	x.fail = fail
}

// PassNodes returns list of storage node public keys that
// passed at least one PDP.
//
// Result must not be mutated.
func (x Result) PassNodes() [][]byte {
	return x.passNodes
}

// SetPassNodes sets list of storage node public keys that
// passed at least one PDP.
func (x *Result) SetPassNodes(keys [][]byte) {
	x.passNodes = keys
}

// FailNodes returns list of storage node public keys that
// failed at least one PDP.
//
// Result must not be mutated.
func (x Result) FailNodes() [][]byte {
	return x.failNodes
}

// SetFailNodes sets list of storage node public keys that
// failed at least one PDP.
//
// Parameter must not be mutated.
func (x *Result) SetFailNodes(keys [][]byte) {
	x.failNodes = keys
}
