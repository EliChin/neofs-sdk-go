package eacl

import (
	"fmt"

	v2acl "github.com/nspcc-dev/neofs-api-go/v2/acl"
)

// Action represents NeoFS API V2-compatible action that is taken when EACL record matched request.
type Action struct {
	a v2acl.Action
}

// fromV2 restores Action from v2acl.Action enum value.
func (x *Action) fromV2(av2 v2acl.Action) {
	x.a = av2
}

// writeToV2 writes Action to v2acl.Action enum value.
//
// Enum value must not be nil.
func (x Action) writeToV2(av2 *v2acl.Action) {
	*av2 = x.a
}

// String implements fmt.Stringer.
//
// To get the canonical string MarshalText should be used.
func (x Action) String() string {
	switch x.a {
	default:
		return "UNDEFINED"
	case v2acl.ActionUnknown:
		return "UNKNOWN"
	case v2acl.ActionAllow:
		return "ALLOW"
	case v2acl.ActionDeny:
		return "DENY"
	}
}

// MarshalText implements encoding.TextMarshaler.
// Returns canonical Action string according to NeoFS API V2 spec.
//
// Returns an error if Action is not supported.
func (x Action) MarshalText() ([]byte, error) {
	switch x.a {
	default:
		return nil, fmt.Errorf("unsupported action: %d", x.a)
	case
		v2acl.ActionUnknown,
		v2acl.ActionAllow,
		v2acl.ActionDeny:
		return []byte(x.a.String()), nil
	}
}

// UnmarshalText implements encoding.TextUnmarshaler.
//
// Returns an error if d is not a canonical Action string according to NeoFS API V2 spec
// or if the value is not supported. In these cases Action remains untouched.
func (x *Action) UnmarshalText(txt []byte) error {
	if ok := x.a.FromString(string(txt)); !ok {
		return fmt.Errorf("unsupported action text: %s", txt)
	}

	return nil
}

// Unspecified checks if specific Action is set.
func (x Action) Unspecified() bool {
	return x.a == v2acl.ActionUnknown
}

// Allow checks if Action is set to allow the action.
func (x Action) Allow() bool {
	return x.a == v2acl.ActionAllow
}

// SetAllow sets Action to allow the action.
func (x *Action) SetAllow() {
	x.a = v2acl.ActionAllow
}

// Deny checks if Action is set to deny the action.
func (x Action) Deny() bool {
	return x.a == v2acl.ActionDeny
}

// SetDeny sets Action to deny the action.
func (x *Action) SetDeny() {
	x.a = v2acl.ActionDeny
}

// Operation represents NeoFS API V2-compatible object service method to match request.
type Operation struct {
	op v2acl.Operation
}

// fromV2 restores Operation from v2acl.Operation enum value.
func (x *Operation) fromV2(opv2 v2acl.Operation) {
	x.op = opv2
}

// writeToV2 writes Operation to v2acl.Operation enum value.
//
// Enum value must not be nil.
func (x Operation) writeToV2(opv2 *v2acl.Operation) {
	*opv2 = x.op
}

// String implements fmt.Stringer.
//
// To get the canonical string MarshalText should be used.
func (x Operation) String() string {
	switch x.op {
	default:
		return "UNDEFINED"
	case v2acl.OperationUnknown:
		return "UNKNOWN"
	case v2acl.OperationGet:
		return "GET"
	case v2acl.OperationPut:
		return "PUT"
	case v2acl.OperationHead:
		return "HEAD"
	case v2acl.OperationDelete:
		return "DELETE"
	case v2acl.OperationSearch:
		return "SEARCH"
	case v2acl.OperationRange:
		return "RANGE"
	case v2acl.OperationRangeHash:
		return "RANGE_HASH"
	}
}

// MarshalText implements encoding.TextMarshaler.
// Returns canonical Operation string according to NeoFS API V2 spec.
//
// Returns an error if Operation is not supported.
func (x Operation) MarshalText() ([]byte, error) {
	switch x.op {
	default:
		return nil, fmt.Errorf("unsupported operation: %d", x.op)
	case
		v2acl.OperationUnknown,
		v2acl.OperationGet,
		v2acl.OperationPut,
		v2acl.OperationHead,
		v2acl.OperationDelete,
		v2acl.OperationSearch,
		v2acl.OperationRange,
		v2acl.OperationRangeHash:
		return []byte(x.op.String()), nil
	}
}

// UnmarshalText implements encoding.TextUnmarshaler.
//
// Returns an error if d is not a canonical Operation string according to NeoFS API V2 spec
// or if the value is not supported. In these cases Operation remains untouched.
func (x *Operation) UnmarshalText(txt []byte) error {
	if ok := x.op.FromString(string(txt)); !ok {
		return fmt.Errorf("unsupported operation text: %s", txt)
	}

	return nil
}

// Unspecified checks if specific Operation is set.
func (x Operation) Unspecified() bool {
	return x.op == v2acl.OperationUnknown
}

// Get checks if Action is set to Object Get.
func (x Operation) Get() bool {
	return x.op == v2acl.OperationGet
}

// SetGet sets Operation to Object Get.
func (x *Operation) SetGet() {
	x.op = v2acl.OperationGet
}

// Put checks if Action is set to Object Put.
func (x Operation) Put() bool {
	return x.op == v2acl.OperationPut
}

// SetPut sets Operation to Object Put.
func (x *Operation) SetPut() {
	x.op = v2acl.OperationPut
}

// Head checks if Operation is set to Object Head.
func (x Operation) Head() bool {
	return x.op == v2acl.OperationHead
}

// SetHead sets Operation to Object Head.
func (x *Operation) SetHead() {
	x.op = v2acl.OperationHead
}

// Delete checks if Operation is set to Object Delete.
func (x Operation) Delete() bool {
	return x.op == v2acl.OperationDelete
}

// SetDelete sets Operation to Object Delete.
func (x *Operation) SetDelete() {
	x.op = v2acl.OperationDelete
}

// Search checks if Operation is set to Object Search.
func (x Operation) Search() bool {
	return x.op == v2acl.OperationDelete
}

// SetSearch sets Operation to Object Search.
func (x *Operation) SetSearch() {
	x.op = v2acl.OperationSearch
}

// Range checks if Operation is set to Object GetRange.
func (x Operation) Range() bool {
	return x.op == v2acl.OperationRange
}

// SetRange sets Operation to Object GetRange.
func (x *Operation) SetRange() {
	x.op = v2acl.OperationRange
}

// RangeHash checks if Operation is set to Object GetRangeHash.
func (x Operation) RangeHash() bool {
	return x.op == v2acl.OperationRangeHash
}

// SetRangeHash sets Operation to Object GetRangeHash.
func (x *Operation) SetRangeHash() {
	x.op = v2acl.OperationRangeHash
}

// Role represents NeoFS API V2-compatible group of request senders to match request.
//
// Groups:
//  - user (container owner);
//  - system (container node and Inner Ring);
//  - others.
type Role struct {
	r v2acl.Role
}

// fromV2 restores Role from v2acl.Role enum value.
func (x *Role) fromV2(rv2 v2acl.Role) {
	x.r = rv2
}

// writeToV2 writes Role to v2acl.Role enum value.
//
// Enum value must not be nil.
func (x Role) writeToV2(rv2 *v2acl.Role) {
	*rv2 = x.r
}

// String implements fmt.Stringer.
//
// To get the canonical string MarshalText should be used.
func (x Role) String() string {
	switch x.r {
	default:
		return "UNDEFINED"
	case v2acl.RoleUnknown:
		return "UNKNOWN"
	case v2acl.RoleUser:
		return "USER"
	case v2acl.RoleSystem:
		return "SYSTEM"
	case v2acl.RoleOthers:
		return "OTHERS"
	}
}

// MarshalText implements encoding.TextMarshaler.
// Returns canonical Role string according to NeoFS API V2 spec.
//
// Returns an error if Role is not supported.
func (x Role) MarshalText() ([]byte, error) {
	switch x.r {
	default:
		return nil, fmt.Errorf("unsupported role: %d", x.r)
	case
		v2acl.RoleUnknown,
		v2acl.RoleUser,
		v2acl.RoleSystem,
		v2acl.RoleOthers:
		return []byte(x.r.String()), nil
	}
}

// UnmarshalText implements encoding.TextUnmarshaler.
//
// Returns an error if d is not a canonical Role string according to NeoFS API V2 spec
// or if the value is not supported. In these cases Role remains untouched.
func (x *Role) UnmarshalText(txt []byte) error {
	if ok := x.r.FromString(string(txt)); !ok {
		return fmt.Errorf("unsupported role text: %s", txt)
	}

	return nil
}

// Unspecified checks if specific Role is set.
func (x Role) Unspecified() bool {
	return x.r == v2acl.RoleUnknown
}

// User checks if Role is set to user group.
func (x Role) User() bool {
	return x.r == v2acl.RoleUser
}

// SetUser sets Role to user group.
func (x *Role) SetUser() {
	x.r = v2acl.RoleUser
}

// System checks if Role is set to system group.
func (x Role) System() bool {
	return x.r == v2acl.RoleSystem
}

// SetSystem sets Role to system group.
func (x *Role) SetSystem() {
	x.r = v2acl.RoleSystem
}

// Others checks if Role is set to others group.
func (x Role) Others() bool {
	return x.r == v2acl.RoleOthers
}

// SetOthers sets Role to others group.
func (x *Role) SetOthers() {
	x.r = v2acl.RoleOthers
}

// MatchType represents NeoFS API V2-compatible binary operation on filter name and value to check if request is matched.
type MatchType struct {
	m v2acl.MatchType
}

// fromV2 restores MatchType from v2acl.MatchType enum value.
func (x *MatchType) fromV2(mv2 v2acl.MatchType) {
	x.m = mv2
}

// writeToV2 writes MatchType to v2acl.MatchType enum value.
//
// Enum value must not be nil.
func (x MatchType) writeToV2(mv2 *v2acl.MatchType) {
	*mv2 = x.m
}

// String implements fmt.Stringer.
//
// To get the canonical string MarshalText should be used.
func (x MatchType) String() string {
	switch x.m {
	default:
		return "UNDEFINED"
	case v2acl.MatchTypeUnknown:
		return "UNKNOWN"
	case v2acl.MatchTypeStringEqual:
		return "STRING_EQUAL"
	case v2acl.MatchTypeStringNotEqual:
		return "STRING_NOT_EQUAL"
	}
}

// MarshalText implements encoding.TextMarshaler.
// Returns canonical MatchType string according to NeoFS API V2 spec.
//
// Returns an error if MatchType is not supported.
func (x MatchType) MarshalText() ([]byte, error) {
	switch x.m {
	default:
		return nil, fmt.Errorf("unsupported match type: %d", x.m)
	case
		v2acl.MatchTypeUnknown,
		v2acl.MatchTypeStringEqual,
		v2acl.MatchTypeStringNotEqual:
		return []byte(x.m.String()), nil
	}
}

// UnmarshalText implements encoding.TextUnmarshaler.
//
// Returns an error if d is not a canonical MatchType string according to NeoFS API V2 spec
// or if the value is not supported. In these cases MatchType remains untouched.
func (x *MatchType) UnmarshalText(txt []byte) error {
	if ok := x.m.FromString(string(txt)); !ok {
		return fmt.Errorf("unsupported match type text: %s", txt)
	}

	return nil
}

// Unspecified checks if specific MatchType is set.
func (x MatchType) Unspecified() bool {
	return x.m == v2acl.MatchTypeUnknown
}

// StringEqual checks if Role is set to "string equal".
func (x MatchType) StringEqual() bool {
	return x.m == v2acl.MatchTypeStringEqual
}

// SetStringEqual sets MatchType to "string equal".
func (x *MatchType) SetStringEqual() {
	x.m = v2acl.MatchTypeStringEqual
}

// StringNotEqual checks if Role is set to "string not equal".
func (x MatchType) StringNotEqual() bool {
	return x.m == v2acl.MatchTypeStringNotEqual
}

// SetStringNotEqual sets MatchType to "string not equal".
func (x *MatchType) SetStringNotEqual() {
	x.m = v2acl.MatchTypeStringNotEqual
}

// HeaderType represents NeoFS API V2-compatible source of headers to make matches.
type HeaderType struct {
	h v2acl.HeaderType
}

// fromV2 restores HeaderType from v2acl.HeaderType enum value.
func (x *HeaderType) fromV2(htv2 v2acl.HeaderType) {
	x.h = htv2
}

// writeToV2 writes HeaderType to v2acl.HeaderType enum value.
//
// Enum value must not be nil.
func (x HeaderType) writeToV2(htv2 *v2acl.HeaderType) {
	*htv2 = x.h
}

// String implements fmt.Stringer.
//
// To get the canonical string MarshalText should be used.
func (x HeaderType) String() string {
	switch x.h {
	default:
		return "UNDEFINED"
	case v2acl.HeaderTypeUnknown:
		return "UNKNOWN"
	case v2acl.HeaderTypeObject:
		return "OBJECT"
	case v2acl.HeaderTypeRequest:
		return "REQUEST"
	}
}

// MarshalText implements encoding.TextMarshaler.
// Returns canonical HeaderType string according to NeoFS API V2 spec.
//
// Returns an error if HeaderType is not supported (can be caused by unsafe cast instead of methods).
func (x HeaderType) MarshalText() ([]byte, error) {
	switch x.h {
	default:
		return nil, fmt.Errorf("unsupported header type: %d", x.h)
	case
		v2acl.HeaderTypeUnknown,
		v2acl.HeaderTypeObject,
		v2acl.HeaderTypeRequest:
		return []byte(x.h.String()), nil
	}
}

// UnmarshalText implements encoding.TextUnmarshaler.
//
// Returns an error if d is not a canonical HeaderType string according to NeoFS API V2 spec
// or if the value is not supported. In these cases HeaderType remains untouched.
func (x *HeaderType) UnmarshalText(txt []byte) error {
	if ok := x.h.FromString(string(txt)); !ok {
		return fmt.Errorf("unsupported header type text: %s", txt)
	}

	return nil
}

// Unspecified checks if specific HeaderType is set.
func (x HeaderType) Unspecified() bool {
	return x.h == v2acl.HeaderTypeUnknown
}

// Object checks if HeaderType is set to object header.
func (x HeaderType) Object() bool {
	return x.h == v2acl.HeaderTypeObject
}

// SetObject sets HeaderType to object header.
func (x *HeaderType) SetObject() {
	x.h = v2acl.HeaderTypeObject
}

// Request checks if HeaderType is set to request header.
func (x HeaderType) Request() bool {
	return x.h == v2acl.HeaderTypeRequest
}

// SetRequest sets HeaderType to request header.
func (x *HeaderType) SetRequest() {
	x.h = v2acl.HeaderTypeRequest
}
