package main

/*
#include "tidb_parser.h"
*/
import "C"

import "unsafe"

func tidbStringToGo(s C.TidbString) string {
	if s.data == nil || s.len <= 0 {
		return ""
	}
	return unsafe.String((*byte)(unsafe.Pointer(s.data)), int(s.len))
}

func resultErrorString(res *C.TidbParseResult) string {
	if res == nil {
		return ""
	}
	return tidbStringToGo(res.err)
}

func firstStatementNode(res *C.TidbParseResult) *C.TidbAstNode {
	if res == nil || res.stmt_count <= 0 || res.statements == nil {
		return nil
	}
	stmts := unsafe.Slice(res.statements, int(res.stmt_count))
	return stmts[0]
}

func firstStatementTypeName(res *C.TidbParseResult) string {
	node := firstStatementNode(res)
	if node == nil {
		return ""
	}
	return tidbStringToGo(node.type_name)
}

func resultHasAnyLiteral(res *C.TidbParseResult) bool {
	return findLiteralInNode(firstStatementNode(res), 0)
}

func findLiteralInNode(node *C.TidbAstNode, depth int) bool {
	if node == nil {
		return false
	}
	if node.literal != nil {
		if int(node.literal.value.kind) != int(C.TIDB_AST_NULL) &&
			int(node.literal.value.kind) != int(C.TIDB_AST_TRUNCATED) {
			return true
		}
	}
	if depth > maxDOMDepth {
		return false
	}
	if node.fields == nil || node.field_count <= 0 {
		return false
	}
	fields := unsafe.Slice(node.fields, int(node.field_count))
	for i := range fields {
		if findLiteralInValue(&fields[i].value, depth+1) {
			return true
		}
	}
	return false
}

func findLiteralInValue(value *C.TidbAstValue, depth int) bool {
	if value == nil {
		return false
	}
	if depth > maxDOMDepth {
		return false
	}
	switch int(value.kind) {
	case int(C.TIDB_AST_NODE):
		return findLiteralInNode(value.node, depth+1)
	case int(C.TIDB_AST_ARRAY):
		if value.array.items == nil || value.array.len <= 0 {
			return false
		}
		items := unsafe.Slice(value.array.items, int(value.array.len))
		for i := range items {
			if findLiteralInValue(&items[i], depth+1) {
				return true
			}
		}
	case int(C.TIDB_AST_MAP):
		if value.map_value.items == nil || value.map_value.len <= 0 {
			return false
		}
		entries := unsafe.Slice(value.map_value.items, int(value.map_value.len))
		for i := range entries {
			if findLiteralInValue(&entries[i].key, depth+1) || findLiteralInValue(&entries[i].value, depth+1) {
				return true
			}
		}
	case int(C.TIDB_AST_OBJECT):
		if value.object.fields == nil || value.object.field_count <= 0 {
			return false
		}
		fields := unsafe.Slice(value.object.fields, int(value.object.field_count))
		for i := range fields {
			if findLiteralInValue(&fields[i].value, depth+1) {
				return true
			}
		}
	case int(C.TIDB_AST_TYPED):
		if value.typed.inner == nil {
			return false
		}
		return findLiteralInValue((*C.TidbAstValue)(unsafe.Pointer(value.typed.inner)), depth+1)
	default:
	}
	return false
}
