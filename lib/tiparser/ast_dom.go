package main

/*
#include "tidb_parser.h"
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>

typedef struct TidbArenaChunk {
  struct TidbArenaChunk* next;
  size_t cap;
  size_t used;
  unsigned char data[];
} TidbArenaChunk;

struct TidbArena {
  TidbArenaChunk* head;
};

static size_t tidb_align_up(size_t n, size_t align) {
  if (align == 0) {
    return n;
  }
  return (n + (align - 1)) & ~(align - 1);
}

static TidbArenaChunk* tidb_arena_chunk_new(size_t cap) {
  TidbArenaChunk* c = (TidbArenaChunk*)calloc(1, sizeof(TidbArenaChunk) + cap);
  if (!c) {
    return NULL;
  }
  c->cap = cap;
  c->used = 0;
  return c;
}

TidbArena* tidb_arena_new(void) {
  TidbArena* a = (TidbArena*)calloc(1, sizeof(TidbArena));
  return a;
}

void tidb_arena_free(TidbArena* a) {
  if (!a) {
    return;
  }
  TidbArenaChunk* c = a->head;
  while (c) {
    TidbArenaChunk* next = c->next;
    free(c);
    c = next;
  }
  free(a);
}

void* tidb_arena_alloc(TidbArena* a, size_t size, size_t align) {
  if (!a) {
    return NULL;
  }
  if (align == 0) {
    align = 1;
  }

  const size_t min_cap = 64 * 1024;
  TidbArenaChunk* c = a->head;
  if (!c) {
    size_t cap = min_cap;
    size_t need = size + align;
    if (cap < need) {
      cap = need;
    }
    c = tidb_arena_chunk_new(cap);
    if (!c) {
      return NULL;
    }
    c->next = a->head;
    a->head = c;
  }

  size_t offset = tidb_align_up(c->used, align);
  if (offset + size > c->cap) {
    size_t cap = c->cap * 2;
    if (cap < min_cap) {
      cap = min_cap;
    }
    size_t need = size + align;
    if (cap < need) {
      cap = need;
    }
    c = tidb_arena_chunk_new(cap);
    if (!c) {
      return NULL;
    }
    c->next = a->head;
    a->head = c;
    offset = tidb_align_up(c->used, align);
  }

  void* p = (void*)(c->data + offset);
  c->used = offset + size;
  return p;
}
*/
import "C"

import (
	"fmt"
	"reflect"
	"runtime/debug"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

const (
	astSchemaVersion = 1
	maxDOMDepth      = 200
)

type arena struct {
	ptr *C.TidbArena
}

func newArena() *arena {
	return &arena{ptr: C.tidb_arena_new()}
}

func (a *arena) alloc(size, align uintptr) unsafe.Pointer {
	if a == nil || a.ptr == nil || size == 0 {
		return nil
	}
	return C.tidb_arena_alloc(a.ptr, C.size_t(size), C.size_t(align))
}

func (a *arena) dupString(s string) C.TidbString {
	p := a.alloc(uintptr(len(s))+1, 1)
	if p == nil {
		return C.TidbString{}
	}
	dst := unsafe.Slice((*byte)(p), len(s)+1)
	copy(dst, s)
	dst[len(s)] = 0
	return C.TidbString{data: (*C.char)(p), len: C.int32_t(len(s))}
}

func (a *arena) dupBytes(b []byte) C.TidbBytes {
	if len(b) == 0 {
		return C.TidbBytes{}
	}
	p := a.alloc(uintptr(len(b)), 1)
	if p == nil {
		return C.TidbBytes{}
	}
	dst := unsafe.Slice((*byte)(p), len(b))
	copy(dst, b)
	return C.TidbBytes{data: (*C.uint8_t)(p), len: C.int32_t(len(b))}
}

func newParseResult() *C.TidbParseResult {
	var zero C.TidbParseResult
	res := (*C.TidbParseResult)(C.calloc(1, C.size_t(unsafe.Sizeof(zero))))
	if res == nil {
		return nil
	}
	a := newArena()
	if a.ptr == nil {
		C.free(unsafe.Pointer(res))
		return nil
	}
	res._arena = a.ptr
	res.schema_version = C.int32_t(astSchemaVersion)
	return res
}

func freeParseResult(result *C.TidbParseResult) {
	if result == nil {
		return
	}
	if result._arena != nil {
		C.tidb_arena_free(result._arena)
		result._arena = nil
	}
	C.free(unsafe.Pointer(result))
}

func setResultError(result *C.TidbParseResult, err error) {
	if result == nil {
		return
	}
	result.ok = 0
	if err == nil {
		result.err = C.TidbString{}
		return
	}
	a := &arena{ptr: result._arena}
	result.err = a.dupString(err.Error())
}

func parserModuleInfo() (module string, version string) {
	const parserModule = "github.com/pingcap/tidb/pkg/parser"

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return parserModule, "unknown"
	}
	if info.Main.Path == parserModule {
		return info.Main.Path, info.Main.Version
	}
	for _, dep := range info.Deps {
		if dep.Path == parserModule {
			return dep.Path, dep.Version
		}
	}
	return parserModule, "unknown"
}

type domEncoder struct {
	arena     *arena
	nodeCache map[uintptr]*C.TidbAstNode
}

func newDOMEncoder(a *arena) *domEncoder {
	return &domEncoder{
		arena:     a,
		nodeCache: make(map[uintptr]*C.TidbAstNode, 4096),
	}
}

func (e *domEncoder) buildIntoResult(res *C.TidbParseResult, stmts []ast.StmtNode, warns []error) error {
	if res == nil {
		return fmt.Errorf("nil result")
	}
	if e == nil || e.arena == nil || e.arena.ptr == nil {
		return fmt.Errorf("nil arena")
	}

	warnStrings := make([]string, 0, len(warns))
	for _, w := range warns {
		if w == nil {
			continue
		}
		warnStrings = append(warnStrings, w.Error())
	}
	if len(warnStrings) > 0 {
		var zero C.TidbString
		warningsPtr := (*C.TidbString)(e.arena.alloc(unsafe.Sizeof(zero)*uintptr(len(warnStrings)), unsafe.Alignof(zero)))
		if warningsPtr == nil {
			return fmt.Errorf("out of memory allocating warnings")
		}
		warnings := unsafe.Slice(warningsPtr, len(warnStrings))
		for i := range warnStrings {
			warnings[i] = e.arena.dupString(warnStrings[i])
		}
		res.warn_count = C.int32_t(len(warnStrings))
		res.warnings = warningsPtr
	}

	stmtCount := len(stmts)
	res.stmt_count = C.int32_t(stmtCount)
	if stmtCount == 0 {
		res.statements = nil
		return nil
	}

	var ptrZero *C.TidbAstNode
	statementsPtr := (**C.TidbAstNode)(e.arena.alloc(unsafe.Sizeof(ptrZero)*uintptr(stmtCount), unsafe.Alignof(ptrZero)))
	if statementsPtr == nil {
		return fmt.Errorf("out of memory allocating statements")
	}
	statementSlice := unsafe.Slice(statementsPtr, stmtCount)
	for i := range stmts {
		statementSlice[i] = e.encodeNode(stmts[i], 0)
	}
	res.statements = statementsPtr
	return nil
}

func (e *domEncoder) encodeNode(node ast.Node, depth int) *C.TidbAstNode {
	if node == nil {
		return nil
	}

	rv := reflect.ValueOf(node)
	if rv.IsValid() && rv.Kind() == reflect.Pointer && rv.IsNil() {
		return nil
	}

	var cacheKey uintptr
	if rv.IsValid() && rv.Kind() == reflect.Pointer && !rv.IsNil() {
		cacheKey = rv.Pointer()
		if existing := e.nodeCache[cacheKey]; existing != nil {
			return existing
		}
	}

	var nodeZero C.TidbAstNode
	out := (*C.TidbAstNode)(e.arena.alloc(unsafe.Sizeof(nodeZero), unsafe.Alignof(nodeZero)))
	if out == nil {
		return nil
	}
	if cacheKey != 0 {
		e.nodeCache[cacheKey] = out
	}

	out.type_name = e.arena.dupString(qualifiedTypeName(reflect.TypeOf(node)))
	out.pos = C.int32_t(node.OriginTextPosition())
	out.field_count = 0
	out.fields = nil
	out.literal = nil

	if depth > maxDOMDepth {
		return out
	}

	if valueExpr, ok := node.(ast.ValueExpr); ok {
		var litZero C.TidbAstLiteral
		lit := (*C.TidbAstLiteral)(e.arena.alloc(unsafe.Sizeof(litZero), unsafe.Alignof(litZero)))
		if lit != nil {
			lit.has_kind = 0
			if k, ok := any(valueExpr).(interface{ Kind() byte }); ok {
				lit.has_kind = 1
				lit.kind = C.uint8_t(k.Kind())
			}

			value := valueExpr.GetValue()
			lit.go_type = e.arena.dupString(fmt.Sprintf("%T", value))
			lit.value = e.encodeLiteralValue(value, depth+1)
			out.literal = lit
		}
	}

	for rv.IsValid() && rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	if !rv.IsValid() || rv.Kind() != reflect.Struct {
		return out
	}

	rt := rv.Type()
	exportedCount := 0
	for i := 0; i < rt.NumField(); i++ {
		if rt.Field(i).IsExported() {
			exportedCount++
		}
	}
	if exportedCount == 0 {
		return out
	}

	var fieldZero C.TidbAstField
	fieldsPtr := (*C.TidbAstField)(e.arena.alloc(unsafe.Sizeof(fieldZero)*uintptr(exportedCount), unsafe.Alignof(fieldZero)))
	if fieldsPtr == nil {
		return out
	}
	fields := unsafe.Slice(fieldsPtr, exportedCount)

	writeIndex := 0
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		if !sf.IsExported() {
			continue
		}
		fields[writeIndex].name = e.arena.dupString(sf.Name)
		fields[writeIndex].value = e.encodeValue(rv.Field(i), depth+1)
		writeIndex++
	}

	out.field_count = C.int32_t(exportedCount)
	out.fields = fieldsPtr
	return out
}

func (e *domEncoder) encodeValue(v reflect.Value, depth int) C.TidbAstValue {
	var out C.TidbAstValue

	if !v.IsValid() {
		out.kind = C.int32_t(C.TIDB_AST_NULL)
		return out
	}

	if depth > maxDOMDepth {
		if node, ok := asNode(v); ok {
			out.kind = C.int32_t(C.TIDB_AST_NODE)
			out.node = e.encodeNode(node, depth)
			return out
		}
		out.kind = C.int32_t(C.TIDB_AST_TRUNCATED)
		return out
	}

	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			out.kind = C.int32_t(C.TIDB_AST_NULL)
			return out
		}
		concrete := v.Elem()
		if !concrete.IsValid() {
			out.kind = C.int32_t(C.TIDB_AST_NULL)
			return out
		}
		if isPrimitiveKind(concrete.Kind()) {
			return e.encodeValue(concrete, depth+1)
		}
		if node, ok := asNode(concrete); ok {
			out.kind = C.int32_t(C.TIDB_AST_NODE)
			out.node = e.encodeNode(node, depth+1)
			return out
		}

		var innerZero C.TidbAstValue
		inner := (*C.TidbAstValue)(e.arena.alloc(unsafe.Sizeof(innerZero), unsafe.Alignof(innerZero)))
		if inner == nil {
			out.kind = C.int32_t(C.TIDB_AST_TRUNCATED)
			return out
		}
		*inner = e.encodeValue(concrete, depth+1)

		out.kind = C.int32_t(C.TIDB_AST_TYPED)
		out.typed.type_name = e.arena.dupString(qualifiedTypeName(concrete.Type()))
		out.typed.inner = inner
		return out
	}

	if node, ok := asNode(v); ok {
		out.kind = C.int32_t(C.TIDB_AST_NODE)
		out.node = e.encodeNode(node, depth+1)
		return out
	}

	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			out.kind = C.int32_t(C.TIDB_AST_NULL)
			return out
		}
		return e.encodeValue(v.Elem(), depth+1)
	case reflect.Bool:
		out.kind = C.int32_t(C.TIDB_AST_BOOL)
		if v.Bool() {
			out.b = 1
		}
		return out
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		out.kind = C.int32_t(C.TIDB_AST_I64)
		out.i64 = C.int64_t(v.Int())
		return out
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		out.kind = C.int32_t(C.TIDB_AST_U64)
		out.u64 = C.uint64_t(v.Uint())
		return out
	case reflect.Float32, reflect.Float64:
		out.kind = C.int32_t(C.TIDB_AST_F64)
		out.f64 = C.double(v.Float())
		return out
	case reflect.String:
		out.kind = C.int32_t(C.TIDB_AST_STRING)
		out.str = e.arena.dupString(v.String())
		return out
	case reflect.Slice:
		if v.IsNil() {
			out.kind = C.int32_t(C.TIDB_AST_NULL)
			return out
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			out.kind = C.int32_t(C.TIDB_AST_BYTES)
			out.bytes = e.arena.dupBytes(v.Bytes())
			return out
		}
		out.kind = C.int32_t(C.TIDB_AST_ARRAY)
		length := v.Len()
		if length == 0 {
			return out
		}
		var itemZero C.TidbAstValue
		itemsPtr := (*C.TidbAstValue)(e.arena.alloc(unsafe.Sizeof(itemZero)*uintptr(length), unsafe.Alignof(itemZero)))
		if itemsPtr == nil {
			out.kind = C.int32_t(C.TIDB_AST_TRUNCATED)
			return out
		}
		items := unsafe.Slice(itemsPtr, length)
		for i := 0; i < length; i++ {
			items[i] = e.encodeValue(v.Index(i), depth+1)
		}
		out.array.items = itemsPtr
		out.array.len = C.int32_t(length)
		return out
	case reflect.Array:
		out.kind = C.int32_t(C.TIDB_AST_ARRAY)
		length := v.Len()
		if length == 0 {
			return out
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			buf := make([]byte, length)
			for i := 0; i < length; i++ {
				buf[i] = byte(v.Index(i).Uint())
			}
			out.kind = C.int32_t(C.TIDB_AST_BYTES)
			out.bytes = e.arena.dupBytes(buf)
			return out
		}
		var itemZero C.TidbAstValue
		itemsPtr := (*C.TidbAstValue)(e.arena.alloc(unsafe.Sizeof(itemZero)*uintptr(length), unsafe.Alignof(itemZero)))
		if itemsPtr == nil {
			out.kind = C.int32_t(C.TIDB_AST_TRUNCATED)
			return out
		}
		items := unsafe.Slice(itemsPtr, length)
		for i := 0; i < length; i++ {
			items[i] = e.encodeValue(v.Index(i), depth+1)
		}
		out.array.items = itemsPtr
		out.array.len = C.int32_t(length)
		return out
	case reflect.Map:
		if v.IsNil() {
			out.kind = C.int32_t(C.TIDB_AST_NULL)
			return out
		}
		out.kind = C.int32_t(C.TIDB_AST_MAP)
		length := v.Len()
		if length == 0 {
			return out
		}
		var entryZero C.TidbAstMapEntry
		entriesPtr := (*C.TidbAstMapEntry)(e.arena.alloc(unsafe.Sizeof(entryZero)*uintptr(length), unsafe.Alignof(entryZero)))
		if entriesPtr == nil {
			out.kind = C.int32_t(C.TIDB_AST_TRUNCATED)
			return out
		}
		keys := v.MapKeys()
		entries := unsafe.Slice(entriesPtr, length)
		for i := range keys {
			entries[i].key = e.encodeValue(keys[i], depth+1)
			entries[i].value = e.encodeValue(v.MapIndex(keys[i]), depth+1)
		}
		out.map_value.items = entriesPtr
		out.map_value.len = C.int32_t(length)
		return out
	case reflect.Struct:
		out.kind = C.int32_t(C.TIDB_AST_OBJECT)
		rt := v.Type()
		exported := 0
		for i := 0; i < rt.NumField(); i++ {
			if rt.Field(i).IsExported() {
				exported++
			}
		}
		if exported == 0 {
			if v.CanInterface() {
				if s, ok := v.Interface().(fmt.Stringer); ok {
					return e.wrapTypedString(rt, s.String())
				}
				return e.wrapTypedString(rt, fmt.Sprintf("%v", v.Interface()))
			}
			return e.wrapTypedString(rt, rt.String())
		}

		var fieldZero C.TidbAstField
		fieldsPtr := (*C.TidbAstField)(e.arena.alloc(unsafe.Sizeof(fieldZero)*uintptr(exported), unsafe.Alignof(fieldZero)))
		if fieldsPtr == nil {
			out.kind = C.int32_t(C.TIDB_AST_TRUNCATED)
			return out
		}
		fields := unsafe.Slice(fieldsPtr, exported)
		writeIndex := 0
		for i := 0; i < rt.NumField(); i++ {
			sf := rt.Field(i)
			if !sf.IsExported() {
				continue
			}
			fields[writeIndex].name = e.arena.dupString(sf.Name)
			fields[writeIndex].value = e.encodeValue(v.Field(i), depth+1)
			writeIndex++
		}
		out.object.type_name = e.arena.dupString(qualifiedTypeName(rt))
		out.object.fields = fieldsPtr
		out.object.field_count = C.int32_t(exported)
		return out
	default:
		if v.CanInterface() {
			return e.wrapTypedString(v.Type(), fmt.Sprintf("%v", v.Interface()))
		}
		return e.wrapTypedString(v.Type(), v.Type().String())
	}
}

func (e *domEncoder) wrapTypedString(t reflect.Type, s string) C.TidbAstValue {
	var innerZero C.TidbAstValue
	inner := (*C.TidbAstValue)(e.arena.alloc(unsafe.Sizeof(innerZero), unsafe.Alignof(innerZero)))
	if inner != nil {
		inner.kind = C.int32_t(C.TIDB_AST_STRING)
		inner.str = e.arena.dupString(s)
	}

	var out C.TidbAstValue
	out.kind = C.int32_t(C.TIDB_AST_TYPED)
	out.typed.type_name = e.arena.dupString(qualifiedTypeName(t))
	out.typed.inner = inner
	return out
}

func (e *domEncoder) encodeLiteralValue(value any, depth int) C.TidbAstValue {
	if depth > maxDOMDepth {
		var out C.TidbAstValue
		out.kind = C.int32_t(C.TIDB_AST_TRUNCATED)
		return out
	}
	if value == nil {
		var out C.TidbAstValue
		out.kind = C.int32_t(C.TIDB_AST_NULL)
		return out
	}
	return e.encodeValue(reflect.ValueOf(value), depth)
}

func asNode(v reflect.Value) (ast.Node, bool) {
	if !v.IsValid() {
		return nil, false
	}
	if v.Kind() == reflect.Pointer && v.IsNil() {
		return nil, false
	}
	if v.CanInterface() {
		if n, ok := v.Interface().(ast.Node); ok && n != nil {
			return n, true
		}
	}
	if v.Kind() == reflect.Struct && v.CanAddr() && v.Addr().CanInterface() {
		if n, ok := v.Addr().Interface().(ast.Node); ok && n != nil {
			return n, true
		}
	}
	return nil, false
}

func isPrimitiveKind(k reflect.Kind) bool {
	switch k {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	default:
		return false
	}
}

func qualifiedTypeName(t reflect.Type) string {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil {
		return ""
	}
	if t.PkgPath() != "" && t.Name() != "" {
		return t.PkgPath() + "." + t.Name()
	}
	return t.String()
}
