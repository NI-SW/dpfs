#ifndef TIDB_PARSER_H
#define TIDB_PARSER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TidbArena TidbArena; // internal

typedef struct TidbString {
  const char* data; // NUL-terminated
  int32_t len;      // excluding NUL
} TidbString;

typedef struct TidbBytes {
  const uint8_t* data;
  int32_t len;
} TidbBytes;

typedef enum TidbAstValueKind {
  TIDB_AST_NULL = 0,
  TIDB_AST_BOOL = 1,
  TIDB_AST_I64 = 2,
  TIDB_AST_U64 = 3,
  TIDB_AST_F64 = 4,
  TIDB_AST_STRING = 5,
  TIDB_AST_BYTES = 6,
  TIDB_AST_NODE = 7,
  TIDB_AST_ARRAY = 8,
  TIDB_AST_MAP = 9,
  TIDB_AST_OBJECT = 10,
  TIDB_AST_TYPED = 11,
  TIDB_AST_TRUNCATED = 12,
} TidbAstValueKind;

typedef struct TidbAstNode TidbAstNode;
typedef struct TidbAstValue TidbAstValue;
typedef struct TidbAstField TidbAstField;
typedef struct TidbAstMapEntry TidbAstMapEntry;

typedef struct TidbAstArray {
  const TidbAstValue* items;
  int32_t len;
} TidbAstArray;

typedef struct TidbAstMap {
  const TidbAstMapEntry* items;
  int32_t len;
} TidbAstMap;

typedef struct TidbAstObject {
  TidbString type_name;
  const TidbAstField* fields;
  int32_t field_count;
} TidbAstObject;

typedef struct TidbAstTyped {
  TidbString type_name;
  const TidbAstValue* inner;
} TidbAstTyped;

struct TidbAstValue {
  int32_t kind;
  TidbString type_name;
  const TidbAstValue* inner;

  int32_t b;
  int64_t i64;
  uint64_t u64;
  double f64;
  TidbString str;
  TidbBytes bytes;

  const TidbAstNode* node;
  TidbAstArray array;
  TidbAstMap map_value;
  TidbAstObject object;
  TidbAstTyped typed;
};

struct TidbAstField {
  TidbString name;
  TidbAstValue value;
};

struct TidbAstMapEntry {
  TidbAstValue key;
  TidbAstValue value;
};

typedef struct TidbAstLiteral {
  int32_t has_kind;
  uint8_t kind;
  TidbString go_type;
  TidbAstValue value;
} TidbAstLiteral;

struct TidbAstNode {
  TidbString type_name;
  int32_t pos;
  int32_t field_count;
  const TidbAstField* fields;
  const TidbAstLiteral* literal;
};

typedef struct TidbParseResult {
  int32_t ok;
  int32_t schema_version;
  int32_t stmt_count;
  const TidbAstNode** statements;
  int32_t warn_count;
  TidbString* warnings;
  TidbString parser_module;
  TidbString parser_version;
  TidbString err;
  TidbArena* _arena;
} TidbParseResult;

// TidbParseSQL parses `sql` and returns a heap-allocated TidbParseResult.
// Caller must free it with TidbFreeParseResult.
TidbParseResult* TidbParseSQL(char* sql);

// TidbFreeParseResult frees a TidbParseResult returned by TidbParseSQL.
void TidbFreeParseResult(TidbParseResult* result);

#ifdef __cplusplus
} // extern "C"

// Convenience wrapper for C++ callers.
static inline TidbParseResult* TidbParseSQLConst(const char* sql) {
  return TidbParseSQL(const_cast<char*>(sql));
}
#endif

#endif // TIDB_PARSER_H
