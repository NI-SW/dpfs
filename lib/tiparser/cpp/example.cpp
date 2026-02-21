#include "tidb_parser.h"

#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <string>

static std::string to_string(TidbString s) {
  if (s.data == nullptr || s.len <= 0) {
    return std::string();
  }
  return std::string(s.data, s.data + s.len);
}

static void print_indent(int n) {
  for (int i = 0; i < n; i++) {
    std::cout << ' ';
  }
}

static void print_value(const TidbAstValue& v, int indent, int depth);

static void print_node(const TidbAstNode* node, int indent, int depth) {
  if (node == nullptr) {
    print_indent(indent);
    std::cout << "<null node>\n";
    return;
  }

  print_indent(indent);
  std::cout << "Node(" << to_string(node->type_name) << ") pos=" << node->pos;
  if (node->literal != nullptr) {
    std::cout << " literal(go_type=" << to_string(node->literal->go_type) << ")";
  }
  std::cout << "\n";

  if (depth <= 0) {
    return;
  }

  if (node->literal != nullptr) {
    print_indent(indent + 2);
    std::cout << "literal.value = ";
    print_value(node->literal->value, indent + 2, depth - 1);
  }

  for (int32_t i = 0; i < node->field_count; i++) {
    const TidbAstField& field = node->fields[i];
    print_indent(indent + 2);
    std::cout << to_string(field.name) << " = ";
    print_value(field.value, indent + 2, depth - 1);
  }
}

static void print_value(const TidbAstValue& v, int indent, int depth) {
  switch (static_cast<TidbAstValueKind>(v.kind)) {
    case TIDB_AST_NULL:
      std::cout << "null\n";
      return;
    case TIDB_AST_BOOL:
      std::cout << (v.b ? "true" : "false") << "\n";
      return;
    case TIDB_AST_I64:
      std::cout << v.i64 << "\n";
      return;
    case TIDB_AST_U64:
      std::cout << v.u64 << "\n";
      return;
    case TIDB_AST_F64:
      std::cout << v.f64 << "\n";
      return;
    case TIDB_AST_STRING:
      std::cout << '"' << to_string(v.str) << "\"\n";
      return;
    case TIDB_AST_BYTES:
      std::cout << "bytes(len=" << v.bytes.len << ")\n";
      return;
    case TIDB_AST_NODE:
      std::cout << "\n";
      if (depth <= 0) {
        print_indent(indent + 2);
        std::cout << "<node depth limit>\n";
        return;
      }
      print_node(v.node, indent + 2, depth - 1);
      return;
    case TIDB_AST_ARRAY: {
      std::cout << "array(len=" << v.array.len << ")\n";
      if (depth <= 0 || v.array.items == nullptr || v.array.len <= 0) {
        return;
      }
      const int32_t limit = v.array.len > 5 ? 5 : v.array.len;
      for (int32_t i = 0; i < limit; i++) {
        print_indent(indent + 2);
        std::cout << "[" << i << "] ";
        print_value(v.array.items[i], indent + 2, depth - 1);
      }
      if (v.array.len > limit) {
        print_indent(indent + 2);
        std::cout << "... (" << (v.array.len - limit) << " more)\n";
      }
      return;
    }
    case TIDB_AST_MAP: {
      std::cout << "map(len=" << v.map_value.len << ")\n";
      if (depth <= 0 || v.map_value.items == nullptr || v.map_value.len <= 0) {
        return;
      }
      const int32_t limit = v.map_value.len > 5 ? 5 : v.map_value.len;
      for (int32_t i = 0; i < limit; i++) {
        const TidbAstMapEntry& entry = v.map_value.items[i];
        print_indent(indent + 2);
        std::cout << "key=";
        print_value(entry.key, indent + 2, depth - 1);
        print_indent(indent + 2);
        std::cout << "value=";
        print_value(entry.value, indent + 2, depth - 1);
      }
      if (v.map_value.len > limit) {
        print_indent(indent + 2);
        std::cout << "... (" << (v.map_value.len - limit) << " more)\n";
      }
      return;
    }
    case TIDB_AST_OBJECT: {
      std::cout << "object(" << to_string(v.object.type_name) << ") fields=" << v.object.field_count
                << "\n";
      if (depth <= 0 || v.object.fields == nullptr || v.object.field_count <= 0) {
        return;
      }
      const int32_t limit = v.object.field_count > 15 ? 15 : v.object.field_count;
      for (int32_t i = 0; i < limit; i++) {
        const TidbAstField& field = v.object.fields[i];
        print_indent(indent + 2);
        std::cout << to_string(field.name) << " = ";
        print_value(field.value, indent + 2, depth - 1);
      }
      if (v.object.field_count > limit) {
        print_indent(indent + 2);
        std::cout << "... (" << (v.object.field_count - limit) << " more)\n";
      }
      return;
    }
    case TIDB_AST_TYPED: {
      std::cout << "typed(" << to_string(v.typed.type_name) << ")\n";
      if (depth <= 0 || v.typed.inner == nullptr) {
        return;
      }
      print_indent(indent + 2);
      std::cout << "value = ";
      print_value(*v.typed.inner, indent + 2, depth - 1);
      return;
    }
    case TIDB_AST_TRUNCATED:
      std::cout << "<truncated>\n";
      return;
    default:
      std::cout << "<unknown kind=" << v.kind << ">\n";
      return;
  }
}

int main(int argc, char** argv) {
  std::string sql = "CREATE TABLE QWER(A INT NOT NULL, B CHAR(20), C BINARY(20))";
  if (argc >= 2) {
    sql = argv[1];
  }

  TidbParseResult* res = TidbParseSQLConst(sql.c_str());
  if (res == nullptr) {
    std::cerr << "TidbParseSQL returned NULL\n";
    return 2;
  }

  if (res->ok) {
    std::cout << "ok=1 schema_version=" << res->schema_version << " stmt_count=" << res->stmt_count
              << " warn_count=" << res->warn_count << "\n";
    std::cout << "parser=" << to_string(res->parser_module) << "@" << to_string(res->parser_version)
              << "\n";
    for (int32_t i = 0; i < res->warn_count; i++) {
      std::cout << "warn[" << i << "]=" << to_string(res->warnings[i]) << "\n";
    }
    for (int32_t i = 0; i < res->stmt_count; i++) {
      std::cout << "statement[" << i << "]:\n";
      print_node(res->statements[i], 2, 6);
    }
  } else {
    std::cout << "ok=0 err=" << to_string(res->err) << "\n";
  }

  const int ok = res->ok;
  TidbFreeParseResult(res);
  return ok ? 0 : 1;
}
