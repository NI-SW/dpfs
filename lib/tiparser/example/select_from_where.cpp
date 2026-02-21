#include "tidb_parser.h"

#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

static std::string to_string(TidbString s) {
  if (s.data == nullptr || s.len <= 0) {
    return std::string();
  }
  return std::string(s.data, s.data + s.len);
}

static bool streq(TidbString s, const char* lit) {
  if (lit == nullptr) {
    return false;
  }
  const size_t n = std::strlen(lit);
  return s.data != nullptr && s.len == static_cast<int32_t>(n) && std::memcmp(s.data, lit, n) == 0;
}

static bool ends_with(const std::string& s, const char* suffix) {
  const size_t n = std::strlen(suffix);
  return s.size() >= n && s.compare(s.size() - n, n, suffix) == 0;
}

static const TidbAstField* find_field(const TidbAstNode* n, const char* name) {
  if (n == nullptr || n->fields == nullptr || n->field_count <= 0) {
    return nullptr;
  }
  for (int32_t i = 0; i < n->field_count; i++) {
    if (streq(n->fields[i].name, name)) {
      return &n->fields[i];
    }
  }
  return nullptr;
}

static const TidbAstValue* unwrap_typed(const TidbAstValue* v) {
  if (v == nullptr) {
    return nullptr;
  }
  if (v->kind == TIDB_AST_TYPED && v->typed.inner != nullptr) {
    return v->typed.inner;
  }
  return v;
}

static const TidbAstNode* as_node(const TidbAstValue* v) {
  v = unwrap_typed(v);
  if (v == nullptr) {
    return nullptr;
  }
  if (static_cast<TidbAstValueKind>(v->kind) == TIDB_AST_NODE) {
    return v->node;
  }
  return nullptr;
}

static std::string cistr_O(const TidbAstValue* v) {
  v = unwrap_typed(v);
  if (v == nullptr) {
    return std::string();
  }
  if (static_cast<TidbAstValueKind>(v->kind) != TIDB_AST_OBJECT) {
    return std::string();
  }
  if (v->object.fields == nullptr || v->object.field_count <= 0) {
    return std::string();
  }
  for (int32_t i = 0; i < v->object.field_count; i++) {
    const TidbAstField& f = v->object.fields[i];
    if (!streq(f.name, "O")) {
      continue;
    }
    const TidbAstValue* fv = unwrap_typed(&f.value);
    if (fv != nullptr && static_cast<TidbAstValueKind>(fv->kind) == TIDB_AST_STRING) {
      return to_string(fv->str);
    }
  }
  return std::string();
}

static std::string hex_bytes(TidbBytes b) {
  if (b.data == nullptr || b.len <= 0) {
    return "0x";
  }
  std::ostringstream oss;
  oss << "0x" << std::hex << std::setfill('0');
  for (int32_t i = 0; i < b.len; i++) {
    oss << std::setw(2) << static_cast<unsigned>(b.data[i]);
  }
  return oss.str();
}

static std::string scalar_value_to_string(const TidbAstValue* v) {
  v = unwrap_typed(v);
  if (v == nullptr) {
    return "null";
  }
  switch (static_cast<TidbAstValueKind>(v->kind)) {
    case TIDB_AST_NULL:
      return "NULL";
    case TIDB_AST_BOOL:
      return v->b ? "true" : "false";
    case TIDB_AST_I64:
      return std::to_string(v->i64);
    case TIDB_AST_U64:
      return std::to_string(v->u64);
    case TIDB_AST_F64: {
      std::ostringstream oss;
      oss << v->f64;
      return oss.str();
    }
    case TIDB_AST_STRING:
      return "'" + to_string(v->str) + "'";
    case TIDB_AST_BYTES:
      return hex_bytes(v->bytes);
    default:
      return "<non-scalar>";
  }
}

static const char* opcode_literal(int64_t op) {
  switch (op) {
    case 1:
      return "AND";
    case 4:
      return "OR";
    case 5:
      return ">=";
    case 6:
      return "<=";
    case 7:
      return "=";
    case 8:
      return "!=";
    case 9:
      return "<";
    case 10:
      return ">";
    case 11:
      return "+";
    case 12:
      return "-";
    case 17:
      return "/";
    case 18:
      return "*";
    case 24:
      return "<=>";
    case 25:
      return "IN";
    case 26:
      return "LIKE";
    default:
      return nullptr;
  }
}

static std::string column_name_to_string(const TidbAstNode* col) {
  if (col == nullptr) {
    return "<null col>";
  }
  const TidbAstField* schemaF = find_field(col, "Schema");
  const TidbAstField* tableF = find_field(col, "Table");
  const TidbAstField* nameF = find_field(col, "Name");
  const std::string schema = schemaF ? cistr_O(&schemaF->value) : "";
  const std::string table = tableF ? cistr_O(&tableF->value) : "";
  const std::string name = nameF ? cistr_O(&nameF->value) : "";

  std::string out;
  if (!schema.empty()) {
    out += schema + ".";
  }
  if (!table.empty()) {
    out += table + ".";
  }
  out += name.empty() ? "<unnamed>" : name;
  return out;
}

static std::string expr_to_string(const TidbAstNode* expr, int depth);

static std::string binary_op_to_string(const TidbAstNode* expr, int depth) {
  const TidbAstField* opF = find_field(expr, "Op");
  const TidbAstField* lF = find_field(expr, "L");
  const TidbAstField* rF = find_field(expr, "R");

  int64_t op = 0;
  if (opF != nullptr) {
    const TidbAstValue* ov = unwrap_typed(&opF->value);
    if (ov != nullptr && static_cast<TidbAstValueKind>(ov->kind) == TIDB_AST_I64) {
      op = ov->i64;
    } else if (ov != nullptr && static_cast<TidbAstValueKind>(ov->kind) == TIDB_AST_U64) {
      op = static_cast<int64_t>(ov->u64);
    }
  }

  const char* opLit = opcode_literal(op);
  std::string opStr = opLit ? std::string(opLit) : ("OP#" + std::to_string(op));
  const TidbAstNode* l = lF ? as_node(&lF->value) : nullptr;
  const TidbAstNode* r = rF ? as_node(&rF->value) : nullptr;
  return "(" + expr_to_string(l, depth - 1) + " " + opStr + " " + expr_to_string(r, depth - 1) + ")";
}

static std::string in_expr_to_string(const TidbAstNode* expr, int depth) {
  const TidbAstNode* lhs = nullptr;
  const TidbAstValue* listV = nullptr;
  bool notIn = false;

  if (const TidbAstField* f = find_field(expr, "Expr"); f != nullptr) {
    lhs = as_node(&f->value);
  }
  if (const TidbAstField* f = find_field(expr, "List"); f != nullptr) {
    listV = unwrap_typed(&f->value);
  }
  if (const TidbAstField* f = find_field(expr, "Not"); f != nullptr) {
    const TidbAstValue* bv = unwrap_typed(&f->value);
    if (bv != nullptr && static_cast<TidbAstValueKind>(bv->kind) == TIDB_AST_BOOL) {
      notIn = bv->b != 0;
    }
  }

  std::string out = "(" + expr_to_string(lhs, depth - 1) + (notIn ? " NOT IN (" : " IN (");
  if (listV != nullptr && static_cast<TidbAstValueKind>(listV->kind) == TIDB_AST_ARRAY && listV->array.items != nullptr &&
      listV->array.len > 0) {
    const int32_t n = listV->array.len;
    for (int32_t i = 0; i < n; i++) {
      const TidbAstNode* item = as_node(&listV->array.items[i]);
      if (i != 0) {
        out += ", ";
      }
      out += expr_to_string(item, depth - 1);
    }
  } else {
    out += "<list>";
  }
  out += "))";
  return out;
}

static std::string expr_to_string(const TidbAstNode* expr, int depth) {
  if (expr == nullptr) {
    return "NULL";
  }
  if (depth <= 0) {
    return "<depth>";
  }

  const std::string t = to_string(expr->type_name);
  if (ends_with(t, "ast.ColumnNameExpr")) {
    const TidbAstField* nameF = find_field(expr, "Name");
    return column_name_to_string(nameF ? as_node(&nameF->value) : nullptr);
  }
  if (ends_with(t, "test_driver.ValueExpr")) {
    if (expr->literal != nullptr) {
      return scalar_value_to_string(&expr->literal->value);
    }
    return "<literal>";
  }
  if (ends_with(t, "ast.BinaryOperationExpr")) {
    return binary_op_to_string(expr, depth);
  }
  if (ends_with(t, "ast.PatternInExpr")) {
    return in_expr_to_string(expr, depth);
  }
  if (ends_with(t, "ast.ParenthesesExpr")) {
    const TidbAstField* innerF = find_field(expr, "Expr");
    return "(" + expr_to_string(innerF ? as_node(&innerF->value) : nullptr, depth - 1) + ")";
  }
  if (ends_with(t, "ast.UnaryOperationExpr")) {
    const TidbAstField* opF = find_field(expr, "Op");
    int64_t op = 0;
    if (opF != nullptr) {
      const TidbAstValue* ov = unwrap_typed(&opF->value);
      if (ov != nullptr && static_cast<TidbAstValueKind>(ov->kind) == TIDB_AST_I64) {
        op = ov->i64;
      } else if (ov != nullptr && static_cast<TidbAstValueKind>(ov->kind) == TIDB_AST_U64) {
        op = static_cast<int64_t>(ov->u64);
      }
    }
    const char* opLit = opcode_literal(op);
    std::string opStr = opLit ? std::string(opLit) : ("OP#" + std::to_string(op));
    const TidbAstField* vF = find_field(expr, "V");
    return "(" + opStr + " " + expr_to_string(vF ? as_node(&vF->value) : nullptr, depth - 1) + ")";
  }

  return "<" + t + ">";
}

struct SimpleSelectFromWhere {
  std::vector<std::string> projections;
  std::string from_table;
  std::string from_alias;
  std::string where;
};

static bool parse_projection_list(const TidbAstNode* selectStmt, SimpleSelectFromWhere* out, std::string* err) {
  const TidbAstField* fieldsF = find_field(selectStmt, "Fields");
  const TidbAstNode* fieldList = fieldsF ? as_node(&fieldsF->value) : nullptr;
  if (fieldList == nullptr) {
    if (err) *err = "SELECT.Fields is NULL";
    return false;
  }

  const TidbAstField* listF = find_field(fieldList, "Fields");
  const TidbAstValue* listV = listF ? unwrap_typed(&listF->value) : nullptr;
  if (listV == nullptr || static_cast<TidbAstValueKind>(listV->kind) != TIDB_AST_ARRAY) {
    if (err) *err = "FieldList.Fields is not an array";
    return false;
  }

  for (int32_t i = 0; i < listV->array.len; i++) {
    const TidbAstNode* selField = as_node(&listV->array.items[i]);
    if (selField == nullptr) {
      out->projections.push_back("<field>");
      continue;
    }

    const TidbAstField* wcF = find_field(selField, "WildCard");
    const TidbAstNode* wildCard = wcF ? as_node(&wcF->value) : nullptr;
    if (wildCard != nullptr) {
      const TidbAstField* schemaF = find_field(wildCard, "Schema");
      const TidbAstField* tableF = find_field(wildCard, "Table");
      const std::string schema = schemaF ? cistr_O(&schemaF->value) : "";
      const std::string table = tableF ? cistr_O(&tableF->value) : "";
      std::string s;
      if (!schema.empty()) s += schema + ".";
      if (!table.empty()) s += table + ".";
      s += "*";
      out->projections.push_back(s);
      continue;
    }

    const TidbAstField* exprF = find_field(selField, "Expr");
    const TidbAstNode* expr = exprF ? as_node(&exprF->value) : nullptr;
    std::string proj = expr_to_string(expr, 200);

    const TidbAstField* asF = find_field(selField, "AsName");
    const std::string alias = asF ? cistr_O(&asF->value) : "";
    if (!alias.empty()) {
      proj += " AS " + alias;
    }
    out->projections.push_back(proj);
  }

  return true;
}

static bool parse_from_clause(const TidbAstNode* selectStmt, SimpleSelectFromWhere* out, std::string* err) {
  const TidbAstField* fromF = find_field(selectStmt, "From");
  const TidbAstNode* from = fromF ? as_node(&fromF->value) : nullptr;
  if (from == nullptr) {
    out->from_table.clear();
    out->from_alias.clear();
    return true;
  }

  const TidbAstField* tableRefsF = find_field(from, "TableRefs");
  const TidbAstNode* join = tableRefsF ? as_node(&tableRefsF->value) : nullptr;
  if (join == nullptr) {
    if (err) *err = "FROM.TableRefs is NULL";
    return false;
  }

  const TidbAstField* leftF = find_field(join, "Left");
  const TidbAstNode* tableSource = leftF ? as_node(&leftF->value) : nullptr;
  if (tableSource == nullptr) {
    if (err) *err = "Join.Left is NULL";
    return false;
  }

  const TidbAstField* sourceF = find_field(tableSource, "Source");
  const TidbAstNode* tableName = sourceF ? as_node(&sourceF->value) : nullptr;
  if (tableName == nullptr) {
    if (err) *err = "TableSource.Source is NULL";
    return false;
  }

  const TidbAstField* schemaF = find_field(tableName, "Schema");
  const TidbAstField* nameF = find_field(tableName, "Name");
  const std::string schema = schemaF ? cistr_O(&schemaF->value) : "";
  const std::string name = nameF ? cistr_O(&nameF->value) : "";
  if (name.empty()) {
    if (err) *err = "TableName.Name is empty";
    return false;
  }
  out->from_table = schema.empty() ? name : (schema + "." + name);

  const TidbAstField* asF = find_field(tableSource, "AsName");
  out->from_alias = asF ? cistr_O(&asF->value) : "";
  return true;
}

static bool parse_where_clause(const TidbAstNode* selectStmt, SimpleSelectFromWhere* out) {
  const TidbAstField* whereF = find_field(selectStmt, "Where");
  const TidbAstNode* where = whereF ? as_node(&whereF->value) : nullptr;
  out->where = where ? expr_to_string(where, 200) : "";
  return true;
}

static bool parse_simple_select_from_where(const TidbAstNode* stmt, SimpleSelectFromWhere* out, std::string* err) {
  if (stmt == nullptr) {
    if (err) *err = "statement is NULL";
    return false;
  }
  const std::string t = to_string(stmt->type_name);
  if (!ends_with(t, "ast.SelectStmt")) {
    if (err) *err = "only SelectStmt is supported, got: " + t;
    return false;
  }
  if (!parse_projection_list(stmt, out, err)) return false;
  if (!parse_from_clause(stmt, out, err)) return false;
  parse_where_clause(stmt, out);
  return true;
}

static std::string join_csv(const std::vector<std::string>& xs) {
  std::string out;
  for (size_t i = 0; i < xs.size(); i++) {
    if (i != 0) out += ", ";
    out += xs[i];
  }
  return out;
}

int main(int argc, char** argv) {
  std::string sql = "select a, b from t where a in (1,2)";
  if (argc >= 2) {
    sql = argv[1];
  }

  TidbParseResult* res = TidbParseSQLConst(sql.c_str());
  if (res == nullptr) {
    std::cerr << "TidbParseSQL returned NULL\n";
    return 2;
  }
  if (!res->ok) {
    std::cerr << "parse error: " << to_string(res->err) << "\n";
    TidbFreeParseResult(res);
    return 1;
  }
  if (res->stmt_count <= 0 || res->statements == nullptr || res->statements[0] == nullptr) {
    std::cerr << "no statements\n";
    TidbFreeParseResult(res);
    return 1;
  }

  SimpleSelectFromWhere q;
  std::string err;
  if (!parse_simple_select_from_where(res->statements[0], &q, &err)) {
    std::cerr << "unsupported: " << err << "\n";
    TidbFreeParseResult(res);
    return 1;
  }

  std::cout << "SQL: " << sql << "\n";
  std::cout << "SELECT: " << join_csv(q.projections) << "\n";
  if (!q.from_table.empty()) {
    std::cout << "FROM: " << q.from_table;
    if (!q.from_alias.empty()) std::cout << " AS " << q.from_alias;
    std::cout << "\n";
  } else {
    std::cout << "FROM: <none>\n";
  }
  std::cout << "WHERE: " << (q.where.empty() ? "<none>" : q.where) << "\n\n";

  std::cout << "Plan:\n";
  std::cout << "  Projection[" << join_csv(q.projections) << "]\n";
  if (!q.where.empty()) {
    std::cout << "    Filter[" << q.where << "]\n";
    std::cout << "      Scan[" << (q.from_table.empty() ? "<none>" : q.from_table) << "]\n";
  } else {
    std::cout << "    Scan[" << (q.from_table.empty() ? "<none>" : q.from_table) << "]\n";
  }

  TidbFreeParseResult(res);
  return 0;
}
