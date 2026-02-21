# Quick Start

本库把 TiDB SQL Parser（`github.com/pingcap/tidb/pkg/parser`）封装为一个 C API 动态库，供 C/C++ 直接调用。

返回值不是 JSON，而是一个 **可直接遍历的 AST DOM（C 结构体）**：你可以在 C++ 侧通过 `type_name + fields[]` 遍历整棵语法树，用于构建自己的 query plan。

---

## 1. 构建产物

```bash
make build
```

生成：

- `build/libtidbparser.(dylib|so)`
- `build/tidb_parser.h`

> 首次构建需要能下载 Go modules（TiDB 依赖较大）。

---

## 2. 最小调用（C++）

```cpp
#include "tidb_parser.h"
#include <iostream>
#include <string>

static std::string to_string(TidbString s) {
  if (!s.data || s.len <= 0) return {};
  return std::string(s.data, s.data + s.len);
}

int main() {
  TidbParseResult* res = TidbParseSQLConst("select 1 from t where a = 1");
  if (!res) return 2;

  if (!res->ok) {
    std::cerr << "parse error: " << to_string(res->err) << "\n";
    TidbFreeParseResult(res);
    return 1;
  }

  for (int32_t i = 0; i < res->stmt_count; i++) {
    const TidbAstNode* root = res->statements[i];
    std::cout << "stmt[" << i << "] type=" << to_string(root->type_name)
              << " pos=" << root->pos << "\n";
  }

  TidbFreeParseResult(res);
  return 0;
}
```

编译/链接（macOS）示例：

```bash
c++ -std=c++17 -O2 -I. demo.cpp -L./build -ltidbparser -Wl,-rpath,@loader_path/build -o demo
./demo
```

编译/链接（Linux）示例：

```bash
c++ -std=c++17 -O2 -I. demo.cpp -L./build -ltidbparser -Wl,-rpath,'$ORIGIN/build' -o demo
./demo
```

---

## 3. 结果结构与生命周期

### 3.1 `TidbParseResult`（入口）

- `ok`：1 成功 / 0 失败
- `stmt_count` + `statements`：语句数量 + 语句根节点指针数组
- `warn_count` + `warnings`：warning 数量 + warning 字符串数组
- `parser_module` / `parser_version`：parser module 信息
- `err`：失败时错误信息（`TidbString`）

### 3.2 内存模型（非常重要）

`TidbParseSQL` 返回的 `TidbParseResult*` 以及其内部所有指针（节点、字段、字符串、数组等）都属于该结果对象：

- 调用方只读访问
- 不要对内部指针单独 `free()`
- 用完只调用一次：`TidbFreeParseResult(result)`

**不要在 `TidbFreeParseResult` 之后继续使用任何内部指针。**

---

## 4. AST DOM 怎么遍历

### 4.1 节点：`TidbAstNode`

- `type_name`：节点全限定类型名（例如 `github.com/pingcap/tidb/pkg/parser/ast.SelectStmt`）
- `pos`：`OriginTextPosition()`（原 SQL 文本中的 byte offset）
- `field_count` + `fields`：该 Go struct 的所有导出字段（递归编码）
- `literal`：若节点实现了 `ast.ValueExpr`，这里会补齐字面量值（可为空）

### 4.2 字段：`TidbAstField`

每个字段：

- `name`：字段名（Go struct 的导出字段名，如 `Where/From/Fields/...`）
- `value`：字段值（`TidbAstValue`）

### 4.3 值：`TidbAstValue.kind`

`TidbAstValue` 是一个 tagged value，通过 `kind` 决定哪个槽位有效：

- `TIDB_AST_NODE`：读 `value.node`
- `TIDB_AST_ARRAY`：读 `value.array.items/value.array.len`
- `TIDB_AST_MAP`：读 `value.map_value.items/value.map_value.len`
- `TIDB_AST_OBJECT`：读 `value.object.type_name/value.object.fields/value.object.field_count`
- `TIDB_AST_TYPED`：读 `value.typed.type_name/value.typed.inner`（用于保留 interface{} 的动态类型）
- 标量：`I64/U64/F64/STRING/BYTES/BOOL/NULL`

---

## 5. 如何识别语句类型（SELECT / CREATE / …）

最简单的方式：看每条语句根节点的 `type_name`：

- `.../ast.SelectStmt`：SELECT
- `.../ast.CreateTableStmt`：CREATE TABLE
- `.../ast.InsertStmt`：INSERT

推荐在 C++ 侧用后缀匹配（避免路径变化）：

```cpp
// ends_with(type_name, "ast.SelectStmt")
```

---

## 6. Query plan 入门例子（SELECT / FROM / WHERE）

已经提供一个“面向语义抽取”的 C++ 例子：`example/select_from_where.cpp`。

它做的事情：

- 从 `ast.SelectStmt` 抽取 projection（`Fields -> FieldList.Fields`）
- 从 `From -> TableRefsClause.TableRefs -> Join.Left -> TableSource.Source -> TableName` 抽取单表名与别名
- 将 `Where` 里常见表达式（`BinaryOperationExpr` / `PatternInExpr` / `ColumnNameExpr` / `ValueExpr`）打印成字符串

构建运行：

```bash
make build
make -C example select-from-where
./example/select-from-where "select a, b from t as tt where a = 1 and b in (2,3)"
```

---

## 7. 调试工具：完整 DOM 打印

如果你想先“看清楚 AST 结构长什么样”，可以用通用 dump：

```bash
make cpp-example
./cpp/example "select 1 from t where a=1"
```

它会递归打印 `Node(type_name) + fields`，适合用来找字段路径。

