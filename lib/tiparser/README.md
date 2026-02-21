# TiDB Parser C API (cgo)

用 `github.com/pingcap/tidb/pkg/parser` 把 TiDB 的 SQL parser 暴露成一个可被 C/C++ 直接调用的动态库。

快速上手请看：`QUICKSTART.md`。

## 产物

- 动态库：`build/libtidbparser.(dylib|so)`
- C 头文件：`tidb_parser.h`（同时会复制一份到 `build/tidb_parser.h`）

`TidbParseSQL` 返回的 `TidbParseResult*` 由调用方负责通过 `TidbFreeParseResult` 释放。

## 构建

```bash
make build
```

> 首次构建需要能下载 Go modules（TiDB 依赖较大）。

## C++ 调用示例

```bash
make cpp-example
./cpp/example "select /* demo */ 1 from t"
```

也可以看更贴近 query plan 的例子（只做 `SELECT ... FROM ... WHERE ...`）：

```bash
make -C example select-from-where
./example/select-from-where "select a, b from t where a in (1,2)"
```

解析结果会以 **可直接遍历的 AST DOM（C 结构体）** 的形式返回在 `TidbParseResult.statements` 中。

### 内存模型

- `TidbParseSQL` 返回的 `TidbParseResult*` 以及其内部所有指针（节点、字段、字符串、数组等）都归该结果所有。
- 调用方只读访问，不要对内部指针单独 `free()`。
- 用完只需要调用一次 `TidbFreeParseResult(result)` 释放整棵 AST 和所有字符串/数组内存。

### 结构说明（核心字段）

- `TidbParseResult`
  - `ok`: 1 成功 / 0 失败
  - `schema_version`: 当前为 1
  - `stmt_count` + `statements`: 语句数量 + 语句根节点数组（每条语句一个 `TidbAstNode*`）
  - `warn_count` + `warnings`: warning 数量 + warning 字符串数组（`TidbString`）
  - `parser_module` / `parser_version`: TiDB parser module 信息
  - `err`: 失败时的错误信息（`TidbString`）
- `TidbAstNode`
  - `type_name`: 节点的全限定类型名（`<pkgPath>.<TypeName>`）
  - `pos`: `OriginTextPosition()`（在原 SQL 文本中的 byte offset）
  - `field_count` + `fields`: 该 Go struct 的所有导出字段（递归编码）
  - `literal`: 若该节点实现了 `ast.ValueExpr`，则补齐字面量信息（可为空）
- `TidbAstValue`
  - `kind`: `TidbAstValueKind`，决定哪个字段有效（例如 `i64/u64/f64/str/node/array/...`）
