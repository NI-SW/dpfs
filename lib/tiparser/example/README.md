# Example: SELECT / FROM / WHERE

这个例子演示 C++ 侧如何只针对 `SELECT ... FROM ... WHERE ...` 子集，从 `TidbParseResult.statements` 里提取：

- projection（select 列表）
- from（单表名 + 可选别名）
- where（简单表达式打印）

## Build & Run

```bash
make build
make -C example select-from-where
./example/select-from-where "select a, b from t where a = 1 and b in (2,3)"
```

