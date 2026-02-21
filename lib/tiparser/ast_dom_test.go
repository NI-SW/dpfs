package main

import "testing"

func TestParseSQLToDOM_SchemaV1(t *testing.T) {
	res := parseSQLToResult("select 1, 'a', 0x12, 1.23 from t where a in (1,2)")
	if res == nil {
		t.Fatal("parseSQLToResult returned NULL")
	}
	defer TidbFreeParseResult(res)

	if res.ok == 0 {
		t.Fatalf("parse failed: %s", resultErrorString(res))
	}

	if got := int(res.schema_version); got != astSchemaVersion {
		t.Fatalf("schema_version mismatch: %d", got)
	}

	if firstStatementTypeName(res) == "" {
		t.Fatal("missing first statement type_name")
	}

	if !resultHasAnyLiteral(res) {
		t.Fatal("expected to find at least one literal in AST DOM")
	}
}
