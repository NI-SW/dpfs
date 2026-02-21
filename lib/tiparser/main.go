package main

/*
#include "tidb_parser.h"
*/
import "C"

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func parseSQLToResult(sql string) *C.TidbParseResult {
	res := newParseResult()
	if res == nil {
		return nil
	}

	arena := &arena{ptr: res._arena}
	parserModule, parserVersion := parserModuleInfo()
	res.parser_module = arena.dupString(parserModule)
	res.parser_version = arena.dupString(parserVersion)

	defer func() {
		if r := recover(); r != nil {
			res.ok = 0
			res.stmt_count = 0
			res.statements = nil
			res.warn_count = 0
			res.warnings = nil
			setResultError(res, fmt.Errorf("panic: %v", r))
		}
	}()

	p := parser.New()
	stmts, warns, err := p.Parse(sql, "", "")
	if err != nil {
		setResultError(res, err)
		return res
	}

	enc := newDOMEncoder(arena)
	if err := enc.buildIntoResult(res, stmts, warns); err != nil {
		setResultError(res, err)
		return res
	}

	res.ok = 1
	return res
}

//export TidbParseSQL
func TidbParseSQL(sql *C.char) *C.TidbParseResult {
	if sql == nil {
		res := newParseResult()
		if res == nil {
			return nil
		}
		arena := &arena{ptr: res._arena}
		parserModule, parserVersion := parserModuleInfo()
		res.parser_module = arena.dupString(parserModule)
		res.parser_version = arena.dupString(parserVersion)
		setResultError(res, fmt.Errorf("sql is NULL"))
		return res
	}

	return parseSQLToResult(C.GoString(sql))
}

//export TidbFreeParseResult
func TidbFreeParseResult(result *C.TidbParseResult) {
	freeParseResult(result)
}

func main() {}
