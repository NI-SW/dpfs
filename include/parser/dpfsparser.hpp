#pragma once

#define PARSER_DEBUG
#ifdef PARSER_DEBUG
#include <stdio.h>
#include <unistd.h>
#include <iostream>
#endif
#include <vector>
#include <utility>
#include <string>
#include <dpfssys/dpfsdata.hpp>
#include <dpfssys/user.hpp>
#include <log/loglocate.h>

struct colTypeDef {
    dpfs_datatype_t type;
    size_t len;
    size_t scale;
    uint8_t constraint = 0;
};

struct ParserParam {

    std::vector<std::string> colNames;
    std::vector<CColumn> cols;
    std::string schemaName;
    std::string objName;
    std::string indexName; // for index create/drop
    // temp table to store the selected
    std::vector<CCollection*> collections;
    // CCollection* collection = nullptr;
    // std::vector<CWhere*> wheres;
    void clear() {

        cols.clear();
        schemaName.clear();
        objName.clear();
        indexName.clear();
        colNames.clear();

    }
};

class TidbParseResult;
class TidbAstNode;

class CParser {
public:
    
    // using byte = unsigned char;
    ParserParam m_parms;
    std::string message;
    const CUser& usr;
    CDatasvc& dataSvc;
    TidbParseResult* m_pParseResult = nullptr;

    CParser(const CUser& user, CDatasvc& dsvc);
    ~CParser();
    /*
        @param sql: SQL string to parse
        @return 0 on success, else on failure
        @note this function will parse the given SQL string
    */
    int operator()(const std::string& sql);

    int parse(const std::string& sql) {
        return (*this)(sql);
    }

    /*
        check user privilege for the given ast
        @return 0 on success, else on failure
    */
    int checkPrivilege();

    /*
        @return 0 on success, else on failure
        @note build execution plan from parse result
    */
    int buildPlan(const std::string& osql, CPlanHandle& out);



private:
    int buildPlanForStmt(const std::string& osql, const TidbAstNode* stmt, CPlanHandle& out);


    // todo build sql execution plan for different statement types
    int buildCreatePlan(const TidbAstNode* stmt, CPlanHandle& out);

    /*
        @param osql: original SQL string, used for plan cache key
        @param stmt: the AST node of the select statement
        @param out: the output plan handle, used to store the built plan and fetch data from storage
        @return 0 on success, else on failure
        @note this function will build the execution plan for the select statement, including the collection to select from, the column sequence to select, and the where clause conditions, and store the plan in the plan cache
    */
    int buildSelectPlan(const std::string& osql, const TidbAstNode* stmt, CPlanHandle& out);
    int buildDropPlan(const TidbAstNode* stmt, CPlanHandle& out);
    int buildInsertPlan(const std::string& osql, const TidbAstNode* stmt, CPlanHandle& out);
    int buildDeletePlan(const std::string& osql, const TidbAstNode* stmt, CPlanHandle& out);
};


class CTableTree {
public:
    CTableTree() = default;
    CTableTree(const std::string& schema, const std::string& table, const std::string& alias = "") : schemaName(schema), tableName(table), aliasName(alias) {

    }
    CTableTree(const CTableTree& other) = delete;

    CTableTree(CTableTree&& other) noexcept {
        schemaName = std::move(other.schemaName);
        tableName = std::move(other.tableName);
        aliasName = std::move(other.aliasName);
        left = other.left;
        right = other.right;
        leaf = other.leaf;
        other.left = nullptr;
        other.right = nullptr;
        other.leaf = true;
    }

    bool isLeaf() const {
        return leaf;
    }
    std::string schemaName;
    std::string tableName;
    std::string aliasName; // for table alias in select statement

private:

    // join is not supported yet, so the flag is not used currently, but we can use it to indicate the join type in the future when join is supported
    bool leaf = true;
    CTableTree* left = nullptr;  
    CTableTree* right = nullptr;

};

enum class exprType : uint8_t {
    columnName = 0,
    valueExpr,
};

struct CWhereObject {

    whereFlag op;
    std::string schemaName;
    std::string tableName;
    std::string colName;

    exprType leftType;
    exprType rightType;

    // 0 for left, 1 for right
    dpfs_datatype_t valueType[2];
    std::string strVal[2];

    union simpleval
    {
        int64_t i64Val;
        uint64_t u64Val;
        double f64Val;
        bool bVal;
    } value[2];
    
};


class CWhereSeq {
public:
    CWhereSeq() = default;
    ~CWhereSeq() = default;
    std::vector<CWhereObject> conditionSeq;



};