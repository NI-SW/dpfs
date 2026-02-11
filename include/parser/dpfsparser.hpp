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


    /*
        check user privilege for the given ast
        @return 0 on success, else on failure
    */
    int checkPrivilege();

    /*
        @return 0 on success, else on failure
        @note build execution plan from parse result
    */
    int buildPlan();
private:
    int buildPlanForStmt(const TidbAstNode* stmt);


    // todo build sql execution plan for different statement types
    int buildCreatePlan(const TidbAstNode* stmt);
    int buildSelectPlan(const TidbAstNode* stmt);
    int buildDropPlan(const TidbAstNode* stmt);
    int buildInsertPlan(const TidbAstNode* stmt);
    int buildDeletePlan(const TidbAstNode* stmt);
};
