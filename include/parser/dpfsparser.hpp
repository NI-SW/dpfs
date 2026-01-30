#pragma once

#define PARSER_DEBUG
#ifdef PARSER_DEBUG
#include <stdio.h>
#include <unistd.h>
#endif
#include <vector>
#include <utility>
#include <string>
#include <dpfssys/dpfsdata.hpp>
#include <collect/where.hpp>
#include <dpfssys/user.hpp>

enum ParserOption : int {
    ParserOptionNone = 0,
    ParserOptionProcedure = 1,
    ParserOptionTable = 2,
    ParserOptionFunc = 3,
    ParserOptionIndex = 4,
    ParserOptionView = 5,
    ParserOptionTrigger = 6,
    ParserOptionSchema = 7,
    ParserOptionSequence = 8,
    ParserOptionAlias = 9,
    ParserOptionType = 10,
    ParserOptionData = 11, // insert/update/delete/select
};

enum ParserOperation : int {
    ParserOperationNone = 0, // other operation
    ParserOperationIRP = 1, // increase (create/insert)
    ParserOperationURP = 2, // update (modify/update)
    ParserOperationDRP = 3, // decrease (drop/delete)
    ParserOperationQRY = 4, // query (select)
};

struct colTypeDef {
    dpfs_datatype_t type;
    size_t len;
    size_t scale;
    uint8_t constraint = 0;
};

struct ParserParam {
    ParserOption objType = ParserOptionNone;
    ParserOperation opType = ParserOperationIRP;
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
        objType = ParserOptionNone;
        opType = ParserOperationIRP;
        cols.clear();
        schemaName.clear();
        objName.clear();
        indexName.clear();
        colNames.clear();

        // if(collection) {
        //     delete collection;
        //     collection = nullptr;
        // }

        // if(where) {
        //     delete where;
        //     where = nullptr;
        // }
    }
};


namespace yy {
/*
    only support right now:

    create table {
        schema name
        table name
        col names
    }
    drop table {
        schema name
        table name
    }

    create index {
        schema name
        index name
        table name
        col names
    }
    drop index {
        schema name
        index name
    }

    select {
        schema name
        table name
        col name
        where conditions
    }
    insert {
        schema name
        table name
        col name
        values
    }
    update {
        schema name
        table name
        col name
        values
        where conditions
    }
    delete {
        schema name
        table name
        where conditions
    }

*/

    class CParser {
    public:
        
        // using byte = unsigned char;
        ::ParserParam m_parms;
        std::string message;
        const CUser& user;
        CDatasvc* dataSvc = nullptr;

        CParser(const CUser& user);
        /*
            @param sql: SQL string to parse
            @return 0 on success, else on failure
            @note this function will parse the given SQL string
        */
        int operator()(const std::string& sql);

        /*
            @note judge the condition between left and right cmpunit with cmp type ct
            @return 0 on success, else on failure
        */
        int judge(const CmpUnit& left, dpfsCmpType ct, const CmpUnit& right);

        /*
            check user privilege for the given schema and object
            @param schemaName: name of the schema
            @param objName: name of the object
            @return 0 on success, else on failure
        */
        int checkPrivilege(const std::string& schemaName, const std::string& objName, tbPrivilege requiredPrivilege);
    };


}