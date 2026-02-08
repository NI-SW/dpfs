#include <parser/dpfsparser.hpp>
#include <tiparser/tidb_parser.h>

#ifdef PARSER_DEBUG
using namespace std;

#endif

constexpr char tiparserPrefix[] = "github.com/pingcap/tidb/pkg/parser/ast.";
constexpr size_t prefixLen = sizeof(tiparserPrefix) - 1; // Exclude null terminator

CParser::CParser(const CUser& user, CDatasvc& dsvc) : usr(user), dataSvc(dsvc) {

}

CParser::~CParser() {
    if (m_pParseResult) {
        TidbFreeParseResult(m_pParseResult);
        m_pParseResult = nullptr;
    }
}
/*
    @param sql: SQL string to parse
    @return 0 on success, else on failure
    @note this function will parse the given SQL string
*/
int CParser::operator()(const std::string& sql) {

    if (m_pParseResult) {
        // free previous parse result if any
        // Assume a function TidbFreeParseResult exists to free the result
        TidbFreeParseResult(m_pParseResult);
        m_pParseResult = nullptr;
    }

    m_pParseResult = TidbParseSQL(const_cast<char*>(sql.c_str()));
    return m_pParseResult ? 0 : -1;
}


/*
    check user privilege for the given schema and object
    @param schemaName: name of the schema
    @param objName: name of the object
    @return 0 on success, else on failure
*/
int CParser::checkPrivilege() {
    if (!m_pParseResult) {
        return -EINVAL; // No parse result to check
    }

    // TODO

    return 0;
}

int CParser::buildPlan() {
    if (!m_pParseResult) {
        return -EINVAL; // No parse result to build plan from
    }
    int rc = 0;
    TidbParseResult*& ast = m_pParseResult;


    // field name : CreateTable, SelectStmtOpts
    ast->stmt_count;

    for (int32_t i = 0; i < ast->stmt_count; ++i) {
        const TidbAstNode* stmt = ast->statements[i];
        // Process each statement node
        rc = buildPlanForStmt(stmt);
        if (rc != 0) {
            return rc;
        }
    }

    return 0;

}

int CParser::buildPlanForStmt(const TidbAstNode* stmt) {
    // Implementation for building plan for a single statement
    if (!stmt) {
        return -EINVAL; // Invalid statement node
    }

    if (stmt->type_name.len < prefixLen) {
        return -EINVAL; // Not a valid Tidb AST node type
    }

    int rc = 0;
    std::string_view typeName(stmt->type_name.data + prefixLen, stmt->type_name.len - prefixLen);


    // TODO : build different execution plan for different statement types, currently just print the type name

    if (typeName == "CreateTableStmt") {
        // Handle CreateTable statement

        std::cout << "Building plan for CreateTable statement" << std::endl;

        // rc = buildCreatePlan();
    } else if (typeName == "SelectStmt") {
        // Handle Select statement
        std::cout << "Building plan for Select statement" << std::endl;

    } else if (typeName == "DropTableStmt") {
        // Handle other statement types
        std::cout << "Building plan for DropTable statement" << std::endl;

    } else if (typeName == "InsertStmt") {
        // Handle Insert statement
        std::cout << "Building plan for Insert statement" << std::endl;

    } else if (typeName == "DeleteStmt") {
        // Handle Delete statement
        std::cout << "Building plan for Delete statement" << std::endl;
    } else {
        return -EINVAL; // Unsupported statement type
    }

    if (rc != 0) {
        return rc; // Return error code if plan building failed
    }

    return 0;
}


int CParser::buildCreatePlan() {

    return 0;
}

int CParser::buildSelectPlan() {
    //TODO

    return 0;
}

int CParser::buildDropPlan() {
    //TODO

    return 0;
}

int CParser::buildInsertPlan() {
    //TODO

    return 0;
}

int CParser::buildDeletePlan() {
    //TODO

    return 0;
}