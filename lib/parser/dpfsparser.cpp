#include <parser/dpfsparser.hpp>
#include "lexer.h"
#include "parser.hpp"

struct yy_buffer_state;
using YY_BUFFER_STATE = yy_buffer_state*;
YY_BUFFER_STATE yy_scan_bytes(const char* bytes, int len, yyscan_t scanner);
void yy_delete_buffer(YY_BUFFER_STATE buffer, yyscan_t scanner);

yy::CParser::CParser(const CUser& user) : user(user) {
    m_parms.clear();
}

// return true if parse success
int yy::CParser::operator()(const std::string& sql) {

    if (sql.empty()) {
        return -EINVAL;
    }
    int rc = 0;
    m_parms.clear();

    std::string lexeme;          // flex extra storage for string tokens

    // Init reentrant scanner with `lexeme` as extra data (see DRIVER macro in lexyy.l)
    yyscan_t scanner = nullptr;

    rc = yylex_init(&scanner);
    if(rc != 0 || !scanner) {
        std::cerr << "failed to init scanner" << std::endl;
        return rc;
    }

    if (yylex_init_extra(&lexeme, &scanner) != 0 || !scanner) {
        std::cerr << "failed to init scanner" << std::endl;
        return rc;
    }


    lexeme.clear();

    // Feed the input string to flex
    // Run parser (constructor takes driver, shared string, and scanner)
    parser parser(this, lexeme, scanner);

    YY_BUFFER_STATE buf = yy_scan_bytes(sql.c_str(), static_cast<int>(sql.size()), scanner);
    if (!buf) {
        std::cerr << "failed to scan bytes for input: " << sql << std::endl;
        rc = -ENOBUFS;
        goto errReturn;
    }

    rc = parser.parse();
    if(rc != 0) goto errReturn;
    std::cout << "SQL: " << sql << " => parse rc=" << rc << std::endl;

    // Clean up buffer before next case
    yy_delete_buffer(buf, scanner);
    

    yylex_destroy(scanner);
    return 0;

errReturn:
    if(buf) {
        yy_delete_buffer(buf, scanner);
    }

    yylex_destroy(scanner);

    return rc;
}

// save the privilege to user's cache
int yy::CParser::checkPrivilege(const std::string& schemaName, const std::string& objName, tbPrivilege requiredPrivilege) {

    // TODO :: CHECK USER PRIVILEGE
    // if user's dbprivilege is admin, return success directly
    if(user.dbprivilege >= dbPrivilege::DBPRIVILEGE_ACCESS) {
        return 0;
    }
    //  1. get user privilege for this table from SYSAUTHS table
    char keyBuf[1024] = {0};
    KEY_T k(keyBuf, 1024);
    int rc = 0;

    // generate key by collection's cmpType
    // for SYSAUTHS key size = 3.
    dataSvc->m_sysSchema->sysauths.m_cmpTyps[0].first;


    CItem outItem(dataSvc->m_sysSchema->sysauths.m_collectionStruct->m_cols);
    rc = dataSvc->m_sysSchema->sysauths.getRow(k, &outItem);
    if(rc != 0) {
        cerr << "Failed to get system boot item." << endl;
        return rc;
    }
    CValue tbPrivilege = outItem.getValue(4);


    // if(!(tbPrivilege.data[0] & requiredPrivilege)) {
    //     message = "User has no enough privilege to access the object " + schemaName + "." + objName;
    //     CItem::delItem(outItem);
    //     return -EACCES;
    // }


    return 0;
}

int yy::CParser::judge(const CmpUnit& left, dpfsCmpType ct, const CmpUnit& right) {
    // process comparison and generate condition
    // generate result set based on comparison
    // 1 遍历已有结果集
    switch (ct) {
        case dpfsCmpType::DPFS_WHERE_EQ: {
            std::cout << "处理等于比较" << std::endl;
        } break;
        case dpfsCmpType::DPFS_WHERE_NE: {
            std::cout << "处理不等于比较" << std::endl;
        } break;
        case dpfsCmpType::DPFS_WHERE_LT: {
            std::cout << "处理小于比较" << std::endl;
        } break;
        case dpfsCmpType::DPFS_WHERE_GT: {
            std::cout << "处理大于比较" << std::endl;
        } break;
        case dpfsCmpType::DPFS_WHERE_LE: {
            std::cout << "处理小于等于比较" << std::endl;
        } break;
        case dpfsCmpType::DPFS_WHERE_GE: {
            std::cout << "处理大于等于比较" << std::endl;
        } break;
        default: {
            break;
        }
    }

    return 0;
}


