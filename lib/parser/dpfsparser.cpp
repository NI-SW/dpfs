#include <parser/dpfsparser.hpp>
#include "lexer.h"
#include "parser.hpp"

struct yy_buffer_state;
using YY_BUFFER_STATE = yy_buffer_state*;
YY_BUFFER_STATE yy_scan_bytes(const char* bytes, int len, yyscan_t scanner);
void yy_delete_buffer(YY_BUFFER_STATE buffer, yyscan_t scanner);

yy::CParser::CParser() {
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

