#pragma once
// #define PARSER_DEBUG
#ifdef PARSER_DEBUG
#include <stdio.h>
#include <unistd.h>
#endif
#include <vector>
#include <utility>
#include <string>
#include <mutex>
extern std::mutex mutex_yyparse;

extern FILE* yyin;
extern FILE* yyout;
extern int yylineno;

enum ParserOption {
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
};


class CParser {
public:
    std::string schemaName;
    std::string name;
    std::vector<std::pair<std::string, std::string>> extraParams;

    using byte = unsigned char;
    ParserOption option;
    CParser() {
        schemaName = "";
        name = "";
        extraParams.clear();
        option = ParserOptionNone;
    }

    // return true if parse success
    bool operator()(const std::string& sql) {

        if (sql.empty()) {
            return false;
        }
        extraParams.clear();
        schemaName.clear();
        name.clear();
        option = ParserOptionNone;

        extern int yyparse (class CParser *pars);
        extern void yyrestart (FILE *input_file);

        FILE* tmpF = nullptr;
        tmpF = fmemopen((void*)sql.c_str(), sql.size(), "r");
        mutex_yyparse.lock();
        yyrestart(tmpF);    // reset stream
        yylineno = 1;       // reset line number


        if(yyparse(this) != 0) {
            mutex_yyparse.unlock();
            fclose(tmpF);
            if(option != ParserOptionNone)
                return true;
            return false;
            // return false;
        }

#ifdef PARSER_DEBUG
        // printf("Schema Name: %s\n", schemaName.c_str());
#endif
        mutex_yyparse.unlock();
        fclose(tmpF);
        

        
        return true;
    }
};

#define YY_USER_INIT yyout = fopen("/dev/null", "w");
