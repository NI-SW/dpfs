
%{
/* #define __STDC__ */
#include <stdio.h>
#include <cstring>
#include "dpfsparser.hpp"
int yylex(void);
extern void dpunput(int c);
extern char* yyget_text(void);
std::mutex mutex_yyparse;
#define yyerror(A,B) ;
#define YYSTYPE std::string

// skip 
void skip_until_token(int next_token) {
    int token;
    while ((token = yylex()) != 0) {
        if (token == next_token || token == ';') {
            // 将token放回输入流，让正常解析流程处理
            dpunput(token);
            return;
        }
    }
}

int pars_id_definition(CParser* parser, const std::string& id, const std::string& objId, ParserOption type) {
    parser->schemaName = id.c_str();
    parser->name = objId.c_str();
    parser->option = type;
    return 0;
}

int pars_column_name(CParser* parser, const std::string& key, const std::string& value) {
    if(key.empty() || value.empty()) {
        return 0;
    }
    printf("key : %s\nval : %s\n", key.c_str(), value.c_str());
    parser->extraParams.emplace_back(std::make_pair(key, value));
    return 0;
}

%}

%parse-param { class CParser *pars }



%token PARS_CREATE_TOKEN
%token PARS_DROP_TOKEN
%token PARS_ALTER_TOKEN
%token PARS_INDEX_TOKEN
%token PARS_TABLE_TOKEN
%token PARS_PROCEDURE_TOKEN
%token PARS_FUNCTION_TOKEN
%token PARS_ID_TOKEN
%token PARS_DASH_TOKEN
%token PARS_RENAME_TOKEN
%token PARS_REPLACE_TOKEN
%token PARS_OR_TOKEN
%token PARS_IF_TOKEN
%token PARS_NOT_TOKEN
%token PARS_EXISTS_TOKEN
%token PARS_DOT_TOKEN
%token PARS_AUTH_TOKEN
%token PARS_SCHEMA_TOKEN
%token PARS_SEQUENCE_TOKEN
%token PARS_VIEW_TOKEN
%token PARS_TRIGGER_TOKEN
%token PARS_ALIAS_TOKEN
%token PARS_PUBLIC_TOKEN
%token PARS_UNIQUE_TOKEN
%token PARS_TYPE_TOKEN
%token PARS_CASCADE_TOKEN
%token PARS_COLUMN_TOKEN
%token PARS_ADD_TOKEN
%token PARS_RESTRICT_TOKEN;

%%

top_statement:
        alter_objects ';' |
        objects_definition ';' |
        objects_drop ';' | 
        rename_objects ';' |       
        schema_definition ';' |
        schema_drop ';' |
        drop_column ';' |
        add_column ';'


column_clause:
        { ; }
        |   PARS_COLUMN_TOKEN { ; }
;

column_drop_param:
        { $$ = "RESTRICT"; }
        |   PARS_CASCADE_TOKEN { $$ = "CASCADE"; }
        |   PARS_RESTRICT_TOKEN { $$ = "RESTRICT"; }
;

drop_column:
        alter_objects PARS_DROP_TOKEN column_clause PARS_ID_TOKEN column_drop_param {
                pars_column_name(pars, "COLNAME", $4);
                pars_column_name(pars, $5, "true");

                // if next token is not drop, skip to drop
                if(strncmp("DROP", yyget_text(), sizeof "DROP")) {
                        skip_until_token(PARS_DROP_TOKEN);
                }
        } |
        drop_column PARS_DROP_TOKEN column_clause PARS_ID_TOKEN column_drop_param { 
                pars_column_name(pars, "COLNAME", $4);
                pars_column_name(pars, $5, "true");

                // if next token is not drop, skip to drop
                if(strncmp("DROP", yyget_text(), sizeof "DROP")) {
                        skip_until_token(PARS_DROP_TOKEN);
                }
        } 

;

add_column:
        alter_objects PARS_ADD_TOKEN column_clause PARS_ID_TOKEN {
                // printf("enter add column clause\n");
                pars_column_name(pars, "COLNAME", $4);

                if(strncmp("ADD", yyget_text(), sizeof "ADD")) {
                        skip_until_token(PARS_ADD_TOKEN);
                }
        } |
        add_column PARS_ADD_TOKEN column_clause PARS_ID_TOKEN { 
                // printf("pars add pcp\n");
                pars_column_name(pars, "COLNAME", $4);

                if(strncmp("ADD", yyget_text(), sizeof "ADD")) {
                        skip_until_token(PARS_ADD_TOKEN);
                }
        } 
;


alter_objects:
        PARS_ALTER_TOKEN objects_token PARS_ID_TOKEN PARS_DOT_TOKEN PARS_ID_TOKEN {
            pars_id_definition(pars, $3, $5, (ParserOption)$2[0]);
            // YYACCEPT; 
            // $$ = '\0';
}       |
        PARS_ALTER_TOKEN objects_token PARS_ID_TOKEN {
            pars_id_definition(pars, "", $3, (ParserOption)$2[0]);
            // YYACCEPT; 
            // $$ = "";
}
;

objects_token:
        PARS_FUNCTION_TOKEN { $$ = ParserOptionFunc; } |
        PARS_PROCEDURE_TOKEN { $$ = ParserOptionProcedure; } |
        PARS_VIEW_TOKEN { $$ = ParserOptionView; } |
        PARS_INDEX_TOKEN { $$ = ParserOptionIndex; } |
        PARS_TRIGGER_TOKEN { $$ = ParserOptionTrigger; } |
        PARS_SEQUENCE_TOKEN { $$ = ParserOptionSequence; } |
        PARS_TABLE_TOKEN { $$ = ParserOptionTable; } |
        PARS_ALIAS_TOKEN { $$ = ParserOptionAlias; } |
        PARS_TYPE_TOKEN { $$ = ParserOptionType; }
;

objects_definition: 
        PARS_CREATE_TOKEN or_replace_clause public_clause objects_token if_notexists_clause PARS_ID_TOKEN{
            pars_id_definition(pars, "", $6, (ParserOption)$4[0]);
            YYACCEPT; 
}       |
        PARS_CREATE_TOKEN or_replace_clause public_clause objects_token if_notexists_clause PARS_ID_TOKEN PARS_DOT_TOKEN PARS_ID_TOKEN {
            pars_id_definition(pars, $6, $8, (ParserOption)$4[0]);
            YYACCEPT; 
} 
;

objects_drop:
        PARS_DROP_TOKEN objects_token if_exists_clause PARS_ID_TOKEN {
            pars_id_definition(pars, "", $4, (ParserOption)$2[0]);
            YYACCEPT; 
}       |
        PARS_DROP_TOKEN objects_token if_exists_clause PARS_ID_TOKEN PARS_DOT_TOKEN PARS_ID_TOKEN {
            pars_id_definition(pars, $4, $6, (ParserOption)$2[0]);
            YYACCEPT; 
}
;

or_replace_clause:
            { ; }
        | PARS_OR_TOKEN PARS_REPLACE_TOKEN { ; }
;

if_exists_clause:
            { ; }
        | PARS_IF_TOKEN PARS_EXISTS_TOKEN { ; }
;

if_notexists_clause:
        { ; }
        | PARS_IF_TOKEN PARS_NOT_TOKEN PARS_EXISTS_TOKEN { ; }
;

rename_objects:
        PARS_RENAME_TOKEN objects_token PARS_ID_TOKEN PARS_DOT_TOKEN PARS_ID_TOKEN {
            pars_id_definition(pars, $3, $5, (ParserOption)$2[0]);
            YYACCEPT; 
} | 
        PARS_RENAME_TOKEN objects_token PARS_ID_TOKEN {
            pars_id_definition(pars, "", $3, (ParserOption)$2[0]);
            YYACCEPT; 
}
;

schema_definition: 
        PARS_CREATE_TOKEN PARS_SCHEMA_TOKEN PARS_ID_TOKEN PARS_AUTH_TOKEN PARS_ID_TOKEN {
            pars_id_definition(pars, $3, $5, ParserOptionSchema);
            YYACCEPT; 
} | 
        PARS_CREATE_TOKEN PARS_SCHEMA_TOKEN PARS_ID_TOKEN {
            pars_id_definition(pars, $3, $3, ParserOptionSchema);
            YYACCEPT; 
} |
        PARS_CREATE_TOKEN PARS_SCHEMA_TOKEN PARS_AUTH_TOKEN PARS_ID_TOKEN {
            pars_id_definition(pars, $4, $4, ParserOptionSchema);
            YYACCEPT; 
}
;

schema_drop:
        PARS_DROP_TOKEN PARS_SCHEMA_TOKEN PARS_ID_TOKEN {
            pars_id_definition(pars, $3, $3, ParserOptionSchema);
            YYACCEPT; 
}
;


public_clause:
            { ; }
        |   PARS_PUBLIC_TOKEN { ; }
;

%%
