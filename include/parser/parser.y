%language "c++"
%skeleton "lalr1.cc"
%require "3.2"


%define api.token.constructor
%define api.value.type variant


%code requires {
        // #define __TESTMK__

        #ifndef __TESTMK__
        #include <parser/dpfsparser.hpp>
        #else
        #include <parser/test.hpp>
        #endif
        
        #ifndef YY_TYPEDEF_YY_SCANNER_T
        #define YY_TYPEDEF_YY_SCANNER_T
        typedef void* yyscan_t;
        // #define YYSTYPE std::string
        #endif

        // 如果你有其他的上下文结构体，也可以在这里声明
        class yy::CParser; 
}

%{
/* #define __STDC__ */
#include <stdio.h>
#include <cstring>
#include <string>



%}

%code {
        // 这里只包含头文件
        #include "lexer.h" 

        yy::parser::symbol_type yylex(yyscan_t yyscanner);

        int pars_id_definition(yy::CParser* parser, const std::string& id, const std::string& objId, ParserOption type) {
                parser->m_parms.schemaName = id.c_str();
                parser->m_parms.objName = objId.c_str();
                parser->m_parms.objType = type;
                return 0;
        }
}

%parse-param { class CParser *pars }
%parse-param { std::string& str }
%lex-param { yyscan_t scanner }
%parse-param { yyscan_t scanner }



// object types
%token PARS_FUNCTION_TOKEN
%token PARS_PROCEDURE_TOKEN
%token PARS_VIEW_TOKEN
%token PARS_INDEX_TOKEN
%token PARS_TRIGGER_TOKEN
%token PARS_SEQUENCE_TOKEN
%token PARS_TABLE_TOKEN
%token PARS_ALIAS_TOKEN
%token PARS_TYPE_TOKEN

// keywords
%token PARS_CREATE_TOKEN
%token PARS_DROP_TOKEN
%token PARS_ALTER_TOKEN
%token PARS_DASH_TOKEN
%token PARS_RENAME_TOKEN
%token PARS_REPLACE_TOKEN
%token PARS_IF_TOKEN
%token PARS_NOT_TOKEN
%token PARS_NULL_TOKEN
%token PARS_EXISTS_TOKEN
%token PARS_DOT_TOKEN
%token PARS_AUTH_TOKEN
%token PARS_SCHEMA_TOKEN
%token PARS_PUBLIC_TOKEN
%token PARS_UNIQUE_TOKEN
%token PARS_CASCADE_TOKEN
%token PARS_COLUMN_TOKEN
%token PARS_ADD_TOKEN
%token PARS_RESTRICT_TOKEN
%token PARS_PRIMARY_TOKEN
%token PARS_KEY_TOKEN

// symbols
%token PARS_LEFT_BRACKET_TOKEN
%token PARS_RIGHT_BRACKET_TOKEN
%token PARS_COMMA_TOKEN

// clauses
%token PARS_SELECT_TOKEN
%token PARS_FROM_TOKEN
%token PARS_WHERE_TOKEN
%token PARS_AND_TOKEN
%token PARS_OR_TOKEN
%token PARS_LIMIT_TOKEN

// where clause
//     =  != <  >  <= >= 
%token EQ NE LT GT LE GE  

%left PARS_OR_TOKEN
%left PARS_AND_TOKEN
%right PARS_NOT_TOKEN

%left EQ NE LT GT LE GE


// data type
%token PARS_BIGINT_TOKEN
%token PARS_INT_TOKEN
%token PARS_FLOAT_TOKEN
%token PARS_DECIMAL_TOKEN
%token PARS_DOUBLE_TOKEN
%token PARS_CHAR_TOKEN
%token PARS_VARCHAR_TOKEN
%token PARS_BINARY_TOKEN
%token PARS_BLOB_TOKEN
%token PARS_TIMESTAMP_TOKEN
%token PARS_BOOL_TOKEN
%token PARS_DATE_TOKEN

// group
%token <std::string> PARS_ID_TOKEN
%token <int64_t> PARS_NUMBER_TOKEN
%token <std::string> PARS_STR_TOKEN

%type <ParserOption> objects_token
%type <colTypeDef> datatype_definition
%type <uint8_t> nullable_definition
%type <uint8_t> primary_key_definition
%type <uint8_t> col_constraint_definition
%type <CmpUnit> cmpunit_clause

%%

top_statement:
        table_definition ';' |
        index_definition ';' |
        // alter_objects ';' |
        // objects_definition ';' |
        objects_drop ';' | 
        // rename_objects ';' |       
        schema_definition ';' |
        schema_drop ';' |
        datatype_definition ';' |
        define_columns ';' |
        select_statement ';'


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

datatype_definition :
        PARS_BIGINT_TOKEN     { $$ = {dpfs_datatype_t::TYPE_BIGINT, 8, 0}; } |
        PARS_INT_TOKEN        { $$ = {dpfs_datatype_t::TYPE_INT, 4, 0}; } |      
        PARS_FLOAT_TOKEN      { $$ = {dpfs_datatype_t::TYPE_FLOAT, 4, 0}; } |   
        PARS_DOUBLE_TOKEN     { $$ = {dpfs_datatype_t::TYPE_DOUBLE, 8, 0}; } |
        PARS_BOOL_TOKEN       { $$ = {dpfs_datatype_t::TYPE_BOOL, 1, 0}; } |      
        PARS_TIMESTAMP_TOKEN  { $$ = {dpfs_datatype_t::TYPE_TIMESTAMP, 10, 0}; } |    
        PARS_DATE_TOKEN       { $$ = {dpfs_datatype_t::TYPE_DATE, 10, 0}; } |

        PARS_DECIMAL_TOKEN PARS_LEFT_BRACKET_TOKEN PARS_NUMBER_TOKEN PARS_COMMA_TOKEN PARS_NUMBER_TOKEN PARS_RIGHT_BRACKET_TOKEN    
        { $$ = { dpfs_datatype_t::TYPE_DECIMAL, $3, $5 }; } |              

        PARS_CHAR_TOKEN PARS_LEFT_BRACKET_TOKEN PARS_NUMBER_TOKEN PARS_RIGHT_BRACKET_TOKEN
        { $$ = {dpfs_datatype_t::TYPE_CHAR, $3, 0}; } |      

        PARS_VARCHAR_TOKEN PARS_LEFT_BRACKET_TOKEN PARS_NUMBER_TOKEN PARS_RIGHT_BRACKET_TOKEN
        { $$ = {dpfs_datatype_t::TYPE_VARCHAR, $3, 0}; } |         

        PARS_BINARY_TOKEN PARS_LEFT_BRACKET_TOKEN PARS_NUMBER_TOKEN PARS_RIGHT_BRACKET_TOKEN
        { $$ = {dpfs_datatype_t::TYPE_BINARY, $3, 0}; } |              

        PARS_BLOB_TOKEN PARS_LEFT_BRACKET_TOKEN PARS_NUMBER_TOKEN PARS_RIGHT_BRACKET_TOKEN
        { $$ = {dpfs_datatype_t::TYPE_BLOB, $3, 0}; }

;



nullable_definition:
        PARS_NOT_TOKEN PARS_NULL_TOKEN { $$ = CColumn::constraint_flags::NOT_NULL; }
;

primary_key_definition:
        PARS_PRIMARY_TOKEN PARS_KEY_TOKEN { $$ = CColumn::constraint_flags::PRIMARY_KEY; }
;

col_constraint_definition:
        nullable_definition { $$ = $1; } |
        primary_key_definition { $$ = $1; } |
        col_constraint_definition nullable_definition { 
                if ($1 & CColumn::constraint_flags::NOT_NULL) {
                        pars->message = "redefinition of NOT NULL constraint";
                        YYABORT;
                }
                $$ = $1 | $2; 
        } |
        col_constraint_definition primary_key_definition { 
                if ($1 & CColumn::constraint_flags::PRIMARY_KEY) {
                        pars->message = "redefinition of PRIMARY KEY constraint";
                        YYABORT;
                }
                $$ = $1 | $2; 
        }
;


// default_definition:
//         { $$ = 0; } |
//         PARS_DEFAULT_TOKEN PARS_NUMBER_TOKEN { 
//                 // $$ = CColumn::constraint_flags::DEFAULT_VALUE; 
//         }
// ;

// default is not supported now
define_columns:
        PARS_ID_TOKEN datatype_definition col_constraint_definition {
                #ifdef PARSER_DEBUG
                printf("定义列: %s 类型: %ld, 长度：%ld, 非空：%d, 主键：%d\n", 
                $1.c_str(), $2.type, $2.len, $3 & CColumn::constraint_flags::NOT_NULL, $3 & CColumn::constraint_flags::PRIMARY_KEY);
                #endif

                pars->m_parms.cols.emplace_back($1, $2.type, $2.len, $2.scale, $3);
        } |
        PARS_ID_TOKEN datatype_definition {
                #ifdef PARSER_DEBUG
                printf("定义列: %s 类型: %ld, 长度：%ld, 非空：%d, 主键：%d\n", 
                $1.c_str(), $2.type, $2.len, 0, 0);
                #endif

                pars->m_parms.cols.emplace_back($1, $2.type, $2.len, $2.scale, 0);
        } |
        define_columns PARS_COMMA_TOKEN PARS_ID_TOKEN datatype_definition col_constraint_definition {
                #ifdef PARSER_DEBUG
                printf("定义列: %s 类型: %ld, 长度：%ld, 非空：%d, 主键：%d\n", 
                $3.c_str(), $4.type, $4.len, $5 & CColumn::constraint_flags::NOT_NULL, $5 & CColumn::constraint_flags::PRIMARY_KEY);
                #endif

                pars->m_parms.cols.emplace_back($3, $4.type, $4.len, $4.scale, $5);
        } |
        define_columns PARS_COMMA_TOKEN PARS_ID_TOKEN datatype_definition  {
                #ifdef PARSER_DEBUG
                printf("定义列: %s 类型: %ld, 长度：%ld, 非空：%d, 主键：%d\n", 
                $3.c_str(), $4.type, $4.len, 0, 0);
                #endif

                pars->m_parms.cols.emplace_back($3, $4.type, $4.len, $4.scale, 0);
        }
;
// testsql : CREATE TABLE QWE.ASD(A INT NOT NULL PRIMARY KEY, B CHAR(20) NOT NULL, C BIGINT, D DECIMAL(10,2), E VARCHAR(50), F BINARY(100), G BLOB(2048));
// alter_objects:
//         PARS_ALTER_TOKEN objects_token PARS_ID_TOKEN PARS_DOT_TOKEN PARS_ID_TOKEN {

// }       |
//         PARS_ALTER_TOKEN objects_token PARS_ID_TOKEN {

// }
// ;

table_definition: 
        PARS_CREATE_TOKEN or_replace_clause public_clause PARS_TABLE_TOKEN if_notexists_clause PARS_ID_TOKEN PARS_LEFT_BRACKET_TOKEN define_columns PARS_RIGHT_BRACKET_TOKEN {
                pars_id_definition(pars, "", $6, ParserOptionTable);
                YYACCEPT; 
}       |
        PARS_CREATE_TOKEN or_replace_clause public_clause PARS_TABLE_TOKEN if_notexists_clause PARS_ID_TOKEN PARS_DOT_TOKEN PARS_ID_TOKEN PARS_LEFT_BRACKET_TOKEN define_columns PARS_RIGHT_BRACKET_TOKEN {
                pars_id_definition(pars, $6, $8, ParserOptionTable); 
                YYACCEPT; 
} 
;

index_definition: 
        PARS_CREATE_TOKEN or_replace_clause public_clause PARS_INDEX_TOKEN if_notexists_clause PARS_ID_TOKEN  {
                pars_id_definition(pars, "", $6, ParserOptionIndex);
                YYACCEPT; 
}       |
        PARS_CREATE_TOKEN or_replace_clause public_clause PARS_INDEX_TOKEN if_notexists_clause PARS_ID_TOKEN PARS_DOT_TOKEN PARS_ID_TOKEN {
                pars_id_definition(pars, $6, $8, ParserOptionIndex); 
                YYACCEPT; 
} 
;

// objects_definition: 
//         PARS_CREATE_TOKEN or_replace_clause public_clause objects_token if_notexists_clause PARS_ID_TOKEN {
//                 pars_id_definition(pars, "", $6, $4);
//                 YYACCEPT; 
// }       |
//         PARS_CREATE_TOKEN or_replace_clause public_clause objects_token if_notexists_clause PARS_ID_TOKEN PARS_DOT_TOKEN PARS_ID_TOKEN {
//                 pars_id_definition(pars, $6, $8, $4); 
//                 YYACCEPT; 
// } 
// ;

objects_drop:
        PARS_DROP_TOKEN objects_token if_exists_clause PARS_ID_TOKEN {
                pars_id_definition(pars, "", $4, $2);
                YYACCEPT; 
}       |
        PARS_DROP_TOKEN objects_token if_exists_clause PARS_ID_TOKEN PARS_DOT_TOKEN PARS_ID_TOKEN {
                pars_id_definition(pars, $4, $6, $2);
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

// rename_objects:
//         PARS_RENAME_TOKEN objects_token PARS_ID_TOKEN PARS_DOT_TOKEN PARS_ID_TOKEN {
                 
//             YYACCEPT; 
// } | 
//         PARS_RENAME_TOKEN objects_token PARS_ID_TOKEN {

//             YYACCEPT; 
// }
// ;

schema_definition: 
        PARS_CREATE_TOKEN PARS_SCHEMA_TOKEN PARS_ID_TOKEN PARS_AUTH_TOKEN PARS_ID_TOKEN {
           
            YYACCEPT; 
} | 
        PARS_CREATE_TOKEN PARS_SCHEMA_TOKEN PARS_ID_TOKEN {

            YYACCEPT; 
} |
        PARS_CREATE_TOKEN PARS_SCHEMA_TOKEN PARS_AUTH_TOKEN PARS_ID_TOKEN {

            YYACCEPT; 
}
;

schema_drop:
        PARS_DROP_TOKEN PARS_SCHEMA_TOKEN PARS_ID_TOKEN {

            YYACCEPT; 
}
;


public_clause:
            { ; }
        |   PARS_PUBLIC_TOKEN { ; }
;
// TODO:: select statements, where clause, limit clause
select_list:
        PARS_ID_TOKEN { pars->m_parms.colNames.emplace_back(std::move($1)); } |
        select_list PARS_COMMA_TOKEN PARS_ID_TOKEN { pars->m_parms.colNames.emplace_back(std::move($3)); }



// condition_expression:
//         PARS_ID_TOKEN EQ PARS_NUMBER_TOKEN {
//                 ; 
//         } |
//         condition_expression PARS_AND_TOKEN PARS_ID_TOKEN EQ PARS_NUMBER_TOKEN {
//                 ; 
//         } |
//         condition_expression PARS_OR_TOKEN PARS_ID_TOKEN EQ PARS_NUMBER_TOKEN {
//                 ; 
//         }
// ;

cmpunit_clause: 
        PARS_ID_TOKEN { $$ = std::move(CmpUnit(cmpUnitType::NODE_IDENTIFIER, $1)); } |
        PARS_NUMBER_TOKEN { $$ = std::move(CmpUnit(cmpUnitType::NODE_NUMBER, $1)); } |
        PARS_STR_TOKEN { $$ = std::move(CmpUnit(cmpUnitType::NODE_STRING, $1)); }
;


// AND操作优先级更高, todo:: 加载表，索引，列等元数据，进行类型检查等
search_condition:
        cmpunit_clause EQ cmpunit_clause { 
                /* 处理 A = B */ 
                // CmpUnit
                

        } | search_condition PARS_AND_TOKEN search_condition { 
                // 遇到 A OR B AND C 时，因为 AND 优先级高，
                // Bison 会选择将 B AND C 归约成一个整体，而不是先把 A OR B 归约。
                std::cout << "归约 AND 表达式" << std::endl; 

        } | search_condition PARS_OR_TOKEN search_condition { 
                std::cout << "归约 OR 表达式" << std::endl;

        } | PARS_LEFT_BRACKET_TOKEN search_condition PARS_RIGHT_BRACKET_TOKEN { 
                // $$ = $2; 
        }
;


where_clause:
        { ; } |
        PARS_WHERE_TOKEN search_condition {
                ; 
        }
;

limit_clause:
        { ; } |
        PARS_LIMIT_TOKEN PARS_NUMBER_TOKEN { ; }
;

select_statement: 
        PARS_SELECT_TOKEN select_list PARS_FROM_TOKEN PARS_ID_TOKEN where_clause limit_clause {
                pars->m_parms.objType = ParserOptionData;
                pars->m_parms.opType = ParserOperationQRY;
                pars->m_parms.schemaName = "";
                pars->m_parms.objName = $4;
                YYACCEPT;
        } |
        PARS_SELECT_TOKEN select_list PARS_FROM_TOKEN PARS_ID_TOKEN PARS_DOT_TOKEN PARS_ID_TOKEN where_clause limit_clause {
                pars->m_parms.objType = ParserOptionData;
                pars->m_parms.opType = ParserOperationQRY;
                pars->m_parms.schemaName = $4;
                pars->m_parms.objName = $6;
                YYACCEPT;
        }

%%

void yy::parser::error(const std::string& msg) {
    std::cerr << "语法错误: " << msg << std::endl;
}