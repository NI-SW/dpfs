%top {

#define YY_DECL yy::parser::symbol_type yylex(yyscan_t yyscanner)
}

%{
#include "parser.hpp"
#define DRIVER ((std::string*)yyget_extra(yyscanner))

%}


%option reentrant noyywrap
%option extra-type="void*"


/* 
 * ID               Schema名规则
*/

ID              [a-z_A-Z][a-z_A-Z0-9]*
OBJ_NAME        [a-z_A-Z][@a-z_A-Z0-9]*
DOT             \.
NUMBER          [0-9]*

%x str
%x comment
%x comment2
%x id

%%

"'" {
        BEGIN(str);
        *DRIVER = "";
}
<str>"''" { 
        *DRIVER += "'"; 
}
<str>"'" {
        BEGIN(INITIAL);
        return(yy::parser::make_PARS_STR_TOKEN(*DRIVER));
}
<str>\n {
        *DRIVER += yytext[0];
}
<str>[^'\n]+ {
        *DRIVER += yytext;
}
<str><<EOF>> {
        fprintf(stderr, "错误：未闭合的字符串\n");
        BEGIN(INITIAL);
        DRIVER->clear();
        return(yy::parser::make_PARS_STR_TOKEN(*DRIVER));
}


"/*"                    BEGIN(comment); /* eat up comment */
<comment>[^*]*          
<comment>"*"+[^*/]*         
<comment>"*"+"/"        BEGIN(INITIAL);

"--"                    BEGIN(comment2);
<comment2>[^\n]*
<comment2>[\n]{1,1}     BEGIN(INITIAL);

\"              {
                        BEGIN(id);
                        *DRIVER = "";
}
<id>[^\"]+      {
                        DRIVER->append(yytext);
}
<id>\"+ {
                        DRIVER->append(yytext, yyleng / 2);

                        if (yyleng % 2) {
                                BEGIN(INITIAL);
                                return(yy::parser::make_PARS_ID_TOKEN(*DRIVER));
                        }
}


[ \r\t\n]+              /* eat up whitespace */

UNIQUE {
    return yy::parser::make_PARS_UNIQUE_TOKEN();
}

RENAME {
    return yy::parser::make_PARS_RENAME_TOKEN();
}

ADD {
    return yy::parser::make_PARS_ADD_TOKEN();
}

ALIAS {
    return yy::parser::make_PARS_ALIAS_TOKEN();
}

CASCADE {
    return yy::parser::make_PARS_CASCADE_TOKEN();
}

COLUMN {
    return yy::parser::make_PARS_COLUMN_TOKEN();
}

SYNONYM {
    return yy::parser::make_PARS_ALIAS_TOKEN();
}

PUBLIC {
    return yy::parser::make_PARS_PUBLIC_TOKEN();
}

TRIGGER {
    return yy::parser::make_PARS_TRIGGER_TOKEN();
}

TYPE {
    return yy::parser::make_PARS_TYPE_TOKEN();
}

VIEW {
    return yy::parser::make_PARS_VIEW_TOKEN();
}

SCHEMA {
    return yy::parser::make_PARS_SCHEMA_TOKEN();
}

AUTHORIZATION {
    return yy::parser::make_PARS_AUTH_TOKEN();
}

ALTER {
    return yy::parser::make_PARS_ALTER_TOKEN();
}

TABLE {
    return yy::parser::make_PARS_TABLE_TOKEN();
}

FUNCTION {
    return yy::parser::make_PARS_FUNCTION_TOKEN();
}

INDEX {
    return yy::parser::make_PARS_INDEX_TOKEN();
}

CREATE {
    return yy::parser::make_PARS_CREATE_TOKEN();
}

OR {
    return yy::parser::make_PARS_OR_TOKEN();
}

REPLACE {
    return yy::parser::make_PARS_REPLACE_TOKEN();
}


RESTRICT {
    return yy::parser::make_PARS_RESTRICT_TOKEN();
}

IF {
    return yy::parser::make_PARS_IF_TOKEN();
}

NOT {
    return yy::parser::make_PARS_NOT_TOKEN();
}

NULL {
    return yy::parser::make_PARS_NULL_TOKEN();
}

EXISTS {
    return yy::parser::make_PARS_EXISTS_TOKEN();
}

DROP {
    return yy::parser::make_PARS_DROP_TOKEN();
}

SEQUENCE {
    return yy::parser::make_PARS_SEQUENCE_TOKEN();
}

PROCEDURE {
    return yy::parser::make_PARS_PROCEDURE_TOKEN();
}

PRIMARY { return yy::parser::make_PARS_PRIMARY_TOKEN(); }
KEY { return yy::parser::make_PARS_KEY_TOKEN(); }


SELECT { return yy::parser::make_PARS_SELECT_TOKEN(); }
FROM { return yy::parser::make_PARS_FROM_TOKEN(); }
WHERE { return yy::parser::make_PARS_WHERE_TOKEN(); }
"="  { return yy::parser::make_EQ(DPFS_WHERE_EQ); } 
"!=" { return yy::parser::make_NE(DPFS_WHERE_NE); } 
"<"  { return yy::parser::make_LT(DPFS_WHERE_LT); } 
">"  { return yy::parser::make_GT(DPFS_WHERE_GT); } 
"<=" { return yy::parser::make_LE(DPFS_WHERE_LE); } 
">=" { return yy::parser::make_GE(DPFS_WHERE_GE); } 


","        { return yy::parser::make_PARS_COMMA_TOKEN(); }
"("        { return yy::parser::make_PARS_LEFT_BRACKET_TOKEN(); }
")"        { return yy::parser::make_PARS_RIGHT_BRACKET_TOKEN(); }
INT        { return yy::parser::make_PARS_INT_TOKEN(); } 
BIGINT     { return yy::parser::make_PARS_BIGINT_TOKEN(); }     
FLOAT      { return yy::parser::make_PARS_FLOAT_TOKEN(); }     
DECIMAL    { return yy::parser::make_PARS_DECIMAL_TOKEN(); }     
DOUBLE     { return yy::parser::make_PARS_DOUBLE_TOKEN(); }     
CHAR       { return yy::parser::make_PARS_CHAR_TOKEN(); }     
VARCHAR    { return yy::parser::make_PARS_VARCHAR_TOKEN(); }     
BINARY     { return yy::parser::make_PARS_BINARY_TOKEN(); }     
BLOB       { return yy::parser::make_PARS_BLOB_TOKEN(); }     
TIMESTAMP  { return yy::parser::make_PARS_TIMESTAMP_TOKEN(); }         
BOOL       { return yy::parser::make_PARS_BOOL_TOKEN(); }     
DATE       { return yy::parser::make_PARS_DATE_TOKEN(); }     



{ID} {
    return yy::parser::make_PARS_ID_TOKEN(yytext);
}

{OBJ_NAME} {
    return yy::parser::make_PARS_ID_TOKEN(yytext);
}

{DOT} {
    return yy::parser::make_PARS_DOT_TOKEN();
}

{NUMBER} {
    return yy::parser::make_PARS_NUMBER_TOKEN(std::stoi(yytext));
}

%%
 
 
// int yywrap() { 
// 
//     return 1;
// }
