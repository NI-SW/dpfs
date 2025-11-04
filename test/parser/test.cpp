#include <parser/dpfsparser.hpp>
#include <unistd.h>
#include <iostream>
using namespace std;
int main(){

    Db2Parser parser;

    string testa;
    while(1) {
        cout << "Enter SQL: ";
        getline(cin, testa);
        // testa = "CREATE  /*  asd */ FUNCTION --asdlkqwe \n /*   asd qwr gf */  /**s klshf jdhg kjerg*/  /*********/ --asjhqweoioiuou --\n  \" \"\" test \"\"\".\"\"\"\"\"  A\"()";
        cout << "get : " << testa << endl;
        if(parser(testa)) {
            printf("Schema Name: %s\n", parser.schemaName.c_str());
            printf("OBJ Name: %s\n", parser.name.c_str());
            printf("Option: %d\n", parser.option);

            printf("\nExtra Params size : %lu\n", parser.extraParams.size());
            for(auto& p : parser.extraParams) {
                printf("Extra Param: key=%s, value=%s\n", p.first.c_str(), p.second.c_str());
            }
        }

        // testa.clear();
        // sleep(1);
        
    }
    return 0;
}


// "CREATE OR REPLACE PROCEDURE \" \"\" AA\"\"\".\"  \"\"  \" ()\nBEGIN\nEND\n"

/*
ALTER TABLE INFO_ADD_MULTICOL_CONS ADD COLUMN SEX1 VARCHAR(10) NOT NULL DEFAULT 'Ů' CONSTRAINT C2 CHECK(SEX1 ='▒▒' OR SEX1 ='Ů') ADD COLUMN SURNAME1 VARCHAR(10) NOT NULL DEFAULT 'LI' UNIQUE
ALTER TABLE INFO_ADD_MULTICOL_CONS DROP COLUMN SEX1 CASCADE DROP COLUMN SURNAME1;
*/