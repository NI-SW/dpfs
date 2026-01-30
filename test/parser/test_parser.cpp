#include <storage/engine.hpp>

#define private public
#include <parser/driver.hpp>
#undef private

#include <unistd.h>
#include <iostream>
#include <log/logbinary.h>
#include <collect/product.hpp>
#include <cassert>
#include <cstring>
#include <vector>
using namespace std;
int main(){
    int rc = 0;
    logrecord log;
    dpfsEngine* engine = newEngine("nvmf");
    std::vector<dpfsEngine*> engines;  
    engines.emplace_back(engine);
	rc = engine->attach_device("trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50658 subnqn:nqn.2016-06.io.spdk:cnode1");
	if (rc) {
		cout << " attach device fail " << endl;
		delete engine;
        return rc;
	}

    CPage* page = new CPage(engines, 128, log);
    CDiskMan dman(page);
    CCollection coll(dman, *page);
    CUser usr;

    usr.userid = 0;
    usr.username = "SYSTEM";
    usr.dbprivilege = DBPRIVILEGE_FATAL;

    // yy::CParser parser(usr);

    dpfsDriver driver(usr);

    string testa;
    while(1) {
        cout << "Enter SQL: ";
        getline(cin, testa);
        // testa = "CREATE  /*  asd */ FUNCTION --asdlkqwe \n /*   asd qwr gf */  /**s klshf jdhg kjerg*/  /*********/ --asjhqweoioiuou --\n  \" \"\" test \"\"\".\"\"\"\"\"  A\"()";
        cout << "get : " << testa << endl;
        rc = driver.execute(testa);
        if(rc == 0) {
            printf("Schema Name: %s\n", driver.parser.m_parms.schemaName.c_str());
            printf("OBJ Name: %s\n", driver.parser.m_parms.objName.c_str());
            printf("Option: %d\n", driver.parser.m_parms.objType);
        } else {
            printf("Parse error: %s\n", driver.parser.message.c_str());
        }

        

        switch (driver.parser.m_parms.objType)
        {
        case ParserOptionTable:
            if(driver.parser.m_parms.opType == ParserOperationIRP) {
                CCollectionInitStruct initStruct;
                initStruct.indexPageSize = 4;
                initStruct.name = driver.parser.m_parms.objName;
                rc = coll.initialize(initStruct);
                if (rc != 0) { 
                    cout << " load collection fail, rc=" << rc << endl;
                }

                printf("Create Table with cols:\n");
                for(auto& col : driver.parser.m_parms.cols) {
                    printf("  Col Name: %s, Type: %d, Len: %u, Scale: %u, Constraint: %d\n", 
                        col.getName(), col.getType(), col.getLen(), col.getScale(), col.getDds().constraints.unionData);
                    coll.addCol(col.getName(), col.getType(), col.getLen(), col.getScale(), col.getDds().constraints.unionData);
                }
                rc = coll.initBPlusTreeIndex();
                if (rc != 0) { 
                    cout << " init bplus tree index fail, rc=" << rc << endl;
                }
                cout << " Create Table success " << endl;
            }
            break;
        case ParserOptionIndex:
            if(driver.parser.m_parms.opType == ParserOperationIRP) {
                printf("Create Index: %s on Table: %s\n", driver.parser.m_parms.indexName.c_str(), driver.parser.m_parms.objName.c_str());
                printf("  Cols:\n");
                for(auto& col : driver.parser.m_parms.cols) {
                    printf("    Col Name: %s, Type: %d, Len: %u, Scale: %u, Constraint: %d\n", 
                        col.getName(), col.getType(), col.getLen(), col.getScale(), col.getDds().constraints.unionData);
                }
            }
            break;
        default:
            break;
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