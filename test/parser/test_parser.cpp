#include <storage/engine.hpp>

#define private public
#include <parser/driver.hpp>
#include <parser/dpfsparser.hpp>
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