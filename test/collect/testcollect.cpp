// #include <storage/engine.hpp>
#include <collect/collect.hpp>
#include <collect/product.hpp>
#include <basic/dpfsconst.hpp>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <iostream>
#include <csignal>
#include <dpfsdebug.hpp>
using namespace std;
std::thread test;

volatile bool g_exit = false;
void sigfun(int sig) {
    cout << "Signal " << sig << " received, exiting..." << endl;
    g_exit = true;
}

int main() {

    int rc = 0;
    // TODO
    bidx sysBidx = {0, 0};
    std::string data = "";
    CProduct* sysdpfs = new CProduct();
    CItem* itm = nullptr;
    CValue* val;


    // sysdpfs->fixedInfo.addCol("VERSION", dpfs_datatype_t::TYPE_BIGINT);
    // sysdpfs->fixedInfo.addCol("CODESET", dpfs_datatype_t::TYPE_VARCHAR, 16);
    // sysdpfs->fixedInfo.addCol("DPFS_NODE_ID", dpfs_datatype_t::TYPE_BIGINT);

    sysdpfs->fixedInfo.addCol("KEY", dpfs_datatype_t::TYPE_VARCHAR, 32);
    sysdpfs->fixedInfo.addCol("VALUE", dpfs_datatype_t::TYPE_VARCHAR, 32);


    itm = CItem::newItem(sysdpfs->fixedInfo.m_cols);
    if(!itm) {
        rc = -ENOMEM;
        return 1;
    }

    val = (CValue*)malloc(sizeof(CValue) + 128);
    if(!val) {
        rc = -ENOMEM;
        return 1;
    }
    // set system version

    val->setData("VERSION", sizeof("VERSION"));
    itm->updateValue(0, val); 

    val->setData(dpfsVersion, sizeof(dpfsVersion));
    itm->updateValue(1, val); 

    printMemory(itm->data, itm->rowLen);
    printf("\n");
    // sysdpfs->fixedInfo.addItem(*itm);

    // set system codeset
    val->setData("CODESET", sizeof("CODESET"));
    itm->updateValue(0, val);

    val->setData("UTF-8", sizeof("UTF-8"));
    itm->updateValue(1, val); 

    printMemory(itm->data, itm->rowLen);
    printf("\n");

    // set system node id
    val->setData("DPFS_NODE_ID", sizeof("DPFS_NODE_ID"));
    itm->updateValue(0, val);

    val->setData("50", sizeof("50"));
    itm->updateValue(1, val);

	printMemory(itm->data, itm->rowLen);
    printf("\n");
    
	
    // printMemory(itm->data, itm->rowLen);
    // printf("\n");

	return 0;
}

