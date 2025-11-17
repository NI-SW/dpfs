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

    /**
     * 
     * 
     * super block的保存和读取
     */
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

    sysdpfs->pid = sysBidx;
    sysdpfs->fixedInfo.m_btreeIndex = false;

    sysdpfs->fixedInfo.addCol("KEY", dpfs_datatype_t::TYPE_CHAR, 32);
    sysdpfs->fixedInfo.addCol("VALUE", dpfs_datatype_t::TYPE_CHAR, 32);


    itm = CItem::newItems(sysdpfs->fixedInfo.m_cols, 10);
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
    rc = itm->updateValue(0, val); 
    cout << "Update rc: " << rc << endl;

    char version[4] = {dpfsVersion[0] + 0x30, dpfsVersion[1] + 0x30, dpfsVersion[2] + 0x30, dpfsVersion[3] + 0x30};
    val->setData(version, sizeof(version));
    rc = itm->updateValue(1, val); 
    cout << "Update rc: " << rc << endl;
    printMemory(itm->data, 128);
    printf("\n");
    itm->nextRow();

    printMemory(itm->data, 128);
    printf("\n");
    

    // set system codeset
    val->setData("CODESET", sizeof("CODESET"));
    itm->updateValue(0, val);

    val->setData("UTF-8", sizeof("UTF-8"));
    itm->updateValue(1, val); 

    itm->nextRow();

    printMemory(itm->data, 128);
    printf("\n");


    // set system node id
    val->setData("DPFS_NODE_ID", sizeof("DPFS_NODE_ID"));
    itm->updateValue(0, val);

    val->setData("50", sizeof("50"));
    itm->updateValue(1, val);

	printMemory(itm->data, 256);
    printf("\n");

    for(auto it : *itm) {
        cout << "Col 0: " << string(it[0].data, it[0].len) << endl;
        cout << "Col 1: " << string(it[1].data, it[1].len) << endl;
    }
    
    sysdpfs->fixedInfo.addItem(*itm);

    // itm->resetScan();
    // print inserted data:
    // CValue nval;
    // do {
    //     nval = itm->getValue(0);
    //     cout << "Col 0: " << string(nval.data, nval.len) << endl;
    //     nval = itm->getValue(1);
    //     cout << "Col 1: " << string(nval.data, nval.len) << endl;
    // } while(itm->nextRow() == 0);
	
    // printMemory(itm->data, itm->rowLen);
    printf("done \n");

	return 0;
}

