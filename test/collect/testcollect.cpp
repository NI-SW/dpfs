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
#define __LOAD__ 0
volatile bool g_exit = false;
void sigfun(int sig) {
    cout << "Signal " << sig << " received, exiting..." << endl;
    g_exit = true;
}

int main() {

    bidx sysBidx = {0, 0};
    std::string data = "";
    CProduct* sysdpfs = nullptr; // = new CProduct();
    CItem* itm = nullptr;
    CValue vv(32);
    CValue* val = &vv;

    // signal(SIGINT, sigfun);
    // signal(SIGKILL, sigfun);
    CDiskMan* dskman = nullptr;
    CTempStorage* tempstor = nullptr;

	dpfsEngine* engine = nullptr; // new CNvmfhost();
	CPage* pge = nullptr;
	int rc = 0;

    char version[4] = {dpfsVersion[0] + 0x30, dpfsVersion[1] + 0x30, dpfsVersion[2] + 0x30, dpfsVersion[3] + 0x30};


	engine = newEngine("nvmf");
	if(!engine) {
		cout << "engine type error" << endl;
		return -1;
	}

	vector<dpfsEngine*> engList;
	engList.emplace_back(engine);
	logrecord testLog;
	testLog.set_async_mode(true);
	engine->set_async_mode(true);
    
	
	// CNvmfhost* nfe = dynamic_cast<CNvmfhost*>(engine);
	// nfe->log.set_loglevel(logrecord::LOG_DEBUG);

	// rc = engine->attach_device("trtype:tcp adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1");
	rc = engine->attach_device("trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50658 subnqn:nqn.2016-06.io.spdk:cnode1");
	// rc = engine->attach_device("trtype:pcie traddr:0000.1b.00.0");  engine->attach_device("trtype:pcie traddr:0000.13.00.0");
	if(rc) {
		cout << " attach device fail " << endl;
		goto errReturn;
	}




	
	pge = new CPage(engList, 50000, testLog);


	cout << "engine size = " << engList[0]->size() << endl;

    dskman = new CDiskMan(pge);

    tempstor = new CTempStorage(*pge, *dskman);

    /**
     * 
     * 
     * super block的保存和读取
     */

    // TODO



    // sysdpfs->fixedInfo.addCol("VERSION", dpfs_datatype_t::TYPE_BIGINT);
    // sysdpfs->fixedInfo.addCol("CODESET", dpfs_datatype_t::TYPE_VARCHAR, 16);
    // sysdpfs->fixedInfo.addCol("DPFS_NODE_ID", dpfs_datatype_t::TYPE_BIGINT);

    sysdpfs = new CProduct(*pge, *dskman, testLog);

    if(__LOAD__) {
        rc = sysdpfs->fixedInfo.loadFrom(sysBidx);
        if(rc) {
            cout << "Load system product fail, rc: " << rc << endl;
            goto errReturn;
        }
        cout << "System product loaded from disk." << endl;

        cout << sysdpfs->fixedInfo.m_cols.size() << " cols loaded." << endl;
        for(auto col : sysdpfs->fixedInfo.m_cols) {
            cout << "Col name: " << col->getName() << ", type: " << (int)col->getType() << ", len: " << col->getLen() << endl;
        }

        goto __DONE;
    }

    sysdpfs->pid = sysBidx;
    sysdpfs->fixedInfo.m_perms.perm.m_btreeIndex = false;

    sysdpfs->fixedInfo.addCol("KEY", dpfs_datatype_t::TYPE_CHAR, 32, 0, CColumn::constraint_flags::PRIMARY_KEY | CColumn::constraint_flags::NOT_NULL);
    sysdpfs->fixedInfo.addCol("VALUE", dpfs_datatype_t::TYPE_CHAR, 32, 0, CColumn::constraint_flags::NOT_NULL);

    sysdpfs->fixedInfo.saveTo({nodeId, 0});

    

    itm = CItem::newItems(sysdpfs->fixedInfo.m_cols, 10);
    if(!itm) {
        rc = -ENOMEM;
        goto errReturn;
    }

    if(!val->maxLen) {
        rc = -ENOMEM;
        goto errReturn;
    }
    // set system version

    val->setData("VERSION", sizeof("VERSION"));
    rc = itm->updateValue(0, val); 
    cout << "Update rc: " << rc << endl;

    
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

    cout << "col 0\t\t\tcol 1" << endl;
    for(auto it : *itm) {
        cout << string(it[0].data, 32) << "\t\t\t" << string(it[1].data, 32) << endl;
    }

    // memory problem here
    sysdpfs->fixedInfo.addItem(*itm);

    // TODO:: test commit function
    sysdpfs->fixedInfo.commit();




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
    __DONE:
    printf("done \n");

    delete tempstor;
    cout << "tmp storage destoried" << endl;
    delete dskman;
    cout << "dskman destoried" << endl;
    delete pge;
    cout << "pge destroied" << endl;
    delete engine;
    cout << "engine destroied" << endl;

	return 0;

    errReturn:
    if(tempstor) {
        delete tempstor;
    }
    if(dskman) {
        delete dskman;
    }
    if(pge) {
        delete pge;
    }
    if(engine) {
        delete engine;
    }
    return -1;
}

