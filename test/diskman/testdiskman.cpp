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
#include <collect/diskman.hpp>
using namespace std;
std::thread test;

volatile bool g_exit = false;
void sigfun(int sig) {
    cout << "Signal " << sig << " received, exiting..." << endl;
    g_exit = true;
}

int main() {

    // signal(SIGINT, sigfun);
    // signal(SIGKILL, sigfun);
    CDiskMan* dskman = nullptr;
    CTempStorage* tempstor = nullptr;

	dpfsEngine* engine = nullptr; // new CNvmfhost();
	CPage* pge = nullptr;
	int rc = 0;
    char* data = new char[dpfs_lba_size * 6];

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


    cout << "init complete" << endl;
    

    memset(data, 'A', dpfs_lba_size * 3);
    rc = tempstor->pushBackData(data, 3);
    if(rc) {
        cout << "pushBackData error rc = " << rc << endl;
        goto errReturn;
    }
    memset(data, 'B', dpfs_lba_size * 3);
    rc = tempstor->pushBackData(data, 3);
    if(rc) {
        cout << "pushBackData error rc = " << rc << endl;
        goto errReturn;
    }

    memset(data, 'C', dpfs_lba_size * 6);
    rc = tempstor->updateData(2, data, 3);
    if(rc < 0) {
        cout << "updateData error rc = " << rc << endl;
        goto errReturn;
    }

    memset(data, 0, dpfs_lba_size * 6);

    tempstor->getData(0, data, 6);

    cout << "data at pos 0: " << string(data, dpfs_lba_size * 3).substr(0, 20) << "..." << endl;
    cout << "data at pos 2: " << string(data + dpfs_lba_size * 2, dpfs_lba_size * 3).substr(0, 20) << "..." << endl;
    cout << "data at pos 3: " << string(data + dpfs_lba_size * 3, dpfs_lba_size * 3).substr(0, 20) << "..." << endl;
    cout << "data at pos 5: " << string(data + dpfs_lba_size * 5, dpfs_lba_size * 1).substr(0, 20) << "..." << endl;
    

    delete tempstor;
    delete dskman;
    delete pge;
    delete engine;
    delete[] data;

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
    if(data) {
        delete[] data;
    }
    return -1;
}

