#include <storage/engine.hpp>
#include <collect/page.hpp>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <iostream>
#include <csignal>
using namespace std;
std::thread test;

volatile bool g_exit = false;
void sigfun(int sig) {
    cout << "Signal " << sig << " received, exiting..." << endl;
    g_exit = true;
}

int main() {

    signal(SIGINT, sigfun);
    signal(SIGKILL, sigfun);

	dpfsEngine* engine = nullptr; // new CNvmfhost();
	int rc = 0;
	engine = newEngine("nvmf");
	if(!engine) {
		cout << "engine type error" << endl;
		return -1;
	}
	rc = engine->attach_device("trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50658 subnqn:nqn.2016-06.io.spdk:cnode1");
	if(rc) {
		cout << " attach device fail " << endl;
	}
	vector<dpfsEngine*> engList;
	engList.emplace_back(engine);
	logrecord testLog;
	
	CPage* pge = new CPage(engList, 128, testLog);

	bidx testbid;

	cout << "engine size = " << engList[0]->size() << endl;

	while(!g_exit) {

		cout << "input group id: ";
		cin >> testbid.gid;
		
		if(testbid.gid == -1) {
			cout << "please input disk idx\n" << endl;
			cout << "input groupid: "; 
			cin >> testbid.gid;
			cout << "input disk block position: ";
			cin >> testbid.bid;

			
			cout << "input data: " << endl;
			string data;
			cin >> data;

			char* zptr = (char*)pge->cacheMalloc(data.size() % dpfs_lba_size == 0 ? data.size() / dpfs_lba_size : data.size() / dpfs_lba_size + 1);
			memcpy(zptr, data.c_str(), data.size());

			rc = pge->put(testbid, zptr, data.size() / dpfs_lba_size + 1, true);
			if(rc) {
				cout << "put operate fail! code : " << rc << endl;
			}
			continue;
		}

		cout << "input disk block position: ";
		cin >> testbid.bid;

		


		cacheStruct* ptr = pge->get(testbid);
		if(!ptr) {
			cout << "error occur, get fail" << endl;
			continue;
		}

		char* myptr = (char*)ptr->zptr;

		cout << myptr << endl;

		cout << "getCount : " << pge->m_getCount << endl;
		cout << "hitCount : " << pge->m_hitCount << endl;
	}


	delete pge;

	delete engine;
	
	
	return 0;
}

