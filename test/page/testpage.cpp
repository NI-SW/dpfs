#include <storage/engine.hpp>
#include <collect/page.hpp>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <iostream>
using namespace std;
std::thread test;

int main() {
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

	while(1) {
		cout << "input group id:" << endl;
		cin >> testbid.gid;
		cout << "input disk block position:" << endl;
		cin >> testbid.bid;

		cacheStruct* ptr = pge->get(testbid);

		char* myptr = (char*)ptr->zptr;

		cout << myptr << endl;
	}

	delete pge;
	delete engine;
	
	
	return 0;
}

