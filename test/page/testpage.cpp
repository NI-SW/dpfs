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

    // signal(SIGINT, sigfun);
    // signal(SIGKILL, sigfun);

	dpfsEngine* engine = nullptr; // new CNvmfhost();
	int rc = 0;
	engine = newEngine("nvmf");
	if(!engine) {
		cout << "engine type error" << endl;
		return -1;
	}
	rc = engine->attach_device("trtype:tcp adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1");
	// rc = engine->attach_device("trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50658 subnqn:nqn.2016-06.io.spdk:cnode1");
	// rc = engine->attach_device("trtype:pcie traddr:0000.1b.00.0");  engine->attach_device("trtype:pcie traddr:0000.13.00.0");
	if(rc) {
		cout << " attach device fail " << endl;
	}
	vector<dpfsEngine*> engList;
	engList.emplace_back(engine);
	logrecord testLog;
	testLog.set_async_mode(true);
	engine->set_async_mode(true);


	
	CPage* pge = new CPage(engList, 4, testLog);

	bidx testbid;

	cout << "engine size = " << engList[0]->size() << endl;
	cacheStruct** ptr = nullptr;
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
		} else if(testbid.gid == -2) {
			break;
		}

		cout << "input disk block position: ";
		cin >> testbid.bid;

		uint64_t ttm = 0;
		
		ptr = new cacheStruct*[testbid.bid + 1];

		chrono::milliseconds ns = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock().now().time_since_epoch());
		for(uint64_t i = 0; i <= testbid.bid; ++i) {
			chrono::microseconds n11 = chrono::duration_cast<chrono::microseconds>(chrono::system_clock().now().time_since_epoch());
			// rc = pge->get(ptr[i], {testbid.gid, i});
			do {
				rc = pge->get(ptr[i], {testbid.gid, i});
				if(rc) {
					cout << "error occur, get fail, count: " << i << endl;
					this_thread::sleep_for(chrono::milliseconds(1000));
				}
			} while(rc);
			chrono::microseconds n12 = chrono::duration_cast<chrono::microseconds>(chrono::system_clock().now().time_since_epoch());
			// cout << "total get time : " << (n12 - n11).count() << endl;
			ttm += (n12 - n11).count();
			// char* myptr = (char*)ptr[0]->zptr;
			// cout << myptr << endl;

			// cout << "getCount : " << pge->m_getCount << endl;
			// cout << "hitCount : " << pge->m_hitCount << endl;
		}
		chrono::milliseconds ns1 = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock().now().time_since_epoch());
		cout << "total get time : " << (ns1 - ns).count() << endl;
		cout << "total add time : " << ttm << " us" << endl;

		for(uint64_t i = 0; i <= testbid.bid; ++i) {
			// testLog.log_inf("waiting for %llu th block, status: %u \n", i, ptr[i]->getStatus());
			while(ptr[i]->getStatus() != cacheStruct::VALID) {
				if(ptr[i]->getStatus() == cacheStruct::ERROR) {
					cout << "block " << i << " read error!" << endl;
					ptr[i]->release();
					break;
				} else if(ptr[i]->getStatus() == cacheStruct::INVALID) {
					cout << "block " << i << " invalid!" << endl;
					ptr[i]->release();
					// this_thread::sleep_for(chrono::milliseconds(10));
					break;
				}
				
			}
			chrono::milliseconds ns2 = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock().now().time_since_epoch());
			// char* myptr = (char*)ptr[i]->zptr;
			// cout << myptr << endl;
			testLog.log_inf("get %llu time : %llu\n", i, (ns2 - ns).count());
			// cout << "get " << i << " time : " << (ns2 - ns).count() << endl;
			ptr[i]->release();
		}

		delete[] ptr;
		ptr = nullptr;
	}


	delete pge;

	delete engine;
	
	
	return 0;
}

