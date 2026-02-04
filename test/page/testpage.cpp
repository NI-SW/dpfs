#include <storage/engine.hpp>
#include <storage/nvmf/nvmf.hpp>
#define private public
#include <collect/page.hpp>
#undef private
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

    // signal(SIGINT, sigfun);
    // signal(SIGKILL, sigfun);
	cacheStruct** ptr = nullptr;
	bidx testbid;
	dpfsEngine* engine = nullptr; // new CNvmfhost();
	CPage* pge = nullptr;
	int rc = 0;
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
	testLog.set_loglevel(logrecord::LOG_DEBUG);
	// engine->set_loglevel(logrecord::LOG_DEBUG);

	// rc = engine->attach_device("trtype:tcp adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1");
	rc = engine->attach_device("trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50658 subnqn:nqn.2016-06.io.spdk:cnode1");
	// rc = engine->attach_device("trtype:pcie traddr:0000.1b.00.0");  engine->attach_device("trtype:pcie traddr:0000.13.00.0");
	if(rc) {
		cout << " attach device fail " << endl;
		goto errReturn;
	}




	
	pge = new CPage(engList, 50000, testLog);


	cout << "engine size = " << engList[0]->size() << endl;
	

	while(!g_exit) {

		cout << "input group id: ";
		cin >> testbid.gid;
		
		if(testbid.gid == (uint64_t)(-1)) {
			cout << "please input disk idx\n" << endl;
			cout << "input groupid: "; 
			cin >> testbid.gid;
			cout << "input disk block position: ";
			cin >> testbid.bid;

			
			cin.clear();
			cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
			cout << "input data: " << endl;
			string data;
			std::getline(cin, data);
			
			data.resize(data.size() * 2);
			memcpy(&data[data.size() / 2 - 1], data.c_str(), data.size() / 2);


			size_t blockNum = data.size() % dpfs_lba_size == 0 ? data.size() / dpfs_lba_size : data.size() / dpfs_lba_size + 1;
			cout << "input data len : " << data.size() << " use block number : " << blockNum << endl;
			char* zptr = (char*)pge->alloczptr(blockNum);
			memcpy(zptr, data.c_str(), data.size());

			rc = pge->put(testbid, zptr, nullptr, blockNum, true);
			if(rc) {
				cout << "put operate fail! code : " << rc << endl;
			}
			continue;
		} else if(testbid.gid == (uint64_t)(-2)) {
			break;
		}
		size_t startPos = 0;
		size_t stepLen = 0;
		cout << "input disk block start position: ";
		cin >> startPos;

		cout << "input disk block end position: ";
		cin >> testbid.bid;

		cout << "input get block step length: ";
		cin >> stepLen;

		uint64_t ttm = 0;
		
		ptr = new cacheStruct*[(testbid.bid - startPos) / stepLen + 1] { 0 };

		chrono::milliseconds ns = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock().now().time_since_epoch());


		for(uint64_t i = startPos, j = 0; i <= testbid.bid; i += stepLen, ++j) {
			chrono::microseconds n11 = chrono::duration_cast<chrono::microseconds>(chrono::system_clock().now().time_since_epoch());
			// rc = pge->get(ptr[i], {testbid.gid, i});
			do {
				rc = pge->get(ptr[j], {testbid.gid, i}, stepLen);
				if(rc) {
					cout << "error occur, get fail, count: " << i << endl;
					this_thread::sleep_for(chrono::milliseconds(2000));
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

		for(uint64_t i = startPos, j = 0; i <= testbid.bid; i += stepLen, ++j) {
			// testLog.log_inf("waiting for %llu th block, status: %u \n", i, ptr[i]->getStatus());
			while(ptr[j]->getStatus() != cacheStruct::VALID) {
				if(ptr[j]->getStatus() == cacheStruct::ERROR) {
					cout << "block " << i << " read error!" << endl;
					ptr[j]->release();
					break;
				} else if(ptr[j]->getStatus() == cacheStruct::INVALID) {
					cout << "block " << i << " invalid!" << endl;
					ptr[j]->release();
					// this_thread::sleep_for(chrono::milliseconds(10));
					break;
				}
				this_thread::sleep_for(chrono::milliseconds(10));
			}
			chrono::milliseconds ns2 = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock().now().time_since_epoch());
			// char* myptr = (char*)ptr[i]->zptr;
			// cout << myptr << endl;
			
			
			
			ptr[j]->read_lock();
			
			char* myptr = (char*)(ptr[j]->getPtr());
			// std::cout << "block " << i << " len: " << ptr[j]->getLen() << " data: " << myptr << std::endl;

			if(i % 1000 == 0)
				testLog.log_inf("get %llu time : %llu\n", j, (ns2 - ns).count());
			testLog.log_inf("block %llu len: %u data: %s\n", i, ptr[j]->getLen(), myptr);
			ptr[j]->read_unlock();

			cout << "block " << i << " data (first 512 bytes): " << endl;
			printMemory(myptr, 512);
			cout << endl;
			// for(int j = 0; j < 512; ++j) {
			// 	printf("%02x ", (unsigned char)myptr[j]);
			// 	if((j + 1) % 16 == 0) {
			// 		printf("\n");
			// 	}
			// }


			// std::cout << "after 4096 " << myptr[4096] << std::endl;
			// std::cout << "4097 " << myptr[4097] << std::endl;



			// cout << "get " << i << " time : " << (ns2 - ns).count() << endl;
			ptr[j]->release();
		}
		chrono::milliseconds ns3 = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock().now().time_since_epoch());
		testLog.log_inf("total get finish time : %llu\n", (ns3 - ns).count());

		cout << " current page size : " << pge->m_currentSizeInByte.load() << endl;

		delete[] ptr;
		ptr = nullptr;
	}


errReturn:

	if(pge) {
		delete pge;
	}

	if(engine) {
		delete engine;
	}
	
	
	return 0;
}

