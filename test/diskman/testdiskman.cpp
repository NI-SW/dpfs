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
        
	}




	
	pge = new CPage(engList, 100, testLog);


	cout << "engine size = " << engList[0]->size() << endl;

    map<int64_t, int64_t> bidx;
    /*
    初始化
    */
    dskman = new CDiskMan(pge);
    dskman->init(0, 1048576);
    dskman->print();
    delete dskman;

    /*
    新加一块磁盘
    */
    dskman = new CDiskMan(pge);
    dskman->addBidx(0, 1048576);
    dskman->init(1, 1048576);//新增一块磁盘
    dskman->load();
    
    dskman->print();
    delete dskman;

    /*
    普通重启加载
    */
    dskman = new CDiskMan(pge);
    dskman->addBidx(0, 1048576);
    dskman->load();
    
    dskman->print();
    delete dskman;


     /*
    改变git=0的大小
    */
    dskman = new CDiskMan(pge);
    dskman->addBidx(0, 1048576);
    dskman->init(0, 2097152);
    dskman->load();
    
    dskman->print();
    delete dskman;

    /*
    分配    
    */
    dskman = new CDiskMan(pge);
    dskman->addBidx(0, 1048576);
    dskman->load();
    {
        size_t pos = dskman->balloc(8);
        cout << "alloc pos=" << pos << ", len=" << 8 << endl;
        bidx[pos] = 8;
    }

    dskman->print();

    {
        size_t pos = dskman->balloc(8);
        cout << "alloc pos=" << pos << ", len=" << 8 << endl;
        bidx[pos] = 8;
    }

    dskman->print();
    delete dskman;

    /*
    随机分配释放10000次
    */
   
    dskman = new CDiskMan(pge);
    dskman->addBidx(0, 1048576);
    dskman->load();
    dskman->print();
    for(int i = 0; i < 10000; i++) {
        int isFree = random() % 2;
        size_t len1 = (random() % 10);
        size_t len2 = 1;
        while(len1--)
        {
            len2 *= 2;
        }
        if(isFree == 0) {
            size_t pos = dskman->balloc(len2);
            cout << "alloc pos=" << pos << ", len=" << len2 << endl;
            bidx[pos] = len2;
        } else {    
            isFree = random() % 2;
            if(isFree != 0)
            {
                if(!bidx.empty()) {
                    size_t pos =random() % bidx.size();
                    auto it = bidx.begin();
                    advance(it, (int)pos);
                    dskman->bfree(it->first, it->second);
                    cout << "free pos=" << it->first << ", len=" << it->second << endl;
                    bidx.erase(it);
                }
            }
        }
        dskman->print();
    }
    delete dskman;

    delete pge;
    delete engine;
    delete[] data;

	return 0;

}

