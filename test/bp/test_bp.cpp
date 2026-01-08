#include <storage/engine.hpp>
#define private public
#define protected public
#include <collect/bp.hpp>
#undef private
#undef protected
#include <log/logbinary.h>
#include <collect/product.hpp>
#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>


static CBPlusTree::KEY_T make_key(uint64_t v) {
    CBPlusTree::KEY_T k{};
    std::memset(k.data, 0, KEYLEN);
    std::memcpy(k.data, &v, sizeof(v));
    return k;
}

static void expect_insert(CBPlusTree& bpt, uint64_t keyVal, uint64_t payload) {
    auto key = make_key(keyVal);
    uint64_t row[2] = {keyVal, payload};
    int rc = bpt.insert(key, row, sizeof(row));
    if(rc != 0) {
        cout << rc << endl;
        return;
    }
    // assert(rc == 0);
}

int main() {
    logrecord log;
    // MockEngine engine(1024);
    int rc = 0;
    bool threw = false;

    dpfsEngine* engine = newEngine("nvmf");
    std::vector<dpfsEngine*> engines;  
    engines.emplace_back(engine);

	// rc = engine->attach_device("trtype:tcp adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1");
	rc = engine->attach_device("trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50658 subnqn:nqn.2016-06.io.spdk:cnode1");
	// rc = engine->attach_device("trtype:pcie traddr:0000.1b.00.0");  engine->attach_device("trtype:pcie traddr:0000.13.00.0");
	if(rc) {
		cout << " attach device fail " << endl;
		delete engine;
        return rc;
	}
    
    // this class must be destruct before engine destruct
    CPage* page = new CPage(engines, 128, log);
    CDiskMan dman(page);
    CProduct owner(*page, dman, log);
    CCollection coll(owner, dman, *page, 0);

    rc = coll.addCol("pk", dpfs_datatype_t::TYPE_BIGINT, sizeof(int64_t), 0, static_cast<uint8_t>(CColumn::constraint_flags::PRIMARY_KEY));
    assert(rc == 0);
    rc = coll.addCol("val", dpfs_datatype_t::TYPE_BIGINT, sizeof(int64_t));
    assert(rc == 0);

    CBPlusTree bpt(coll, *page, dman, 4);



    try {
        expect_insert(bpt, 1, 1001);
        expect_insert(bpt, 2, 1002);
        expect_insert(bpt, 3, 1003);
    } catch (const std::exception& ex) {
        threw = true;
        std::cerr << "insert threw exception: " << ex.what() << std::endl;
    } catch (...) {
        threw = true;
        std::cerr << "insert threw non-std exception" << std::endl;
    }

    assert(!threw && "insert should not throw on fresh tree");
    assert(bpt.m_root.bid != 0);
    assert(!bpt.m_commitCache.empty());

    size_t leafWithKeys = 0;
    for (auto& entry : bpt.m_commitCache) {
        auto& nd = entry.second;
        if (nd.nodeData && nd.nodeData->hdr->leaf && nd.nodeData->hdr->count > 0) {
            ++leafWithKeys;
        }
    }
    assert(leafWithKeys > 0);

    std::cout << "insert smoke test passed" << std::endl;

    char out[1024] { 0 };
    uint32_t actureLen = 0;

    bpt.search(make_key(2), out, sizeof(out), &actureLen);

    cout << "search result: " << endl;
    for(int i = 0;i < 128; ++i) {
        printf("%02X ", (unsigned char)out[i]);
        if(i % 16 == 15) {
            printf("\n");
        }

    }
    cout << "search result len: " << actureLen << endl;

    
    bpt.printTree();

    while(1) {
        cout << " insert key val (0 to exit): ";
        uint64_t keyVal = 0;
        cin >> keyVal;
        if(keyVal == 0) {
            break;
        }

        expect_insert(bpt, keyVal, keyVal + 1000);

        bpt.printTree();    
    }


    if(page) {
        delete page;
    }
    if(engine) {
		delete engine;
	}

    return 0;

errReturn:
    if(page) {
        delete page;
    }
	if(engine) {
		delete engine;
	}
	
	
	return 0;
}
