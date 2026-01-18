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

#define __LOAD__ 0

const bidx BOOTDIX = {0, 1};


uint8_t gdata[MAXKEYLEN] { 0 };

static KEY_T make_key(uint64_t v) {
    
    std::memset(gdata, 0, MAXKEYLEN);
    std::memcpy(gdata, &v, sizeof(v));
    return { gdata, sizeof(v) };
}

static void expect_insert(CBPlusTree& bpt, uint64_t keyVal, uint64_t payload) {
    auto key = make_key(keyVal);
    uint64_t row[3] = {keyVal, payload, payload + 50};
    cout << "key data : " << *(uint64_t*)key.data << ", len=" << (int)key.len << endl;
    int rc = bpt.insert(key, row, sizeof(row));
    if (rc != 0) {
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
	if (rc) {
		cout << " attach device fail " << endl;
		delete engine;
        return rc;
	}
    
    // this class must be destruct before engine destruct
    CPage* page = new CPage(engines, 128, log);
    CDiskMan dman(page);
    CCollection* coll = new CCollection(dman, *page);


    if (__LOAD__) {
        rc = coll->loadFrom(BOOTDIX);
        if (rc != 0) {
            cout << " load collection fail, rc=" << rc << endl;
        }
        
    } else {
        CCollectionInitStruct initStruct;
        initStruct.indexPageSize = 1;
        rc = coll->initialize(initStruct);
        if (rc != 0) {
            cout << " load collection fail, rc=" << rc << endl;
        }
        rc = coll->addCol("pk", dpfs_datatype_t::TYPE_BIGINT, sizeof(int64_t), 0, static_cast<uint8_t>(CColumn::constraint_flags::PRIMARY_KEY));
        assert(rc == 0);
        rc = coll->addCol("val", dpfs_datatype_t::TYPE_BIGINT, sizeof(int64_t));
        assert(rc == 0);
        rc = coll->addCol("val2", dpfs_datatype_t::TYPE_BIGINT, sizeof(int64_t));
        assert(rc == 0);

        coll->initBPlusTreeIndex();
    }
    
    // CBPlusTree bpt(*coll, *page, dman, 4);

    CBPlusTree& bpt = *coll->m_btreeIndex;

#if __LOAD__ == 1

#else
    try {
        // for(int i = 1; i < 23; ++i) {
        //     if (i == 13) {
        //         continue;
        //     }
        //     expect_insert(bpt, i, 1000 + i);
        // }
        expect_insert(bpt, 1, 1000 + 1);
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
#endif
    bpt.printTree();

    cout << " test insert " << endl;
    while(1) {
        cout << " insert key val (0 to exit): ";
        uint64_t keyVal = 0;
        cin >> keyVal;
        if (keyVal == 0) {
            break;
        }

        expect_insert(bpt, keyVal, keyVal + 1000);

        bpt.printTree();    
    }


    cout << " test remove " << endl;
    while(1) {
        cout << " remove key val (0 to exit): ";
        uint64_t keyVal = 0;
        cin >> keyVal;
        if (keyVal == 0) {
            break;
        }
        auto key = make_key(keyVal);
        rc = bpt.remove(key);
        if (rc != 0) {
            cout << " remove key " << keyVal << " fail, rc=" << rc << endl;
        }
        bpt.printTree();
    }

    cout << " test search " << endl;

    while(1) {
        cout << " search key val (0 to exit): ";
        uint64_t keyVal = 0;
        cin >> keyVal;
        if (keyVal == 0) {
            break;
        }

        char out[1024] { 0 };
        uint32_t actureLen = 0;

        rc = bpt.search(make_key(keyVal), out, sizeof(out), &actureLen);
        if (rc != 0) {
            if (rc == -2) {
                cout << " search key " << keyVal << " not found " << endl;
                continue;
            }
            cout << " search key " << keyVal << " fail, rc=" << rc << endl;
            continue;
        }

        cout << "search result: | Row Data (len " << actureLen << "): " << endl;
        for(int i = 0;i < 32; ++i) {
            printf("%02X", (unsigned char)out[i]);
        }
        printf("\n");
    }


    cout << " print data by iterator " << endl;
    try {
        // auto it = bpt.begin();
        auto it = bpt.search(make_key(5));
        auto end = bpt.end();
        rc = it.loadNode();
        if (rc != 0) {

        } else {
            for (; it != end; --it) {
                char keyData[128] = {0};
                uint32_t keyLen = 0;
                char payloadData[256] = {0};
                uint32_t payloadLen = 0;
                rc = it.loadData(keyData, sizeof(keyData), keyLen, payloadData, sizeof(payloadData), payloadLen);
                if (rc != 0) {
                    cout << " load data fail, rc=" << rc << endl;
                    continue;
                }
                // cout << " load data success " << endl;
                // cout << " key len: " << keyLen << ", payload len: " << payloadLen << endl;
                cout << " Key: ";
                for (uint32_t i = 0; i < keyLen; ++i) {
                    printf("%02X", (unsigned char)keyData[i]);
                }
                cout << ", Payload: ";
                for (uint32_t i = 0; i < payloadLen; ++i) {
                    printf("%02X", (unsigned char)payloadData[i]);
                }
                cout << std::endl;

                // cout << "Key: " << keyData << ", Payload: " << payloadData << endl;
            }
        }
    } catch (const std::exception& ex) {
        std::cerr << "Iterator threw exception: " << ex.what() << std::endl;
    } catch (...) {
        std::cerr << "Iterator threw non-std exception" << std::endl;
    }

    rc = bpt.commit();
    if (rc != 0) {
        cout << " commit b+ tree fail, rc=" << rc << endl;
        goto errReturn;
    }

    //TODO TEST SAVE AND LOAD TREE FROM DISK
    rc = coll->saveTo(BOOTDIX);
    if (rc != 0) {
        cout << " save collection fail, rc=" << rc << endl;
        goto errReturn;
    }
    

    cout << " test finished " << endl;

    if (coll) {
        delete coll;
    }
    if (page) {
        delete page;
    }
    // maybe some unfinished async work that cause core dump
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (engine) {
		delete engine;
	}

    return 0;

errReturn:    
    if (coll) {
        delete coll;
    }
    if (page) {
        delete page;
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
	if (engine) {
		delete engine;
	}
	
	return 0;
}
