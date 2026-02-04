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

#define __LOAD__ 1

#define __TEST_INSERT__
#define __TEST_DELETE__
#define __TEST_SEARCH__
#define __TEST_UPDATE__
#define __TEST_ITERATOR__
#define __TEST_INDEX_CREATE__
#define __TEST_INDEX_SEARCH__
#define __PRINT_INDEX_TREE__
#define __TEST_INDEX_ITERATOR__
#define __TEST_SCAN_ITERATOR__



const bidx BOOTDIX = {0, 1};


uint8_t gdata[MAXKEYLEN] { 0 };
int searchByIndex(CCollection* coll, const std::vector<std::string>& colNames, const std::vector<CValue>& keyVals);

static KEY_T make_key(uint64_t v1, uint64_t v2, const CBPlusTree& bpt) {
    
    std::memset(gdata, 0, MAXKEYLEN);
    std::memcpy(gdata, &v1, sizeof(v1));
    std::memcpy(gdata + sizeof(v1), &v2, sizeof(v2));
    return { gdata, sizeof(v1) + sizeof(v2), bpt.cmpTyps };
}

static void expect_insert(CBPlusTree& bpt, uint64_t val1, uint64_t val2, uint64_t payload) {
    auto key = make_key(val1, val2, bpt);
    uint64_t row[3] = {val1, val2, payload};
    cout << "key data : " << *(uint64_t*)key.data << " key data2 : " << *((uint64_t*)key.data + 1) << ", len=" << (int)key.len << endl;
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
    std::vector<CValue> keyVals;
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
        rc = coll->addCol("val", dpfs_datatype_t::TYPE_BIGINT, sizeof(int64_t), 0, static_cast<uint8_t>(CColumn::constraint_flags::PRIMARY_KEY));
        assert(rc == 0);
        rc = coll->addCol("val2", dpfs_datatype_t::TYPE_BIGINT, sizeof(int64_t));
        assert(rc == 0);
        
        rc = coll->saveTo(BOOTDIX);
        if (rc != 0) {
            cout << " save collection fail, rc=" << rc << endl;
        }
        coll->initBPlusTreeIndex();
    }
    
    // CBPlusTree bpt(*coll, *page, dman, 4);

    CBPlusTree& bpt = *coll->m_btreeIndex;

#if __LOAD__ == 1

#else
    try {
        expect_insert(bpt, 1, 4, 481);
        expect_insert(bpt, 2, 1, 646);
        expect_insert(bpt, 2, 3, 785);
        expect_insert(bpt, 2, 6, 235);
        expect_insert(bpt, 4, 2, 125);
        expect_insert(bpt, 4, 4, 456);
        // expect_insert(bpt, 3, 6, 999);

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

    uint64_t* row = new uint64_t[coll->m_collectionStruct->m_cols.size()];
#ifdef __TEST_INSERT__
    cout << "-------------------------------------------------------------------------------------------test insert-------------------------------------------------------------------------" << endl;
    while(1) {
        
        cout << " insert key val (0 to exit): " << endl;
        for(uint32_t i = 0; i < coll->m_collectionStruct->m_cols.size(); ++i) {
            uint64_t val = 0;
            const CColumn& col = coll->m_collectionStruct->m_cols[i];
            cout << " insert to :";
            cout << " col " << i << " name: " << col.getName() << ", len: " << (int)col.getLen() << endl;
            cout << " ispk: " << (col.getDds().constraints.unionData & CColumn::constraint_flags::PRIMARY_KEY) << endl;
            cout << "insert for col " << i << ": ";
            cin >> val;
            row[i] = val;
            if(row[0] == 0) {
                break;
            }
        }

        if(row[0] == 0) {
            break;
        }

        CItem itm(coll->m_collectionStruct->m_cols);

        rc = itm.updateValue(0, (uint8_t*)&row[0], sizeof(uint64_t)); if (rc < 0) { cout << " update fail rc = " << rc << endl; }
        rc = itm.updateValue(1, (uint8_t*)&row[1], sizeof(uint64_t)); if (rc < 0) { cout << " update fail rc = " << rc << endl; }
        rc = itm.updateValue(2, (uint8_t*)&row[2], sizeof(uint64_t)); if (rc < 0) { cout << " update fail rc = " << rc << endl; }
        rc = itm.nextRow(); if (rc != 0) { cout << " nextRow fail rc = " << rc << endl; }

        
        // expect_insert(bpt, row[0], row[1], row[2]);
        rc = coll->addItem(itm);
        if (rc != 0) {
            cout << " insert key " << row[0] << " fail, rc=" << rc << endl;
            cout << "message : " << coll->message << endl;
        }

        bpt.printTree();    
    }
#endif

#ifdef __TEST_DELETE__
    cout << "-------------------------------------------------------------------------------------------test delete-------------------------------------------------------------------------" << endl;
    while(1) {
        cout << " remove key val (0 to exit): ";

        for(uint32_t i = 0; i < coll->m_collectionStruct->m_cols.size(); ++i) {
            uint64_t val = 0;
            const CColumn& col = coll->m_collectionStruct->m_cols[i];
            if(!(col.getDds().constraints.unionData & CColumn::constraint_flags::PRIMARY_KEY)) {
                continue;
            }
            cout << " remove key: ";
            cout << " col " << i << " name: " << col.getName() << ", len: " << (int)col.getLen() << endl;
            cout << " ispk: " << (col.getDds().constraints.unionData & CColumn::constraint_flags::PRIMARY_KEY) << endl;
            cout << "remove for key col " << i << ": ";
            cin >> val;
            row[i] = val;
            if(row[0] == 0) {
                break;
            }
        }

        if(row[0] == 0) {
            break;
        }

        auto key = make_key(row[0], row[1], bpt);
        rc = bpt.remove(key);
        if (rc != 0) {
            cout << " remove key " << row[0] << " fail, rc=" << rc << endl;
        }
        bpt.printTree();
    }
#endif

#ifdef __TEST_UPDATE__
    cout << "-------------------------------------------------------------------------------------------test update-------------------------------------------------------------------------" << endl;
    while(1) {
        cout << " update key val (0 to exit): ";

        for(uint32_t i = 0; i < coll->m_collectionStruct->m_cols.size(); ++i) {
            uint64_t val = 0;
            const CColumn& col = coll->m_collectionStruct->m_cols[i];
            // if(!(col.getDds().constraints.unionData & CColumn::constraint_flags::PRIMARY_KEY)) {
            //     continue;
            // }
            cout << " update key: ";
            cout << " col " << i << " name: " << col.getName() << ", len: " << (int)col.getLen() << endl;
            cout << " ispk: " << (col.getDds().constraints.unionData & CColumn::constraint_flags::PRIMARY_KEY) << endl;
            cout << "update for key col " << i << ": ";
            cin >> val;
            row[i] = val;
            if(row[0] == 0) {
                break;
            }
        }

        if(row[0] == 0) {
            break;
        }

        auto key = make_key(row[0], row[1], bpt);
        rc = bpt.update(key, row, sizeof(uint64_t) * coll->m_collectionStruct->m_cols.size());
        if (rc != 0) {
            cout << " update key " << row[0] << " fail, rc=" << rc << endl;
        }
        bpt.printTree();
    }
#endif

#ifdef __TEST_SEARCH__
    cout << "-------------------------------------------------------------------------------------------test search-------------------------------------------------------------------------" << endl;

    while(1) {
        cout << " search key val (0 to exit): ";

        for(uint32_t i = 0; i < coll->m_collectionStruct->m_cols.size(); ++i) {
            uint64_t val = 0;
            const CColumn& col = coll->m_collectionStruct->m_cols[i];
            if(!(col.getDds().constraints.unionData & CColumn::constraint_flags::PRIMARY_KEY)) {
                continue;
            }
            cout << " search key: ";
            cout << " col " << i << " name: " << col.getName() << ", len: " << (int)col.getLen() << endl;
            cout << " ispk: " << (col.getDds().constraints.unionData & CColumn::constraint_flags::PRIMARY_KEY) << endl;
            cout << "search for key col " << i << ": ";
            cin >> val;
            row[i] = val;
            if(row[0] == 0) {
                break;
            }
        }
        if(row[0] == 0) {
            break;
        }

        char out[1024] { 0 };
        uint32_t actureLen = 0;

        rc = bpt.search(make_key(row[0], row[1], bpt), out, sizeof(out), &actureLen);
        if (rc != 0) {
            if (rc == -2) {
                cout << " search key " << row[0] << "," << row[1] << " not found " << endl;
                continue;
            }
            cout << " search key " << row[0] << "," << row[1] << " fail, rc=" << rc << endl;
            continue;
        }

        cout << "search result: | Row Data (len " << actureLen << "): " << endl;
        for(int i = 0;i < 32; ++i) {
            printf("%02X", (unsigned char)out[i]);
        }
        printf("\n");
    }
#endif

#ifdef __TEST_ITERATOR__
    cout << "-------------------------------------------------------------------------------------------test print data by iterator-------------------------------------------------------------------------" << endl;
    try {
        // auto it = bpt.begin();
        auto it = bpt.search(make_key(5, 10, bpt));
        auto end = bpt.end();
        // rc = it.loadNode();
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

#endif

    CIndexInitStruct idxInit;

    rc = bpt.commit();
    if (rc != 0) {
        cout << " commit b+ tree fail, rc=" << rc << endl;
        goto errReturn;
    }

    // after commit, status is error

#ifdef __TEST_INDEX_CREATE__
    // test index
    cout << "-------------------------------------------------------------------------------------------test create index-------------------------------------------------------------------------" << endl;
    idxInit.name = "idx_test_bp_idx";
    idxInit.colNames.emplace_back("val");
    idxInit.colNames.emplace_back("val2");
    idxInit.id = 1;
    idxInit.indexPageSize = 4;
    rc = coll->createIdx(idxInit);
    if (rc != 0) {
        cout << " create index fail, rc=" << rc << endl;
        cout << " message: " << coll->message << endl;
        // goto errReturn;
    }
#endif
    

#ifdef __TEST_INDEX_SEARCH__
    // search primary key
    cout << "-------------------------------------------------------------------------------------------test search by index-------------------------------------------------------------------------" << endl;
    row[0] = 6;
    row[1] = 235;
    keyVals.emplace_back(sizeof(uint64_t));
    keyVals[0].setData(&row[0], sizeof(uint64_t));
    keyVals.emplace_back(sizeof(uint64_t));
    keyVals[1].setData(&row[1], sizeof(uint64_t));
    {
        CItem tmpItm(coll->m_collectionStruct->m_cols);
        rc = coll->getByIndex({"val", "val2"}, keyVals, tmpItm);
        // rc = searchByIndex(coll, {"val", "val2"}, keyVals);
        if (rc != 0) {
            cout << " search by index fail, rc=" << rc << endl;
            goto errReturn;
        }

        cout << " get data :: " << endl;
        for(uint32_t i = 0; i < coll->m_collectionStruct->m_cols.size(); ++i) {
            CValue val = tmpItm.getValue(i);
            cout << " col " << i << " name: " << coll->m_collectionStruct->m_cols[i].getName() << ", len: " << (int)val.len << ", data: ";
            for (uint32_t j = 0; j < val.len; ++j) {
                printf("%02X", ((unsigned char*)val.data)[j]);
            }
            cout << endl;
        }
        // printMemory(tmpItm.getValue(0).data, tmpItm.getValue(0).len);
 
    }

#endif


#ifdef __PRINT_INDEX_TREE__

    cout << "-------------------------------------------------------------------------------------------print index tree-------------------------------------------------------------------------" << endl;
    for(int i = 0; i < coll->m_indexTrees.size(); ++i) {
        cout << " print index tree " << i << endl;
        CBPlusTree& indexTree = *coll->m_indexTrees[i];
        indexTree.printTree();
    }

#endif

#ifdef __TEST_INDEX_ITERATOR__
{
    // get index iter
    cout << "-------------------------------------------------------------------------------------------test get index iterator-------------------------------------------------------------------------" << endl;
    CCollection::CIdxIter indexIter;
    row[0] = 6;
    row[1] = 235;
    keyVals.emplace_back(sizeof(uint64_t));
    keyVals[0].setData(&row[0], sizeof(uint64_t));
    keyVals.emplace_back(sizeof(uint64_t));
    keyVals[1].setData(&row[1], sizeof(uint64_t));

    rc = coll->getIdxIter({"val", "val2"}, keyVals, indexIter);
    if (rc != 0) {
        cout << " get index iterator fail, rc=" << rc << endl;
        goto errReturn;
    }

    // use index iter to get main tree data
    CItem tmpItm(coll->m_collectionStruct->m_cols);
    rc = 0;
    while (rc == 0) {
        rc = coll->getByIndexIter(indexIter, tmpItm);
        if (rc != 0) {
            cout << " get by index iterator fail, rc=" << rc << endl;
            goto errReturn;
        }

        cout << " get data :: " << endl;
        for(uint32_t i = 0; i < coll->m_collectionStruct->m_cols.size(); ++i) {
            CValue val = tmpItm.getValue(i);
            cout << " col " << i << " name: " << coll->m_collectionStruct->m_cols[i].getName() << ", len: " << (int)val.len << ", data: ";
            for (uint32_t j = 0; j < val.len; ++j) {
                printf("%02X", ((unsigned char*)val.data)[j]);
            }
            cout << endl;
        }

        rc = ++indexIter;
    }

}
#endif


#ifdef __TEST_SCAN_ITERATOR__
{
    cout << "-------------------------------------------------------------------------------------------test get scan iterator-------------------------------------------------------------------------" << endl;
    CCollection::CIdxIter scanIter;
    rc = coll->getScanIter(scanIter);
    if (rc != 0) {
        cout << " get scan iterator fail, rc=" << rc << endl;
        goto errReturn;
    }

    // use scan iter to get main tree data
    CItem tmpItm(coll->m_collectionStruct->m_cols);
    rc = 0;
    while (rc == 0) {
        rc = coll->getByScanIter(scanIter, tmpItm);
        if (rc != 0) {
            cout << " get by scan iterator fail, rc=" << rc << endl;
            goto errReturn;
        }

        cout << " get data :: " << endl;
        for(uint32_t i = 0; i < coll->m_collectionStruct->m_cols.size(); ++i) {
            CValue val = tmpItm.getValue(i);
            cout << " col " << i << " name: " << coll->m_collectionStruct->m_cols[i].getName() << ", len: " << (int)val.len << ", data: ";
            for (uint32_t j = 0; j < val.len; ++j) {
                printf("%02X", ((unsigned char*)val.data)[j]);
            }
            cout << endl;
        }

        rc = ++scanIter;
    }

}
#endif


    //TODO TEST SAVE AND LOAD TREE FROM DISK
    rc = coll->saveTo(BOOTDIX);
    if (rc != 0) {
        cout << " save collection fail, rc=" << rc << endl;
        goto errReturn;
    }
    

    // TODO :: TEST IS PASS, COMPLETE SQL PARSER
    cout << " test pass " << endl;

    if (coll) {
        delete coll;
    }
    if (page) {
        delete page;
    }
    // maybe some unfinished async work that cause core dump
    // std::this_thread::sleep_for(std::chrono::seconds(1));
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


#define __INDEX_SEARCH_DEBUG__

int searchByIndex(CCollection* coll, const std::vector<std::string>& colNames, const std::vector<CValue>& keyVals) {
    cout << " test search by index " << endl;
    int rc = 0;
    CCollectIndexInfo* indexInfo = nullptr;

    bool match = false;
    for (uint32_t i = 0; i < coll->m_collectionStruct->m_indexInfos.size(); ++i) {
        cout << " index " << i << " name: " << coll->m_collectionStruct->m_indexInfos[i].name << endl;

        if (coll->m_collectionStruct->m_indexInfos[i].cmpKeyColNum != colNames.size()) {
            continue;
        }
        match = true;
        auto& keyseq = coll->m_collectionStruct->m_indexInfos[i].keySequence;
        for (uint32_t j = 0; j < coll->m_collectionStruct->m_indexInfos[i].cmpKeyColNum; ++j) {
            #ifdef __INDEX_SEARCH_DEBUG__
            cout << "   col " << j << " len: " << (uint16_t)coll->m_collectionStruct->m_cols[keyseq[j]].getNameLen() << " name: " << coll->m_collectionStruct->m_cols[keyseq[j]].getName() << endl;
            cout << "   search col len << " << colNames[j].size() << " name: " << colNames[j] << endl;
            #endif
            if (colNames[j].size() != coll->m_collectionStruct->m_cols[keyseq[j]].getNameLen()) {
                // cout << " length not match " << endl;
                match = false;
                break;
            }

            if (memcmp(coll->m_collectionStruct->m_cols[keyseq[j]].getName(), colNames[j].c_str(), coll->m_collectionStruct->m_cols[keyseq[j]].getNameLen()) != 0) {
                // cout << " name not match " << endl;
                match = false;
                break;
            }
        }

        if (match) {
            indexInfo = &coll->m_collectionStruct->m_indexInfos[i];
            break;
        }
    }

    if (!match) {
        coll->message = "no index found for given columns";
        return -ENOENT;
    }

    std::vector<std::pair<uint8_t, dpfs_datatype_t>> cmpTp;
    
    for(uint32_t i = 0; i < indexInfo->cmpKeyColNum; ++i) {
        cmpTp.emplace_back(std::make_pair(indexInfo->cmpTypes[i].colLen, indexInfo->cmpTypes[i].colType));
    }

    CBPlusTree index(coll->m_page, coll->m_diskMan, indexInfo->indexPageSize, indexInfo->indexHigh, 
    indexInfo->indexRoot, indexInfo->indexBegin, indexInfo->indexEnd, indexInfo->indexKeyLen, indexInfo->indexRowLen, cmpTp);


    char keydata[1024];
    KEY_T indexKey(keydata, indexInfo->indexKeyLen, cmpTp);
    uint32_t offset = 0;
    for(int i = 0; i < keyVals.size(); ++i) {
        std::memcpy(keydata + offset, keyVals[i].data, keyVals[i].len);
        offset += keyVals[i].len;
    }

    // use MAX_COL_NUM but only first two columns are valid
    const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM> indexCols(indexInfo->indexCol, indexInfo->idxColNum);

    CItem itm(indexCols);


    rc = index.search(indexKey, itm.data, itm.rowLen);
    if(rc != 0) {
        return rc;
    }

    itm.endIter.m_pos = 1;
    itm.endIter.m_ptr = itm.data + itm.rowLen;

    cout << " get data :: " << endl;
    CValue pkVal = itm.getValue(1);
    printMemory(pkVal.data, pkVal.len);

    // use this pkdata to find in original bplus tree

    KEY_T oriKey(pkVal.data, pkVal.len, coll->m_cmpTyps);

    CItem pkItm(coll->m_collectionStruct->m_cols);
    rc = coll->getRow(oriKey, &pkItm);
    if (rc != 0) {
        cout << " get row by primary key fail, rc=" << rc << endl;
        return rc;
    }

    cout << " get original data by primary key :: " << endl;
    for (uint32_t i = 0; i < coll->m_collectionStruct->m_cols.size(); ++i) {
        CValue val = pkItm.getValue(i);
        cout << " col " << i << " len: " << val.len << " data: ";
        printMemory(val.data, val.len);
    }



    return 0;
}
