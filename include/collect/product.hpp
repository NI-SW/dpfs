#pragma once
#include <collect/collect.hpp>
#include <vector>
#include <cstdint>
/*
* 对于一批产品，每件产品的信息使用固定长度存储，使用柔性数组，对于可变长度的位置（交易链表），存储其块号与偏移量
* 
* 功能需求：
* 1. 支持按产品ID快速定位产品信息
* 2. 支持产品信息的增删改查操作
* 3. 支持批量操作，提高效率
* 4. 支持并发访问，保证数据一致性
* 5. 支持查看全部产品列表，支持产品模糊查询
*/

struct CTraceBack {
    // bidx of the trace back
    bidx idx;
    // length of the trace back info
    size_t len = 0;
    // trace back info
    void* info = nullptr;
};


class CProduct {
public:
    CProduct() {};
    ~CProduct() {};
    
    // product id for system product is {nodeId, 0}
    bidx pid;

    /*
        @param info: pointer to the trace back info
        @param len: length of the info
        @param idx: bidx of the trace back
        @return 0 on success, else on failure
        @note store the trace back info in the product trace back structure
    */
    int traceBack(CTraceBack*& info, const bidx& idx);
    // fixed info for product
    
    uint32_t collectionCount = 0;
    CCollection* collections = nullptr;

    CCollection fixedInfo;
    CCollection products;

    int save() {
        // TODO save product info to disk
        /*
            struct :
            |PID(8B)
            |fixedInfo(defined when save, data is sequential storaged)
            |varCollectionCount(4B)
            |varCollectionInfo(use B+ Tree to storage data, 16B for b+ tree head pointer)
            |productsInfo|
        */
        return 0;
    }

    int load() {
        // TODO load product info from disk
        return 0;
    }

    //
    int addCollection(const std::string& name);

    /* 
        pointer to the collection in disk
        user defined methods for product management, in system, find in memory first, if not, load from disk
        may use unordered_map or bitmap to manage the collections in memory?
    */
    std::vector<uint64_t> tabBlockAddrsses;

};


void qwertest() {

    sizeof(CProduct);
    return;
}