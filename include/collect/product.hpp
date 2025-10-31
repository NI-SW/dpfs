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



class CProduct {
public:
    CProduct();
    ~CProduct();
    
    // product id for system product is {nodeId, 0}
    bidx pid;
    // fixed info for product
    CCollection fixedInfo;

    //
    int addCollection(const std::string& name);

    struct collectStatus{
        // block address of the collection
        uint64_t blockAddr = 0;
        // pointer to the collection in memory
        CCollection* tab = nullptr;
        // whether the collection is loaded in memory
        bool loaded = false;
        // whether the collection is modified
        bool dirty = false;
    };

    // load from disk
    struct tableInfo {
        // status of the collection
        collectStatus status;
        // length of the collection name
        uint8_t nameLen = 0;
        // name of the collection
        char* name = nullptr;
    };

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