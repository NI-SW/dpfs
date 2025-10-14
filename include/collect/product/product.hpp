#include <collect/collect.hpp>
#include <vector>
#include <cstdint>
/*
* 对于一批产品，每件产品的信息使用固定长度存储，使用柔性数组，对于可变长度的位置（交易链表），存储其块号与偏移量
* 
*/



class CProduct {
public:
    CProduct();
    ~CProduct();
    
    // product id
    bidx pid;
    // fixed info for product
    CCollection fixedInfo;

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