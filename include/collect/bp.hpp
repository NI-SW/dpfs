// b+ tree for product index
#pragma once
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <vector>
#include <threadlock.hpp>
#include <basic/dpfsconst.hpp>
#include <collect/page.hpp>
#include <collect/diskman.hpp>
#include <collect/collect.hpp>
#include <dpendian.hpp>
#include <unordered_map>

#define __DEBUG__

#ifdef __DEBUG__
#include <iostream>
#include <string>
using namespace std;
constexpr int indOrder = 4;
constexpr int rowOrder = 4;

static int myabort() {
    return -ERANGE;
    abort();
    return 0;
}
#define ERANGE myabort()
#endif

// if key length exceed maxKeyLen, when search compare only first maxKeyLen bytes
constexpr uint32_t maxSearchKeyLen = 32;
constexpr uint32_t maxInrowLen = 256;

constexpr uint32_t ROWPAGESIZE = 32; 

// parent + prev + next + keyCnt + isLeaf + reserve
constexpr uint8_t hdrSize = 8 + 8 + 8 + 2 + 1 + 5; 
constexpr uint8_t KEYLEN = maxSearchKeyLen;
constexpr dpfs_datatype_t KEYTYPE = dpfs_datatype_t::TYPE_BIGINT;
constexpr uint8_t PAGESIZE = 4;
constexpr int ROWLEN = 512; // default row length

/*
enum class dpfs_datatype_t : uint8_t {
    TYPE_NULL = 0,
    TYPE_INT,
    TYPE_BIGINT,
    TYPE_FLOAT,
    TYPE_DECIMAL,
    TYPE_DOUBLE,
    TYPE_CHAR,
    TYPE_VARCHAR,
    TYPE_BINARY,
    TYPE_BLOB,
    MAX_TYPE
};
*/

// template<uint8_t KEYLEN = 8>
class CBPlusTree {
public:

using child_t = uint64_t;

    // template<uint8_t KEYLEN = 8, dpfs_datatype_t KEYTYPE = dpfs_datatype_t::TYPE_BIGINT>
    struct KEY_T {
        // dpfs_datatype_t type = dpfs_datatype_t::TYPE_NULL;
        // uint8_t m_len = KEYLEN;
        uint8_t data[KEYLEN];

        KEY_T& operator=(const KEY_T& other) noexcept {
            std::memcpy(data, other.data, KEYLEN);
            return *this;
        }

        bool operator==(const KEY_T& other) const noexcept {
            switch(KEYTYPE) {
                case dpfs_datatype_t::TYPE_INT:
                    return *(int32_t*)data == *(int32_t*)other.data;
                    break;
                case dpfs_datatype_t::TYPE_FLOAT:
                    return *(float*)data == *(float*)other.data;
                    break;
                case dpfs_datatype_t::TYPE_BIGINT:
                    return *(int64_t*)data == *(int64_t*)other.data;
                    break;
                case dpfs_datatype_t::TYPE_DOUBLE:
                    return *(double*)data == *(double*)other.data;
                    break;
                case dpfs_datatype_t::TYPE_CHAR:
                case dpfs_datatype_t::TYPE_VARCHAR:
                    return std::strncmp((const char*)data, (const char*)other.data, KEYLEN) == 0;
                    break;
                case dpfs_datatype_t::TYPE_BLOB:
                case dpfs_datatype_t::TYPE_BINARY:
                    return std::memcmp(data, other.data, KEYLEN) == 0;
                    break;
                default:
                    break;
            }
            return std::memcmp(data, other.data, KEYLEN) == 0;
        }
        bool operator<(const KEY_T& other) const noexcept {
            switch(KEYTYPE) {
                case dpfs_datatype_t::TYPE_INT:
                    return *(int32_t*)data < *(int32_t*)other.data;
                    break;
                case dpfs_datatype_t::TYPE_FLOAT:
                    return *(float*)data < *(float*)other.data;
                    break;
                case dpfs_datatype_t::TYPE_BIGINT:
                    return *(int64_t*)data < *(int64_t*)other.data;
                    break;
                case dpfs_datatype_t::TYPE_DOUBLE:
                    return *(double*)data < *(double*)other.data;
                    break;
                case dpfs_datatype_t::TYPE_CHAR:
                case dpfs_datatype_t::TYPE_VARCHAR:
                    return std::strncmp((const char*)data, (const char*)other.data, KEYLEN) < 0;
                    break;
                case dpfs_datatype_t::TYPE_BLOB:
                case dpfs_datatype_t::TYPE_BINARY:
                    return std::memcmp(data, other.data, KEYLEN) < 0;
                    break;
                default:
                    break;
            }
            
            return std::memcmp(data, other.data, KEYLEN) < 0;
        }
    };

    // tiny RAII helper for CSpin
    struct CSpinGuard {
        explicit CSpinGuard(CSpin& l) : lock(l) { lock.lock(); }
        ~CSpinGuard() { lock.unlock(); }
        CSpin& lock;
    };

    /*

    {ONE BLOCK FOR LEAF NODE}
                |parent(8B)|prev(8B)|next(8B)|isLeaf(1B)|keyCnt(2B)|reserve(5B)|key|value|...|...|key|value|
                                                                                |
    nodeData->                                                                 data[]


    {ONE BLOCK FOR INDEX NODE}
                |parent(8B)|prev(8B)|next(8B)|isLeaf(1B)|keyCnt(2B)|reserve(5B)|keys...|childs...|
                                                                    |       |
    nodeData->                                                    data[]  keyCnt * KEYLEN

    Data region layout is compact; all block ids are stored as uint64_t (bid) and
    gid is inherited from the current node's gid. VALUE is stored as uint64_t
    reinterpretation of VALUE_T (pointer or integral payload).

    */
    class CKeyVec;
    class CChildVec;
    class CRowVec;

    // for one node data in b+ tree
    struct NodeData {
        explicit NodeData(CPage& page) : m_page(page) {};
        NodeData(const NodeData& nd) = delete;
        NodeData(NodeData&& nd) noexcept;
        NodeData& operator=(NodeData&& nd);
        // use pointer as reference
        NodeData& operator=(NodeData* nd);

        ~NodeData();
        // int initNode(bool isLeaf, int32_t order);
        // use data from zptr
        int initNode(bool isLeaf, int32_t order);
        int initNodeByLoad(bool isLeaf, int32_t order, void* zptr);
        /*
            @param key: key to insert
            @param lchild: left child bid, 0 if do not update
            @param rchild: right child bid, it must be valid
            @return 0 on success, else on failure
        */
        int insertChild(const KEY_T& key, uint64_t lchild, uint64_t rchild);

        /*
            @param key: key to insert
            @param rowData: pointer to row data
            @param dataLen: length of row data
            @return 0 on success, else on failure
        */
        int pushBackChild(const KEY_T& key, uint64_t childBid);

        /*
            @param fromNode: node to copy from
            @param concateKey: key to insert between two child vectors
            @return 0 on success, else on failure
        */
        int pushBackChilds(const NodeData& fromNode, const KEY_T& concateKey) noexcept;

        /*
            @param key: key to insert
            @param rowData: pointer to row data
            @param dataLen: length of row data
            @return 0 on success, else on failure
        */
        int pushFrontChild(const KEY_T& key, uint64_t childBid);

        /*
            @param fromNode: node to copy from
            @param concateKey: key to insert between two child vectors
            @return 0 on success, else on failure
            @note performance is worse than pushBackChilds, may be optimize later
        */
        int pushFrontChilds(const NodeData& fromNode, const KEY_T& concateKey) noexcept; 

        /*
            @param key: key to insert
            @param rowData: pointer to row data
            @param dataLen: length of row data
            @return 0 on success, else on failure
        */
        int insertRow(const KEY_T& key, const void* rowData, size_t dataLen);

        /*
            @param key: key to insert
            @param rowData: pointer to row data
            @param dataLen: length of row data
            @return 0 on success, else on failure
        */
        int pushBackRow(const KEY_T& key, const void* rowData, size_t dataLen);
        int pushBackRows(const NodeData& fromNode) noexcept;

        /*
            @param key: key to insert
            @param rowData: pointer to row data
            @param dataLen: length of row data
            @return 0 on success, else on failure
        */
        int pushFrontRow(const KEY_T& key, const void* rowData, size_t dataLen);
        int pushFrontRows(const NodeData& fromNode) noexcept;

        int popFrontRow();
        int popBackRow();
        int popFrontChild();
        int popBackChild();

        bidx self{ 0, 0 };
        // key start position
        KEY_T* keys = nullptr;
        child_t* children = nullptr;   // internal child pointers

        CKeyVec* keyVec = nullptr;
        CChildVec* childVec = nullptr;
        CRowVec* rowVec = nullptr;
        CPage& m_page;
        cacheStruct* pCache = nullptr;
        
        /*
            @return number of keys in the node
        */
        int size() noexcept;
        /*
            @param target: target NodeData to copy from
            @param begin: begin position (inclusive)
            @param end: end position (exclusive)
            @return 0 on success, else on failure
        */
        int assign(const NodeData& target, int begin, int end) noexcept;
        /*
            @param begin: begin position (inclusive)
            @param end: end position (exclusive)
            @return 0 on success, else on failure
            @note for split node only
        */
        int erase(int begin, int end) noexcept;

        /*
            @param key: key to remove
            @return 0 on success, else on failure
            @note if is not leaf node can only be used to remove first or end key
        */
        int erase(const KEY_T& key) noexcept;

        struct nd {

            // nd() noexcept {
            //     hdr = nullptr;
            //     data = nullptr;
            //     size = 0;
            // }

            nd(bool isLeaf, void* dataPtr) noexcept {
                data = reinterpret_cast<uint8_t*>(dataPtr);
                hdr = reinterpret_cast<decltype(hdr)>(data);
                hdr->leaf = isLeaf ? 1 : 0;
                if (isLeaf) {
                    size = ROWPAGESIZE * dpfs_lba_size;
                } else {
                    size = PAGESIZE * dpfs_lba_size;
                }
            }

            // nd(bool isLeaf = false) {
            //     if (isLeaf) {
            //         size = PAGESIZE * 4096 * rowCoefficient;
            //     } else {
            //         size = PAGESIZE * 4096;
            //     }
            //     data = new uint8_t[size];
            //     if (!data) {
            //         throw std::bad_alloc();
            //     }
            //     hdr = reinterpret_cast<decltype(hdr)>(data);
            //     std::memset(data, 0, size);
            // }

            nd(nd&& nd) noexcept {
                this->data = nd.data;
                this->size = nd.size;
                hdr = reinterpret_cast<decltype(hdr)>(data);
                nd.data = nullptr;
                nd.hdr = nullptr;
            }

            ~nd() {
                // if (data) {
                //     delete[] data;
                // }
            }

            #pragma pack(push, 1)
            struct hdr_t {
                uint64_t parent = 0; // parent bid
                uint64_t prev = 0;   // leaf prev bid
                uint64_t next = 0;   // leaf next bid
                uint16_t count = 0;
                uint8_t leaf = true;
                // if host is big endian, use it as a flag to indicate data is converted in memory
                uint8_t isConverted = 0;
                uint8_t childIsLeaf = 0;
                char reserve[3];
            }* hdr;
            #pragma pack(pop)
            // point to dma ptr, mange by outside
            uint8_t* data = nullptr;
            uint32_t size = 0;
        }* nodeData = nullptr;
        uint32_t maxKeySize = 0; 
        bool inited = false;
        bool isRef = false;
    };

    // max key count in one node
    const static uint32_t maxkeyCount = ((PAGESIZE * 4096 - sizeof(NodeData::nd::hdr_t)) - sizeof(uint64_t)) / (KEYLEN + sizeof(uint64_t));

    /*
        @note class to manage key vector without dynamic memory allocate
    */
    class CKeyVec {
    public:
        CKeyVec(NodeData& nd) : vecSize(nd.nodeData->hdr->count), keys(reinterpret_cast<KEY_T*>(nd.nodeData->data + sizeof(NodeData::nd::hdr_t))) {
            // extra one reserve for split action
            maxSize = nd.maxKeySize;
        }
        ~CKeyVec() = default;

        /*
            @param begin: begin pointer (inclusive)
            @param end: end pointer (exclusive)
            @return 0 on success, else on failure
            @note assign keys from begin to end to this vector
        */
        int assign(const KEY_T* begin, const KEY_T* end) noexcept {
            size_t newSize = static_cast<size_t>(end - begin);
            if (newSize > maxSize) {
                return -ERANGE;
            }
            std::memcpy(keys->data, begin, newSize * sizeof(KEY_T));
            vecSize = static_cast<uint16_t>(newSize);

            return 0;
        }

        /*
            @param key: key to insert
            @return position of the key: success
                    -ERANGE on exceed max size
            @note insert key to the end of the vector
        */
        int push_back(const KEY_T& key) noexcept {
            if (vecSize >= maxSize) {
                return -ERANGE;
            }
            keys[vecSize] = key;
            ++vecSize;
            return vecSize - 1;
        }

        int concate_back(const CKeyVec& fromVec) noexcept {
            if (vecSize + fromVec.vecSize > maxSize) {
                return -ERANGE;
            }
            std::memcpy(&keys[vecSize], fromVec.keys, fromVec.vecSize * sizeof(KEY_T));
            vecSize += fromVec.vecSize;
            return 0;
        }

        int push_front(const KEY_T& key) noexcept {
            if (vecSize >= maxSize) {
                return -ERANGE;
            }
            std::memmove(&keys[1], &keys[0], vecSize * sizeof(KEY_T));
            keys[0] = key;
            ++vecSize;
            return 0;
        }

        int concate_front(const CKeyVec& fromVec) noexcept {
            if (vecSize + fromVec.vecSize > maxSize) {
                return -ERANGE;
            }
            std::memmove(&keys[fromVec.vecSize], &keys[0], vecSize * sizeof(KEY_T));
            std::memcpy(&keys[0], fromVec.keys, fromVec.vecSize * sizeof(KEY_T));
            vecSize += fromVec.vecSize;
            return 0;
        }


        /*
            @param key: key to insert
            @return position of the key on success, -ERANGE on exceed max size
            @note insert key to the vector in sorted order

        */
        int insert(const KEY_T& key) noexcept {
            // find the pos of the key to insert
            auto pos = std::lower_bound(keys, keys + vecSize, key);
            if (pos != keys + vecSize && *pos == key) {
                // Key already exists
                return -EEXIST;
            }
            if (vecSize >= maxSize) {
                return -ERANGE;
            }
            std::memmove(pos + 1, pos, (keys + vecSize - pos) * sizeof(KEY_T));
            *pos = key;
            ++vecSize;
            return pos - keys;
        }

        int erase(const KEY_T& key) noexcept {
            auto pos = std::lower_bound(keys, keys + vecSize, key);
            if (pos == keys + vecSize || !(*pos == key)) {
                // key not found
                return -ENOENT;
            }
            std::memmove(pos, pos + 1, (keys + vecSize - pos - 1) * sizeof(KEY_T));
            --vecSize;
            return pos - keys;
        }

        /*
            @param begin: begin position (inclusive)
            @param end: end position (exclusive)
            @return 0 on success, else on failure
            @note erase keys from begin to end
        */
        int erase (const uint64_t& begin, const uint64_t& end) noexcept {
            if(end < begin) {
                return -EINVAL;
            }
            if (begin >= vecSize) {
                return -ENOENT;
            }
            std::memmove(&keys[begin], &keys[end], (vecSize - end) * sizeof(KEY_T));
            vecSize -= static_cast<uint16_t>(end - begin);
            return 0;
        }

        int pop_back() noexcept {
            if (vecSize == 0) {
                return -ENOENT;
            }
            --vecSize;
            return 0;
        }

        int pop_front() noexcept {
            if (vecSize == 0) {
                return -ENOENT;
            }
            std::memmove(&keys[0], &keys[1], (vecSize - 1) * sizeof(KEY_T));
            --vecSize;
            return 0;
        }

        /*
            @param key: key to search
            @return position of the key on success, -ENOENT if not found
            @note search key in the vector
        */
        int search(const KEY_T& key) const {
            auto pos = std::lower_bound(keys, keys + vecSize, key);
            // if (pos == keys + vecSize || !(*pos == key)) {
            //     return -ENOENT;
            // }
            return pos - keys;
        }

        /*
            @param pos: position to get key
            @param outkey: output reference key pointer
            @return 0 on success, else on failure
        */
        int at(uint32_t pos, KEY_T*& outkey) const noexcept {
            if (pos >= vecSize) {
                return -ERANGE;
            }
            outkey = &keys[pos];
            return 0;
        }

        KEY_T& operator[](uint32_t pos) const {
            if (pos >= vecSize) {
                throw std::out_of_range("Index out of range");
            }
            return keys[pos];
        }

        uint32_t size() const noexcept {
            return vecSize;
        }

    private:
        friend class CBPlusTree;
        decltype(NodeData::NodeData::nd::hdr_t::count)& vecSize;
        // first key pointer
        KEY_T* const keys;
        uint32_t maxSize = 0; // = ((PAGESIZE * 4096 - sizeof(NodeData::nd::hdr_t)) - sizeof(uint64_t)) / (KEYLEN + sizeof(uint64_t));
    };


    /*
        @note class to manage key vector without dynamic memory allocate
    */
    class CChildVec {
    public:
        
        CChildVec(NodeData& nd) : vecSize(nd.nodeData->hdr->count),  child(reinterpret_cast<child_t*>(nd.children)) {
            maxSize = nd.maxKeySize + 1;
        }
        ~CChildVec() = default;

        int clear() noexcept {
            // vecSize = 0;
            return 0;
        }
        int insert(uint32_t pos, child_t val) noexcept {
            if (pos > maxSize) {
                return -EINVAL;
            } else if(this->size() + 1 > maxSize) {
                // out of space
                return -ERANGE;
            }
            /*
            example  
                [1,2,3,4]


                [3]
                | |
                | [3,4,5]
                |
                [1,2] 



                [5,15,24,37]
                |
            [0,1,2,3,4]      ----->    [-1,0,1]  [2,3,4]



                [2,5,15,24,37]
                | |
                | [2,3,4]
                |
            [-1,0,1]


                [2,5,15,24,37]
                    |
                    [5,8,9,10,12] -> [5,8,9,10,12,14]

                [2,5,10,15,24,37]
                    |  |
                    |  [10,12,14]
                    |
                [5,8,9] 
                    
                [2,5,10,15,24,37] ====> [2,5,10]  [15,24,37]

                           [15]
                        /       \
                       /         \        
                      /           \
                     /             \
             [2,5,10]               [15,24,37]
                 |  |               |  
                 |  |               |
                 |  [10,12,14]      null
                 [5,8,9]

            */
            std::memmove(&child[pos + 1], &child[pos], (vecSize - pos) * sizeof(child_t));
            child[pos] = val;
            return 0;
        }

        child_t& at(uint32_t pos) const {
            if (pos >= maxSize) {
                throw std::out_of_range("Index out of range");
            }
            return child[pos];
        }

        /*
        
address         0 1 2 3 4 5 6 7 8
                1 2 3 4 5 6 7 8 9
                size = 9

                address of 5 = 4
                address of 9 = 8
                mid, end
                5 6 7 8 9
                size = 5

        */

        /*
            @param begin: begin pointer (inclusive)
            @param end: end pointer (exclusive)
            @return 0 on success, else on failure
            @note assign keys from begin to end to this vector
        */
        int assign(const child_t* begin, const child_t* end) noexcept {
            size_t newSize = static_cast<size_t>(end - begin);
            if (newSize > maxSize) {
                return -ERANGE;
            }
            std::memcpy(child, begin, newSize * sizeof(child_t));
            return 0;
        }

        child_t* begin() {
            return &child[0];
        }

        child_t* end() {
            return &child[this->size()];
        }

        int erase(int begin, int end) noexcept {
            if(end < begin) {
                return -EINVAL;
            }
            if (begin >= vecSize + 1) {
                return -ENOENT;
            }
            std::memmove(&child[begin], &child[end], (this->size() - end) * sizeof(child_t));
            return 0;
        }

        int pop_back() noexcept {
            if (vecSize == 0) {
                return -ENOENT;
            }
            memset(&child[this->size() - 1], 0, sizeof(child_t));
            return 0;
        }

        int pop_front() noexcept {
            if (vecSize == 0) {
                return -ENOENT;
            }
            std::memmove(&child[0], &child[1], (this->size() - 1) * sizeof(child_t));
            memset(&child[this->size() - 1], 0, sizeof(child_t));
            return 0;
        }

        int push_front(child_t val) noexcept {
            if (this->size() + 1 > maxSize) {
                return -ERANGE;
            }
            std::memmove(&child[1], &child[0], this->size() * sizeof(child_t));
            child[0] = val;
            return 0;
        }

        int concate_front(const CChildVec& fromVec) noexcept {
            if (this->size() + fromVec.size() > maxSize) {
                return -ERANGE;
            }
            std::memmove(&child[fromVec.size()], &child[0], this->size() * sizeof(child_t));
            std::memcpy(&child[0], fromVec.child, fromVec.size() * sizeof(child_t));
            return 0;
        }
        
        int push_back(child_t val) noexcept {
            if (this->size() + 1 > maxSize) {
                return -ERANGE;
            }
            child[this->size()] = val;
            return 0;
        }

        int concate_back(const CChildVec& fromVec) noexcept {
            if (this->size() + fromVec.size() > maxSize) {
                return -ERANGE;
            }
            std::memcpy(&child[this->size()], fromVec.child, fromVec.size() * sizeof(child_t));
            return 0;
        }

        child_t& operator[](uint32_t pos) const {
            return at(pos);
        }

        uint32_t size() const noexcept {
            return vecSize + 1;
        }

    private:
        // vecSize is change by keyVec
        decltype(NodeData::nd::hdr_t::count)& vecSize;
        // first key pointer
        child_t* const child;
        uint32_t maxSize = 0; // = ((PAGESIZE * 4096 - sizeof(NodeData::nd::hdr_t)) - sizeof(uint64_t)) / (KEYLEN + sizeof(uint64_t)) + 1;
    };

    /*
        @note class to manage key vector without dynamic memory allocate
    */
    //TODO finishing this class
    struct ROW_T {

        void* operator new(size_t sz) = delete;
        void* operator new[](size_t sz) = delete;

        dpfs_datatype_t type = dpfs_datatype_t::TYPE_NULL;
        // uint8_t m_len = KEYLEN;
        ROW_T(void* rowData, dpfs_datatype_t dtype) : type(dtype) {
            data = reinterpret_cast<decltype(data)>(rowData);
            rowhead = reinterpret_cast<decltype(rowhead)>(rowData);
        }
        ~ROW_T() {

        }

        struct {
            uint64_t pointerCol[2];
        } *rowhead;

        uint8_t* data = nullptr;

        // use data type to determine how to interpret the data
        // dpfs_datatype_t type = dpfs_datatype_t::TYPE_NULL;
        bool operator<(const ROW_T& other) const noexcept {
            return std::memcmp(data, other.data, ROWLEN) < 0;
        }


        bool operator==(const ROW_T& other) const noexcept {
            return std::memcmp(data, other.data, ROWLEN) == 0;
        }

    };

    class CRowVec {
    public:
        CRowVec(NodeData& nd, size_t rowLen) : vecSize(nd.nodeData->hdr->count) {
            maxKeySize = nd.maxKeySize;
            row = reinterpret_cast<uint8_t*>(nd.nodeData->data + sizeof(NodeData::nd::hdr_t) + (KEYLEN * maxKeySize));
        }
        ~CRowVec() = default;

        /*
            @param pos: position to insert
            @param data: data pointer
            @param dataLen: data length
            @return 0 on success, else on failure
            @note insert key to the vector in sorted order
        */
        int insert(int pos, const void* data, size_t dataLen) noexcept {
            int rc = 0;
            if(dataLen > ROWLEN) {
                // TODO:: use pointer to store data exceed row length
                // ROW_T rowPtr(row + pos * ROWLEN);
                rc = -ENOBUFS;
                return rc;
            }

            memmove(&row[(pos + 1) * ROWLEN], &row[pos * ROWLEN], (vecSize - pos) * ROWLEN);
            std::memcpy(&row[pos * ROWLEN], data, dataLen);
            return 0;
        }

        int push_back(const void* data, size_t dataLen) noexcept {
            int rc = 0;
            if(dataLen > ROWLEN) {
                return -ENOBUFS;
            }
            std::memcpy(&row[vecSize * ROWLEN], data, dataLen);
            return 0;
        }

        int pop_back() noexcept {
            if (vecSize == 0) {
                return -ENOENT;
            }
            memset(&row[(vecSize - 1) * ROWLEN], 0, ROWLEN);
            return 0;
        }

        int concate_back(const CRowVec& fromVec) noexcept {
            if (vecSize + fromVec.vecSize > maxKeySize) {
                return -ERANGE;
            }
            std::memcpy(&row[vecSize * ROWLEN], fromVec.row, fromVec.vecSize * ROWLEN);
            return 0;
        }

        int push_front(const void* data, size_t dataLen) noexcept {
            int rc = 0;
            if(dataLen > ROWLEN) {
                return -ENOBUFS;
            }
            memmove(&row[ROWLEN * 1], &row[0], vecSize * ROWLEN);
            std::memcpy(&row[0], data, dataLen);
            return 0;
        }

        int pop_front() noexcept {
            if (vecSize == 0) {
                return -ENOENT;
            }
            memmove(&row[0], &row[ROWLEN * 1], (vecSize - 1) * ROWLEN);
            memset(&row[(vecSize - 1) * ROWLEN], 0, ROWLEN);
            return 0;
        }

        int concate_front(const CRowVec& fromVec) noexcept {
            if (vecSize + fromVec.vecSize > maxKeySize) {
                return -ERANGE;
            }
            memmove(&row[fromVec.vecSize * ROWLEN], &row[0], vecSize * ROWLEN);
            std::memcpy(&row[0], fromVec.row, fromVec.vecSize * ROWLEN);
            return 0;
        }

        int assign(const uint8_t* begin, const uint8_t* end) noexcept {
            size_t newSize = static_cast<size_t>((end - begin) / ROWLEN);
            if (newSize > maxKeySize) {
                return -ERANGE;
            }
            std::memcpy(row, begin, newSize * ROWLEN);
            return 0;
        }

        /*
            @param pos position of the key
            @return 0 on success, else on failure.
            @note remove the row data that relate the key
        */
        int erase(const uint64_t& pos) noexcept {
            if (pos >= vecSize) {
                return -ENOENT;
            }
            memmove(&row[pos * ROWLEN], &row[(pos + 1) * ROWLEN], (vecSize - pos - 1) * ROWLEN);
            return 0;
        }

        // must be used before erase the key
        int erase(const uint64_t& begin, const uint64_t& end) noexcept {
            if(end < begin) {
                return -EINVAL;
            }
            if (begin >= vecSize) {
                return -ENOENT;
            }
            
            memmove(&row[begin * ROWLEN], &row[end * ROWLEN], (vecSize - end) * ROWLEN);
            return 0;
        }

        /*
            @param pos position of the key
            @param out the row data will be place here
            @param bufLen the buffer length of pointer out
            @param actureLen indicate acture length of the row
        */
        int at(uint32_t pos, uint8_t* out, uint32_t bufLen, uint32_t* actureLen) const noexcept {
            if (pos >= vecSize) {
                if (actureLen != nullptr) {
                    *actureLen = 0;
                }
                return -ERANGE;
            }
            // TODO:: RETURN ACTURE LEN of the row, may larger than ROWLEN(in-row data)
            if (actureLen != nullptr) {
                // TODO:: calculate acture length of the row
                *actureLen = ROWLEN;
            }
            if (bufLen < ROWLEN) {
                return -ENOMEM;
            }
            std::memcpy(out, &row[pos * ROWLEN], ROWLEN);
            return 0;
        }

        /*
            @param pos position of the key
            @param out the row data pointer will be place here
            @param bufLen the buffer length of pointer out
            @param actureLen indicate acture length of the row
        */
        int reference_at(uint32_t pos, uint8_t*& out, uint32_t* actureLen) const noexcept {
            if (pos >= vecSize) {
                if (actureLen != nullptr) {
                    *actureLen = 0;
                }
                return -ERANGE;
            }
            // TODO:: RETURN ACTURE LEN of the row, may larger than ROWLEN(in-row data)
            if (actureLen != nullptr) {
                // TODO:: calculate acture length of the row
                *actureLen = ROWLEN;
            }

            out = &row[pos * ROWLEN];
            return 0;
        }

        /*
            @return max size of the row vector
        */
        uint32_t msize() const noexcept {
            return maxKeySize;
        }

        uint8_t* data() {
            return row;
        }

    private:
        friend class NodeData;
        const decltype(NodeData::nodeData->hdr->count)& vecSize;
        // first key pointer
        uint8_t* row = nullptr;
        uint32_t maxKeySize = 0;
    };
    
    CBPlusTree(const CCollection& collection, CPage& pge, CDiskMan& cdm, size_t pageSize = 4);
    ~CBPlusTree();

    /*
        @param key key to insert
        @param row row buffer pointer
        @param len length of the input value
        @return 0 on success, else on failure
    */
    int insert(const KEY_T& key, const void* row, uint32_t len);

    /*
        @note split leaf node
        @param left: left node data
        @param right: right node data, this is the node to be created
        @param upKey: key to push up to parent
    */
    void split_leaf(NodeData& left, NodeData& right, KEY_T& upKey) {
        right.nodeData->hdr->leaf = true;
        right.self = allocate_node(true);
        uint32_t mid = left.keyVec->size() / 2;
        
        right.assign(left, static_cast<int>(mid), left.keyVec->size());
        left.erase(static_cast<int>(mid), left.keyVec->size());

        // adjust leaf links
        // [left] <--> [next]  =====> [left] <--> [right] <--> [next]
        right.nodeData->hdr->next = left.nodeData->hdr->next;
        right.nodeData->hdr->prev = left.self.bid;
        // set the next node's prev to right node
        if (right.nodeData->hdr->next) update_prev(right.nodeData->hdr->next, right.self.bid);

        // [left].next = [right]
        left.nodeData->hdr->next = right.self.bid;
        // set upkey the first key of right node
        upKey = (*right.keyVec)[0];
        // update parent of the right node
        // right.nodeData->hdr->parent = left.nodeData->hdr->parent;
        

    }

    /*
        @param idx: node index to split
        @param key: key to insert
        @param val: value pointer to insert
        @param valLen: length of the value
        @param upKey: key to push up to parent
        @param upChild: child node index to push up to parent
        @param isLeaf: indicate whether current node is leaf node
        @return 0 on success, SPLIT on split, else on failure
        @note insert the data or split internal node when necessary
    */
    int32_t insert_recursive(const bidx& idx, const KEY_T& key, const void* val, size_t valLen, KEY_T& upKey, bidx& upChild, bool isLeaf) {
        CBPlusTree::NodeData node(m_page);
        int32_t rc = load_node(idx, node, isLeaf);
        if (rc != 0) return rc;

        // leaf node founded, inster the data
        if (node.nodeData->hdr->leaf) {
            // TODO:: IMPLEMENT LOWER_BOUND METHOD FOR KEY COMPARISON
            // KEY_T* it = std::lower_bound(node.keys, node.keys + node.nodeData->hdr.count + 1, key);

            // int pos = node.keyVec->search(key); // check if key already exists

            rc = node.pCache->lock();
            if(rc != 0) {
                return rc;
            }

            rc = node.insertRow(key, val, valLen);
            if (rc) {
                node.pCache->unlock();
                return rc;
            }

            if (node.keyVec->size() < m_rowOrder) {
                node.pCache->unlock();
                store_node(node);
                return rc;
            }

            NodeData right(m_page);
            right.initNode(true, m_rowOrder);

            // upKey is used to return up key to parent
            split_leaf(node, right, upKey);
            node.pCache->unlock();
            // update upchild object, at outside it will be used to update parent node
            upChild = right.self;
            store_node(node);
            store_node(right);

            return SPLIT;
        }
        // if not leaf, go to child node
        
        // >= key => right child, < key => left child
        int32_t idxChild = child_index(node, key);
        // TODO CREATE NEW NODE?
        if(idxChild == -ERANGE) {
            idxChild = 0;
        }
        KEY_T childUp{};
        bidx childNew{nodeId, 0};



        rc = insert_recursive({nodeId, node.childVec->at(idxChild)}, key, val, valLen, childUp, childNew, node.nodeData->hdr->childIsLeaf);
        if (rc < 0) return rc;
        if (rc == SPLIT) {
            // child node is split, need insert up key and new child pointer to current node


            /*
            
          ／＞\\\\フ
         |    _  _l
        ／` ミ＿꒳ノ
       /         |
      /   ヽ     ﾉ
      │    |  |  |
  ／￣|    |  |  |
  | (￣ヽ＿_ヽ_)__)
  ＼二つ
                  [2,5,15,24,37]
                    |
                    [5,8,9,10,12] -> [5,8,9,10,12,14]

                    upkey = 10
                    upchild = [10,12,14]
                    oldchild = [5,8,9]

                    (UK)
                     |
                [2,5,10,15,24,37]
                    |  |
                    |  [10,12,14]->(upchild)
                    |
                [5,8,9] 
                                                                           
                [2,5,10,15,24,37] ====> [2,5,10]  [15,24,37](upchild) [15]->upkey

                           [15]
                        /       \
                       /         \        
                      /           \
                     /             \
             [2,5,10]               [15,24,37]
                 |  |               |  
                 |  |               |
                 |  [10,12,14]      null
                 [5,8,9]

            */
            
            // rc = node.pCache->lock();
            // if(rc != 0) {
            //     return rc;
            // }

            CTemplateGuard g(*node.pCache);

            // left child not change, only insert key and right child
            rc = node.insertChild(childUp, 0, childNew.bid);
            if (rc != 0) return rc;
            
            if(node.keyVec->size() < m_indexOrder) {
                 //node.pCache->unlock();
                return store_node(node);
            }
            // need split
            NodeData right(m_page);

            rc = split_internal(node, right, upKey);
            if (rc != 0) {
                //node.pCache->unlock();
                return rc;
            }
            
            //node.pCache->unlock();

            // use upChild to return new created right node index
            upChild = right.self;
            store_node(node);
            store_node(right);
            return SPLIT;
        }
        return store_node(node);
    }

    int split_internal(NodeData& left, NodeData& right, KEY_T& upKey) {

        int rc = 0;
        rc = right.initNode(false, m_indexOrder);
        if (rc != 0) {
            return rc;
        }
        int keymid = left.keyVec->size() / 2;
        upKey = (*left.keyVec)[keymid];
        rc = right.keyVec->assign(left.keyVec->keys + keymid + 1, left.keyVec->keys + left.keyVec->size());
        if (rc != 0) return rc;
        rc = right.childVec->assign(&left.childVec->at(keymid + 1), left.childVec->end());
        if (rc != 0) return rc;

        rc = left.erase(keymid, left.keyVec->size());
        if(rc != 0) {
            return rc;
        }

        // right.nodeData->hdr->parent = left.nodeData->hdr->parent;
        right.nodeData->hdr->childIsLeaf = left.nodeData->hdr->childIsLeaf;
        right.self = allocate_node(false);
        if(right.self.bid == 0) {
            return -ENOSPC;
        }
        return rc;

    }

    int32_t child_index(const NodeData& n, const KEY_T& k) const {
        int32_t pos = n.keyVec->search(k);
        //  10 22 23 25 28  (21)
        // 0  1  2  3  4  5
        // if k != 22
        // -> pos = 1 -> child = 1
        // if find k == 22
        // -> pos = 1 -> child = 2

        KEY_T* keyptr = nullptr;
        if((n.keyVec->at(pos, keyptr) == 0 && *keyptr == k)) {
            ++pos;
        }

        if(pos > n.keyVec->size()) {
            return -ERANGE;
        }

        return pos;
    }

    /*
        @note ensure root node exists
        @return 0 on success, else on failure
    */
    int ensure_root() {
        int rc = 0;
        // if root exists, return
        if (m_root.bid != 0) return 0;

        NodeData root(m_page);
        root.initNode(true, m_rowOrder);
        root.self = allocate_node(true);
        if (root.self.bid == 0) return -ENOSPC;
        root.nodeData->hdr->childIsLeaf = 0;

        rc = store_node(root);
        if (rc != 0) {
            m_diskman.bfree(root.self.bid, ROWPAGESIZE);
            return rc;
        }

        m_root = root.self;
        high = 1;
        return 0;
    }

    bidx allocate_node(bool isLeaf) {
        bidx id{0, 0};
        id.gid = nodeId;
        if(!isLeaf) {
            id.bid = m_diskman.balloc(PAGESIZE);
        } else {
            id.bid = m_diskman.balloc(ROWPAGESIZE);
        }
        return id;
    }

    int free_node(bidx lba, bool isLeaf) {
        if(!isLeaf) {
            return m_diskman.bfree(lba.bid, PAGESIZE);
        } else {
            return m_diskman.bfree(lba.bid, ROWPAGESIZE);
        }

        return 0;
    }

    /*
        @param key: key to search
        @param out: output value
        @param len: length of the output buffer
        @param actualLen: actual length of the output value
        @return 0 on success, nagetive on failure, positive on warning
        @note if return value > 0, it means output buffer is too small, data is truncated
    */
    int search(const KEY_T& key, void* out, uint32_t len, uint32_t* actualLen = nullptr);
    

    /*
        @param idx: node index to remove
        @param key: key to remove
        @param upKey: key to push up to parent
        @param upChild: child node index to push up to parent
        @param isLeaf: indicate whether current node is leaf node
        @return 0 on success, COMBINE on combine, else on failure
        @note remove the data or combine internal node when necessary
    */
    int32_t remove_recursive(const bidx& idx, const KEY_T& key, KEY_T& upKey, bidx& upChild, bool isLeaf);
    
    
    /*
        @param node: node data
        @param idxChild: index of the child that need to be combined
        @param childUp: key to push up to parent
        @param childNew: new child index if combine is happened
        @return 0 on success, else on failure
        @note combine child node at idxChild with its sibling, and node should be locked before call this function
    */
    int32_t combine_child(NodeData& node, int32_t idxChild, KEY_T& childUp, bidx& childNew);
    /*
        @param parent: parent node data
        @param fromNode: node to borrow from
        @param toNode: node to borrow to
        @param isLeaf: indicate whether current node is leaf node
        @param fromLeft: indicate whether borrow from left sibling
        @param targetIdx: index of the target child in parent node
        @return 0 on success, else on failure
        @note borrow one key from sibling node
    */
    int32_t borrowKey(NodeData& parent, NodeData& fromNode, NodeData& toNode, bool isLeaf, bool fromLeft, int targetIdx);
    int32_t mergeNode(NodeData& parent, NodeData& fromNode, NodeData& toNode, bool isLeaf, bool fromLeft, int targetIdx);
    /*
        @param key: key to update
        @param input: input value pointer
        @param len: length of the input value
        @return 0 on success, else on failure
    */
    int update(const KEY_T& key, const void* input, uint32_t len);
    
    /*
        @param key: key to remove
        @return 0 on success, else on failure
    */
    int remove(const KEY_T& key);

private:
    enum InsertResult { 
        OK = 0, 
        SPLIT = 1,      // split node
        COMBINE = 2,    // combine two nodes, or borrow from sibling
        LEFT = 3,       // remove the left of child, need update the index key
        EMPTY = 4,      // node is empty after delete
    };

    /*
        return the size of one index node in lba unit
    */
    size_t node_lba_len() const noexcept {
        return PAGESIZE;
    }

    union nodeHdr {
        nodeHdr(){}
        #pragma pack(push, 1)
        struct hdr_t {
            uint64_t parent = 0; // parent bid
            uint64_t prev = 0;   // leaf prev bid
            uint64_t next = 0;   // leaf next bid
            uint16_t count = 0;
            uint8_t leaf = true;
            uint8_t isConverted = 0;
            uint8_t childIsLeaf = 0;
            char reserve[3];
        } hdr;
        uint8_t data[PAGESIZE * dpfs_lba_size];
        #pragma pack(pop)
    };

    /*
        @param idx: node index to load
        @param out: output node data
        @return 0 on success, else on failure
        @note load node data from storage
    */
    // TODO:: update code finish loadnode and storenode functions...
    int load_node(const bidx& idx, NodeData& out, bool isLeaf) {

        auto it = m_commitCache.find(idx);
        if(it != m_commitCache.end()) {
            // pointer assignment as reference
            out = &it->second;
            return 0;
        }
        
        cacheStruct* p = nullptr;
        int rc = 0;
        rc = m_page.get(p, idx, isLeaf ? ROWPAGESIZE : PAGESIZE);

        if (rc != 0 || p == nullptr) return rc == 0 ? -EIO : rc;
        uint8_t* base = reinterpret_cast<uint8_t*>(p->getPtr());

        nodeHdr* nodedatahdr;
        nodedatahdr = (nodeHdr*)base;

        out.pCache = p;
        out.self = idx;

        rc = out.initNodeByLoad(isLeaf, isLeaf ? m_rowOrder : m_indexOrder, base);
        if(rc != 0) {
            out.pCache->release();
            return rc;
        }


        if(B_END) {
            // TODO convert keys and values to host endian if needed
            // storage is always little endian, if host is big endian, need convert

            /*
            rc = out.pCache->lock();
            if(rc != 0) {
                out.pCache->release();
                return rc;
            }


            if(out.nodeData->hdr->isConverted) {
                // already converted
                // do nothing
            } else {

                // write lock the cache for conversion
                rc = out.pCache->lock();
                if(rc != 0) {
                    out.pCache->release();
                    return rc;
                }
                if(!out.nodeData->hdr->isConverted) {
                    cpyFromleTp(out.nodeData->hdr->parent, out.nodeData->hdr->parent);
                    cpyFromleTp(out.nodeData->hdr->prev, out.nodeData->hdr->prev);
                    cpyFromleTp(out.nodeData->hdr->next, out.nodeData->hdr->next);
                    cpyFromleTp(out.nodeData->hdr->count, out.nodeData->hdr->count);
                    cpyFromleTp(out.nodeData->hdr->leaf, out.nodeData->hdr->leaf);
                    // convert keys
                    // if(isNumberType(this->keyType)) {
                    //     // convert keys to host endian if needed
                    //     for(size_t i = 0; i < out.nodeData->hdr->count; ++i) {
                    //         cpyFromleTp(out.keys[i], out.keys[i]);
                    //     }
                    // }
                    if(out.nodeData->hdr->leaf) {
                        // TODO convert the values to host endian if needed
                    } else {
                        // TODO convert children to host endian if needed
                        // for(size_t i = 0; i < out.nodeData->hdr->count + 1; ++i) {
                        //     cpyFromleTp(out.children[i], out.children[i]);
                        // }

                    }
                    out.nodeData->hdr->isConverted = 1;
                }
                out.pCache->unlock();

            }
            */
        }

        return 0;
    }

    /*
        @param node: node data to store
        @return 0 on success, else on failure
        @note store node data to storage
    */
    int store_node(NodeData& node) {
        // save the node that to be stored, until commit is called ?
        // if node is reference, no need to store
        if(node.isRef) return 0;

        int rc = 0;
        if(!node.pCache) {
            rc = m_page.put(node.self, node.nodeData->data, nullptr, node.nodeData->hdr->leaf ? ROWPAGESIZE : PAGESIZE, false, &node.pCache);
            // fetch the cache struct
            if(rc != 0) return rc;
        }
        
        m_commitCache.emplace(node.self, std::move(node));
        return 0;
    }

    int commit() {
        int lastIndicator = 0;
        int rc = 0;

        size_t sz = m_commitCache.size();
        for(auto& node : m_commitCache) {
            if(B_END) {
                // TODO convert back to little endian if needed

                // before storing, convert back to little endian
                node.second.nodeData->hdr->isConverted = 0;
            }

            if(sz <= 1) {            
                rc = m_page.writeBack(node.second.pCache, &lastIndicator);
            } else {
                rc = m_page.writeBack(node.second.pCache, nullptr);
            }
            if(rc != 0) {
                return rc;
            }
            --sz;
        }

        while(lastIndicator != 1) {
            // wait for last write back complete
        }
        m_commitCache.clear();
        return 0;
    }

    /*
        change the child node's parent link
        @param child: child node index
        @param parentBid: new parent bid
    */
    // void update_parent(const bidx& child, uint64_t parentBid) {
    //     NodeData n(m_page);
    //     if (child.bid == 0) return;
    //     if (load_node(child, n, false) == 0) {
    //         n.nodeData->hdr->parent = parentBid;
    //         store_node(n);
    //     }
    // }

    void printTree();

    void printTreeRecursive(const bidx& idx, bool isLeaf, int level);


    void update_prev(uint64_t bid, uint64_t newPrev) {
        NodeData n(m_page);
        bidx idx{nodeId, bid};
        if (load_node(idx, n, true) == 0) {
            n.nodeData->hdr->prev = newPrev;
            store_node(n);
        }
    }


private:
    bidx m_root;
    size_t m_indexOrder = 0;
    size_t m_rowOrder = 0;

    uint32_t keyOffset = 0;
    uint32_t childOffset = 0;
    uint32_t rowOffset = 0;

    size_t m_nodeSize = 0; // page size in bytes
    size_t m_pageSize = 0; // requested page blocks per fetch
    CPage& m_page;
    CDiskMan& m_diskman;
    const CCollection& m_collection;
    uint8_t keyLen = 0;
    dpfs_datatype_t keyType = dpfs_datatype_t::TYPE_NULL;
    // indicate the col maybe variable-length type
    bool m_colVariable = false;
    uint32_t m_rowLen = 0;
    // if high == 1, root is leaf node
    uint32_t high = 0;
    std::unordered_map<bidx, NodeData> m_commitCache;
    CSpin m_lock;


};
