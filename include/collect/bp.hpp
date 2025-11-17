// b+ tree for product index
#include <vector>
#include <threadlock.hpp>
#include <basic/dpfsconst.hpp>
#include <collect/page.hpp>
#include <collect/diskman.hpp>
#include <collect/collect.hpp>  

/*

{ONE BLOCK FOR LEAF ONDE}
|front block(8B)|value|value|...|value|next block(8B)|

{ONE BLOCK FOR INDEX NODE}
|child block(8B)|KEY_T(sizeof(KEY_T))|child block(8B)|key|...|child block(8B)|key|child block(8B)|

*/

// template<class KEY_T = int, class VALUE_T = int>

class CBptNode {
public:
    CBptNode(bool isLeaf, size_t order) : m_isLeaf(isLeaf), m_order(order) {}
    ~CBptNode() {}
    bool m_isLeaf;
private:
    size_t m_order = 0;
    
    // std::vector<void*> m_keys;
    // std::vector<void*> m_values;
    // std::vector<CBptNode*> m_children;

};

// class defaultCmpFunc {
// public:
//     // less than
//     bool operator()(int a, int b) {
//         return a < b;
//     }
// };

// template<class KEY_T = size_t, class VALUE_T = void*, class COMPARE_FUNC = defaultCmpFunc>
class CBPlusTree {
public:
    using KEY_T = size_t;
    using VALUE_T = void*;
    /*
        @param m: order of the b+ tree
        @param pge: page manager
        @param cdm: disk block manager
        @note page size will determine the node size of the b+ tree
    */
    CBPlusTree(const CCollection& collection, CPage& pge, CDiskMan& cdm, size_t m = 256, size_t pageSize = 4) : 
    m_order(m), m_pageSize(pageSize), 
    m_page(pge), m_diskman(cdm), 
    m_collection(collection) {
        // caculate node size and determine node Size
        // key size + child pointer size with extra child size for index node
        // index node size (by lba size)
        m_nodeSize = (m_order * (sizeof(KEY_T) + sizeof(size_t)) + sizeof(size_t));
        if(m_nodeSize % dpfs_lba_size != 0) {
            // m_nodeSize += (dpfs_lba_size - (m_nodeSize % dpfs_lba_size));
            m_nodeSize = m_nodeSize / dpfs_lba_size + 1;
        } else {
            m_nodeSize = m_nodeSize / dpfs_lba_size;
        }
    }

    ~CBPlusTree() {

    }

    /*
        @param key: key to insert
        @param value: value to insert
        @return 0 on success, else on failure
        @note insert key-value pair into the b+ tree
    */
    int insert(const KEY_T& key, const VALUE_T& value) {
        int rc = 0;
        cacheStruct* p = nullptr;
        // empty tree
        if(m_rootb.gid == 0 && m_rootb.bid == 0) {
            // create root node
            // allocate disk block for root node
            m_rootb.bid = m_diskman.balloc(m_pageSize);
            if(m_rootb.bid == 0) {
                rc = -ENOSPC;
                goto errReturn;
            }
            
            // init root node

            // get the memory block for root node
            rc = m_page.get(p, m_rootb, m_pageSize);
            if(rc != 0) {
                goto errReturn;
            }

            rc = p->lock();
            if(rc != 0) {
                goto errReturn;
            }

            void* ptr = p->getPtr();
            size_t psz = p->getLen();

            
            
            // update m_rootb
        }

        
        

        // rc = search(key, value);
        return 0;
errReturn:

        return rc;
    }

    int remove(const KEY_T& key);

    int search(const KEY_T& key, VALUE_T& value);

    int update(const KEY_T& key, const VALUE_T& value);


    

private:
    // b+ tree head node in disk
    bidx m_rootb = {0, 0};
    // root node pointer
    void* m_root = nullptr;
    // order of the b+ tree
    size_t m_order = 0;
    // unit of alloced block size
    size_t m_nodeSize = 0;
    size_t m_pageSize = 0;

    // page manager
    CPage& m_page;
    // disk block manager
    CDiskMan& m_diskman;
    // the collection that owned the b+ tree
    const CCollection& m_collection;

    CSpin m_lock;
};
