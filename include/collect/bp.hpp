// b+ tree for product index
#include <vector>
#include <threadlock.hpp>
#include <basic/dpfsconst.hpp>
#include <collect/page.hpp>
#include <collect/diskman.hpp>


/*

{ONE BLOCK FOR LEAF ONDE}
|front block(8B)|next block(8B)|value|value|...|value|

{ONE BLOCK FOR INDEX NODE}
|child block(8B)|key|child block(8B)|key|...|child block(8B)|key|cgild block(8B)|

*/

template<class KEY_T = int, class VALUE_T = int>
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

template<class KEY_T = int, class VALUE_T = int>
class CBPlusTree {
public:

    /*
        @param m: order of the b+ tree
        @param pge: page manager
        @param cdm: disk block manager
        @note page size will determine the node size of the b+ tree
    */
    CBPlusTree(size_t m, CPage& pge, CDiskMan& cdm) : m_order(m), m_page(pge), m_diskman(cdm) {
        // caculate node size and ditermine node Size
        m_nodeSize = m_order * (sizeof(KEY_T) + sizeof(VALUE_T)) + sizeof(bool) + sizeof(size_t);
        
    }

    ~CBPlusTree() {

    }

    /*
        @param key: key to insert
        @param value: value to insert
        @return 0 on success, else on failure
        @note insert key-value pair into the b+ tree
    */
    int insert(const KEY& key, const VALUE& value) {

        return 0;
    }

    int remove(const KEY& key);

    int search(const KEY& key, VALUE& value);

    int update(const KEY& key, const VALUE& value);

private:
    // b+ tree head node in disk
    bidx m_bidx = {0, 0};
    // root node pointer
    void* m_root = nullptr;
    // order of the b+ tree
    size_t m_order = 0;
    // unit of alloced block size
    size_t m_nodeSize = 0;

    // page manager
    CPage& m_page;
    // disk block manager
    CDiskMan& m_diskman;

    CSpin m_lock;
};
