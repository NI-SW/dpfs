/*
    define cache and storage exchange method for collect.
*/
#pragma once
#include <basic/dpfscache.hpp>
#include <storage/engine.hpp>
#include <log/logbinary.h>
#include <shared_mutex>
#include <condition_variable>
extern const size_t maxBlockLen;
extern const size_t maxMemBlockLimit;

class CPage;
class PageClrFn;


// block index
struct bidx {
    bidx() : gid(0), bid(0) {}
    bidx(uint64_t g, uint64_t b) : gid(g), bid(b) {}
    bidx(const bidx& b) { gid = b.gid; bid = b.bid; }
    bidx(bidx&& b) { gid = b.gid; bid = b.bid; }
    
    // disk group id
    uint64_t gid;
    // block id in the disk group
    uint64_t bid;
    bool operator==(const bidx& target) const noexcept {
        return (target.gid == gid && target.bid == bid);
    }
    
    bool operator!=(const bidx& target) const noexcept {
        return !(*this == target);
    }

    bidx& operator=(const bidx& other) noexcept {
        gid = other.gid;
        bid = other.bid;
        return *this;
    }
};

namespace std {
    // hash func of bidx
    template<>
    struct hash<bidx> {
        size_t operator()(const bidx& s) const noexcept {
            return std::hash<uint64_t>{}(s.gid) ^ std::hash<uint64_t>{}(s.bid);
        }
    };
}



struct cacheStruct {
    cacheStruct(CPage* cp);
private:
    friend class CPage;
    friend class PageClrFn;
    // friend void page_get_cb(void* arg, const dpfs_compeletion* dcp);
    // 16B
    bidx idx = {0, 0};
    // 8B pointer to dpfsEngine::zmalloc() memory, for example if use nvmf, this pointer point to dma memory
    void* zptr = nullptr;
    // 1B
    CSpin csLock;
    // 2B
    std::atomic<uint16_t> readRefs = 0;
    
    // 3.875B block number, number of blocks, not len of bytes, one block equal to <dpfs_lba_size> bytes usually 4KB
    uint32_t len : 31;
    // 0.125B
    uint32_t dirty : 1;

    // 2B
    std::atomic<uint16_t> status = VALID;
    // 2B
    std::atomic<uint16_t> refs = 0;
    // 8B
    CPage* page = nullptr;

public:

    /*
        @note return pointer to the data block
    */
    void* getPtr() noexcept {
        return zptr;
    }

    /*
        @note return lba length of the cache block
    */
    uint32_t getLen() noexcept {
        return len;
    }

    enum statusEnum : uint16_t {
        VALID = 1000,              // valid data in zptr
        WRITING = 1001,            // writing to disk, zptr not change
        READING = 1002,            // reading from disk, zptr may change
        INVALID = 1003,            // cache invalid
        ERROR = 1004,              // error occur when read or write
    };

    static const std::vector<std::string> statusEnumStr;

    inline const std::atomic<uint16_t>& getStatus() const noexcept {
        return status;
    }

    inline void setStatus(uint16_t st) noexcept {
        status = st;
    }
    
    // after use, you must call release to recycle the memory
    void release();
    
    // if need change zptr, you must lock first
    /*
        @note this func will make sure status is VALID, and change memory status to dirty
        @return 0 on success, else on failure
    */
    int lock() noexcept {
        begin:
        while(status != VALID || readRefs > 0) {
            switch(status) {
                case INVALID:
                case ERROR:
                    return -status;
                default:
                    break;
            }
        }
        csLock.lock();
        if(status != VALID || readRefs > 0) {
            csLock.unlock();
            goto begin;
        }

        // always consider change zptr, set dirty flag
        dirty = true;

        return 0;
    }

    /*
        @note this func will make sure status is VALID or WRITING
        @return 0 on success, else on failure
    */
    int read_lock() noexcept {
        begin:
        while(status != VALID && status != WRITING) {
            switch(status) {
                case INVALID:
                case ERROR:
                    return -status;
                default:
                    break;
            }
        }
        csLock.lock();
        if(status != VALID && status != WRITING) {
            csLock.unlock();
            goto begin;
        }
        ++readRefs;
        csLock.unlock();

        return 0;
    }

    /*
        @return true if lock success, else false
    */
    bool try_lock() noexcept {
        if(status != VALID || readRefs > 0) {
            return false;
        }
        if(!csLock.try_lock()) {
            return false;
        }
        if(status != VALID || readRefs > 0) {
            csLock.unlock();
            return false;
        }
        return true;
    }

    void unlock() noexcept {
        csLock.unlock();
        return;
    }

    void read_unlock() noexcept {
        --readRefs;
        return;
    }

};

/*
    Functor, called by template class CDpfsCache, func: write back cache to storage.
*/
class PageClrFn {
public:
    // PageClrFn(void* clrFnArg) : engine_list(*static_cast<std::vector<dpfsEngine*>*>(clrFnArg)) {}
    PageClrFn(void* clrFnArg);
    ~PageClrFn();
    void clearCache(cacheStruct*& p, int* finish_indicator = nullptr);
    void operator()(cacheStruct*& p, int* finish_indicator = nullptr);
    // void flush(std::list<CDpfsCache<bidx, cacheStruct*, PageClrFn>::cacheIter*> p);
    void flush(const std::list<void*>& cacheList);
    CPage* cp;
    std::thread clfThd;
    bool m_exit = false;
    struct pageClrSt {
        pageClrSt() : pCache(nullptr), finish_indicator(nullptr) {}
        pageClrSt(cacheStruct* p, int* fi) : pCache(p), finish_indicator(fi) {}
        cacheStruct* pCache = nullptr;
        int* finish_indicator = nullptr;
    };
    std::queue<pageClrSt> m_flushQueue;
    std::condition_variable m_convar;
    CSpin m_queueMutex;
};


// struct cbvar {
//     cacheStruct* cptr = nullptr;
//     dpfs_engine_cb_struct* cbs = nullptr;
//     CPage* page = nullptr;
// };

/*
    from all dpfs engine, make cache
*/
class CPage {
public:

    /*
        @param engine_list storage engine.
        @param cacheSize cache pool size
        @param log used to out put info
        @param maxCacheSizeInByte max cache size in byte, default is unlimited(0)
    */
    CPage(std::vector<dpfsEngine*>& engine_list, size_t cacheSize, logrecord& log, size_t maxCacheSizeInByte = 0);
    ~CPage();

    /*
        @param ptr this func is an async func, when ptr is not null, the callback is done
        @param idx index of the block
        @param len number of blocks, 1 block is 4096B
        @return 0 on success, else on failure
        @note unless there is a large object that need be storaged by multiple blocks, len should be set to 1
    */
    int get(cacheStruct*& ptr, const bidx& idx, size_t len = 1);

    /*
        @param idx the storage index, indicate which disk group and disk block
        @param zptr the pointer to the data that will be insert into cache system or write to the storage
        @param finish_indicator pointer to an integer, if not null, when write back is finished, *finish_indicator will be set to 1 if success, set to -1 if error occur
        @param len length of the data blocks, 1 block is 4096B
        @param wb  true if need to write back to disk immediate
        @param pCache if not null, will return the cacheStruct pointer of the stored block
        @return 0 on success, else on failure
        @note flush data from zptr to cache, bewarn zptr will be take over by CPage
    */
    int put(const bidx& idx, void* zptr, int* finish_indicator = nullptr, size_t len = 1, bool wb = false, cacheStruct** pCache = nullptr);

    /*
        @param cache the block that you want to write back to disk
        @param finish_indicator pointer to an integer, if not null, when write back is finished, *finish_indicator will be set to 1 if success, set to -1 if error occur
        @return 0 on success, else on failure
    */
    int writeBack(cacheStruct* cache, int* finish_indicator = nullptr);

    /*
        @note fresh the cache block to the first of lru system
        @param cache the block that you want to fresh
        @return 0 on success, EEXIST if cache pointer is change, else on failure
    */
    int fresh(cacheStruct*& cache);

    /*
        @return 0 on success, else on failure
        @note flush data from cache to disk immediate
    */
    int flush();

    /*
        @param sz size of cache block for storage, 1 block equal to dpfs_lba_size (default is 4096B)
        @return return zptr if success, nullptr on failure
    */
    void* cacheMalloc(size_t sz) = delete;
    
    /*
        @note free zptr memory(alloc by dpfsEngine::zmalloc)
        @param zptr pointer to memory
        @param sz size of memory(unit is block, 1 block = dpfs_lba_size bytes usually 4096B)
    */
    void freezptr(void* zptr, size_t sz);

    /*
        @note alloc zptr memory(from dpfsEngine::zmalloc)        
        @param sz size of cache block for storage, 1 block equal to dpfs_lba_size (default is 4096B)
        @return return zptr if success, nullptr on failure
    */
    void* alloczptr(size_t sz);

    // 4B + 4B
    uint32_t m_hitCount;
    uint32_t m_getCount;

private:
    friend PageClrFn;
    friend cacheStruct;

    /*
        @note free cache struct memory
    */
    void freecs(cacheStruct* cs);
    /*
        @note alloc cache struct memory
    */
    cacheStruct* alloccs();
    void freecbs(dpfs_engine_cb_struct* cbs);
    dpfs_engine_cb_struct* alloccbs();
    

    // 24B + 3B
    std::vector<CSpin> m_zptrLock;
    CSpin m_csmLock;
    CSpin m_cbmLock;
    bool m_exit;

    // 8B (reference)
    logrecord& m_log;

    // 8B (reference)
    std::vector<dpfsEngine*>& m_engine_list;

    // 24B
    // map length to ptr, alloc by dpfsEngine::zmalloc length = (dpfs_lba_size * index)
    std::vector<std::queue<void*>> m_zptrList;

    // storage cacheStruct malloced by get and put
    // 80B + 80B
    std::queue<cacheStruct*> m_cacheStructMemList;
    // std::list<cacheStruct*> m_cacheStructMemList;
    std::queue<dpfs_engine_cb_struct*> m_cbMemList;
    // std::list<dpfs_engine_cb_struct*> m_cbMemList;


    // 104B
    // search data by disk group id and disk block id, deconstruct first, or may cause crash(PageClrFn use some func of CPage)
    // make sure m_cache is destructed first when destruct CPage
    CDpfsCache<bidx, cacheStruct*, PageClrFn> m_cache;

    atomic<size_t> m_currentSizeInByte = 0;
    size_t m_maxSizeInByte = 0;

    CSpin m_cacheLock;
};

class cacheLocker {
public:
    cacheLocker(cacheStruct*& cs, CPage& pge) : m_cs(cs), m_page(pge) {
        
    }

    virtual ~cacheLocker() {

    }

    /*
        @note lock the cache, if cache pointer is change, reload the cache and try to lock again, if cache is not valid, return error
        @return 0 on success, else on failure
    */
    int lock() {
        int rc = 0;
        rc = m_cs->lock();
        if (rc == 0) {
            return 0;
        }

        // lock fail, try to reload cache
        rc = m_page.fresh(m_cs);
        if (rc == EEXIST) {
            // if cache pointer is change
            isChange = true;
            reinitStruct();
            return m_cs->lock();
        } else if (rc != 0) {
            return rc;
        }

        return m_cs->lock();
    }

    /*
        @note lock the cache, if cache pointer is change, reload the cache and try to lock again, if cache is not valid, return error
        @return 0 on success, else on failure
    */
    int read_lock() {
        int rc = 0;
        rc = m_cs->read_lock();
        if (rc == 0) {
            return 0;
        }

        // lock fail, try to reload cache
        rc = m_page.fresh(m_cs);
        if (rc == EEXIST) {
            // if cache pointer is change
            isChange = true;
            reinitStruct();
            return m_cs->read_lock();
        } else if (rc != 0) {
            return rc;
        }

        return m_cs->read_lock();
    }

    void unlock() {
        m_cs->unlock();
    }

    void read_unlock() {
        m_cs->read_unlock();
    }

    bool isChanged() const noexcept {
        return isChange;
    }

    /*
        @note if pointer is change, this function will be called, if your struct use the pointer, reconstruct the struct.
    */
    virtual int reinitStruct() {
        return 0;
    }

    void* getPtr() noexcept {
        return m_cs->getPtr();
    }

    private:
    bool isChange = false;
    // cache struct pointer
    cacheStruct*& m_cs;
    CPage& m_page;

};

// void testSize(){
//     sizeof(CDpfsCache<bidx, cacheStruct*, PageClrFn>);
//     sizeof(CPage);
//     sizeof(std::list<cacheStruct*>);
//     sizeof(std::deque<cacheStruct*>);
//     sizeof(CSpin);
//     sizeof(std::shared_mutex);
//     sizeof(std::atomic<uint16_t>);
//     sizeof(cacheStruct);
//     sizeof(std::queue<cacheStruct*>);
//     sizeof(std::vector<CSpin>);
// }