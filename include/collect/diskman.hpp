#pragma once
#include <collect/page.hpp>
#include <collect/Cbt.hpp>
#include <vector>
#include <list>

// diskman debug flag
#define __DMDEBUG__

#ifdef __DMDEBUG__
static CSpin dm_glock;
static size_t current_pos = 200;
#endif

/*
    manage dpfs engine space
*/
extern uint64_t nodeId;
extern const uint64_t tmpBlockLbaLen;
extern const uint64_t tmpBlockLen;

class CDiskMan {
public:
    CDiskMan() = delete;
    CDiskMan(CPage* pge) : m_page(pge), m_cbt(pge){};
    ~CDiskMan() = default;

    /*
        @param lba_count: number of logic blocks to allocate
        @return starting lba of the allocated space on success, else 0
        @note allocate a continuous space of lba_count logic blocks and return the starting lba of the allocated space
    */
    size_t balloc(size_t lba_count) {
#ifdef __DMDEBUG__
        dm_glock.lock();
        size_t ret = current_pos;
        current_pos += lba_count;
        dm_glock.unlock();
        return ret;
#endif
        return m_cbt.Get(lba_count);
    }

    /*
        @param lba_start: starting lba of the space to free
        @param lba_count: number of logic blocks to free
        @return 0 on success, else on failure
        @note free a continuous space of lba_count logic blocks starting from lba_start
    */
    int bfree(size_t lba_start, size_t lba_count) {
#ifdef __DMDEBUG__
        return 0;
#endif
        m_cbt.Put(lba_start, lba_count);
        return 0;
    }

    /*
        @return 0 on success, else on failure
        @note init a new disk or load disk space information from the engine
    */
    int load()
    {
        m_cbt.Load();
        return 0;
    }

    int init(uint64_t gid, int64_t count)
    {
        m_cbt.Init(gid, count);
        return 0;
    }

     int addBidx(uint64_t gid, int64_t count)
    {
        m_cbt.AddBidx(gid, count);
        return 0;
    }

    void print() {
        m_cbt.Print();
    }
    CPage* m_page;
    Cbt m_cbt;

};

// const size_t temp_storage_step_len = 10;

/*
    temp storage use if data too long
    TODO::
    1. write a size() function.
    2. write iterator to iterate the data in temp storage

*/
class CTempStorage {
    public:
    CTempStorage() = delete;

    CTempStorage(CPage& pge, CDiskMan& dskman) : m_page(pge), m_diskman(dskman) {

    };
    ~CTempStorage() {
        for(auto& dbi : m_dataBlockList) {
            m_diskman.bfree(dbi.blkPos.bid, dbi.lba_len);
        }
    };

    /*
        @param data: pointer to the data to be stored
        @param len: length of the data to be stored (unit is lba size, default 4096B)
        @return 0 on success, else on failure
        @note store data into temp storage
    */
    int pushBackData(const void* data, size_t lba_len);

    /*
        @param pos: position of the data to get (unit is lba size, default 4096B)
        @param data: pointer to the buffer to store the data
        @param lba_len: length of the data to get (unit is lba size, default 4096B)
        @param real_lba_len: indicaete actual length of data readed
        @return 0 on success, else on failure
        @note get data from temp storage
    */
    int getData(size_t pos, void* data, size_t lba_len, size_t& real_lba_len);

    /*
        @param pos: position of the data to update (unit is lba size, default 4096B)
        @param data: pointer to the data to update
        @param len: length of the data to update (unit is lba size, default 4096B)
        @return number of blocks updated on success, else on failure
        @note update data in temp storage
    */
    int updateData(size_t pos, void* data, size_t lba_len);
    
    /*
        @param data: pointer to buffer to store the last saved data
        @param len: storage max block length of last saved data block
        @return 0 on success, else on failure
        @note read from last saved data
    */
    int back(void* data, size_t& len);

    /*
        @return 0 on success, else on failure
        @note release all temp storage space
    */
    int clear();

    /*
        @return size of the temp storage (unit is lba size, default 4096B)
        @note get total size of the temp storage
    */
    size_t size() noexcept {
        if(m_dataBlockList.size() == 0) {
            return 0;
        }
        return m_dataBlockList.size() > 1 ? 
        (m_dataBlockList.size() - 1) * tmpBlockLbaLen + m_dataBlockList.back().lba_len :
        m_dataBlockList[0].lba_len;
    }
    

    private:
    CPage& m_page;
    CDiskMan& m_diskman;
    
    // storage tmp data block info
    struct DataBlockInfo {

        
        DataBlockInfo() noexcept {
            
        };
        
        DataBlockInfo(DataBlockInfo&& other) noexcept {
            offset = other.offset;
            lba_len = other.lba_len;
            blkPos = other.blkPos;
        }
        ~DataBlockInfo() noexcept {

        }
        // position in the temp storage
        size_t offset = 0; 
        // length of the data block
        size_t lba_len = 0;
        bidx blkPos {0, 0};
    };

    std::vector<DataBlockInfo> m_dataBlockList;



    /*
        [0] -> {
                    OFFSET: 0|
                    LEN: 3| 
                    LIST: {|BLK0|BLK1|BLK2|} 
                }
        [1] -> {
                    OFFSET: 3|
                    END: 3| 
                    LIST: {|BLK3|BLK4|BLK5|} 
                }
        ... -> ...

    */
 
};




/*
deprecated

// iterator class
public:
    // the iterator for temp storage is read only
    class iterator {
    public:
        iterator(CTempStorage* tst) : m_tst(tst) {

        }

        iterator(const iterator& other) :  m_tst(other.m_tst) {
            m_pos = other.m_pos;
        }

        iterator& operator=(const iterator& other) {
            if (this != &other) {
                m_tst = other.m_tst;
                m_pos = other.m_pos;
            }
            return *this;
        }

        iterator& operator++() {
            
            
            return *this;
        }

        bool operator!=(const iterator& other) const {
            return (m_pos != other.m_pos);
        }

        iterator& operator*() noexcept {
            
            return *this;
        }

        struct diskIterDataStruct { 
            void* dataPtr = nullptr; 
            size_t len = 0; 
        };

        const diskIterDataStruct& operator[](size_t index) noexcept {
            int rc = 0;
            if(!val.dataPtr) {
                val.dataPtr = malloc(tmpBlockLbaLen * dpfs_lba_size);
                if(!val.dataPtr) {
                    val.len = 0;
                    return val;
                }
            }
            size_t real_len = 0;
            m_tst->getData(m_pos, &val.dataPtr, val.len, real_len);
            return {val.dataPtr, real_len};
        }

    private:
        friend class CTempStorage;
        CTempStorage* m_tst; 
        size_t m_pos = 0;
        diskIterDataStruct val;
    };



    const iterator& begin() const noexcept {
        return beginIter;
    }

    const iterator& end() const noexcept {
        return endIter;
    }

private:
    iterator beginIter;
    iterator endIter;

*/