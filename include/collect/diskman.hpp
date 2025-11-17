#include <collect/page.hpp>
#include <collect/Cbt.hpp>
/*
    manage dpfs engine space
*/
extern uint64_t nodeId;
class CDiskMan {
public:
    CDiskMan() = delete;
    CDiskMan(CPage* pge) : m_page(pge) {};
    ~CDiskMan() = default;

    /*
        @param lba_count: number of logic blocks to allocate
        @return starting lba of the allocated space on success, else 0
        @note allocate a continuous space of lba_count logic blocks and return the starting lba of the allocated space
    */
    size_t balloc(size_t lba_count) {
        return m_cbt.Get(lba_count);
    }

    /*
        @param lba_start: starting lba of the space to free
        @param lba_count: number of logic blocks to free
        @return 0 on success, else on failure
        @note free a continuous space of lba_count logic blocks starting from lba_start
    */
    int bfree(size_t lba_start, size_t lba_count) {
        m_cbt.Put(lba_start, lba_count);
        return 0;
    }

    /*
        @return 0 on success, else on failure
        @note init a new disk or load disk space information from the engine
    */
    int init();
    Cbt m_cbt;
    CPage* m_page;
    
};

/*
    temp storage use if data too long
*/
class CTempStorage {
    public:
    CTempStorage() = delete;
    CTempStorage(CPage& pge, CDiskMan& dskman) : m_page(pge), m_diskman(dskman) {

    };
    ~CTempStorage() = default;

    int pushBackData(void* data, size_t len) {
        // TODO
        return 0;
    }

    int getData(size_t pos, void* data, size_t len) {
        // TODO
        return 0;
    }

    private:
    CPage& m_page;
    CDiskMan& m_diskman;

    
    /*
        [0] -> {
                    BEGIN: 0|
                    END: 2| 
                    LIST: {|BLK0|BLK1|BLK2|} 
                }
        [1] -> {
                    BEGIN: 3|
                    END: 5| 
                    LIST: {|BLK3|BLK4|BLK5|} 
                }
        ... -> ...

    */
 
};