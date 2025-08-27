#include <storage/engine.hpp>
/*
    manage dpfs engine space
*/
class CDiskMan {
public:
    CDiskMan() = delete;
    CDiskMan(dpfsEngine* engine) : eng(engine) {};
    ~CDiskMan() = default;

    /*
        @param lba_count: number of logic blocks to allocate
        @return starting lba of the allocated space on success, else 0
        @note allocate a continuous space of lba_count logic blocks and return the starting lba of the allocated space
    */
    size_t balloc(size_t lba_count) noexcept;

    /*
        @param lba_start: starting lba of the space to free
        @param lba_count: number of logic blocks to free
        @return 0 on success, else on failure
        @note free a continuous space of lba_count logic blocks starting from lba_start
    */
    int bfree(size_t lba_start, size_t lba_count) noexcept;

    /*
        @return 0 on success, else on failure
        @note init a new disk or load disk space information from the engine
    */
    int init();

    dpfsEngine* eng;
    
};