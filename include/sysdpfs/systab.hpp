#include <collect/collect.hpp>


class CSysTables {
public:
    CSysTables(dpfsEngine& dpfs_eng) : dpfs_engine(dpfs_eng) {};
    ~CSysTables() {};
    std::vector<CCollection*> collections; // system collection list
    dpfsEngine& dpfs_engine;

    /*
        @note read the system table from the storage engine, and load the collections
        @return 0 on success, else on failure
    */
    bool readSuper() { return true; }

    bool load() { return true; }
    char data[];
};


