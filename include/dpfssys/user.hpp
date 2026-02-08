#pragma once
#include <stdint.h>
#include <string>
#include <basic/dpfscache.hpp>

// privilege to db
enum class dbPrivilege : uint8_t {
    DBPRIVILEGE_NONE = 0,   // no privilege to any other objects
    DBPRIVILEGE_ACCESS,     // access all objects(include system objects)
    DBPRIVILEGE_CONTROL,    // control all objects(exclude system objects)
    DBPRIVILEGE_SYSTEM,     // can modify system level objects(update and insert, delete is prohibited)
    DBPRIVILEGE_FATAL,      // can even destroy the database(delete, truncate and drop operation is granted)
};

// privilege to table, this values should be stored in USERAUTH table
enum class tbPrivilege : uint16_t {
    TBPRIVILEGE_SELECT = 1 << 0,   // access data of the table
    TBPRIVILEGE_INSERT = 1 << 1,   // insert data of the table
    TBPRIVILEGE_UPDATE = 1 << 2,   // update data of the table
    TBPRIVILEGE_DELETE = 1 << 3,   // delete data of the table
    TBPRIVILEGE_DROP = 1 << 4,     // drop the table
    TBPRIVILEGE_FULL = 1 << 5,     // full control of the table
};

// user login status
// when connect, use key to find user record in USERAUTH table
class CUser {
public:
    CUser() : privilegeCache(100, &clrfunction) {};
    ~CUser() {
        
    };
    int32_t userid = 1000;
    std::string username = "NULLID";
    std::string currentSchema = "NULLID";
    dbPrivilege dbprivilege = dbPrivilege::DBPRIVILEGE_NONE;

    /*
        dbprivilege
        tbprivilege
        tempTable Pointer
    */
    struct cacheStruct {
        uint32_t tid = 0;
        uint16_t privilege = 0;
    };

    class clrfn {
        public:
        clrfn(void* arg = nullptr) {

        }
        ~clrfn() {}
        void operator()(cacheStruct , int* finish_indicator = nullptr) { 
            if (finish_indicator) *finish_indicator = 1;
            return; 
        }
        void flush(const std::list<void*>& cacheList) {
            return;
        }
    };

    // table id - privilege number
    clrfn clrfunction;
    CDpfsCache<uint64_t, cacheStruct, clrfn> privilegeCache;

};
