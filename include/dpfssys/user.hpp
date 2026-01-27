#include <stdint.h>
#include <string>
// privilege to db
enum dbPrivilege : uint8_t {
    DBPRIVILEGE_NONE = 0,   // no privilege to any other objects
    DBPRIVILEGE_ACCESS,     // access all objects(include system objects)
    DBPRIVILEGE_CONTROL,    // control all objects(exclude system objects)
    DBPRIVILEGE_SYSTEM,     // can modify system level objects(update and insert, delete is prohibited)
    DBPRIVILEGE_FATAL,      // can even destroy the database(delete, truncate and drop operation is granted)
};

// privilege to table, this values should be stored in USERAUTH table
enum tbPrivilege : uint16_t {
    TBPRIVILEGE_SELECT = 1 << 0,   // access data of the table
    TBPRIVILEGE_INSERT = 1 << 1,   // insert data of the table
    TBPRIVILEGE_UPDATE = 1 << 2,   // update data of the table
    TBPRIVILEGE_DELETE = 1 << 3,   // delete data of the table
    TBPRIVILEGE_DROP = 1 << 4,     // drop the table
    TBPRIVILEGE_FULL = 1 << 5,     // full control of the table
};

// user login status
class CUser {
public:
    CUser() {};
    ~CUser() {};
    int32_t userid = 0;

    std::string username;
    dbPrivilege dbprivilege;

    

};