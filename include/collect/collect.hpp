/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#include <dpfsdebug.hpp>
#include <dpendian.hpp>
#include <string>
#include <cstring>
#include <vector>
#include <storage/engine.hpp>
class CProduct;
class CCollection;
class CColumn;
constexpr size_t MAX_COL_NAME_LEN = 128;
/*
use for table;

CREATE TABLE goodsid.storage (
    KEY DATA TYPE,
);
*/

struct smallData {
    uint8_t size = 0;
    uint8_t data[];
};

struct shortData {
    uint16_t size = 0;
    uint8_t data[];
};

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


// data describe struct size = 8
struct dds_field {
    uint32_t len = 0; // for string or binary type
    uint16_t scale = 0;
    dpfs_datatype_t type = dpfs_datatype_t::TYPE_NULL;
    bool operator==(const dds_field& other) const {
        return (type == other.type && scale == other.scale && len == other.len);
    }
};

// using dpfs_data_group = std::pair<CColumn*, dds_field>; // <COLNAME, <数据类型, 数据长度>>

/*
FIXED TABLE 

|COLCOUNT|COL1|COL2|...|ITEM1|ITEM2|...
COLCOUNT : 4B
COL1 : |LEN|COLTYPE|COLLEN|COLSCALE|COLNAME|
COL2 : |LEN|COLTYPE|COLLEN|COLSCALE|COLNAME|
...
ITEM : |COL1DATA|COL2DATA|COL3DATA|...
...

*/
class CColumn {
public:
    CColumn() = delete;

    bool operator==(const CColumn& other) const noexcept {
        if (colName.size != other.colName.size)
            return false;

        return (!strncmp((const char*)colName.data, (const char*)other.colName.data, colName.size) && 
            (type == other.type && scale == other.scale && len == other.len));
    }

    // generate a dpfs column
    static CColumn* newCol(const std::string& colName, dpfs_datatype_t dataType, size_t dataLen = 0, size_t scale = 0) noexcept {
        if (colName.size() == 0 || colName.size() > MAX_COL_NAME_LEN) {
            return nullptr;
        }
        CColumn* k = (CColumn*)malloc(sizeof(CColumn) + colName.size() + 1);
        if(!k) {
            return nullptr;
        }
        memcpy(k->colName.data, colName.c_str(), colName.size() + 1);
        // k->colName.data[colName.size()] = '\0';
        k->colName.size = colName.size() + 1;
        k->type = dataType;
        k->len = dataLen;
        k->scale = scale;
        return k;
    }

    static CColumn* newCol(const CColumn& other) noexcept {
        if (other.colName.size == 0) {
            return nullptr;
        }

        CColumn* k = (CColumn*)malloc(sizeof(CColumn) + other.colName.size);
        if(!k) {
            return nullptr;
        }
        memcpy(k->colName.data, other.colName.data, other.colName.size);
        // k->colName.data[other.colName.size] = '\0';
        k->colName.size = other.colName.size;

        k->type = other.type;
        k->len = other.len;
        k->scale = other.scale;
        return k;
    }

    // delete a dpfs column and push the pointer to nullptr
    static void delCol(CColumn*& col) noexcept {
        if (col) {
            free(col);
        }
        col = nullptr;
    }

    uint8_t getNameLen() const noexcept {
        return colName.size;
    }

    const char* getName() const noexcept {
        return (const char*)colName.data;
    }

    uint32_t getLen() const noexcept {
        return len;
    }

    uint16_t getScale() const noexcept {
        return scale;
    }

    dpfs_datatype_t getType() const noexcept {
        return type;
    }

    // return data describe struct
    dds_field getDds() const noexcept{
        dds_field dds = {this->len, this->scale, this->type};
        return dds;
    }

    size_t getStorageSize() const noexcept {
        return sizeof(CColumn) + colName.size;
    }

    void* operator new(size_t size) = delete;
    ~CColumn() = delete;
private:

    // size = sizeof(dds) + sizeof(nameLen)
    // dds_field dds;

    // for string or binary type
    uint32_t len;
    // for decimal
    uint16_t scale;
    dpfs_datatype_t type;
    smallData colName;
};

/*
    like a col in a row
*/
class CValue {
public:
    CValue(){};
    ~CValue(){};

    /*
        @param other: CValue to copy from
        @return copied bytes on success, else on failure
    */
    size_t copy(const CValue& other) noexcept {
        len = other.len;
        // std::copy(other.data, other.data + *len, data);
        memcpy(data, other.data, len);
        return len;
    }

    
    // need to process big or little endian
    size_t len = 0;

    // one row data
    //
    // row in lba1
    //                   <lba1>
    //                   rowlock        col1    col2    col3
    //  CItem[0].data = |locked|reserve|CValue*|CValue*|CValue*|
    //                                  |
    //                             CValue->data
    //
    // if use mix storage, the new col storage method like this:
    // row in lba2
    //                   <lba2>
    //                   rowlock        new col--------------------
    //  CItem[1].data = |locked|reserve|CValue*|                  |
    //                                  |                         |
    //                             CValue->data                   |
    //                                                            |
    // can use REORG to rebuild the table ::                      |
    //                   <lba>                                    ↓
    //                   rowlock        col1   col2   col3   col4
    //  CItem[0].data = |locked|reserve|CValue|CValue|CValue|CVAlue|
    //                                  |
    //                             CValue->data
    char data[];

};

/*
like a row
*/
class CItem {
public:

    /*
        @param cs column info
        @param value of row data
    */
    CItem(vector<CColumn*>& cs) : cols(cs) {

    }
    ~CItem();

    /*
        @param pos: position of the item in the item list
        @return CValue pointer on success, else nullptr
    */
    virtual const CValue* getValue(int pos) const noexcept {
        return &values[pos];
    }

    /*
        @param col: column of the item, maybe column name
        @return CValue pointer on success, else nullptr
        @note this function will search the item list for the column, and possible low performance
    */
    virtual const CValue* getValueByKey(CColumn* col) const noexcept {
        for(size_t i = 0; i < cols.size(); ++i) {
            if (*cols[i] == *col) {
                return &values[i];
            }
        }
        return nullptr;
    }

    /*
        @param pos: position of the item in the item list
        @param value: CValue pointer to update
        @return bytes updated on success, else on failure
    */
    virtual int updateValue(int pos, CValue* value) noexcept {
        if(cols[pos]->getLen() < value->len) {
            return -E2BIG;
        }
        return values[pos].copy(*value);
    }

    /*
        @param col: col of the item, maybe column name
        @param value: CValue pointer to update
        @return CValue pointer on success, else nullptr
    */
    virtual int updateValueByKey(CColumn* col, CValue* value) noexcept {
        for(size_t i = 0; i < cols.size(); ++i) {
            if (*cols[i] == *col) {
                return values[i].copy(*value);
            }
        }
        // values not found
        return -ENXIO;
    }

    /*
        @return 0 on success, else on failure
        @note commit the item changes to the storage.

        need to do : 
        write commit log, and then write data, finally update the index
    */
    virtual int commit() {

        return 0;
    };

    std::vector<CColumn*>& cols;

    // row lock, if row is update but not committed, the row is locked
    bool* locked;

    // pointer to the value started in the caches
    CValue* values = nullptr;
    
};

/*
like a table
    use row lock?
    for manage a collection of items

    storage struct {
        keys => |keytype (now 1B)| col name len 4B | dataType 1B | data len 8B
    }
*/
class CCollection {
public:
    // use ccid to locate the collection (search in system collection table)
    /*
        @param engine: dpfsEngine reference to the storage engine
        @param ccid: CCollection ID, used to identify the collection info, 0 for new collection
        @note this constructor will create a new collection with the given engine and ccid
    */
    CCollection(dpfsEngine& engine, uint32_t id = 0) : dpfs_engine(engine), m_ccid(id), caches(m_cols) { 
        m_updatable = false;
        m_insertable = false;
        m_detelable = false;
        m_dirty = false;
        m_name = "dpfs_dummy";
        m_cols.clear();
        m_cols.reserve(16);
        
    };
    ~CCollection() {};

    enum class storageType {
        COL = 0,    // storage data by column   -> easy to add col
        ROW,        // storage data by row      -> easy to query
        MIX,        // storage data by mix of col and row
    };

    int setName(const std::string& name) {
        m_name = name;
        return 0;
    }

    /*
        @param col: column to add
        @param type: data type of the col
        @param len: length of the data, if not specified, will use 0
        @param scale: scale of the data, only useful for decimal type
        @return 0 on success, else on failure
        @note this function will add the col to the collection
    */
    int addCol(const std::string& colName, dpfs_datatype_t type, size_t len = 0, size_t scale = 0) {
        if(colName.size() > MAX_COL_NAME_LEN) {
            return -ENAMETOOLONG; // Key length exceeds maximum allowed size
        }

        CColumn* col = nullptr;

        switch(type) {
            case dpfs_datatype_t::TYPE_DECIMAL: 
            case dpfs_datatype_t::TYPE_CHAR:
            case dpfs_datatype_t::TYPE_VARCHAR:
            case dpfs_datatype_t::TYPE_BINARY:
            case dpfs_datatype_t::TYPE_BLOB:
                if (len == 0) {
                    return -EINVAL; // Invalid length for string or binary type
                }
                col = CColumn::newCol(colName, type, len, scale);
                break;
            case dpfs_datatype_t::TYPE_INT:
                len = sizeof(int32_t);
                col = CColumn::newCol(colName, type, len, scale);
                break;
            case dpfs_datatype_t::TYPE_BIGINT:
                len = sizeof(int64_t);
                col = CColumn::newCol(colName, type, len, scale);
                break;
            case dpfs_datatype_t::TYPE_FLOAT:
                len = sizeof(float);
                col = CColumn::newCol(colName, type, len, scale);
                break;
            case dpfs_datatype_t::TYPE_DOUBLE:
                len = sizeof(double);
                col = CColumn::newCol(colName, type, len, scale);
                break;
            default:
                return -EINVAL; // Invalid data type
        }
        // generate column fail
        if(!col) {
            return -ENOMEM;
        }
        m_lock.lock();
        m_cols.emplace_back(col);
        m_lock.unlock();
        m_dirty = true;
        m_needreorg = true;
        return 0;
    }

    /*
        @param col: column to remove(or column to remove)
        @return 0 on success, else on failure
        @note this function will remove the col from the collection, and update the index
    */
    int removeCol(const std::string& colName){
        for(vector<CColumn*>::iterator it = m_cols.begin(); it != m_cols.end(); ++it) {
            if((*it)->getNameLen() != colName.size() + 1) {
                continue;
            }
            if(memcmp((*it)->getName(), colName.c_str(), (*it)->getNameLen()) == 0) {
                m_lock.lock();
                m_cols.erase(it);
                m_lock.unlock();
                m_dirty = true;
                m_needreorg = true;
                break;
            }
        }

        return -ENODATA;
    };

    /*
        @param item: CItem pointer to add
        @return 0 on success, else on failure
        @note this function will add the item to the collection, and update the index
    */
    virtual int addItem(CItem* item) { 
        // TODO

        return 0;
    };

    /*
        @param item: CItem pointer to add
        @return 0 on success, else on failure
        @note this function will add the item to the collection, and update the index
    */
    virtual int addItems(std::vector<CItem*>& items) { 
        // TODO

        return 0; 
    };
    
    virtual int deleteItem(int pos) { 
        // TODO

        return 0; 
    };

    /*
        @param pos: position of the row in the row list
        @return CValue pointer on success, else nullptr
    */
    virtual const CValue* const getRow(size_t pos) { 
        // TODO 
        if(pos >= caches.start_pos && pos < caches.end_pos) {
            
        }

        return nullptr;
    };

    /*
        @return total items in the collection
    */
    virtual int getItemCount() { 
        // TODO 

        return 0; 
    };

    /*
        @return 0 on success, else on failure
        @note commit the item changes to the storage.

        need to do : 
        write commit log, and then write data, finally update the index
    */
    virtual int commit() { 

        return 0;
    };

    // permission of operations
    bool m_select : 1;
    bool m_updatable : 1;
    bool m_insertable : 1;
    bool m_detelable : 1;
    bool m_ddl : 1;
    // if dirty trigger a commit will flush data struct change to storage
    bool m_dirty : 1;
    // if data struct is changed, need a restore to reorganize data in storage
    bool m_needreorg : 1;
    // not used
    bool reserve : 1;

    // the product that owns this collection
    CProduct* m_owner;
    // name of the collection
    std::string m_name;
    // columns in the collection
    std::vector<CColumn*> m_cols;
    // inner locker
    CSpin m_lock;


    // ccid: CCollection ID, used to identify the collection info
    uint32_t m_ccid;

    /*
        use cache to storage pending data, if cache is dirty, lock the row untile change is rolled back or committed
        @note this is a simple cache implementation, not a full cache system, use LRU?
    */
    struct cache {
        // row start position
        size_t start_pos = 0;
        // row end position
        size_t end_pos = 0;
        CItem item;
        bool dirty = false; // if the cache is dirty, need to commit

        cache(std::vector<CColumn*>& cols) : item(cols) {

        }

        /*
            load data from storage.
        */
        int loadCache(size_t start_pos, size_t end_pos, CValue* start_value) {
            dirty = false;
        }

        /*
            save changes to storage.
        */
        int storeCache() {

            dirty = false;
            return 0;
        }

        class index {
            
        };
    };

    cache caches;

    /*
        storage user defined data for the collection.
    */
    // std::vector<CItem*> items;

    dpfsEngine& dpfs_engine;
    

};

