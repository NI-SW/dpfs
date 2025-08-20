/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */

#include <dpendian.hpp>
#include <string>
#include <vector>
#include <storage/engine.hpp>
class CCollection;
class CKey;
struct dds_field;
enum dpfs_datatype_t {
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

using dpfs_data_group = std::pair<CKey, dds_field>; // <KEY, <数据类型, 数据长度>>

// data describe struct
struct dds_field {
    dpfs_datatype_t type;
    size_t len; // for string or binary type
    size_t scale;
};

class CKey {
public:
    CKey() = delete;
    CKey(const CKey& other) {
        switch (other.type) {
            case Type::Integer:
                intKey = other.intKey;
                break;
            case Type::Float:
                floatKey = other.floatKey;
                break;
            case Type::String:
                strKey = other.strKey;
                break;
            case Type::Binary:
                binaryKey = new uint8_t[other.len];
                std::copy(other.binaryKey, other.binaryKey + other.len, binaryKey);
                break;
            default:
                break; // Both are None
        }
    }
    CKey(const std::string& key) : strKey(key), type(Type::String), len(key.size()) {};
    CKey(int64_t key) : intKey(key), type(Type::Integer), len(sizeof(int64_t)) {};
    CKey(double key) : floatKey(key), type(Type::Float), len(sizeof(double)) {};
    CKey(const uint8_t* key, size_t length) : binaryKey(new uint8_t[length]), type(Type::Binary), len(length) {
        std::copy(key, key + length, binaryKey);
    };

    bool operator==(const CKey& other) const {
        if (type != other.type || len != other.len) 
            return false;

        switch (type) {
            case Type::Integer:
                return intKey == other.intKey;
            case Type::Float:
                return floatKey == other.floatKey;
            case Type::String:
                return strKey == other.strKey;
            case Type::Binary:
                return std::equal(binaryKey, binaryKey + len, other.binaryKey);
            default:
                return true; // Both are None
        }
    }

    ~CKey() {
        if(type == Type::Binary) {
            delete[] binaryKey;
        }
    }

    enum class Type {
        None,
        String,
        Integer,
        Float,
        Binary
    };

    union {
        int64_t intKey;         // 整数键
        double floatKey;        // 浮点键
        std::string strKey;     // 字符串键
        uint8_t* binaryKey;      // 二进制键
    };

    Type type = Type::None;  // 默认类型为None
    uint16_t len = 0;

};

class CValue {
public:
    CValue(){};
    ~CValue(){};

    /*
        @param other: CValue to copy from
        @return copied bytes on success, else on failure
    */
    size_t copy(const CValue& other) {
        *len = *other.len;
        std::copy(other.data, other.data + *len, data);
        return *len;
    }

    
    // need to process big or little endian
    size_t* len = 0;

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
    //                   rowlock        col1    col2    col3    col4
    //  CItem[0].data = |locked|reserve|CValue*|CValue*|CValue*|CVAlue|
    //                                  |
    //                             CValue->data
    char data[];
    
};



class CItem {
public:
    CItem(const std::vector<dpfs_data_group>& ck, CValue* value) : keys(ck) {

    }
    ~CItem();

    /*
        @param pos: position of the item in the item list
        @return CValue pointer on success, else nullptr
    */
    virtual const CValue* getValue(int pos) const {
        return values[pos];
    }

    /*
        @param key: key of the item, maybe column name
        @return CValue pointer on success, else nullptr
        @note this function will search the item list for the key, and possible low performance
    */
    virtual const CValue* getValueByKey(CKey key) const {
        for(size_t i = 0; i < keys.size(); ++i) {
            if (keys[i].first == key) {
                return values[i];
            }
        }
        return nullptr;
    }

    /*
        @param pos: position of the item in the item list
        @param value: CValue pointer to update
        @return bytes updated on success, else on failure
    */
    virtual int updateValue(int pos, CValue* value) {
        return values[pos]->copy(*value);
    }

    /*
        @param key: key of the item, maybe column name
        @param value: CValue pointer to update
        @return CValue pointer on success, else nullptr
    */
    virtual int updateValueByKey(CKey key, CValue* value) {
        for(size_t i = 0; i < keys.size(); ++i) {
            if (keys[i].first == key) {
                return values[i]->copy(*value);
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
    virtual int commit() = 0;

    const std::vector<dpfs_data_group>& keys;
    // row lock, if row is update but not committed, the row is locked
    bool* locked;
    // from flexible array member, to store values
    std::vector<CValue*> values;

    // storage row data
    char date[];
};

/*
    use row lock
    for manage a collection of items

    storage struct {
        keys => |keytype (now 1B)| key name len 4B | dataType 1B | data len 8B
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
    CCollection(dpfsEngine& engine, int32_t id = 0) : dpfs_engine(engine), ccid(id) { };
    ~CCollection() {};

    enum class storageType {
        COL = 0,    // storage data by column   -> easy to add key
        ROW,        // storage data by row      -> easy to query
        MIX,        // storage data by mix of col and row
    };

    /*
        @param key: CKey to add
        @param type: data type of the key
        @param len: length of the data, if not specified, will use 0
        @param scale: scale of the data, only useful for decimal type
        @return 0 on success, else on failure
        @note this function will add the key to the collection
    */
    int addKey(const CKey& key, dpfs_datatype_t type, size_t len = 0, size_t scale = 0) {
        if(key.len > 128) {
            return -ENAMETOOLONG; // Key length exceeds maximum allowed size
        }

        dds_field dds = {type, len, scale};

        switch(type) {
            case TYPE_DECIMAL: 
            case TYPE_CHAR:
            case TYPE_VARCHAR:
            case TYPE_BINARY:
            case TYPE_BLOB:
                if (len == 0) {
                    return -EINVAL; // Invalid length for string or binary type
                }

                keys.emplace_back(std::make_pair(key, dds));
                return 0;
            case TYPE_INT:
                len = sizeof(int32_t);
                break;
            case TYPE_BIGINT:
                len = sizeof(int64_t);
                break;
            case TYPE_FLOAT:
                len = sizeof(float);
                break;
            case TYPE_DOUBLE:
                len = sizeof(double);
                break;
            default:
                return -EINVAL; // Invalid data type
        }
        keys.emplace_back(std::make_pair(key, dds));


        return 0;
    }

    /*
        @param key: CKey to remove(or column to remove)
        @return 0 on success, else on failure
        @note this function will remove the key from the collection, and update the index
    */
    int removeKey(const CKey& key){ return 0; };

    /*
        @param item: CItem pointer to add
        @return 0 on success, else on failure
        @note this function will add the item to the collection, and update the index
    */
    virtual int addItem(CItem* item) { return 0; };

    /*
        @param item: CItem pointer to add
        @return 0 on success, else on failure
        @note this function will add the item to the collection, and update the index
    */
    virtual int addItems(std::vector<CItem*>& items) { return 0; };
    
    virtual int deleteItem(int pos) { return 0; };

    /*
        @param pos: position of the item in the item list
        @return CItem pointer on success, else nullptr
    */
    virtual const CItem* const getItem(int pos) { return 0; };

    /*
        @param key: key of the item, maybe column name
        @return CItem pointer on success, else nullptr
        @note this function will search the item list for the key, and possible low performance
    */
    virtual const CItem* const getItemByKey(CKey key) { return 0; };

    /*
        @return number of items in the collection
    */
    virtual int getItemCount() { return 0; };

    /*
        @return 0 on success, else on failure
        @note commit the item changes to the storage.

        need to do : 
        write commit log, and then write data, finally update the index
    */
    virtual int commit() { return 0; };

    std::string owner;
    std::string name;
    std::vector<dpfs_data_group> keys;
    // col ptr for logic block
    std::vector<size_t> col_ptr;
    // ccid: CCollection ID, used to identify the collection info
    int32_t ccid;

    /*
        use cache to storage pending data, if cache is dirty, lock the row untile change is rolled back or committed
        @note this is a simple cache implementation, not a full cache system
    */
    struct cache {
        size_t start_pos = 0;
        size_t end_pos = 0;
        // size_t count = 0;
        std::vector<CItem*> items; // cached items
        bool dirty = false; // if the cache is dirty, need to commit
    };
    /*
        storage user defined data for the collection.
    */
    // std::vector<CItem*> items;

    dpfsEngine& dpfs_engine;
    
    
};

