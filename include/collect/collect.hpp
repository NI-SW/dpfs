/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
// #include <dpfsdebug.hpp>
#pragma once
#include <dpendian.hpp>
#include <string>
#include <cstring>
#include <vector>
#include <collect/page.hpp>
#include <collect/diskman.hpp>

class CBPlusTree;
class CProduct;
class CCollection;
class CColumn;
constexpr size_t MAX_COL_NAME_LEN = 128;
constexpr size_t MAX_NAME_LEN = 256;
constexpr size_t MAX_COL_NUM = 128;

/*
COLCOUNT : 4B
COL1 : |NAMELEN|COLTYPE|COLLEN|COLSCALE|COLNAME|
COL2 : |NAMELEN|COLTYPE|COLLEN|COLSCALE|COLNAME|
*/

// max collection info length
constexpr size_t MAX_ClT_INFO_LEN =  4 /*col count len*/ + 
((4 /*col info len*/ + 1 /*type*/ + 4 /*col data len*/ + 2 /*col scale*/ + MAX_COL_NAME_LEN) * MAX_COL_NUM) + /*col info len*/
1/*perm*/ + 1/*namelen*/ + 4/*ccid*/ + 16 /*data root*/; /* collection info */

// max collection info length in lba size
constexpr size_t MAX_CLT_INFO_LBA_LEN = MAX_ClT_INFO_LEN % dpfs_lba_size == 0 ? 
    MAX_ClT_INFO_LEN / dpfs_lba_size : 
    (MAX_ClT_INFO_LEN / dpfs_lba_size + 1);

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

inline bool isVariableType(const dpfs_datatype_t& type) noexcept {
    return (type == dpfs_datatype_t::TYPE_VARCHAR || type == dpfs_datatype_t::TYPE_BLOB);
}

inline bool isNumberType(const dpfs_datatype_t& type) noexcept {
    return (
        type == dpfs_datatype_t::TYPE_BIGINT || 
        type == dpfs_datatype_t::TYPE_DOUBLE ||
        type == dpfs_datatype_t::TYPE_FLOAT ||
        type == dpfs_datatype_t::TYPE_INT
    );
}

// data describe struct size = 8
struct dds_field {
    uint32_t len = 0; // for string or binary type
    uint16_t scale = 0;
    dpfs_datatype_t type = dpfs_datatype_t::TYPE_NULL;
    bool operator==(const dds_field& other) const {
        return (type == other.type && scale == other.scale && len == other.len);
    }
};


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
    static CColumn* newCol(const std::string& colName, dpfs_datatype_t dataType, size_t dataLen = 0, size_t scale = 0, uint8_t constraint = 0) noexcept {
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
        k->constraints.unionData = constraint;

        // k->offSet = 0;
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
        k->constraints.unionData = other.constraints.unionData;
 
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

    /*
        @return length of the column data(indicate max length, if is variable string type)
    */
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

    /*
        @param k: the key of the data
        @return 0 on success, else on failure
        @note return the key that used in b+ tree
    */
    size_t getKey(size_t& k) const noexcept {
        switch (type)
        {
        case dpfs_datatype_t::TYPE_INT:
            k = *((int32_t*)(colName.data));
            break;
        case dpfs_datatype_t::TYPE_BIGINT:
            k = *((int64_t*)(colName.data));
            break;
        // not support now
        case dpfs_datatype_t::TYPE_FLOAT:
        case dpfs_datatype_t::TYPE_DOUBLE:
            return -EINVAL;
            // k = *((float*)(colName.data));
            // k = *((double*)(colName.data));
        default:
            break;
        }
        return 0;
    }
    
    enum constraint_flags : uint8_t {
        DEFAULT_VALUE = 1 << 0,
        NOT_NULL = 1 << 1,
        PRIMARY_KEY = 1 << 2,
        UNIQUE = 1 << 3,
        AUTO_INC = 1 << 4
    };

private:

    // size = sizeof(dds) + sizeof(nameLen)
    // dds_field dds;
    friend class CValue;
    friend class CCollection;
    friend class CItem;
    // offset of the first row.
    // uint32_t offSet;
    // for string or binary type(max len)

    // column data length
    uint32_t len;
    // for decimal
    uint16_t scale;



    union {
        struct {
            uint8_t defaultValue : 1;
            uint8_t notNull : 1;
            uint8_t primaryKey : 1;
            uint8_t unique : 1;
            uint8_t autoInc : 1;
            uint8_t reserved : 3;
        } ops;
        uint8_t unionData;
    } constraints;
    dpfs_datatype_t type;
    smallData colName;
};

/*
    like a col in a row
*/
class CValue {
public:
    CValue(){};
    CValue(uint32_t sz) {
        data = (char*)malloc(sz);
        if(!data) {
            len = 0;
        }
        this->maxLen = sz;
    }

    ~CValue() {
        if(maxLen > 0 && data) {
            free(data);
        }
    }

    CValue(const CValue& other) noexcept {
        len = other.len;
        if(other.maxLen > 0) {
            // deep copy
            data = (char*)malloc(other.maxLen);
            if(data) {
                memcpy(data, other.data, other.len);
                maxLen = other.maxLen;
            } else {
                data = 0;
                maxLen = 0;
            }

        } else {
            data = other.data;
            maxLen = 0;
        }
    }

    CValue(CValue&& other) noexcept {
        len = other.len;
        data = other.data;
        maxLen = other.maxLen;
        other.data = nullptr;
        other.maxLen = 0;
    }

    CValue& operator=(const CValue& other) noexcept {
        if (this != &other) {
            len = other.len;
            data = other.data;
        }
        return *this;
    }

    /*
        @param other: CValue to copy from
        @return copied bytes on success, else on failure
    */
    // size_t copy(const CValue& other) noexcept {
    //     len = other.len;
    //     // std::copy(other.data, other.data + *len, data);
    //     memcpy(data, other.data, len);
    //     return len;
    // }

    int setData(const void* data, uint32_t size) noexcept {
        len = size;
        memcpy(this->data, data, size);
        return 0;
    }
    
    // need to process big or little endian
    uint32_t len = 0;
    uint32_t maxLen = 0;

    // one row data
    //
    // row in lba1
    //                   <lba1>
    //                   rowlock        col1    col2    col3
    //  CItem[0].data = |locked|reserve|char*|char*|char*|
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
    //                   rowlock        col1  col2  col3  col4
    //  CItem[0].data = |locked|reserve|char*|char*|char*|char*|
    //                                  |
    //                             CValue->data
    char* data = nullptr;

};

/*
like a row
*/
class CItem {
public:


    /*
        @param pos: position of the col in one item list
        @return valid CValue on success, else failure
    */
    CValue getValue(size_t pos) const noexcept;

    /*
        @param col: column of the item, maybe column name
        @return CValue pointer on success, else nullptr
        @note this function will search the item list for the column, and possible low performance
    */
    CValue getValueByKey(CColumn* col) const noexcept;
   
    /*
        @param pos: position of the item in the item list
        @param value: CValue pointer to update
        @return bytes updated on success, else on failure
    */
    int updateValue(size_t pos, CValue* value) noexcept;
    
    /*
        @param col: col of the item, maybe column name
        @param value: CValue pointer to update
        @return CValue pointer on success, else nullptr
    */
    int updateValueByKey(CColumn* col, CValue* value) noexcept;
    

    /*
        @param writeBack write to storage immediate if true
        @return 0 on success, else on failure
        @note commit the item changes to the storage.
        need to do : 
        write commit log, and then write data, finally update the index
    */
    // int commit(bool writeBack = false);
    
    /*
        @param cs: column info
        @return CItem pointer on success, else nullptr
        @note this function will create a new CItem with the given column info
    */
    static CItem* newItem(const std::vector<CColumn*>& cs) noexcept;
    
    /*
        @param cs: column info
        @param maxRowNumber: maximum number of rows in the CItem
        @return CItem pointer on success, else nullptr
        @note this function will create a new CItem with the given column info
    */
    static CItem* newItems(const std::vector<CColumn*>& cs, size_t maxRowNumber) noexcept;
    static void delItem(CItem*& item) noexcept;

    /*
        @note switch to next row
        @return 0 on success, else on failure
    */
    int nextRow() noexcept;

    int resetScan() noexcept;
    
    class rowIter {
    public:
        rowIter(CItem* item) : m_item(item) {
            m_ptr = item->data;
        }

        rowIter(const rowIter& other) {
            m_item = other.m_item;
            m_ptr = other.m_ptr;
            m_pos = other.m_pos;
        }

        rowIter& operator=(const rowIter& other) {
            if (this != &other) {
                m_item = other.m_item;
                m_ptr = other.m_ptr;
                m_pos = other.m_pos;
            }
            return *this;
        }

        rowIter& operator++() {
            
            for(auto& pos : m_item->cols) {
                if(isVariableType(pos->type)) {
                    // first 4 bytes is actual length
                    uint32_t vallEN = *(uint32_t*)((char*)m_ptr);
                    m_ptr += sizeof(uint32_t) + vallEN;
                } else {
                    m_ptr += pos->getLen();
                }
            }

            ++m_pos;
            return *this;
        }

        bool operator!=(const rowIter& other) const {
            return (m_pos != other.m_pos);
        }

        rowIter& operator*() noexcept {
            // CValue val;
            // if(m_item->cols[m_pos]->type == dpfs_datatype_t::TYPE_VARCHAR) {
            //     // first 4 bytes is actual length
            //     val.len = *(uint32_t*)((char*)m_ptr);
            //     val.data = (char*)m_ptr + sizeof(uint32_t);
            // } else {
            //     val.len = m_item->cols[m_pos]->len;
            //     val.data = (char*)m_ptr;
            // }
            // return val;
            return *this;
        }

        CValue operator[](size_t index) const noexcept {
            CValue val;
            size_t offSet = 0;
            for(size_t i = 0; i < index; ++i) {
                if(isVariableType(m_item->cols[i]->type)) {
                    offSet += sizeof(uint32_t) + (*(uint32_t*)((char*)m_ptr + offSet));
                } else {
                    offSet += m_item->cols[i]->len;
                }
            }

            if(m_item->cols[index]->type == dpfs_datatype_t::TYPE_VARCHAR) {
                // first 4 bytes is actual length
                val.len = *(uint32_t*)((char*)m_ptr + offSet);
                val.data = (char*)m_ptr + offSet + sizeof(uint32_t);
            } else {
                val.len = m_item->cols[index]->len;
                val.data = (char*)m_ptr + offSet;
            }
            return val;
        }

    private:
        friend class CItem;
        CItem* m_item = nullptr; 
        char* m_ptr = nullptr;
        size_t m_pos = 0;
    };

    rowIter beginIter;
    rowIter endIter;

    const rowIter& begin() const noexcept {
        return beginIter;
    }

    const rowIter& end() const noexcept {
        return endIter;
    }
    

    // private:

    /*
        @param cs column info
        @param value of row data
    */
    CItem(const std::vector<CColumn*>& cs);
    CItem(const CItem& other) = delete;
    ~CItem();
    int dataCopy(size_t pos, const CValue* value) noexcept;
    size_t getDataOffset(size_t pos) const noexcept;
    // int resetOffset(size_t begPos) noexcept;

    const std::vector<CColumn*>& cols;

    // row lock, if row is update but not committed, the row is locked
    bool locked : 1;
    bool error : 1;
    bool : 6;

    // when lock a row, lock page first, then lock the row

    size_t maxRowNumber = 1;
    size_t rowNumber = 0;
    size_t rowLen = 0;
    size_t validLen = 0;
    char* rowPtr = nullptr;
    char data[];
    // CValue* values = nullptr;
};

/*
    process where clause
*/
class CWhere {
public:
    CWhere() {};
    ~CWhere() {};
private:
    // condition tree

};

/*
    class to manage Sequential Storaged data
    variable length data is not allowed.
*/
class CSeqStorage {
public:
    CSeqStorage(bidx h, std::vector<CColumn*>& cls, CPage& pge) : head(h), cols(cls), m_page(pge) {
        count = 0;
        rowLen = 0;
        for (auto& col : cols) {
            rowLen += col->getLen();
            if(isVariableType(col->getType())) {
                // variable length type not allowed
                throw std::invalid_argument("Variable length type not allowed in CSeqStorage");
            }
        }
    };
    ~CSeqStorage() {};

    // ||||
    int getItem(size_t pos) {
        // calculate the bidx
        bidx idx = head;
        idx.bid += (pos * rowLen) / 4096;
        
        cacheStruct* cache = nullptr;
        int rc = m_page.get(cache, idx, 1);
        if (rc != 0) {
            return rc;
        }
        
        // process offset
        // TODO
        // size_t offset = (pos * rowLen) % 4096;
        
        return 0;
    }


private:
    // storage type
    bidx head = {0, 0};
    uint32_t count = 0;
    uint32_t rowLen = 0;
    std::vector<CColumn*>& cols;
    CPage& m_page;


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
    CCollection(CProduct& owner, CDiskMan& dskman, CPage& pge, uint32_t id = 0);

    ~CCollection() {
        if(tmpData) {
            free(tmpData);
            tmpData = nullptr;
        }

    };

    enum class storageType {
        COL = 0,    // storage data by column   -> easy to add col
        ROW,        // storage data by row      -> easy to query
        MIX,        // storage data by mix of col and row
    };

    int setName(const std::string& name) {
        if(name.size() > MAX_NAME_LEN) {
            return -ENAMETOOLONG; // Key length exceeds maximum allowed size
        }
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
    int addCol(const std::string& colName, dpfs_datatype_t type, size_t len = 0, size_t scale = 0, uint8_t constraint = 0) {
        if(colName.size() > MAX_COL_NAME_LEN) {
            return -ENAMETOOLONG; // Key length exceeds maximum allowed size
        }

        if(m_cols.size() >= MAX_COL_NUM) {
            return -E2BIG; // Too many columns
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
                // col = CColumn::newCol(colName, type, len, scale);
                break;
            case dpfs_datatype_t::TYPE_INT:
                len = sizeof(int32_t);
                // col = CColumn::newCol(colName, type, len, scale);
                break;
            case dpfs_datatype_t::TYPE_BIGINT:
                len = sizeof(int64_t);
                // col = CColumn::newCol(colName, type, len, scale);
                break;
            case dpfs_datatype_t::TYPE_FLOAT:
                len = sizeof(float);
                // col = CColumn::newCol(colName, type, len, scale);
                break;
            case dpfs_datatype_t::TYPE_DOUBLE:
                len = sizeof(double);
                // col = CColumn::newCol(colName, type, len, scale);
                break;
            default:
                return -EINVAL; // Invalid data type
        }

        col = CColumn::newCol(colName, type, len, scale, constraint);
        // if generate column fail
        if(!col) {
            return -ENOMEM;
        }

        m_lock.lock();
        m_cols.emplace_back(col);
        m_lock.unlock();
        
        m_perms.perm.m_dirty = true;
        m_perms.perm.m_needreorg = true;
        return 0;
    }

    /*
        @param col: column to remove(or column to remove)
        @return 0 on success, else on failure
        @note this function will remove the col from the collection, and update the index
    */
    int removeCol(const std::string& colName) {
        for(std::vector<CColumn*>::iterator it = m_cols.begin(); it != m_cols.end(); ++it) {
            if((*it)->getNameLen() != colName.size() + 1) {
                continue;
            }
            if(memcmp((*it)->getName(), colName.c_str(), (*it)->getNameLen()) == 0) {
                m_lock.lock();
                m_cols.erase(it);
                m_lock.unlock();
                m_perms.perm.m_dirty = true;
                m_perms.perm.m_needreorg = true;
                break;
            }
        }

        return -ENODATA;
    };

    /*
        @param item: CItem pointer to add
        @return 0 on success, else on failure
        @note this function will add the item to the collection, and update the index, while not commit, storage the change to temporary disk block
    */
    int addItem(const CItem& item);

    /*
        @param item: CItem pointer to add
        @return 0 on success, else on failure
        @note this function will add the item to the collection, and update the index
    */
    int addItems(std::vector<CItem*>& items) = delete;
    
    int deleteItem(int pos);

    /*
        @param pos: position of the row in the row list
        @return CValue pointer on success, else nullptr
    */
    CItem* getRow(size_t index);

    /*
        @return total items in the collection
    */
    int getItemCount();

    /*
        @param colName: column name to search
        @param value: CValue reference to search
        @return number of items found on success, else on failure
    */
    int searchItem(const std::string& colName, CValue& value);// conditions

    /*
        @param results: vector of CItem pointers to store the results
        @param number: number of items to get
        @return number of item on success, 0 on no more items, else on failure
        @note this function will get the result items from the collection
    */
    int getResults(std::vector<CItem*>& results, size_t number);

    /*
        @return 0 on success, else on failure
        @note commit the item changes to the storage.

        need to do : 
        write commit log, and then write data, finally update the index
    */
    int commit();

    /*

        |permission|NameLen|Name|colSize|Cols|DataRoot|
        permission : 1B
        NameLen : 4B
        Name : NameLen B
        colSize : 4B
        Cols : colSize * variable B
        DataRoot : bidx 8B
    */

    /*
        @note save the collection info to the storage
        @return 0 on success, else on failure
    */
    int saveTo(const bidx& head) {

        /*
            |perm 1B|nameLen 1B|dataroot 2B|collection name|columns|
        */
        int rc = 0;

        uint32_t pos = 0;
        char tmpBuf[MAX_CLT_INFO_LBA_LEN * dpfs_lba_size];
        memset(tmpBuf, 0, MAX_CLT_INFO_LBA_LEN * dpfs_lba_size);

        // record permissions
        tmpBuf[pos] = m_perms.permByte;
        ++pos;

        // record data root
        cpy2le(tmpBuf + pos, (char*)&m_dataRoot.gid, sizeof(uint64_t));
        pos += sizeof(uint64_t);

        cpy2le(tmpBuf + pos, (char*)&m_dataRoot.bid, sizeof(uint64_t));
        pos += sizeof(uint64_t);

        // record collection name
        uint8_t nameLen = m_name.size();
        cpy2le(tmpBuf + pos, (char*)&nameLen, sizeof(uint8_t));
        pos += sizeof(uint8_t);
        memcpy(tmpBuf + pos, m_name.c_str(), nameLen);
        pos += nameLen;

        // record columns
        uint8_t colSize = m_cols.size();
        cpy2le(tmpBuf + pos, (char*)&colSize, sizeof(uint8_t));
        pos += sizeof(uint8_t);

        /*
            |ColNameLen|ColName|ColType|ColLen|ColScale|ColConstraint|
        */
        for (auto& col : m_cols) {

            uint8_t colNameLen = col->getNameLen();
            // record col name len
            tmpBuf[pos] = colNameLen;
            ++pos;
            // record col name
            memcpy(tmpBuf + pos, col->getName(), colNameLen);
            pos += colNameLen;

            // record col type, len, scale
            tmpBuf[pos] = static_cast<uint8_t>(col->getType());
            ++pos;
            cpy2le(tmpBuf + pos, (char*)&col->len, sizeof(uint32_t));
            pos += sizeof(uint32_t);
            cpy2le(tmpBuf + pos, (char*)&col->scale, sizeof(uint16_t));
            pos += sizeof(uint16_t);
            cpy2le(tmpBuf + pos, (char*)&col->constraints.unionData, sizeof(uint8_t));
            pos += sizeof(uint8_t);

        }

        int indicate = 0;
        size_t lba_used = pos / dpfs_lba_size + (pos % dpfs_lba_size == 0 ? 0 : 1);
        void* zptr = m_page.alloczptr(lba_used);
        if (!zptr) {
            rc = -ENOMEM;
            goto errReturn;
        }

        memcpy(zptr, tmpBuf, pos);

        rc = this->m_page.put(head, zptr, &indicate, lba_used, true);
        if(rc) {
            goto errReturn;
        }

        while(!indicate) {
            // write back not finished, wait
        }

        if(indicate == -1) {
            rc = -EIO;
            goto errReturn;
        }

        return 0;

        errReturn:
        if(zptr) {
            m_page.freezptr(zptr, MAX_CLT_INFO_LBA_LEN);
        }
        return rc;
    }

    /*
        @note load the collection info from the storage
        @return 0 on success, else on failure
    */
    int loadFrom(const bidx& head) {

        int rc = 0;

        uint8_t nameLen = 0;
        uint8_t colSize = 0;

        uint32_t pos = 0;
        char tmpBuf[MAX_CLT_INFO_LBA_LEN * dpfs_lba_size];
        memset(tmpBuf, 0, MAX_CLT_INFO_LBA_LEN * dpfs_lba_size);

        cacheStruct* ce = nullptr;

        // acquire collection info block from storage
        rc = m_page.get(ce, head, MAX_CLT_INFO_LBA_LEN);
        if (rc != 0) {
            goto errReturn;
        }

        if(!ce) {
            rc = -EIO;
            goto errReturn;
        }
        rc = ce->read_lock();
        if(rc) {
            // if lock fail
            rc = -EIO;
            goto errReturn;
        }
        memcpy(tmpBuf, ce->getPtr(), ce->getLen() * dpfs_lba_size);

        ce->read_unlock();
        ce->release();
        ce = nullptr;

        // read from tmp data
        // read permissions
        m_perms.permByte = tmpBuf[pos];
        ++pos;
        // read data root
        rc = cpyFromle((char*)&m_dataRoot.gid, tmpBuf + pos, sizeof(uint64_t));
        if(rc) {
            goto errReturn;
        }
        pos += sizeof(uint64_t);
        rc = cpyFromle((char*)&m_dataRoot.bid, tmpBuf + pos, sizeof(uint64_t));
        if(rc) {
            goto errReturn;
        }
        pos += sizeof(uint64_t);

        // read collection name
        
        rc = cpyFromle((char*)&nameLen, tmpBuf + pos, sizeof(uint8_t));
        if(rc) {
            goto errReturn;
        }
        pos += sizeof(uint8_t);
        m_name.assign(tmpBuf + pos, nameLen);
        pos += nameLen;

        // read columns
        
        rc = cpyFromle((char*)&colSize, tmpBuf + pos, sizeof(uint8_t));
        if(rc) {
            goto errReturn;
        }
        pos += sizeof(uint8_t);

        /*
            COL : |NAMELEN|COLNAME|COLTYPE|COLLEN|COLSCALE|

        */
        for (size_t i = 0; i < colSize; ++i) {
            uint8_t colNameLen = tmpBuf[pos];
            ++pos;
            std::string colName(tmpBuf + pos, colNameLen);
            pos += colNameLen;

            dpfs_datatype_t colType = static_cast<dpfs_datatype_t>(tmpBuf[pos]);
            ++pos;

            uint32_t colLen = 0;
            rc = cpyFromle((char*)&colLen, tmpBuf + pos, sizeof(uint32_t));
            if(rc) {
                goto errReturn;
            }
            pos += sizeof(uint32_t);

            uint16_t colScale = 0;
            rc = cpyFromle((char*)&colScale, tmpBuf + pos, sizeof(uint16_t));
            if(rc) {
                goto errReturn;
            }
            pos += sizeof(uint16_t);
            uint8_t colConstraint = 0;
            rc = cpyFromle((char*)&colConstraint, tmpBuf + pos, sizeof(uint8_t));
            if(rc) {
                goto errReturn;
            }
            pos += sizeof(uint8_t);

            addCol(colName, colType, colLen, colScale, colConstraint);

            // CColumn* col = CColumn::newCol(colName, colType, colLen, colScale);
            // if(!col) {
            //     rc = -ENOMEM;
            //     goto errReturn;
            // }
            // m_lock.lock();
            // m_cols.emplace_back(col);
            // m_lock.unlock();
        }

        return 0;

        errReturn:
        if(ce) {
            ce->release();
            ce = nullptr;
        }
        return rc;
    }

private:
    /*
        @param data the buffer
        @param len buffer length
        @note save the data into tmpdatablock
    */
    int saveTmpData(const void* data, size_t len);

public:

    friend class CBPlusTree;

    CPage& m_page;
    CDiskMan& m_diskMan;
    CTempStorage m_tempStorage;
    // unit = B
    size_t curTmpDataLen = 0;
    char* tmpData = nullptr;

    union{
        struct perms {
            // permission of operations
            bool m_select : 1;
            bool m_updatable : 1;
            bool m_insertable : 1;
            bool m_detelable : 1;
            bool m_ddl : 1;
            // if dirty trigger a commit will flush data change to storage
            bool m_dirty : 1;
            // if data struct is changed, need a restore to reorganize data in storage
            bool m_needreorg : 1;
            // wether use b+ tree index
            bool m_btreeIndex : 1;
            bool m_systab : 1;
            bool : 7;
        } perm;
        uint16_t permByte = 0;
    } m_perms;
    //above 1B

    // inner locker 1B
    CSpin m_lock;
    // ccid: CCollection ID, used to identify the collection info 4B
    uint32_t m_ccid;
    // 16B
    bidx m_dataRoot {0, 0};
    // 16B
    bidx m_dataEndPos {0, 0};

    CBPlusTree* m_btreeIndex = nullptr;

    // the product that owns this collection
    // bidx m_ownerBid;
    CProduct& m_owner;

    // name of the collection
    std::string m_name;
    // columns in the collection

    // TODO:: 考虑列描述信息单独保存，以节省存储空间
    std::vector<CColumn*> m_cols;
    // primary key position in the columns
    uint8_t m_pkPos = 0;

    std::string message;

    // b plus tree head pointer or head block of seq storage
};

/*

    BLOCK 1 |basic info| include collections info, etc.
    BLOCK 2 |fixed info| change is not allowed
*/



// void qqqwwweee() {
//     sizeof(CCollection);
// }