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
#include <basic/dpfsvec.hpp>

class CBPlusTree;
class CProduct;
class CCollection;
class CColumn;
constexpr size_t MAX_COL_NAME_LEN = 64;
constexpr size_t MAX_NAME_LEN = 128;
constexpr size_t MAX_COL_NUM = 128;

constexpr size_t MAX_COLLECTION_INFO_LBA_SIZE = 4;

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
    CColumn(const std::string& colName, dpfs_datatype_t dataType, size_t dataLen = 0, size_t scale = 0, uint8_t constraint = 0) {
        if (colName.size() == 0 || colName.size() > MAX_COL_NAME_LEN) {
            throw std::invalid_argument("Invalid column name length");
        }

        memcpy(dds.colAttrs.colName, colName.c_str(), colName.size() + 1);
        dds.colAttrs.colNameLen = colName.size() + 1;
        dds.colAttrs.type = dataType;
        dds.colAttrs.len = dataLen;
        dds.colAttrs.scale = scale;
        dds.colAttrs.constraints.unionData = constraint;
    }

    CColumn(const CColumn& other) noexcept {

        memcpy(dds.colAttrs.colName, other.dds.colAttrs.colName, other.dds.colAttrs.colNameLen);
        dds.colAttrs.colNameLen = other.dds.colAttrs.colNameLen;
        dds.colAttrs.type = other.dds.colAttrs.type;
        dds.colAttrs.len = other.dds.colAttrs.len;
        dds.colAttrs.scale = other.dds.colAttrs.scale;
        dds.colAttrs.constraints.unionData = other.dds.colAttrs.constraints.unionData;
    }

    CColumn(CColumn&& other) noexcept {

        memcpy(dds.colAttrs.colName, other.dds.colAttrs.colName, other.dds.colAttrs.colNameLen);
        dds.colAttrs.colNameLen = other.dds.colAttrs.colNameLen;
        dds.colAttrs.type = other.dds.colAttrs.type;
        dds.colAttrs.len = other.dds.colAttrs.len;
        dds.colAttrs.scale = other.dds.colAttrs.scale;
        dds.colAttrs.constraints.unionData = other.dds.colAttrs.constraints.unionData;
    }

    CColumn& operator=(const CColumn& other) noexcept {
        
        memcpy(dds.colAttrs.colName, other.dds.colAttrs.colName, other.dds.colAttrs.colNameLen);
        dds.colAttrs.colNameLen = other.dds.colAttrs.colNameLen;
        dds.colAttrs.type = other.dds.colAttrs.type;
        dds.colAttrs.len = other.dds.colAttrs.len;
        dds.colAttrs.scale = other.dds.colAttrs.scale;
        dds.colAttrs.constraints.unionData = other.dds.colAttrs.constraints.unionData;

        return *this;
    }

    CColumn& operator=(CColumn&& other) noexcept {
        
        memcpy(dds.colAttrs.colName, other.dds.colAttrs.colName, other.dds.colAttrs.colNameLen);
        dds.colAttrs.colNameLen = other.dds.colAttrs.colNameLen;
        dds.colAttrs.type = other.dds.colAttrs.type;
        dds.colAttrs.len = other.dds.colAttrs.len;
        dds.colAttrs.scale = other.dds.colAttrs.scale;
        dds.colAttrs.constraints.unionData = other.dds.colAttrs.constraints.unionData;

        return *this;
    }

    void* operator new(size_t size) = delete;
    ~CColumn() {

    }

    bool operator==(const CColumn& other) const noexcept {
        if (dds.colAttrs.colNameLen != other.dds.colAttrs.colNameLen)
            return false;

        return (!strncmp((const char*)dds.colAttrs.colName, (const char*)other.dds.colAttrs.colName, dds.colAttrs.colNameLen) && 
            (dds.colAttrs.type == other.dds.colAttrs.type && dds.colAttrs.scale == other.dds.colAttrs.scale && dds.colAttrs.len == other.dds.colAttrs.len));
    }

    // delete a dpfs column and push the pointer to nullptr
    static void delCol(CColumn*& col) noexcept {
        if (col) {
            free(col);
        }
        col = nullptr;
    }

    uint8_t getNameLen() const noexcept {
        return dds.colAttrs.colNameLen;
    }

    const char* getName() const noexcept {
        return (const char*)dds.colAttrs.colName;
    }

    uint32_t getLen() const noexcept { return dds.colAttrs.len; }
    uint16_t getScale() const noexcept { return dds.colAttrs.scale; }
    dpfs_datatype_t getType() const noexcept { return dds.colAttrs.type; }
    size_t getStorageSize() const noexcept { return sizeof(dds); }

    // return data describe struct
    dds_field getDds() const noexcept{
        dds_field dds = {this->dds.colAttrs.len, this->dds.colAttrs.scale, this->dds.colAttrs.type};
        return dds;
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

    CColumn() = default;

    // column data describe struct
    union colDds_t {
        colDds_t() {
            memset(data, 0, sizeof(data));
        }

        ~colDds_t() = default;
        
        struct colAttr_t {

            // column data length
            uint32_t len;
            // for decimal
            uint16_t scale;
            // 1B
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
            uint8_t colNameLen = 0;
            char colName[MAX_COL_NAME_LEN];
            // smallData colName;
        } colAttrs;
        uint8_t data[sizeof(colAttr_t)];

    } dds;

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
    CValue getValueByKey(const CColumn& col) const noexcept;
   
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
    int updateValueByKey(const CColumn& col, CValue* value) noexcept;
    

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
    static CItem* newItem(const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM>& cs) noexcept;
    
    /*
        @param cs: column info
        @param maxRowNumber: maximum number of rows in the CItem
        @return CItem pointer on success, else nullptr
        @note this function will create a new CItem with the given column info
    */
    static CItem* newItems(const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM>& cs, size_t maxRowNumber) noexcept;
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
                if(isVariableType(pos.dds.colAttrs.type)) {
                    // first 4 bytes is actual length
                    uint32_t vallEN = *(uint32_t*)((char*)m_ptr);
                    m_ptr += sizeof(uint32_t) + vallEN;
                } else {
                    m_ptr += pos.getLen();
                }
            }

            ++m_pos;
            return *this;
        }

        bool operator!=(const rowIter& other) const {
            return (m_pos != other.m_pos);
        }

        rowIter& operator*() noexcept {
            return *this;
        }

        CValue operator[](size_t index) const noexcept {
            CValue val;
            size_t offSet = 0;
            for(size_t i = 0; i < index; ++i) {
                if(isVariableType(m_item->cols[i].dds.colAttrs.type)) {
                    offSet += sizeof(uint32_t) + (*(uint32_t*)((char*)m_ptr + offSet));
                } else {
                    offSet += m_item->cols[i].dds.colAttrs.len;
                }
            }

            if(m_item->cols[index].dds.colAttrs.type == dpfs_datatype_t::TYPE_VARCHAR) {
                // first 4 bytes is actual length
                val.len = *(uint32_t*)((char*)m_ptr + offSet);
                val.data = (char*)m_ptr + offSet + sizeof(uint32_t);
            } else {
                val.len = m_item->cols[index].dds.colAttrs.len;
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
    CItem(const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM>& cs);
    CItem(const CItem& other) = delete;
    ~CItem();
    int dataCopy(size_t pos, const CValue* value) noexcept;
    size_t getDataOffset(size_t pos) const noexcept;
    // int resetOffset(size_t begPos) noexcept;

    const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM>& cols;

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
    TODO process where clause
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
    CSeqStorage(bidx h, std::vector<CColumn>& cls, CPage& pge) : head(h), cols(cls), m_page(pge) {
        count = 0;
        rowLen = 0;
        for (auto& col : cols) {
            rowLen += col.getLen();
            if(isVariableType(col.getType())) {
                // variable length type not allowed
                throw std::invalid_argument("Variable length type not allowed in CSeqStorage");
            }
        }
    };
    ~CSeqStorage() {};

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
    std::vector<CColumn>& cols;
    CPage& m_page;


};

// static uint16_t defaultPerms = 0b0000000010011111;

struct CCollectionInitStruct {
    CCollectionInitStruct() {
        m_perms.permByte = 0b0000000010011111;
        name = "dpfs_dummy";
        id = 0;
        indexPageSize = 4;
    };
    ~CCollectionInitStruct() = default;
    uint32_t id = 0;
    uint8_t indexPageSize = 4;
    std::string name;
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
    CCollection();
    /*
        @param engine: dpfsEngine reference to the storage engine
        @param ccid: CCollection ID, used to identify the collection info, 0 for new collection
        @note this constructor will create a new collection with the given engine and ccid
    */
    CCollection(CProduct& owner, CDiskMan& dskman, CPage& pge);

    ~CCollection();

    // now only support row storage
    // enum class storageType {
    //     COL = 0,    // storage data by column   -> easy to add col
    //     ROW,        // storage data by row      -> easy to query
    //     MIX,        // storage data by mix of col and row
    // };

    int setName(const std::string& name);

    /*
        @param col: column to add
        @param type: data type of the col
        @param len: length of the data, if not specified, will use 0
        @param scale: scale of the data, only useful for decimal type
        @return 0 on success, else on failure
        @note this function will add the col to the collection
    */
    int addCol(const std::string& colName, dpfs_datatype_t type, size_t len = 0, size_t scale = 0, uint8_t constraint = 0);

    /*
        @param col: column to remove(or column to remove)
        @return 0 on success, else on failure
        @note this function will remove the col from the collection, and update the index
    */
    int removeCol(const std::string& colName);

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

        need TODO : 
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
    int saveTo(const bidx& head);

    /*
        @note load the collection info from the storage
        @return 0 on success, else on failure
    */
    int loadFrom(const bidx& head);

    /*
        @param id: CCollection ID, used to identify the collection info
        @return 0 on success, else on failure
        @note initialize the collection with the given id
    */
    int initialize(const CCollectionInitStruct& initStruct = CCollectionInitStruct());

    /*
        init b plus tree index for the collection, must be called after initialize and columns are set
        @return 0 on success, else on failure
    */
    int initBPlusTreeIndex() noexcept;

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


    struct collectionStruct {
    
        collectionStruct(void* dataPtr, size_t sz) : 
            data((uint8_t*)dataPtr),
            ds((dataStruct_t*)dataPtr),
            m_cols(ds->m_colsData, ds->m_colSize),
            size(sz) {
        };

        ~collectionStruct() {
            data = nullptr;
            ds = nullptr;
            size = 0;
        };
        
        struct dataStruct_t{
            dataStruct_t() = delete;
            ~dataStruct_t(){};

            //above 1B
            // 16B
            bidx m_dataRoot {0, 0};
            // 16B
            bidx m_dataBegin {0, 0};
            bidx m_dataEnd {0, 0};
            // ccid: CCollection ID, used to identify the collection info 4B
            uint32_t m_ccid = 0;
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
            uint8_t m_btreeHigh = 0;
            // default b plus tree index page size 4 * 4096 = 16KB
            uint8_t m_indexPageSize = 4;
            // name of the collection
            CColumn m_colsData[MAX_COL_NUM];
            uint8_t m_colSize = 0;
            uint8_t m_nameLen = 0;
            char m_name[MAX_NAME_LEN];
        }* ds = nullptr;
        uint8_t* data = nullptr;
        size_t size = 0;
        // TODO:: 考虑列描述信息单独保存，以节省存储空间
        CFixLenVec<CColumn, uint8_t, MAX_COL_NUM> m_cols;
    }* m_collectionStruct = nullptr;


    // inner locker 1B
    CSpin m_lock;
    CBPlusTree* m_btreeIndex = nullptr;
    // the product that owns this collection
    CProduct& m_owner;
    // columns in the collection
    // primary key position in the columns
    uint8_t m_pkPos = 0;
    std::string message;

    bool inited = false;
    // b plus tree head pointer or head block of seq storage

private:
    cacheStruct* m_cltInfoCache = nullptr;

};


