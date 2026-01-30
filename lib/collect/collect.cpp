#include <collect/collect.hpp>
#include <collect/bp.hpp>
#include <collect/product.hpp>

uint64_t nodeId = 0;

CColumn::CColumn(const std::string& colName, dpfs_datatype_t dataType, size_t dataLen, size_t scale, uint8_t constraint) {
    if (colName.size() == 0 || colName.size() > MAX_COL_NAME_LEN) {
        throw std::invalid_argument("Invalid column name length");
    }

    memcpy(dds.colAttrs.colName, colName.c_str(), colName.size() + 1);
    dds.colAttrs.colNameLen = colName.size();
    dds.colAttrs.type = dataType;
    dds.colAttrs.len = dataLen;
    dds.colAttrs.scale = scale;
    dds.colAttrs.constraints.unionData = constraint;
}

CColumn::CColumn(const CColumn& other) noexcept {

    memcpy(dds.colAttrs.colName, other.dds.colAttrs.colName, other.dds.colAttrs.colNameLen);
    dds.colAttrs.colNameLen = other.dds.colAttrs.colNameLen;
    dds.colAttrs.type = other.dds.colAttrs.type;
    dds.colAttrs.len = other.dds.colAttrs.len;
    dds.colAttrs.scale = other.dds.colAttrs.scale;
    dds.colAttrs.constraints.unionData = other.dds.colAttrs.constraints.unionData;
}

CColumn::CColumn(CColumn&& other) noexcept {

    memcpy(dds.colAttrs.colName, other.dds.colAttrs.colName, other.dds.colAttrs.colNameLen);
    dds.colAttrs.colNameLen = other.dds.colAttrs.colNameLen;
    dds.colAttrs.type = other.dds.colAttrs.type;
    dds.colAttrs.len = other.dds.colAttrs.len;
    dds.colAttrs.scale = other.dds.colAttrs.scale;
    dds.colAttrs.constraints.unionData = other.dds.colAttrs.constraints.unionData;
}

CColumn& CColumn::operator=(const CColumn& other) noexcept {
    
    memcpy(dds.colAttrs.colName, other.dds.colAttrs.colName, other.dds.colAttrs.colNameLen);
    dds.colAttrs.colNameLen = other.dds.colAttrs.colNameLen;
    dds.colAttrs.type = other.dds.colAttrs.type;
    dds.colAttrs.len = other.dds.colAttrs.len;
    dds.colAttrs.scale = other.dds.colAttrs.scale;
    dds.colAttrs.constraints.unionData = other.dds.colAttrs.constraints.unionData;

    return *this;
}

CColumn& CColumn::operator=(CColumn&& other) noexcept {
    
    memcpy(dds.colAttrs.colName, other.dds.colAttrs.colName, other.dds.colAttrs.colNameLen);
    dds.colAttrs.colNameLen = other.dds.colAttrs.colNameLen;
    dds.colAttrs.type = other.dds.colAttrs.type;
    dds.colAttrs.len = other.dds.colAttrs.len;
    dds.colAttrs.scale = other.dds.colAttrs.scale;
    dds.colAttrs.constraints.unionData = other.dds.colAttrs.constraints.unionData;

    return *this;
}

CColumn::~CColumn() {

}

bool CColumn::operator==(const CColumn& other) const noexcept {
    if (dds.colAttrs.colNameLen != other.dds.colAttrs.colNameLen)
        return false;

    return (!strncmp((const char*)dds.colAttrs.colName, (const char*)other.dds.colAttrs.colName, dds.colAttrs.colNameLen) && 
        (dds.colAttrs.type == other.dds.colAttrs.type && dds.colAttrs.scale == other.dds.colAttrs.scale && dds.colAttrs.len == other.dds.colAttrs.len));
}

uint8_t CColumn::getNameLen() const noexcept {
    return dds.colAttrs.colNameLen;
}

const char* CColumn::getName() const noexcept {
    return (const char*)dds.colAttrs.colName;
}

dds_field CColumn::getDds() const noexcept{
    dds_field dds(this->dds.colAttrs.len, this->dds.colAttrs.scale, this->dds.colAttrs.type, this->dds.colAttrs.constraints.unionData);
    return dds;
}

/*
    @param pos: position of the item in the item list, begin with 0
    @return CValue pointer on success, else nullptr
*/
CValue CItem::getValue(size_t pos) const noexcept {
    CValue val;
    if(pos >= cols.size()) {
        return val;
    }

    size_t offSet = getDataOffset(pos);

    if(cols[pos].dds.colAttrs.type == dpfs_datatype_t::TYPE_VARCHAR) {
        // first 4 bytes is actual length
        val.len = *(uint32_t*)((char*)rowPtr + offSet);
        val.data = (char*)rowPtr + offSet + sizeof(uint32_t);
    } else {
        val.len = cols[pos].dds.colAttrs.len;
        val.data = (char*)rowPtr + offSet;
    }

    return val;
}

// int CItem::resetOffset(size_t begPos) noexcept {
//     for(size_t i = begPos + 1; i < cols.size(); ++i) {
//         cols[i]->offSet = cols[i - 1]->offSet + cols[i - 1]->len;
//         // isVariableType(cols[i - 1]->type) ? cols[i]->offSet += 4 : 0;
//     }
//     return 0;
// }

size_t CItem::getDataOffset(size_t pos) const noexcept {

    return rowOffsets[pos];
    /*
    size_t offSet = 0;
    for(size_t i = 0; i < pos; ++i) {
        // if(isVariableType(cols[i].dds.colAttrs.type)) {
        //     offSet += sizeof(uint32_t) + (*(uint32_t*)(rowPtr + offSet));
        // } else {
        //     offSet += cols[i].dds.colAttrs.len;
        // }
        offSet += cols[i].dds.colAttrs.len;
    }
    return offSet;
    */
}

int CItem::dataCopy(size_t pos, const CValue& value) noexcept {
    // data offset in row
    size_t offSet = getDataOffset(pos);

    // if(isVariableType(cols[pos].dds.colAttrs.type)) {
    //     // first 4 bytes is actual length
    //     uint32_t actualLen = value.len;
    //     if(actualLen > cols[pos].dds.colAttrs.len) {
    //         return -E2BIG;
    //     }
    //     // use col offset of row to find the pointer and copy data
    //     *(uint32_t*)((char*)rowPtr + offSet) = actualLen;
    //     memcpy(((char*)rowPtr + offSet + sizeof(uint32_t)), value.data, actualLen);
    // } else {

    //     memcpy(((char*)rowPtr + offSet), value.data, value.len);
    //     if(value.len < cols[pos].dds.colAttrs.len) {
    //         memset(((char*)rowPtr + offSet + value.len), 0, cols[pos].dds.colAttrs.len - value.len);
    //     }
    // }

    memcpy(((char*)rowPtr + offSet), value.data, value.len);
    if(value.len < cols[pos].dds.colAttrs.len) {
        memset(((char*)rowPtr + offSet + value.len), 0, cols[pos].dds.colAttrs.len - value.len);
    }
    return value.len;
}

int CItem::dataCopy(size_t pos, const void* ptr, size_t len) noexcept {
    // data offset in row
    size_t offSet = getDataOffset(pos);

    // if(isVariableType(cols[pos].dds.colAttrs.type)) {
    //     // first 4 bytes is actual length
    //     uint32_t actualLen = len;
    //     if(actualLen > cols[pos].dds.colAttrs.len) {
    //         return -E2BIG;
    //     }
    //     // use col offset of row to find the pointer and copy data
    //     *(uint32_t*)((char*)rowPtr + offSet) = actualLen;
    //     memcpy(((char*)rowPtr + offSet + sizeof(uint32_t)), ptr, actualLen);
    // } else {

    //     memcpy(((char*)rowPtr + offSet), ptr, len);
    //     if(len < cols[pos].dds.colAttrs.len) {
    //         memset(((char*)rowPtr + offSet + len), 0, cols[pos].dds.colAttrs.len - len);
    //     }
    // }
    memcpy(((char*)rowPtr + offSet), ptr, len);
    if(len < cols[pos].dds.colAttrs.len) {
        memset(((char*)rowPtr + offSet + len), 0, cols[pos].dds.colAttrs.len - len);
    }
    return len;
}


/*
    @param col: column of the item, maybe column name
    @return CValue pointer on success, else nullptr
    @note this function will search the item list for the column, and possible low performance
*/
CValue CItem::getValueByKey(const CColumn& col) const noexcept {
    CValue val;

    for(size_t i = 0; i < cols.size(); ++i) {
        if (cols[i] == col) {
            size_t offSet = getDataOffset(i);
            if(cols[i].dds.colAttrs.type == dpfs_datatype_t::TYPE_VARCHAR) {
                // first 4 bytes is actual length
                val.len = *(uint32_t*)((char*)rowPtr + offSet);
                val.data = (char*)rowPtr + offSet + 4;
            } else {
                val.len = cols[i].dds.colAttrs.len;
                val.data = (char*)rowPtr + offSet;
            }
            break;
            // return (CValue*)((char*)data + offSet + (sizeof(CValue) * i));
        }
    }
    return val;
}

/*
    @param pos: position of the item in the item list
    @param value: CValue pointer to update
    @return bytes updated on success, else on failure
*/
int CItem::updateValue(size_t pos, const CValue& value) noexcept {
    if(pos >= cols.size()) {
        return -EINVAL;
    }
    if(cols[pos].getLen() < value.len) {
        return -E2BIG;
    }
    return dataCopy(pos, value);
}

/*
    @param pos: position of the item in the item list
    @param value: CValue pointer to update
    @return bytes updated on success, else on failure
*/
int CItem::updateValue(size_t pos, const void* ptr, size_t len) noexcept {
    if(pos >= cols.size()) {
        return -EINVAL;
    }
    if(cols[pos].getLen() < len) {
        return -E2BIG;
    }
    return dataCopy(pos, ptr, len);
}

/*
    @param col: col of the item, maybe column name
    @param value: CValue pointer to update
    @return CValue pointer on success, else nullptr
*/
int CItem::updateValueByKey(const CColumn& col, const CValue& value) noexcept {
    for(size_t i = 0; i < cols.size(); ++i) {
        if (cols[i] == col) {
            return dataCopy(i, value);
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
int CCollection::commit() { 
    // TODO
    int rc = 0;
    if(!m_collectionStruct->ds->m_perms.perm.m_dirty) {
        return 0;
    }

    void* zptr = nullptr;
    // lba length
    size_t zptrLen = 0;
    bidx tmpDataRoot = m_collectionStruct->ds->m_dataRoot;
    // len for root block
    size_t rootBlockLen = 0;


    if(!m_collectionStruct->ds->m_perms.perm.m_btreeIndex) {
        // not b+ tree index, all data can be save to storage only once
        if(m_collectionStruct->ds->m_dataRoot != bidx({0, 0})) {
            // save to storage root block
            rc = -EOPNOTSUPP;
            message = "commit to a fixed Collection index is not allow.";
            goto errReturn;
        }

        // total data block info structs in m_tempStorage
        size_t totalDataBlocks = m_tempStorage.size();
        if(totalDataBlocks == 0 && curTmpDataLen == 0) {
            // no data to commit
            m_collectionStruct->ds->m_perms.perm.m_dirty = false;
            return 0;
        }
        totalDataBlocks += curTmpDataLen % dpfs_lba_size == 0 ? curTmpDataLen / dpfs_lba_size : (curTmpDataLen / dpfs_lba_size + 1);

        // total root block length, including temp storage and remaining tmp data
        rootBlockLen = totalDataBlocks;

        uint64_t bid = m_diskMan.balloc(rootBlockLen);
        if(bid == 0) {
            rc = -ENOSPC;
            message = "no space left in the disk group.";
            goto errReturn;
        }

        m_collectionStruct->ds->m_dataRoot = {nodeId, bid};
        // return by m_tempstorage, indicate actual lba length got
        size_t real_lba_len = 0;

        // indicator for write complete
        int indicator = 0;
        for(size_t pos = 0; pos < totalDataBlocks; ) {
            if(m_tempStorage.size() <= 0) {
                break;
            }

            size_t write_lba_len = std::min(tmpBlockLbaLen, totalDataBlocks);
            // get data from temp storage
            zptrLen = write_lba_len;
            zptr = m_page.alloczptr(zptrLen);
            if(!zptr) {
                zptrLen = 0;
                rc = -ENOMEM;
                goto errReturn;
            }


            // from temp storage get data, save to zptr, then put to root of the collection
            rc = m_tempStorage.getData(pos, zptr, write_lba_len, real_lba_len);            
            if(rc < 0) {
                goto errReturn;
            }

            
            // write data to storage immediately
            if(pos == totalDataBlocks - 1 && curTmpDataLen == 0) {
                if(curTmpDataLen) {
                    // last write with remaining data
                    rc = m_page.put(tmpDataRoot, (char*)zptr, nullptr, write_lba_len, true);
                } else {
                    // last write with no remaining data
                    rc = m_page.put(tmpDataRoot, (char*)zptr, &indicator, write_lba_len, true);
                }
            } else {
                rc = m_page.put(tmpDataRoot, (char*)zptr, nullptr, write_lba_len, true);
            }
            if(rc < 0) {
                goto errReturn;
            }
            tmpDataRoot.bid += write_lba_len;
            zptrLen = 0;
            pos += real_lba_len;

        }

        if(curTmpDataLen) {
            // write remaining data
            zptrLen = curTmpDataLen % dpfs_lba_size == 0 ? curTmpDataLen / dpfs_lba_size : (curTmpDataLen / dpfs_lba_size + 1);
            zptr = m_page.alloczptr(zptrLen);
            if(!zptr) {
                zptrLen = 0;
                rc = -ENOMEM;
                goto errReturn;
            }
            memset(zptr, 0, zptrLen * dpfs_lba_size);
            memcpy(zptr, tmpData, curTmpDataLen);

            // write data to storage immediately
            rc = m_page.put(tmpDataRoot, (char*)zptr, &indicator, zptrLen, true);
            if(rc < 0) {
                goto errReturn;
            }
            tmpDataRoot.bid += zptrLen;
            zptrLen = 0;
            curTmpDataLen = 0;
            memset(tmpData, 0, tmpBlockLen);

            m_collectionStruct->ds->m_dataEnd = tmpDataRoot;
        }

        // wait for last write complete
        while(indicator == 0) {
            
        }

        m_tempStorage.clear();
        curTmpDataLen = 0;
        memset(tmpData, 0, curTmpDataLen);

    } else {
        // m_owner.m_log.log_inf("commit collection with b+ tree index.\n");
        // CBPlusTree bpt(&m_page, &m_diskMan, nodeId, m_dataRoot, m_log);

        // TODO::
        // write commit log
        // write data to storage
        m_btreeIndex->commit();
        // write to this->m_dataRoot
        // update index
    }


    m_collectionStruct->ds->m_perms.perm.m_dirty = false;

    return rc;
errReturn:

    if(zptr && zptrLen > 0) {
        m_page.freezptr(zptr, zptrLen);
    }

    return rc;
};

/*
    @param cs: column info
    @return CItem pointer on success, else nullptr
    @note this function will create a new CItem with the given column info
*/
CItem* CItem::newItem(const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM>& cs) noexcept {
    size_t len = 0;
    std::vector<size_t> rowOffsets;
    rowOffsets.reserve(cs.size() + 1);
    rowOffsets.emplace_back(0);
    for(size_t i = 0; i < cs.size(); ++i) {
        len += cs[i].getLen();
        rowOffsets.emplace_back(len);
        // isVariableType(cs[i].dds.colAttrs.type) ? len += 4 : 0;
    }

    // row data in data[]
    CItem* item = (CItem*)malloc(sizeof(CItem) + len);
    if(!item) {
        return nullptr;
    }
    new (item) CItem(cs);
    item->rowLen = len;
    item->maxRowNumber = 1;
    item->rowNumber = 0;
    item->rowPtr = item->data;
    item->rowOffsets.swap(rowOffsets);
    return item;
}

/*
    @param cs: column info
    @return CItem pointer on success, else nullptr
    @note this function will create a new CItem with the given column info
*/
CItem* CItem::newItems(const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM>& cs, size_t maxRowNumber) noexcept {
    size_t len = 0;
    std::vector<size_t> rowOffsets;
    rowOffsets.reserve(cs.size() + 1);
    rowOffsets.emplace_back(0);
    for(size_t i = 0; i < cs.size(); ++i) {
        len += cs[i].getLen();
        rowOffsets.emplace_back(len);
        // isVariableType(cs[i].dds.colAttrs.type) ? len += 4 : 0;
    }

    // row data in data[]
    CItem* item = (CItem*)malloc(sizeof(CItem) + len * maxRowNumber);
    if(!item) {
        return nullptr;
    }
    new (item) CItem(cs);
    item->rowLen = len;
    item->maxRowNumber = maxRowNumber;
    item->rowNumber = 1;
    item->rowPtr = item->data;
    item->rowOffsets.swap(rowOffsets);
    return item;
}

void CItem::delItem(CItem*& item) noexcept {
    if(item) {
        free(item);
    }
    item = nullptr;
}

// private:

/*
    @param cs column info
    @param value of row data
*/
CItem::CItem(const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM>& cs) : 
cols(cs), 
beginIter(this), 
endIter(this) {

    size_t len = 0;
    std::vector<size_t> rowOffsets;
    rowOffsets.reserve(cs.size() + 1);
    rowOffsets.emplace_back(0);
    for(size_t i = 0; i < cs.size(); ++i) {
        len += cs[i].getLen();
        rowOffsets.emplace_back(len);
        // isVariableType(cs[i].dds.colAttrs.type) ? len += 4 : 0;
    }

    // row data in data[]
    data = (char*)malloc(len);
    if(!data) {
        throw std::bad_alloc();
    }

    this->rowLen = len;
    this->maxRowNumber = 1;
    this->rowNumber = 0;
    this->rowPtr = data;
    this->rowOffsets.swap(rowOffsets);

    locked = false;
    error = false;
    beginIter.m_pos = 0;
    beginIter.m_ptr = data;
    endIter.m_pos = 0;
    endIter.m_ptr = data;
}

CItem::CItem(const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM>& cs, size_t maxRowNumber) : 
cols(cs), 
beginIter(this), 
endIter(this) {
    size_t len = 0;
    std::vector<size_t> rowOffsets;
    rowOffsets.reserve(cs.size() + 1);
    rowOffsets.emplace_back(0);
    for(size_t i = 0; i < cs.size(); ++i) {
        len += cs[i].getLen();
        rowOffsets.emplace_back(len);
        // isVariableType(cs[i].dds.colAttrs.type) ? len += 4 : 0;
    }

    // row data in data[]
    data = (char*)malloc(len * maxRowNumber);
    if(!data) {
        throw std::bad_alloc();
    }

    this->rowLen = len;
    this->maxRowNumber = maxRowNumber;
    this->rowNumber = 1;
    this->rowPtr = data;
    this->rowOffsets.swap(rowOffsets);

    locked = false;
    error = false;
    beginIter.m_pos = 0;
    beginIter.m_ptr = data;
    endIter.m_pos = 0;
    endIter.m_ptr = data;
}

int CItem::nextRow() noexcept {
    if(rowNumber + 1 >= maxRowNumber) {
        return -ERANGE;
    }
    ++rowNumber;
    endIter.m_pos = rowNumber;

    for(auto& pos : cols) {
        // if(isVariableType(pos.dds.colAttrs.type)) {
        //     // first 4 bytes is actual length
        //     uint32_t vallEN = *(uint32_t*)((char*)rowPtr);
        //     rowPtr += sizeof(uint32_t) + vallEN;
        //     validLen += sizeof(uint32_t) + vallEN;
        // } else {
        //     rowPtr += pos.getLen();
        //     validLen += pos.getLen();
        // }
        rowPtr += pos.getLen();
        validLen += pos.getLen();
    }

    return 0;
}

int CItem::resetScan() noexcept {
    rowNumber = 0;
    rowPtr = data;
    return 0;
}

CItem::~CItem() {
    if (data) {
        free(data);
        data = nullptr;
    }
}

// use ccid to locate the collection (search in system collection table)
/*
    @param engine: dpfsEngine reference to the storage engine
    @param ccid: CCollection ID, used to identify the collection info, 0 for new collection
    @note this constructor will create a new collection with the given engine and ccid
*/
CCollection::CCollection(CDiskMan& dskman, CPage& pge) : 
// m_owner(owner), 
m_page(pge), 
m_diskMan(dskman), 
m_tempStorage(m_page, m_diskMan) { 
    
};

CCollection::~CCollection() {
    if(tmpData) {
        free(tmpData);
        tmpData = nullptr;
    }
    if(m_cltInfoCache) {
        m_cltInfoCache->release();
        m_cltInfoCache = nullptr;
    } else {
        if(m_collectionStruct) {
            m_page.freezptr(m_collectionStruct->data, MAX_CLT_INFO_LBA_LEN);
        }
    }
    if(m_collectionStruct) {
        delete m_collectionStruct;
        m_collectionStruct = nullptr;
    }
    inited = false;

};

int CCollection::setName(const std::string& name) {
    if(name.size() > MAX_NAME_LEN) {
        return -ENAMETOOLONG; // Key length exceeds maximum allowed size
    }
    memset(m_collectionStruct->ds->m_name, 0, MAX_NAME_LEN);
    m_collectionStruct->ds->m_nameLen = name.size() + 1;
    mempcpy(m_collectionStruct->ds->m_name, name.c_str(), name.size() + 1);

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
int CCollection::addCol(const std::string& colName, dpfs_datatype_t type, size_t len, size_t scale, uint8_t constraint) {
    if(colName.size() > MAX_COL_NAME_LEN) {
        return -ENAMETOOLONG; // Key length exceeds maximum allowed size
    }

    if(m_collectionStruct->m_cols.size() >= MAX_COL_NUM) {
        return -E2BIG; // Too many columns
    }

    // CColumn* col = nullptr;

    switch(type) {
        case dpfs_datatype_t::TYPE_DECIMAL: 
        case dpfs_datatype_t::TYPE_CHAR:
        case dpfs_datatype_t::TYPE_VARCHAR:
        case dpfs_datatype_t::TYPE_BINARY:
        case dpfs_datatype_t::TYPE_BLOB:
            if (len == 0) {
                return -EINVAL; // Invalid length for string or binary type
            }
            if (len > MAX_COL_LEN) {
                return -E2BIG; // Length exceeds maximum allowed size for string or binary type
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
        case dpfs_datatype_t::TYPE_TIMESTAMP:
            len = 10;
            // col = CColumn::newCol(colName, type, len, scale);
            break;
        default:
            return -EINVAL; // Invalid data type
    }

    // col = CColumn::newCol(colName, type, len, scale, constraint);
    // // if generate column fail
    // if(!col) {
    //     return -ENOMEM;
    // }

    if(constraint & CColumn::constraint_flags::PRIMARY_KEY) {
        m_pkPos = m_collectionStruct->m_cols.size();
    }
    m_lock.lock();
    m_collectionStruct->m_cols.emplace_back(colName, type, len, scale, constraint);
    m_lock.unlock();
    
    this->m_collectionStruct->ds->m_perms.perm.m_dirty = true;
    this->m_collectionStruct->ds->m_perms.perm.m_needreorg = true;
    m_rowLen += len;
    return 0;
}

/*
    @param col: column to remove(or column to remove)
    @return 0 on success, else on failure
    @note this function will remove the col from the collection, and update the index
*/
int CCollection::removeCol(const std::string& colName) {
    for(auto it = m_collectionStruct->m_cols.begin(); it != m_collectionStruct->m_cols.end(); ++it) {
        if((*it).getNameLen() != colName.size() + 1) {
            continue;
        }
        if(memcmp((*it).getName(), colName.c_str(), (*it).getNameLen()) == 0) {
            m_lock.lock();
            m_collectionStruct->m_cols.erase(it);
            m_lock.unlock();
            m_collectionStruct->ds->m_perms.perm.m_dirty = true;
            m_collectionStruct->ds->m_perms.perm.m_needreorg = true;
            break;
        }
    }

    return -ENODATA;
};

/*
    @param data the buffer
    @param len buffer length
    @return 0 on success, else on failure
    @note save the data into tmpdatablock
*/
int CCollection::saveTmpData(const void* data, size_t len){

    const uint8_t* datap = (const uint8_t*)data;
    if(curTmpDataLen + len > tmpBlockLen * dpfs_lba_size) {
        // need to flush temp storage

        
        // save in-length data, switch to new tmpBlock
        memcpy(tmpData + curTmpDataLen, datap, tmpBlockLen - curTmpDataLen);
        // cross copied data
        datap += tmpBlockLen - curTmpDataLen;
        // decline copied data len
        len -= tmpBlockLen - curTmpDataLen;
        // put extra data to next add item function call
        
        m_tempStorage.pushBackData(tmpData, tmpBlockLen % dpfs_lba_size == 0 ? tmpBlockLen / dpfs_lba_size : (tmpBlockLen / dpfs_lba_size + 1));
        curTmpDataLen = 0;
        memset(tmpData, 0, tmpBlockLen);

        return saveTmpData(datap, len);
        
    }

    memcpy(tmpData + curTmpDataLen, datap, len);
    curTmpDataLen += len;
    // m_tempStorage.pushBackData(tmpData, tmpBlockLen);

    return 0;
}

/*
    @param item: item to add
    @return 0 on success, else on failure
    @note this function will add the item to the collection storage and update the index
*/
int CCollection::addItem(const CItem& item) {

    // save item to storage and update index
    // search where the item should be in storage, use b plus tree to storage the data
    
    // bpt.insert(key, value);
    int rc = 0;
    const char* dataPtr = item.data;
    size_t validLen = 0;


    if(m_collectionStruct->ds->m_perms.perm.m_btreeIndex) {
        // separate key and value from item
        for(auto it = item.begin(); it != item.end(); ++it) {
            char keyBuf[MAXKEYLEN];
            KEY_T key(keyBuf, MAXKEYLEN, m_btreeIndex->cmpTyps);

            key.len = item.cols[m_pkPos].getLen();
            // size_t offSet = item.getDataOffset(m_pkPos);

            CValue keyVal = it[m_pkPos];
            memcpy(key.data, keyVal.data, keyVal.len);

            /*
                |KEY|data....|
            */

            #ifdef __COLLECT_DEBUG__
            cout << " key :: " << endl;
            printMemory(key.data, key.len);
            cout << " value :: " << endl;
            printMemory(it.getRowPtr(), it.getRowLen());

            cout << "pos " << (it - item.begin()) << " insert key len " << key.len << " value len " << it.getRowLen() << endl;
            cout << "------------------------" << endl;
            #endif

            m_btreeIndex->insert(key, it.getRowPtr(), it.getRowLen());
        }

        // use b plus tree to index the item
    }

    char* rowPtr = item.rowPtr;
    // get len of the data
    // const char* dataPtr = item.data;
    // size_t validLen = 0; // item.validLen;
    // char* rowPtr = item.rowPtr;
    for(size_t i = 0; i < item.rowNumber; ++i){
        for(auto& pos : item.cols) {
            // if(isVariableType(pos.dds.colAttrs.type)) {
            //     // first 4 bytes is actual length
            //     //last row ptr
            //     uint32_t vallEN = *(uint32_t*)((char*)rowPtr);
            //     rowPtr += sizeof(uint32_t) + vallEN;
            //     validLen += sizeof(uint32_t) + vallEN;
            // } else {
            //     rowPtr += pos.getLen();
            //     validLen += pos.getLen();
            // }
            rowPtr += pos.getLen();
            validLen += pos.getLen();
        }
    }

    if(!tmpData) {
        // tmpblklen = 10 blk for now
        tmpData = (char*)malloc(tmpBlockLen);
        if(!tmpData) {
            rc = -ENOMEM;
            goto errReturn;
        }
    }
    
    saveTmpData(dataPtr, validLen);

    return 0;

errReturn:

    return rc;
}

/*
    @note save the collection info to the storage
    @return 0 on success, else on failure
*/
int CCollection::saveTo(const bidx& head) {

    /*
        |perm 1B|nameLen 1B|dataroot 2B|collection name|columns|
    */
    int rc = 0;
    int indicate = 0;

    if(!inited) {
        return -EINVAL;
    }

    if(B_END) {
        // TODO : convert to storage endian (convert to little endian)
        // target -> m_collectionStruct.data
    }

    if(m_cltInfoCache) {
        // if is load from disk, write back the disk rather than put new block
        rc = m_page.writeBack(m_cltInfoCache, &indicate);
        if(rc != 0) {
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
    }   

    if(!m_collectionStruct) {
        rc = -EINVAL;
        goto errReturn;
    }

    rc = this->m_page.put(head, m_collectionStruct->data, &indicate, MAX_COLLECTION_INFO_LBA_SIZE, true, &m_cltInfoCache);
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

    return rc;
}

/*
    @note load the collection info from the storage
    @return 0 on success, else on failure
*/
int CCollection::loadFrom(const bidx& head) {
    if(inited) {
        return -EALREADY;
    }
    int rc = 0;
    
    cacheStruct* ce = nullptr;

    // acquire collection info block from storage
    rc = m_page.get(ce, head, MAX_COLLECTION_INFO_LBA_SIZE);
    if (rc != 0) {
        goto errReturn;
    }
    if(!ce) {
        rc = -EIO;
        goto errReturn;
    }


    m_collectionStruct = new collectionStruct(ce->getPtr(), ce->getLen() * dpfs_lba_size);
    if(!m_collectionStruct) {
        rc = -ENOMEM;
        goto errReturn;
    }

    if(B_END) {
        // TODO: convert to host endian
        
    }
    
    m_rowLen = 0;
    for(uint32_t i = 0; i < m_collectionStruct->m_cols.size(); ++i) {
        m_rowLen += m_collectionStruct->m_cols[i].getLen();
    }

    if(m_collectionStruct->ds->m_perms.perm.m_btreeIndex) {
        rc = initBPlusTreeIndex();
        if(rc != 0) {
            goto errReturn;
        }
        // m_btreeIndex = new CBPlusTree(*this, m_page, m_diskMan, m_collectionStruct->ds->m_indexPageSize);
        // if(!m_btreeIndex) {
        //     return -ENOMEM;
        // }
    }
    m_cltInfoCache = ce;
    inited = true;
    m_collectionBid = head;
    return 0;

    errReturn:
    if(ce) {
        ce->release();
        ce = nullptr;
    }

    if(m_collectionStruct) {
        delete m_collectionStruct;
        m_collectionStruct = nullptr;
    }
    return rc;
}

/*
    @param id: CCollection ID, used to identify the collection info
    @return 0 on success, else on failure
    @note initialize the collection with the given id
*/
int CCollection::initialize(const CCollectionInitStruct& initStruct) {
    if(inited) {
        return 0;
    }

    void* zptr = m_page.alloczptr(MAX_CLT_INFO_LBA_LEN);
    if(!zptr) {
        return -ENOMEM;
    }

    m_collectionStruct = new collectionStruct(zptr, MAX_CLT_INFO_LBA_LEN * dpfs_lba_size);
    if(!m_collectionStruct) {
        m_page.freezptr(zptr, MAX_CLT_INFO_LBA_LEN);
        return -ENOMEM;
    }

    m_collectionStruct->ds->m_ccid = initStruct.id;
    memcpy(m_collectionStruct->ds->m_name, initStruct.name.c_str(), initStruct.name.size());
    m_collectionStruct->ds->m_nameLen = initStruct.name.size();


    m_collectionStruct->ds->m_perms.permByte = initStruct.m_perms.permByte;
    m_collectionStruct->ds->m_indexPageSize = initStruct.indexPageSize;
    m_collectionStruct->m_cols.clear();

    curTmpDataLen = 0;

    inited = true;
    return 0;
}

int CCollection::initBPlusTreeIndex() noexcept {
    if(!m_collectionStruct->ds->m_perms.perm.m_btreeIndex) {
        message = "collection has no b+ tree index.";
        return -EINVAL;
    }
    if(!m_btreeIndex) {

        size_t keyLen = 0;
        m_rowLen = 0;
        for(uint32_t i = 0; i < m_collectionStruct->m_cols.size(); ++i) {
            const CColumn& col = m_collectionStruct->m_cols[i];

            if (col.getDds().constraints.ops.primaryKey) {
                m_cmpTyps.emplace_back(std::make_pair(col.getLen(), col.getType()));
                keyLen += static_cast<uint8_t>(col.getLen());
            }
            uint32_t dataLen = col.getLen();
            if (dataLen > maxInrowLen) {
                dataLen = maxInrowLen;
            }
            m_rowLen += dataLen;
        }
        
        // if key is too big, the index performance may degrade, so return error.
        if(keyLen > MAXKEYLEN) {
            message = "key length exceed maximum allowed size.";
            return -E2BIG;
        }

        if (m_rowLen > MAXROWLEN) {
            message = "row length exceed maximum allowed size.";
            return -E2BIG;
        }

        m_keyLen = keyLen;

        auto& ds = m_collectionStruct->ds;

        m_btreeIndex = new CBPlusTree(m_page, m_diskMan, ds->m_indexPageSize, 
        ds->m_btreeHigh,
        ds->m_dataRoot,
        ds->m_dataBegin,
        ds->m_dataEnd,
        m_keyLen,
        m_rowLen,
        m_cmpTyps);
        if(!m_btreeIndex) {
            message = "allocate b+ tree index fail.";
            return -ENOMEM;
        }
    }
    return 0;
}

// get one row
int CCollection::getRow(KEY_T key, CItem* out) const {

    if(!m_collectionStruct->ds->m_perms.perm.m_btreeIndex) {
        message = "collection has no b+ tree index.";
        return -EINVAL;
    }

    int rc = m_btreeIndex->search(key, out->data, out->rowLen);
    if(rc != 0) {
        message = "search key in b+ tree index fail.";
        return rc;
    }

    out->endIter.m_pos = 1;
    out->endIter.m_ptr = out->data + out->rowLen;

    return 0;
}

int CCollection::createIdx(const CIndexInitStruct& initStruct) {

    auto& indexVec = m_collectionStruct->m_indexInfos;

    if (indexVec.size() > MAX_INDEX_NUM) {
        message = "exceed maximum index number.";
        return -E2BIG;
    }

    if (initStruct.colNames.size() > MAX_INDEX_COL_NUM) {
        message = "exceed maximum index column number.";
        return -E2BIG;
    }

    if (initStruct.name.size() > MAX_NAME_LEN) {
        message = "index name too long.";
        return -ENAMETOOLONG;
    }

    if (initStruct.colNames.size() == 0) {
        message = "no index column specified.";
        return -EINVAL;
    }
    int rc = 0;

    rc = m_cltInfoCache->read_lock();
    if (rc != 0) {
        message = "collection cache read lock fail.";
        return rc;
    }

    // check if index name already exists
    for (auto& idxInfo : indexVec) {
        if (static_cast<size_t>(idxInfo.nameLen) != initStruct.name.size()) {
            continue;
        }

        if (memcmp(idxInfo.name, initStruct.name.c_str(), idxInfo.nameLen) == 0) {
            m_cltInfoCache->read_unlock();
            message = "index name already exists.";
            return -EEXIST;
        }
    }
    
    m_cltInfoCache->read_unlock();


    uint32_t backPos = indexVec.size();
    // pushback new index info
    CTemplateGuard lockGuard(*m_cltInfoCache);
    if(lockGuard.returnCode() != 0) {
        message = "collection cache lock fail.";
        return lockGuard.returnCode();
    }

    indexVec.emplace_back();
    
    auto& idxInfo = indexVec[backPos];

    idxInfo.indexPageSize = initStruct.indexPageSize;
    idxInfo.indexHigh = 0;
    idxInfo.indexRoot = {0, 0};
    idxInfo.indexBegin = {0, 0};
    idxInfo.indexEnd = {0, 0};    

    // name
    idxInfo.nameLen = initStruct.name.size();
    // copy with null terminator
    mempcpy(idxInfo.name, initStruct.name.c_str(), initStruct.name.size() + 1);

    // init compare types
    CFixLenVec<cmpType, uint8_t, MAX_INDEX_COL_NUM> cmpTyps(idxInfo.cmpTypes, idxInfo.cmpKeyColNum);

    uint32_t collectionPkLen = 0;

    for(auto& cmpTyp : m_cmpTyps) {
        collectionPkLen += cmpTyp.first;
    }

    // index primary key length, not origin table pk length
    uint32_t pkLen = 0;
    uint8_t idxSeqPos = 0;
    // full fill the index key info
    for(auto& nms : initStruct.colNames) {
        uint8_t pos = 0;
        for(uint32_t i = 0; i < m_collectionStruct->m_cols.size(); ++i) {
            auto& col = m_collectionStruct->m_cols[i];
            if(col.getNameLen() != nms.size()) {
                ++pos;
                continue;
            }

            if(memcmp(col.getName(), nms.c_str(), col.getNameLen()) == 0) {
                // found the column
                idxInfo.keySequence[idxSeqPos] = pos;
                cmpTyps.emplace_back();
                cmpTyps[idxSeqPos].colLen = col.getLen();
                cmpTyps[idxSeqPos].colType = col.getType();
                ++idxSeqPos;
                pkLen += col.getLen();
                break;
            }
            ++pos;
        }
    }

    if (cmpTyps.size() == 0) {
        message = "no valid index column found.";
        return -EINVAL;
    }

    CFixLenVec<CColumn, uint8_t, 2> idxCols(idxInfo.indexCol, idxInfo.idxColNum);
    idxInfo.indexKeyLen = pkLen;
    idxInfo.indexRowLen = collectionPkLen + pkLen;
    idxCols.emplace_back("IDXPKCOL", dpfs_datatype_t::TYPE_BINARY, pkLen, 0,  CColumn::constraint_flags::NOT_NULL | CColumn::constraint_flags::PRIMARY_KEY);
    idxCols.emplace_back("PKDATACOL", dpfs_datatype_t::TYPE_BINARY, collectionPkLen, 0, CColumn::constraint_flags::NOT_NULL);


    if(m_btreeIndex && this->inited) {
        // compare types for index, this cmpTp is different from origin collection cmpTyps
        // this cmpTp contains the index columns, and index pk are saved as binary type
        std::vector<std::pair<uint8_t, dpfs_datatype_t>> cmpTp;
        
        for(uint32_t i = 0; i < cmpTyps.size(); ++i) {
            cmpTp.emplace_back(std::make_pair(cmpTyps[i].colLen, cmpTyps[i].colType));
        }

        CBPlusTree indexTree(m_page, m_diskMan, idxInfo.indexPageSize, idxInfo.indexHigh, 
        idxInfo.indexRoot, idxInfo.indexBegin, idxInfo.indexEnd, pkLen, collectionPkLen + pkLen, cmpTp);


        auto it = m_btreeIndex->begin();
        auto end = m_btreeIndex->end();
        rc = it.loadNode();
        if (rc != 0) {
            message = "load b+ tree node fail.";
            return rc;
        }
        char* rowPtr = new char[m_rowLen];
        for(; it != end; ++it) {
            // for each item in the collection, insert into the index
            uint32_t rowLenIndicator = 0;
            char originPkData[MAXKEYLEN * 2];
            uint32_t keyLenIndicator = 0;
            
            CItem item(m_collectionStruct->m_cols);


            // save origin pk data to second col position
            rc = it.loadData(originPkData + pkLen, MAXKEYLEN, keyLenIndicator, item.data, m_rowLen, rowLenIndicator); 
            if (rc != 0) {
                delete[] rowPtr;
                message = "load b+ tree data fail.";
                return rc;
            }

            // extract index key and pk data from rowPtr
            // contain (indexPKdata + originPKdata)
            // char idxPkData[MAXKEYLEN * 2];

            auto& itb = item.begin();

            uint32_t copiedData = 0;
            for(int i = 0; i < idxInfo.idxColNum; ++i) {
                CValue val = itb[idxInfo.keySequence[i]];
                memcpy(originPkData + copiedData, val.data, val.len);
                copiedData += val.len;
            }


            KEY_T idxKey(originPkData, copiedData, cmpTp);


            // insert into index tree
            rc = indexTree.insert(idxKey, originPkData, keyLenIndicator + pkLen);
            if (rc != 0) {
                delete[] rowPtr;
                message = "insert index key fail.";
                cout << " now tree :: " << endl;
                indexTree.printTree();
                return rc;
            }
#ifdef __COLLECT_DEBUG__
            cout << " get key :: " << endl;
            printMemory(idxKey.data, idxKey.len);
            cout << " get pk data :: " << endl;
            printMemory(originPkData, keyLenIndicator);
            // indexTree.printTree();
#endif
        }
#ifdef __COLLECT_DEBUG__
        cout << "\n\n\n\n\n\n\n\n\n\n" << endl;
        indexTree.printTree();
#endif
        delete[] rowPtr;

        rc = indexTree.commit();
        if (rc != 0) {
            message = "commit index tree fail.";
            return rc;
        }
    }



    return 0;

}

// TODO :: TEST DESTROY
/*
    @return 0 on success, else on failure
    @note destroy the collection and free the resources
*/
int CCollection::destroy() {
    if (!inited) {
        return -EINVAL;
    }

    if (!m_collectionStruct) {
        return -EINVAL;
    }
    int rc = 0;
    if (m_btreeIndex) {
        rc = m_btreeIndex->destroy(); if (rc != 0) return rc;
        delete m_btreeIndex;
        m_btreeIndex = nullptr;

        // free index blocks
        for(uint32_t i = 0; i < m_collectionStruct->m_indexInfos.size(); ++i) {
            auto& info = m_collectionStruct->m_indexInfos[i];
            std::vector<std::pair<uint8_t, dpfs_datatype_t>> cmpTp;
            cmpTp.reserve(info.cmpKeyColNum);
            for(uint32_t i = 0; i < info.cmpKeyColNum; ++i) {
                cmpTp.emplace_back(std::make_pair(info.cmpTypes[i].colLen, info.cmpTypes[i].colType));
            }

            CBPlusTree indexTree(m_page, m_diskMan, info.indexPageSize, info.indexHigh, info.indexRoot,
                info.indexBegin, info.indexEnd, info.indexKeyLen, info.indexRowLen, cmpTp);
            rc = indexTree.destroy(); if (rc != 0) return rc;
            
            m_diskMan.bfree(m_collectionStruct->m_indexInfos[i].indexRoot.bid, MAX_COLLECTION_INFO_LBA_SIZE);
        }
    } else {
        // limit support right now
        // no b plus tree index
        // free data blocks
        m_diskMan.bfree(m_collectionStruct->ds->m_dataRoot.bid, MAX_COLLECTION_INFO_LBA_SIZE);
    }

    // free collection info block
    m_diskMan.bfree(m_collectionBid.bid, MAX_COLLECTION_INFO_LBA_SIZE);

    return 0;
}


int CCollection::searchByIndex(const std::vector<std::string>& colNames, const std::vector<CValue>& keyVals, CItem& out) const {

    int rc = 0;
    CCollectIndexInfo* indexInfo = nullptr;
    bool match = false;
    for (uint32_t i = 0; i < m_collectionStruct->m_indexInfos.size(); ++i) {

        if (m_collectionStruct->m_indexInfos[i].cmpKeyColNum != colNames.size()) {
            continue;
        }
        match = true;
        auto& keyseq = m_collectionStruct->m_indexInfos[i].keySequence;
        for (uint32_t j = 0; j < m_collectionStruct->m_indexInfos[i].cmpKeyColNum; ++j) {

            if (colNames[j].size() != m_collectionStruct->m_cols[keyseq[j]].getNameLen()) {
                // cout << " length not match " << endl;
                match = false;
                break;
            }

            if (memcmp(m_collectionStruct->m_cols[keyseq[j]].getName(), colNames[j].c_str(), m_collectionStruct->m_cols[keyseq[j]].getNameLen()) != 0) {
                // cout << " name not match " << endl;
                match = false;
                break;
            }
        }

        if (match) {
            indexInfo = &m_collectionStruct->m_indexInfos[i];
            break;
        }
    }

    if (!match) {
        message = "no index found for given columns";
        return -ENOENT;
    }

    std::vector<std::pair<uint8_t, dpfs_datatype_t>> cmpTp;
    cmpTp.reserve(indexInfo->cmpKeyColNum);
    for(uint32_t i = 0; i < indexInfo->cmpKeyColNum; ++i) {
        cmpTp.emplace_back(std::make_pair(indexInfo->cmpTypes[i].colLen, indexInfo->cmpTypes[i].colType));
    }

    CBPlusTree index(m_page, m_diskMan, indexInfo->indexPageSize, indexInfo->indexHigh, 
    indexInfo->indexRoot, indexInfo->indexBegin, indexInfo->indexEnd, indexInfo->indexKeyLen, indexInfo->indexRowLen, cmpTp);


    char keydata[1024];
    KEY_T indexKey(keydata, indexInfo->indexKeyLen, cmpTp);
    uint32_t offset = 0;
    for(int i = 0; i < keyVals.size(); ++i) {
        std::memcpy(keydata + offset, keyVals[i].data, keyVals[i].len);
        offset += keyVals[i].len;
    }

    // use MAX_COL_NUM but only first two columns are valid
    const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM> indexCols(indexInfo->indexCol, indexInfo->idxColNum);

    CItem itm(indexCols);

    rc = index.search(indexKey, itm.data, itm.rowLen);
    if(rc != 0) {
        message = " search index key in b+ tree fail";
        return rc;
    }

    itm.endIter.m_pos = 1;
    itm.endIter.m_ptr = itm.data + itm.rowLen;


    CValue pkVal = itm.getValue(1);

    // use this pkdata to find in original bplus tree

    KEY_T oriKey(pkVal.data, pkVal.len, m_cmpTyps);


    rc = getRow(oriKey, &out);
    if (rc != 0) {
        message = " get row by primary key fail";
        return rc;
    }

    return 0;
}
