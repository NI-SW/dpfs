#include <collect/collect.hpp>
#include <collect/bp.hpp>
#include <collect/product.hpp>
uint64_t nodeId = 0;


/*
    @param pos: position of the item in the item list
    @return CValue pointer on success, else nullptr
*/
CValue CItem::getValue(size_t pos) const noexcept {
    CValue val;
    if(pos < 0 || pos >= cols.size()) {
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
    if(pos < 0 || pos >= cols.size()) {
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
    if(pos < 0 || pos >= cols.size()) {
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
CItem::CItem(const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM>& cs) : cols(cs), beginIter(this), endIter(this) {
    locked = false;
    error = false;
    beginIter.m_pos = 0;
    beginIter.m_ptr = data;
    endIter.m_pos = 0;
    endIter.m_ptr = nullptr;
}

int CItem::nextRow() noexcept {
    if(rowNumber + 1 >= maxRowNumber) {
        return -ERANGE;
    }
    ++rowNumber;
    endIter.m_pos = rowNumber + 1;

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

}

// use ccid to locate the collection (search in system collection table)
/*
    @param engine: dpfsEngine reference to the storage engine
    @param ccid: CCollection ID, used to identify the collection info, 0 for new collection
    @note this constructor will create a new collection with the given engine and ccid
*/
CCollection::CCollection(CDiskMan& dskman, CPage& pge) : 
// m_owner(owner), 
m_diskMan(dskman), 
m_page(pge), 
m_tempStorage(m_page, m_diskMan) { 
    
};

CCollection::~CCollection() {
    if(tmpData) {
        free(tmpData);
        tmpData = nullptr;
    }
    if(m_collectionStruct) {
        delete m_collectionStruct;
        m_collectionStruct = nullptr;
    }
    if(m_cltInfoCache) {
        m_cltInfoCache->release();
        m_cltInfoCache = nullptr;
    }

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

    // col = CColumn::newCol(colName, type, len, scale, constraint);
    // // if generate column fail
    // if(!col) {
    //     return -ENOMEM;
    // }

    m_lock.lock();
    m_collectionStruct->m_cols.push_back({colName, type, len, scale, constraint});
    // m_cols.emplace_back(col);
    m_lock.unlock();
    
    this->m_collectionStruct->ds->m_perms.perm.m_dirty = true;
    this->m_collectionStruct->ds->m_perms.perm.m_needreorg = true;
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

    if(curTmpDataLen + len > tmpBlockLen * dpfs_lba_size) {
        // need to flush temp storage

        
        // save in-length data, switch to new tmpBlock
        memcpy(tmpData + curTmpDataLen, data, tmpBlockLen - curTmpDataLen);
        // cross copied data
        data += tmpBlockLen - curTmpDataLen;
        // decline copied data len
        len -= tmpBlockLen - curTmpDataLen;
        // put extra data to next add item function call
        
        m_tempStorage.pushBackData(tmpData, tmpBlockLen % dpfs_lba_size == 0 ? tmpBlockLen / dpfs_lba_size : (tmpBlockLen / dpfs_lba_size + 1));
        curTmpDataLen = 0;
        memset(tmpData, 0, tmpBlockLen);

        return saveTmpData(data, len);
        
    }

    memcpy(tmpData + curTmpDataLen, data, len);
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
    char* rowPtr = item.rowPtr;

    if(m_collectionStruct->ds->m_perms.perm.m_btreeIndex) {
        // separate key and value from item
        for(size_t i = 0; i < item.rowNumber; ++i){
            char keyBuf[255];
            KEY_T key(keyBuf, 255);

            int pkinRow = 0;
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
                // if is primary key column
                if(pkinRow == m_pkPos) {
                    // key part
                    // use b plus tree to index the item
                    key.len = validLen;
                    key.KEYTYPE = pos.dds.colAttrs.type;
                    memcpy(key.data, rowPtr, key.len);

                }

                ++pkinRow;
            }
            /*
                |KEY|data....|
            */
           m_btreeIndex->insert(key, rowPtr, validLen);
        }

        // use b plus tree to index the item
    }

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

    rc = this->m_page.put(head, m_collectionStruct->data, &indicate, MAX_COLLECTION_INFO_LBA_SIZE, true);
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

    for(int i = 0; i < m_collectionStruct->m_cols.size(); ++i) {
        m_rowLen += m_collectionStruct->m_cols[i].getDds().len;
    }

    if(m_collectionStruct->ds->m_perms.perm.m_btreeIndex) {
        m_btreeIndex = new CBPlusTree(*this, m_page, m_diskMan, m_collectionStruct->ds->m_indexPageSize);
        if(!m_btreeIndex) {
            return -ENOMEM;
        }
    }
    m_cltInfoCache = ce;
    inited = true;
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
        return -ENOMEM;
    }

    m_collectionStruct->ds->m_ccid = initStruct.id;
    memcpy(m_collectionStruct->ds->m_name, initStruct.name.c_str(), initStruct.name.size() + 1);
    m_collectionStruct->ds->m_nameLen = initStruct.name.size() + 1;


    m_collectionStruct->ds->m_perms.permByte = initStruct.m_perms.permByte;
    m_collectionStruct->ds->m_indexPageSize = initStruct.indexPageSize;
    m_collectionStruct->m_cols.clear();

    curTmpDataLen = 0;

    inited = true;
    return 0;
}



int CCollection::initBPlusTreeIndex() noexcept {
    if(!m_collectionStruct->ds->m_perms.perm.m_btreeIndex) {
        return -EINVAL;
    }
    if(!m_btreeIndex) {
        m_btreeIndex = new CBPlusTree(*this, m_page, m_diskMan, m_collectionStruct->ds->m_indexPageSize);
        if(!m_btreeIndex) {
            return -ENOMEM;
        }
    }
    return 0;
}