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

    if(cols[pos]->type == dpfs_datatype_t::TYPE_VARCHAR) {
        // first 4 bytes is actual length
        val.len = *(uint32_t*)((char*)rowPtr + offSet);
        val.data = (char*)rowPtr + offSet + sizeof(uint32_t);
    } else {
        val.len = cols[pos]->len;
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
        if(isVariableType(cols[i]->type)) {
            offSet += sizeof(uint32_t) + (*(uint32_t*)(rowPtr + offSet));
        } else {
            offSet += cols[i]->len;
        }
    }
    return offSet;
}

int CItem::dataCopy(size_t pos, const CValue* value) noexcept {
    // data offset in row
    size_t offSet = getDataOffset(pos);

    if(isVariableType(cols[pos]->type)) {
        // first 4 bytes is actual length
        uint32_t actualLen = value->len;
        if(actualLen > cols[pos]->len) {
            return -E2BIG;
        }
        // use col offset of row to find the pointer and copy data
        *(uint32_t*)((char*)rowPtr + offSet) = actualLen;
        memcpy(((char*)rowPtr + offSet + sizeof(uint32_t)), value->data, actualLen);
    } else {

        memcpy(((char*)rowPtr + offSet), value->data, value->len);
        if(value->len < cols[pos]->len) {
            memset(((char*)rowPtr + offSet + value->len), 0, cols[pos]->len - value->len);
        }
    }

    return value->len;
}

/*
    @param col: column of the item, maybe column name
    @return CValue pointer on success, else nullptr
    @note this function will search the item list for the column, and possible low performance
*/
CValue CItem::getValueByKey(CColumn* col) const noexcept {
    CValue val;

    for(size_t i = 0; i < cols.size(); ++i) {
        if (*cols[i] == *col) {
            size_t offSet = getDataOffset(i);
            if(cols[i]->type == dpfs_datatype_t::TYPE_VARCHAR) {
                // first 4 bytes is actual length
                val.len = *(uint32_t*)((char*)rowPtr + offSet);
                val.data = (char*)rowPtr + offSet + 4;
            } else {
                val.len = cols[i]->len;
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
int CItem::updateValue(size_t pos, CValue* value) noexcept {
    if(pos < 0 || pos >= cols.size()) {
        return -EINVAL;
    }
    if(cols[pos]->getLen() < value->len) {
        return -E2BIG;
    }
    return dataCopy(pos, value);
}

/*
    @param col: col of the item, maybe column name
    @param value: CValue pointer to update
    @return CValue pointer on success, else nullptr
*/
int CItem::updateValueByKey(CColumn* col, CValue* value) noexcept {
    for(size_t i = 0; i < cols.size(); ++i) {
        if (*cols[i] == *col) {
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
    if(!m_perms.perm.m_dirty) {
        return 0;
    }

    void* zptr = nullptr;
    // lba length
    size_t zptrLen = 0;
    bidx tmpDataRoot = m_dataRoot;
    // len for root block
    size_t rootBlockLen = 0;


    if(!m_perms.perm.m_btreeIndex) {
        // not b+ tree index, all data can be save to storage only once
        if(m_dataRoot != bidx({0, 0})) {
            // save to storage root block
            rc = -EOPNOTSUPP;
            message = "commit to a fixed Collection index is not allow.";
            goto errReturn;
        }

        // total data block info structs in m_tempStorage
        size_t totalDataBlocks = m_tempStorage.size();
        if(totalDataBlocks == 0  && curTmpDataLen == 0) {
            // no data to commit
            m_perms.perm.m_dirty = false;
            return 0;
        }


        // lba size
        zptrLen = curTmpDataLen % dpfs_lba_size == 0 ? curTmpDataLen / dpfs_lba_size : (curTmpDataLen / dpfs_lba_size + 1);
        zptr = m_page.alloczptr(zptrLen);
        if(!zptr) {
            zptrLen = 0;
            rc = -ENOMEM;
            goto errReturn;
        }

        // total root block length, including temp storage and remaining tmp data
        rootBlockLen = m_tempStorage.size() + (curTmpDataLen % dpfs_lba_size == 0 ? curTmpDataLen / dpfs_lba_size : curTmpDataLen / dpfs_lba_size + 1);

        m_dataRoot = {nodeId, m_diskMan.balloc(rootBlockLen)};
        if(m_dataRoot.bid == 0) {
            rc = -ENOSPC;
            message = "no space left in the disk group.";
            goto errReturn;
        }


        // return by m_tempstorage, indicate actual lba length got
        size_t real_lba_len = 0;

        // indicator for write complete
        int indicator = 0;
        for(size_t pos = 0; pos < totalDataBlocks; ) {
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

            m_dataEndPos = tmpDataRoot;
        }

        // wait for last write complete
        while(indicator == 0) {
            
        }

        m_tempStorage.clear();
        curTmpDataLen = 0;
        memset(tmpData, 0, curTmpDataLen);

    } else {
        m_owner.m_log.log_inf("commit collection with b+ tree index is not implemented yet.\n");
        // CBPlusTree bpt(&m_page, &m_diskMan, nodeId, m_dataRoot, m_log);

        // TODO::
        // write commit log
        // write data to storage
        // write to this->m_dataRoot
        // update index
    }


    m_perms.perm.m_dirty = false;

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
CItem* CItem::newItem(const std::vector<CColumn*>& cs) noexcept {
    size_t len = 0;
    for(size_t i = 0; i < cs.size(); ++i) {
        len += cs[i]->getLen();
        isVariableType(cs[i]->type) ? len += 4 : 0;
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
    return item;
}

/*
    @param cs: column info
    @return CItem pointer on success, else nullptr
    @note this function will create a new CItem with the given column info
*/
CItem* CItem::newItems(const std::vector<CColumn*>& cs, size_t maxRowNumber) noexcept {
    size_t len = 0;
    for(size_t i = 0; i < cs.size(); ++i) {
        len += cs[i]->getLen();
        isVariableType(cs[i]->type) ? len += 4 : 0;
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
CItem::CItem(const std::vector<CColumn*>& cs) : cols(cs), beginIter(this), endIter(this) {
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
        if(isVariableType(pos->type)) {
            // first 4 bytes is actual length
            uint32_t vallEN = *(uint32_t*)((char*)rowPtr);
            rowPtr += sizeof(uint32_t) + vallEN;
            validLen += sizeof(uint32_t) + vallEN;
        } else {
            rowPtr += pos->getLen();
            validLen += pos->getLen();
        }
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
CCollection::CCollection(CProduct& owner, CDiskMan& dskman, CPage& pge, uint32_t id) : 
m_owner(owner), 
m_ccid(id), 
m_diskMan(dskman), 
m_page(pge), 
m_tempStorage(m_page, m_diskMan) { 
    
    m_perms.perm.m_updatable = false;
    m_perms.perm.m_insertable = false;
    m_perms.perm.m_detelable = false;
    m_perms.perm.m_dirty = false;
    m_perms.perm.m_btreeIndex = true;
    m_name = "dpfs_dummy";
    m_cols.clear();
    m_cols.reserve(16);
    
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


int CCollection::addItem(const CItem& item) {

    // save item to storage and update index
    // search where the item should be in storage, use b plus tree to storage the data
    
    // bpt.insert(key, value);
    int rc = 0;
    if(m_perms.perm.m_btreeIndex) {
        
        // use b plus tree to index the item
    }

    // get len of the data
    const char* dataPtr = item.data;
    size_t validLen = 0; // item.validLen;
    char* rowPtr = item.rowPtr;
    for(size_t i = 0; i < item.rowNumber; ++i){
        for(auto& pos : item.cols) {
            if(isVariableType(pos->type)) {
                // first 4 bytes is actual length
                //last row ptr
                uint32_t vallEN = *(uint32_t*)((char*)rowPtr);
                rowPtr += sizeof(uint32_t) + vallEN;
                validLen += sizeof(uint32_t) + vallEN;
            } else {
                rowPtr += pos->getLen();
                validLen += pos->getLen();
            }
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





