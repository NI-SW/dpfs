#include <collect/diskman.hpp>
// tmp block len by lba size
const uint64_t tmpBlockLbaLen = 10;
// tmp block len by BYTE
const uint64_t tmpBlockLen = tmpBlockLbaLen * dpfs_lba_size;

/*
    @param data: pointer to the data to be stored
    @param len: length of the data to be stored (unit is lba size, default 4096B)
    @return 0 on success, else on failure
    @note store data into temp storage
*/
int CTempStorage::pushBackData(const void* data, size_t lba_len) {
    // TODO
    cacheStruct* p = nullptr;
    int rc = 0;
    DataBlockInfo dbi;
    
    if(!m_dataBlockList.empty()) {
        // if the last one is full, need to create new data block
        if(m_dataBlockList.back().lba_len >= tmpBlockLbaLen) {
            // init data block info
            dbi.offset = m_dataBlockList.back().offset + m_dataBlockList.back().lba_len;
            dbi.lba_len = std::min(lba_len, tmpBlockLbaLen);
            dbi.blkPos.gid = nodeId;
            dbi.blkPos.bid = m_diskman.balloc(tmpBlockLbaLen);

            void* zptr = m_page.alloczptr(tmpBlockLbaLen);
            if(zptr == nullptr) {
                rc = -ENOMEM;
                goto errReturn;
            }

            if(dbi.blkPos.bid == 0) {
                rc = -ENOSPC;
                goto errReturn;
            }

            memcpy(zptr, data, std::min(lba_len, tmpBlockLbaLen) * dpfs_lba_size);
            // put data to the cache
            m_page.put(dbi.blkPos, zptr, nullptr, tmpBlockLbaLen);

            m_dataBlockList.emplace_back(std::move(dbi));

            if(lba_len > tmpBlockLbaLen) {
                // need to store extra data
                rc = pushBackData((char*)data + (tmpBlockLbaLen * dpfs_lba_size), lba_len - tmpBlockLbaLen);
                if(rc) {
                    goto errReturn;
                }
            }
            goto done;

        } else {

            // save in-length data, put extra data to new block
            DataBlockInfo& lastDbi = m_dataBlockList.back();
            size_t inLen = tmpBlockLbaLen - lastDbi.lba_len;
            lastDbi.lba_len += std::min(lba_len, inLen);

            rc = m_page.get(p, lastDbi.blkPos, tmpBlockLbaLen);
            if(rc != 0) {
                goto errReturn;
            }
            rc = p->lock();
            if(rc != 0) {
                goto errReturn;
            }
            memcpy((char*)p->getPtr() + lastDbi.lba_len * dpfs_lba_size, data, std::min(lba_len, inLen) * dpfs_lba_size);
            p->unlock();
            p->release();
            p = nullptr;

            // save extra data to new block
            if(lba_len <= inLen) 
                goto done;

            rc = pushBackData((char*)data + (inLen * dpfs_lba_size), lba_len - inLen);
            if(rc) {
                goto errReturn;
            }
        }
    }


    // need to create new data block info
    dbi.offset = 0;
    dbi.lba_len = lba_len;
    dbi.blkPos.gid = nodeId;
    dbi.blkPos.bid = m_diskman.balloc(tmpBlockLbaLen);
    if(dbi.blkPos.bid == 0) {
        rc = -ENOSPC;
        goto errReturn;
    }

    // write data to disk
    rc = m_page.get(p, dbi.blkPos, tmpBlockLbaLen);
    if(rc != 0) {
        goto errReturn;
    }
    rc = p->lock();
    if(rc != 0) {
        goto errReturn;
    }
    memcpy(p->getPtr(), data, lba_len * dpfs_lba_size);
    p->unlock();
    p->release();

    m_dataBlockList.emplace_back(std::move(dbi));

    // !! ++(*endIter).m_pos;

    
done:

    return 0;

    errReturn:
    if(p) {
        p->release();
    }
    if(dbi.blkPos.bid) {
        m_diskman.bfree(dbi.blkPos.bid, lba_len);
    }
    return rc;
}

/*
    @param pos: position of the data to get (unit is lba size, default 4096B)
    @param data: pointer to the buffer to store the data
    @param lba_len: length of the data to get (unit is lba size, default 4096B)
    @param real_lba_len: indicaete actual length of data readed
    @return 0 on success, else on failure
    @note get data from temp storage
*/
int CTempStorage::getData(size_t pos, void* data, size_t lba_len, size_t& readed_lba_len) {
    // TODO
    cacheStruct* p = nullptr;
    int rc = 0;
    size_t read_len = 0;
    size_t recursive_readed_lba_len = 0;
    readed_lba_len = 0;


    const DataBlockInfo& dbi = m_dataBlockList[pos / tmpBlockLbaLen];
    
    // if is cross block read
    /*
            |<-------LBA_LEN-------->|
        |    block1      |   block2    |
        |<->|<-read_len->|<--------->|
        |  pos
        |
    dbi.offset
    */

    read_len = std::min(lba_len, dbi.lba_len - (pos - dbi.offset));
    rc = m_page.get(p, dbi.blkPos, dbi.lba_len);
    if(rc != 0) {
        goto errReturn;
    }

    rc = p->lock();
    if(rc != 0) {
        goto errReturn;
    }
    memcpy(data, (char*)p->getPtr() + (pos - dbi.offset) * dpfs_lba_size, read_len * dpfs_lba_size);
    p->unlock();
    p->release();

    readed_lba_len += read_len;

    // if need read next block
    if(read_len < lba_len) {
        size_t nextLen = lba_len - read_len;
        rc = getData(dbi.offset + dbi.lba_len, (char*)data + read_len * dpfs_lba_size, nextLen, recursive_readed_lba_len);
        if(rc < 0) {
            goto errReturn;
        }

        readed_lba_len += recursive_readed_lba_len;
    }


    return 0;
    errReturn:
    if(p) {
        p->release();
    }
    return rc;
}

/*
    @param pos: position of the data to update (unit is lba size, default 4096B)
    @param data: pointer to the data to update
    @param len: length of the data to update (unit is lba size, default 4096B)
    @return number of blocks updated on success, else on failure
    @note update data in temp storage
*/
int CTempStorage::updateData(size_t pos, void* data, size_t len) {
    // TODO
    cacheStruct* p = nullptr;
    int rc = 0;
    size_t total_write_len = 0;
    size_t write_len = 0;
    for(auto& dbi : m_dataBlockList) {
        if(pos >= dbi.offset && pos < (dbi.offset + dbi.lba_len)) {
            // found the data block
            
            // if len larger than left length in this block, need to update next block
            if(len > dbi.lba_len - (pos - dbi.offset)) {
                size_t nextLen = len - (dbi.lba_len - (pos - dbi.offset));
                rc = updateData(dbi.offset + dbi.lba_len, (char*)data + (dbi.lba_len - (pos - dbi.offset)) * dpfs_lba_size, nextLen);
                if(rc < 0) {
                    goto errReturn;
                }
                total_write_len += rc;
            }

            write_len = std::min(len, dbi.lba_len - (pos - dbi.offset));
            rc = m_page.get(p, dbi.blkPos, dbi.lba_len);
            if(rc != 0) {
                goto errReturn;
            }
            rc = p->lock();
            if(rc != 0) {
                goto errReturn;
            }
            memcpy((char*)p->getPtr() + (pos - dbi.offset) * dpfs_lba_size, data, write_len * dpfs_lba_size);
            p->unlock();
            p->release();
            return write_len + total_write_len;
        }
    }

    return 0;
    errReturn:
    if(p) {
        p->release();
    }
    return rc;
}

/*
    @param data: pointer to buffer to store the last saved data
    @param len: max block length of last saved data
    @return 0 on success, else on failure
    @note read from last saved data
*/
int CTempStorage::back(void* data, size_t& len) {
    //TODO 
    if(m_dataBlockList.empty()) {
        return -ENODATA;
    }
    DataBlockInfo& dbi = m_dataBlockList.back();
    len = dbi.lba_len;

    cacheStruct* p = nullptr;
    int rc = 0;

    // read from disk
    rc = m_page.get(p, dbi.blkPos, dbi.lba_len);
    if(rc != 0) {
        goto errReturn;
    }

    // wait for disk ready
    rc = p->read_lock();
    if(rc != 0) {
        goto errReturn;
    }
    memcpy(data, p->getPtr(), dbi.lba_len * dpfs_lba_size);
    p->read_unlock();
    p->release();


    return 0;
    errReturn:
    if(p) {
        p->release();
    }
    return rc;
}

int CTempStorage::clear() {
    // TODO
    for(auto& dbi : m_dataBlockList) {
        m_diskman.bfree(dbi.blkPos.bid, dbi.lba_len);
    }
    m_dataBlockList.clear();
    return 0;
}