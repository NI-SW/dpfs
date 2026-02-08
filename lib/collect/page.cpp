#include <collect/page.hpp>
#include <memory.h>

// #define __PGDEBUG__

#ifdef __PGDEBUG__
#include <iostream>
using namespace std;
#endif
extern uint64_t nodeId;

#include <log/loglocate.h>


// max block len, manage memory block alloced by dpfsEngine::zmalloc
const size_t maxBlockLen = 40;
// max memory block number limited for each block list
const size_t maxMemBlockLimit = 100;

const std::vector<std::string> cacheStruct::statusEnumStr = {"VALID", "WRITING", "READING", "INVALID", "ERROR"};

cacheStruct::cacheStruct(CPage* cp) : page(cp) {
    len = 0;
    dirty = 0;
}

void cacheStruct::release() {
    if(--refs <= 0) {
        status = INVALID;
        page->freezptr(zptr, len);
        page->freecs(this);
    }
}

CPage::CPage(std::vector<dpfsEngine*>& engine_list, size_t cacheSize, logrecord& log, size_t maxCacheSizeInByte) : 
m_log(log),
m_engine_list(engine_list), 
// implicit conversion from CPage* to PageClrFn
m_cache(cacheSize, this) {
    // the length larger then 10 need Special Processing
    m_zptrList.resize(maxBlockLen);
    m_zptrLock.resize(maxBlockLen);
    m_exit = 0;
    m_hitCount = 0;
    m_getCount = 0;
    m_maxSizeInByte = maxCacheSizeInByte;
}

CPage::~CPage() {
    m_exit = 1;
}

// void page_get_cb(void* arg, const dpfs_compeletion* dcp) {
//     cbvar* cbv = reinterpret_cast<cbvar*>((char*)arg);
//     if(!cbv) {
//         return;
//     }
//     cacheStruct* cptr = cbv->cptr;
//     dpfs_engine_cb_struct* cbs = cbv->cbs;
//     CPage* pge = static_cast<CPage*>(cbv->page);
//     if(dcp->return_code) {
//         cptr->status = cacheStruct::ERROR;
//         pge->m_log.log_error("write fail code: %d, msg: %s\n", dcp->return_code, dcp->errMsg);
//         pge->freecbs(cbs);
//         return;
//     }
//     cptr->status = cacheStruct::VALID;
//     pge->freecbs(cbs);
// }

int CPage::get(cacheStruct*& cptr, const bidx& idx, size_t len) {
    if(idx.gid >= m_engine_list.size()) {
        return -ERANGE;
    }

    if (cptr) {
        cptr->release();
        cptr = nullptr;
    }

    int rc = 0;

    // cacheStruct* cs;
    // dpfsEngine memory pointer
    void* zptr = nullptr;
    bool updateCache = false;
    size_t oldSize = 0;

    CSpinGuard guard(m_cacheLock);

    CDpfsCache<bidx, cacheStruct*, PageClrFn>::cacheIter* ptr = m_cache.getCache(idx);
    if(ptr) {
        m_log.log_debug("found cache in LRU, gid=%llu bid=%llu len=%llu\n", idx.gid, idx.bid, ptr->cache->len);

        // if len is not match, need update cache, reload from disk
        if(ptr->cache->len < len || ptr->cache->status == cacheStruct::ERROR || ptr->cache->status == cacheStruct::INVALID) {
            updateCache = true;
        }
    }

    // reget the cache if not found or need update
    if(!ptr || updateCache) {

        // alloc zptr
        zptr = alloczptr(len);
        if(!zptr) {
            // can't malloc big block, reutrn error
            m_log.log_error("Can't malloc large block, size = %llu Bytes\n", dpfs_lba_size * len);
            rc = -ENOMEM;
            goto errReturn;
        }

        // alloc cache block struct 
        if(updateCache) {
            cptr = ptr->cache;
            oldSize = cptr->len * dpfs_lba_size;
        } else {
            cptr = alloccs();
        }
        if(!cptr) {
            m_log.log_error("can't malloc cacheStruct, no memory\n");
            rc = -ENOMEM;
            goto errReturn;
        }

        rc = cptr->lock();
        if(rc) {
            // TODO process error

            m_log.log_error("lock cache err before get, gid=%llu bid=%llu len=%llu rc = %d, status: %s\n", idx.gid, idx.bid, len, rc, cacheStruct::statusEnumStr[cptr->status - 1000].c_str());
            goto errReturn;
        }
        
        // change status to reading
        cptr->status = cacheStruct::READING;

        // if is update cache, need not change idx, just change zptr and len
        if(updateCache) {
            freezptr(cptr->zptr, cptr->len);
        }

        cptr->zptr = zptr;
        cptr->len = len;
        cptr->unlock();

        dpfs_engine_cb_struct* cbs = alloccbs();// new dpfs_engine_cb_struct;
        if(!cbs) {
            m_log.log_error("can't malloc dpfs_engine_cb_struct, no memory\n");
            rc = -ENOMEM;
            goto errReturn;
        }


        // use lambda to define callback func
        cbs->m_cb = [&cptr, cbs](void* arg, const dpfs_compeletion* dcp) {
            CPage* pge = static_cast<CPage*>(arg);
            if(!pge) {
                // free(cbs);
                cptr->status = cacheStruct::ERROR;
                pge->m_log.log_error("internal error: page pointer is null in read callback\n");
                
                pge->freecbs(cbs);
                delete cbs;
                return;
            }

            // pge->m_log.log_debug("callback for %p called idx : { gid=%llu bid=%llu }, len = %llu\n", cptr, cptr->idx.gid, cptr->idx.bid, cptr->len);

            if(dcp->return_code) {
                cptr->status = cacheStruct::ERROR;
                pge->m_log.log_error("write fail code: %d, msg: %s\n", dcp->return_code, dcp->errMsg);
                pge->freecbs(cbs);
                return;
            }
            cptr->status = cacheStruct::VALID;
            pge->freecbs(cbs);
            
        };
        cbs->m_arg = this;
        
        // read from disk
        rc = m_engine_list[idx.gid]->read(idx.bid, zptr, len, cbs);
        if(rc < 0) {
            m_log.log_error("read from disk err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            freecbs(cbs);
            goto errReturn;
        }



        if(updateCache) {
            cptr->refs += 1;
            m_currentSizeInByte += (len * dpfs_lba_size - oldSize);
        } else {
            cptr->idx = idx;
            // one for lru, one for user
            cptr->refs += 2;
            // insert to cache

            if (m_maxSizeInByte == 0) {
                // unlimited cache size
            } else {
                while(m_currentSizeInByte > m_maxSizeInByte - (len * dpfs_lba_size)) {
                    // need free some cache
                    m_log.log_notic("cache size exceed max size %llu Bytes, current size %llu Bytes, try to free some cache\n", m_maxSizeInByte, m_currentSizeInByte.load());
                    rc = m_cache.popBackCache();
                    if (rc == -ENOENT) {
                        // no more cache can be freed
                        break;
                    }
                }
            }

            do {
                rc = m_cache.insertCache(cptr->idx, cptr);
                if(rc) {
                    m_log.log_error("insert to cache err, gid=%llu bid=%llu len=%llu rc = %d, NO MEMORY\n", cptr->idx.gid, cptr->idx.bid, cptr->len, rc);
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            } while (rc);
            m_currentSizeInByte += len * dpfs_lba_size;
        }



        ++m_getCount;
        
        return 0;
    }
    ++m_getCount;
    ++m_hitCount;

    // add a reference count
    ++ptr->cache->refs;
    cptr = ptr->cache;

    return 0;

errReturn:
    if(cptr) {
        freecs(cptr);
        cptr = nullptr;
    }
    if(zptr) {
        freezptr(zptr, len);
        zptr = nullptr;
    }
    return rc;
}

int CPage::put(const bidx& idx, void* zptr, int* finish_indicator, size_t len, bool wb, cacheStruct** pCache) {
    // if(idx.gid >= m_engine_list.size()) {
    //     return -ERANGE;
    // }

    if(idx.gid != 0) {
        // write is only allowed on gid 0
        return -EPERM;
    }

    int rc = 0;
    cacheStruct* cs = nullptr;
    CDpfsCache<bidx, cacheStruct*, PageClrFn>::cacheIter* cptr = nullptr;

    CSpinGuard guard(m_cacheLock);

    // update cache
    cptr = m_cache.getCache(idx);
    // if this idx is not loaded on cache
    if(!cptr) {
        
        // get a cache struct object.
        cs = alloccs();
        if(!cs) {
            goto errReturn;
        }

        cs->zptr = zptr;
        cs->idx = idx;
        cs->len = len;
        cs->status = cacheStruct::VALID;
        cs->dirty = 1;
        ++cs->refs;

        if (m_maxSizeInByte == 0) {
            // unlimited cache size
        } else {
            while(m_currentSizeInByte > m_maxSizeInByte - (len * dpfs_lba_size)) {
                // need free some cache
                m_log.log_notic("cache size exceed max size %llu Bytes, current size %llu Bytes, try to free some cache\n", m_maxSizeInByte, m_currentSizeInByte.load());
                rc = m_cache.popBackCache();
                if (rc == -ENOENT) {
                    // no more cache can be freed
                    break;
                }
            }
        }

        // insert the cache to lru system
        rc = m_cache.insertCache(idx, cs);
        if(rc) {
            m_log.log_error("insert to cache err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            --cs->refs;
            goto errReturn;
        }
        m_currentSizeInByte += len * dpfs_lba_size;
        if(pCache) {
            ++cs->refs;
            *pCache = cs;
        }
        

    } else {
        // if loaded on cache, update it
        // write lock
        rc = cptr->cache->lock();
        if(rc) {
            m_log.log_error("lock cache err before put, gid=%llu bid=%llu len=%llu rc = %d, status: %s\n", idx.gid, idx.bid, len, rc, cacheStruct::statusEnumStr[cptr->cache->status - 1000].c_str());
            goto errReturn;
        }

        // recycle the memory
        freezptr(cptr->cache->zptr, cptr->cache->len);

        // update the cache
        cptr->cache->zptr = zptr;
        cptr->cache->len = len;
        cptr->cache->dirty = 1;
        cptr->cache->unlock();

        if(pCache) {
            ++cptr->cache->refs;
            *pCache = cptr->cache;
        }
    }


    // if write back immediate
    if(wb) {
        cptr = m_cache.getCache(idx);
        if(!cptr) {
            m_log.log_error("can't find cache before write back, gid=%llu bid=%llu len=%llu\n", idx.gid, idx.bid, len);
            rc = -ENOENT;
            goto errReturn;
        }

        dpfs_engine_cb_struct* cbs = alloccbs();// alloc dpfs_engine_cb_struct;
        if(!cbs) {
            m_log.log_error("can't malloc dpfs_engine_cb_struct, no memory\n");
            rc = -ENOMEM;
            goto errReturn;
        }
        cbs->m_arg = this;
        // call back function for disk write
        cbs->m_cb = [cptr, cbs, finish_indicator](void* arg, const dpfs_compeletion* dcp) {
            if(cptr->cache->getStatus() == cacheStruct::INVALID || cptr->cache->getStatus() == cacheStruct::ERROR) {
                // already invalid, just return
                if(finish_indicator) {
                    *finish_indicator = 1;
                }
                reinterpret_cast<CPage*>(arg)->freecbs(cbs);
                return;
            }

            CPage* pge = static_cast<CPage*>(arg);
            if(dcp->return_code) {
                cptr->cache->status = cacheStruct::ERROR;
                pge->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
                // write error
                // TODO process error
                if(finish_indicator) {
                    *finish_indicator = -1;
                }
                pge->freecbs(cbs);
                return;
            }
            if(finish_indicator) {
                *finish_indicator = 1;
            }
            cptr->cache->status = cacheStruct::VALID;
            cptr->cache->dirty = 0;
            pge->freecbs(cbs);
        };   
    

        rc = cptr->cache->lock();
        if(rc) {
            // TODO process error
            m_log.log_error("cache status invalid before write back, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            // to indicate error
            if(!finish_indicator) {
                // TODO
                return 0;
            }

            zptr = alloczptr(1);
            if(!zptr) {
                m_log.log_error("can't malloc small block for error indication, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
                goto errReturn;
            }

            // write to 0 to refresh the indicator
            rc = m_engine_list[idx.gid]->write(0, zptr, 1, cbs);
            if(rc < 0) {
                m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            }
            return 0;
        }
        cptr->cache->status = cacheStruct::WRITING;
        cptr->cache->unlock();

        rc = m_engine_list[idx.gid]->write(idx.bid, zptr, len, cbs);
        

        if(rc < 0) {
            // if error, unlock immediately, else unlock in callback
            m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            // write error
            // TODO
            goto errReturn;
        }
        

    } else {
        if(cs){
            cs->dirty = 1;
        } else if(cptr) {
            cptr->cache->dirty = 1;
        } else {
            rc = -EFAULT;
            goto errReturn;
            // should not reach here
        }
    }


    return 0;

errReturn:
    if(cs) {
        freecs(cs);
    }

    if(pCache) {
        *pCache = nullptr;
    }

    return rc;
}

int CPage::writeBack(cacheStruct *cache, int* finish_indicator) {
    // TODO check if it need a lock 
    int rc = 0;



    dpfs_engine_cb_struct* cbs = alloccbs();// new dpfs_engine_cb_struct;
    if(!cbs) {
        m_log.log_error("can't malloc dpfs_engine_cb_struct, no memory\n");
        rc = -ENOMEM;
        goto errReturn;
    }

    cbs->m_arg = this;
    // call back function for disk write
    cbs->m_cb = [cache, cbs, finish_indicator](void* arg, const dpfs_compeletion* dcp) {
        
        if(cache->getStatus() == cacheStruct::INVALID || cache->getStatus() == cacheStruct::ERROR) {
            // already invalid, just return
            if(finish_indicator) {
                *finish_indicator = 1;
            }
            reinterpret_cast<CPage*>(arg)->freecbs(cbs);
            return;
        }

        CPage* pge = static_cast<CPage*>(arg);
        if(dcp->return_code) {
            cache->status = cacheStruct::ERROR;
            pge->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
            // write error
            // TODO process error
            if(finish_indicator) {
                *finish_indicator = -1;
            }
            pge->freecbs(cbs);
            return;
        }
        if(finish_indicator) {
            *finish_indicator = 1;
        }
        cache->status = cacheStruct::VALID;
        cache->dirty = 0;
        pge->freecbs(cbs);
    };   

    rc = cache->lock();
    if(rc) {
        // TODO process error
        m_log.log_error("cache status invalid before write back, gid=%llu bid=%llu len=%llu rc = %d\n", cache->idx.gid, cache->idx.bid, cache->getLen(), rc);
        // to indicate error
        if(!finish_indicator) {
            // TODO
            return 0;
        }

        void* zptr = alloczptr(1);
        if(!zptr) {
            m_log.log_error("can't malloc small block for error indication, gid=%llu bid=%llu len=%llu rc = %d\n", cache->idx.gid, cache->idx.bid, cache->getLen(), rc);
            return -ENOMEM;
        }

        // write to 0 to refresh the indicator
        rc = m_engine_list[cache->idx.gid]->write(0, zptr, 1, cbs);
        if(rc < 0) {
            m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", cache->idx.gid, cache->idx.bid, cache->getLen(), rc);
        }
        return 0;
    }

    cache->status = cacheStruct::WRITING;
    cache->unlock();
    rc = m_engine_list[cache->idx.gid]->write(cache->idx.bid, cache->getPtr(), cache->getLen(), cbs);
    if(rc < 0) {
        // if error, unlock immediately, else unlock in callback
        m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", cache->idx.gid, cache->idx.bid, cache->getLen(), rc);
        // write error
        // TODO
        // write error bug memory is valid
        cache->status = cacheStruct::VALID;
        goto errReturn;
    }

    return 0;
errReturn:
    return rc;
}

// TODO , not test yet, maybe error exist
int CPage::fresh(cacheStruct*& cache) {
    if (!cache) {
        return -EINVAL;
    }

    if(cache->idx.gid >= m_engine_list.size()) {
        return -ERANGE;
    }


    int rc = 0;



    CSpinGuard guard(m_cacheLock);

    CDpfsCache<bidx, cacheStruct*, PageClrFn>::cacheIter* ptr = m_cache.getCache(cache->idx);
    if(ptr) {
        // if the cache is found in lru, check the size
        m_log.log_debug("found cache in LRU, gid=%llu bid=%llu len=%llu\n", cache->idx.gid, cache->idx.bid, ptr->cache->len);

        // if len is not match, return error
        if (ptr->cache->len != cache->len) {
            m_log.log_error("cache length mismatch in fresh, gid=%llu bid=%llu cache len=%llu input len=%llu\n", cache->idx.gid, cache->idx.bid, ptr->cache->len, cache->len);
            return -EINVAL;
        }

        if (ptr->cache == cache) {
            // same cache, just return
            return 0;
        }
        // cache pointer is change
        cache->release();
        cache = ptr->cache;
        cache->refs += 1;
        return EEXIST;
    }

    // cache is not in lru, need insert


    // change status to reading
    cache->status = cacheStruct::READING;


    dpfs_engine_cb_struct* cbs = alloccbs();// new dpfs_engine_cb_struct;
    if(!cbs) {
        m_log.log_error("can't malloc dpfs_engine_cb_struct, no memory\n");
        rc = -ENOMEM;
        goto errReturn;
    }


    // use lambda to define callback func
    cbs->m_cb = [&cache, cbs](void* arg, const dpfs_compeletion* dcp) {
        CPage* pge = static_cast<CPage*>(arg);
        if(!pge) {
            // free(cbs);
            cache->status = cacheStruct::ERROR;
            pge->m_log.log_error("internal error: page pointer is null in read callback\n");
            
            pge->freecbs(cbs);
            delete cbs;
            return;
        }

        // pge->m_log.log_debug("callback for %p called idx : { gid=%llu bid=%llu }, len = %llu\n", cptr, cptr->idx.gid, cptr->idx.bid, cptr->len);

        if(dcp->return_code) {
            cache->status = cacheStruct::ERROR;
            pge->m_log.log_error("write fail code: %d, msg: %s\n", dcp->return_code, dcp->errMsg);
            pge->freecbs(cbs);
            return;
        }
        cache->status = cacheStruct::VALID;
        pge->freecbs(cbs);
        
    };
    cbs->m_arg = this;
    
    // read from disk
    rc = m_engine_list[cache->idx.gid]->read(cache->idx.bid, cache->getPtr(), cache->getLen(), cbs);
    if(rc < 0) {
        m_log.log_error("read from disk err, gid=%llu bid=%llu len=%llu rc = %d\n", cache->idx.gid, cache->idx.bid, cache->getLen(), rc);
        goto errReturn;
    }


    // insert to cache

    if (m_maxSizeInByte == 0) {
        // unlimited cache size
    } else {
        while(m_currentSizeInByte > m_maxSizeInByte - (cache->len * dpfs_lba_size)) {
            // need free some cache
            m_log.log_notic("cache size exceed max size %llu Bytes, current size %llu Bytes, try to free some cache\n", m_maxSizeInByte, m_currentSizeInByte.load());
            rc = m_cache.popBackCache();
            if (rc == -ENOENT) {
                // no more cache can be freed
                break;
            }
        }
    }

    do {
        rc = m_cache.insertCache(cache->idx, cache);
        if(rc) {
            m_log.log_error("insert to cache err, gid=%llu bid=%llu len=%llu rc = %d, NO MEMORY\n", cache->idx.gid, cache->idx.bid, cache->len, rc);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    } while (rc);
    m_currentSizeInByte += cache->len * dpfs_lba_size;
    
    // + 1 for lru
    cache->refs += 1;

    return 0;

errReturn:
    if (cbs) {
        freecbs(cbs);
    }
    return rc;
}

int CPage::flush() {
    // TODO 

    return 0;
}

// for class outside call
// void* CPage::cacheMalloc(size_t sz) {
//     if(sz == 0) {
//         return nullptr;
//     }
//     --sz;
//     void* zptr = nullptr;
//     if(sz < maxBlockLen) {
//         if(m_zptrList[sz].empty()) {
//             return m_engine_list[0]->zmalloc((sz + 1) * dpfs_lba_size);
//         }
//         m_zptrLock[sz].lock();
//         if(m_zptrList[sz].empty()) {
//             m_zptrLock[sz].unlock();
//             return m_engine_list[0]->zmalloc((sz + 1) * dpfs_lba_size);
//         }
//         zptr = m_zptrList[sz].front();
//         m_zptrList[sz].pop_front();
//         m_zptrLock[sz].unlock();
//     } else {
//         zptr = m_engine_list[0]->zmalloc((sz + 1) * dpfs_lba_size);
//     }
//     return zptr;
// }

void CPage::freecs(cacheStruct* cs) {
    if(!cs) {
        return;
    }
    m_csmLock.lock();
    m_cacheStructMemList.push(cs);
    m_csmLock.unlock();
}

cacheStruct* CPage::alloccs() {
    cacheStruct* cs = nullptr;
    if(!m_cacheStructMemList.empty()) {
        m_csmLock.lock();
        cs = m_cacheStructMemList.front();
        m_cacheStructMemList.pop();
        m_csmLock.unlock();
    } else {
        cs = new cacheStruct(this);
        if(!cs) {
            return nullptr;
        }
    }
    cs->status = cacheStruct::VALID;
    cs->refs = 0;
    cs->readRefs = 0;
    cs->dirty = 0;
    return cs;
}

void CPage::freezptr(void* zptr, size_t sz) {
    if(!zptr) {
        return;
    }

    if(sz == 0) {
        return;
    }
    --sz;

    if(sz < maxBlockLen) {
        if(m_zptrList[sz].size() < maxMemBlockLimit) {
            m_zptrLock[sz].lock();
            m_zptrList[sz].push(zptr);
            m_zptrLock[sz].unlock();
        } else {
            m_engine_list[0]->zfree(zptr);
        }
    } else {
        m_engine_list[0]->zfree(zptr);
    }
}

void* CPage::alloczptr(size_t sz) {
    if(sz == 0) {
        return nullptr;
    }
    --sz;
    void* zptr = nullptr;
    if(sz < maxBlockLen) {
        if(!m_zptrList[sz].empty()) {
            m_zptrLock[sz].lock();

            if(m_zptrList[sz].empty()) {
                m_zptrLock[sz].unlock();
                return m_engine_list[0]->zmalloc((sz + 1) * dpfs_lba_size);
            }
            zptr = m_zptrList[sz].front();
            m_zptrList[sz].pop();
            m_zptrLock[sz].unlock();
            memset(zptr, 0, (sz + 1) * dpfs_lba_size);
        } else {
            zptr = m_engine_list[0]->zmalloc((sz + 1) * dpfs_lba_size);
        }
    } else {
        zptr = m_engine_list[0]->zmalloc((sz + 1) * dpfs_lba_size);
    }
    return zptr;
}

void CPage::freecbs(dpfs_engine_cb_struct* cbs) {
    if(!cbs) {
        return;
    }
    // cbs->m_arg = nullptr;
    // cbs->m_acb = nullptr;
    m_cbmLock.lock();
    m_cbMemList.push((dpfs_engine_cb_struct*)cbs);
    m_cbmLock.unlock();

}


dpfs_engine_cb_struct* CPage::alloccbs() {
    dpfs_engine_cb_struct* cbs = nullptr;
    // cbvar* cbv = nullptr;
    if(!m_cbMemList.empty()) {
        m_cbmLock.lock();
        if(m_cbMemList.empty()) {
            m_cbmLock.unlock();
            // cbs = (dpfs_engine_cb_struct*)malloc(sizeof(dpfs_engine_cb_struct) + sizeof(cbvar));
            cbs = new dpfs_engine_cb_struct;
            goto allocReturn;
        }
        cbs = m_cbMemList.front();
        m_cbMemList.pop();
        m_cbmLock.unlock();
    } else {
        // cbs = (dpfs_engine_cb_struct*)malloc(sizeof(dpfs_engine_cb_struct) + sizeof(cbvar));
        cbs = new dpfs_engine_cb_struct;
        if(!cbs) {
            goto errReturn;
        }
    }

allocReturn:
    // cbs->m_acb = nullptr;
    // cbs->m_arg = nullptr;

    // cbv = reinterpret_cast<cbvar*>((char*)cbs + sizeof(dpfs_engine_cb_struct));

    // cbv->cptr = nullptr;
    // cbv->cbs = nullptr;
    // cbv->page = nullptr;


    return cbs;

errReturn:
    return nullptr;
}

PageClrFn::PageClrFn(void* clrFnArg) : cp(reinterpret_cast<CPage*>(clrFnArg)) {
    m_exit = false;
    clfThd = std::thread([this]() {

        std::mutex pclck;
	    std::unique_lock<std::mutex> lk(pclck);

        std::queue<pageClrSt> localQueue;

        while(!m_exit) {
            m_convar.wait_for(lk, std::chrono::seconds(1));
            if (m_flushQueue.empty()) {
                continue;
            }

            m_queueMutex.lock();
            localQueue.swap(m_flushQueue);
            m_queueMutex.unlock();

            while(!localQueue.empty()) {
                auto cacheItem = localQueue.front();
                localQueue.pop();
                clearCache(cacheItem.pCache, cacheItem.finish_indicator);
            }
        }

    });

}

PageClrFn::~PageClrFn() {

    m_exit = true;
    if (clfThd.joinable()) {
        clfThd.join();
    }

}

// write back only
void PageClrFn::operator()(cacheStruct*& p, int* finish_indicator) {
    if (!p) {
        if (finish_indicator) {
            *finish_indicator = 1;
        }
        return;
    }
    if (p->idx.gid != nodeId) {
        // write is only allowed on nodeId
        p->release();
        return;
    }
    
    // send cache to flush queue
    m_queueMutex.lock();
    m_flushQueue.emplace(p, finish_indicator);
    m_queueMutex.unlock();

    m_convar.notify_one();

}

void PageClrFn::flush(const std::list<void*>& cacheList) {

    // reinterpret_cast<CDpfsCache<bidx, cacheStruct*, PageClrFn>::cacheIter*>(cacheList.back());

    cacheStruct* p = nullptr;
    for(auto& it : cacheList) {
        p = reinterpret_cast<CDpfsCache<bidx, cacheStruct*, PageClrFn>::cacheIter*>(it)->cache;
        
        if(p->idx.gid != 0 || p->dirty == 0) {
            // write is only allowed on gid 0
            continue;
        }
        int rc = 0;
        void* zptr = p->zptr;
        dpfs_engine_cb_struct* cbs = this->cp->alloccbs(); //new dpfs_engine_cb_struct;
        if(!cbs) {
            cp->m_log.log_error("can't malloc dpfs_engine_cb_struct, no memory, write back fail!, gid: %llu, bid: %llu\n", p->idx.gid, p->idx.bid);
            return;
        }

        // lock and change status to writing
        rc = p->lock();
        if(rc) {
            cp->m_log.log_error("cache status invalid before write back, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            return;
        }
        p->status = cacheStruct::WRITING;
        p->unlock();

        cbs->m_arg = this;

        // call back function for disk write
        cbs->m_cb = [&p, cbs](void* arg, const dpfs_compeletion* dcp) {

            PageClrFn* pcf = reinterpret_cast<PageClrFn*>(arg);
            
            if(dcp->return_code) {
                pcf->cp->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
                // write error
                // TODO process error
                p->status = cacheStruct::VALID;
                pcf->cp->freecbs(cbs);
                return;
            }
            p->status = cacheStruct::VALID;
            p->dirty = 0;
            pcf->cp->freecbs(cbs);
        };

        rc = cp->m_engine_list[p->idx.gid]->write(p->idx.bid, zptr, p->len, cbs);
        if(rc < 0) {

            cp->m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            // write error
            // TODO


        }

    }

    if(!p) {
        return;
    }

    // wait for last one write back finish
    while(p->status == cacheStruct::WRITING) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

void PageClrFn::clearCache(cacheStruct*& p, int* finish_indicator) {
    if (!p) {
        if (finish_indicator) {
            *finish_indicator = 1;
        }
        return;
    }
    if (p->idx.gid != nodeId) {
        // write is only allowed on gid 0
        p->release();
        return;
    }
    // TODO :: maybe use multi thread to write back, to avoid lock conflict

    int rc = 0;
    // find disk group by gid, find block on disk by bid, write p, block number = blkNum
    // p->p must by alloc by engine_list->zmalloc

    void* zptr = p->zptr;

    // alloc call back function struct
    dpfs_engine_cb_struct* cbs = this->cp->alloccbs();
    if(!cbs) {
        cp->m_log.log_error("can't malloc dpfs_engine_cb_struct, no memory, write back fail!, gid: %llu, bid: %llu\n", p->idx.gid, p->idx.bid);
        return;
    }

    cbs->m_arg = this;
    // call back function for disk write
    cbs->m_cb = [&p, cbs, finish_indicator](void* arg, const dpfs_compeletion* dcp) {

        if(p->getStatus() == cacheStruct::INVALID || p->getStatus() == cacheStruct::ERROR) {
            // already invalid, just return
            if(finish_indicator) {
                *finish_indicator = 1;
            }
            p->release();
            reinterpret_cast<PageClrFn*>(arg)->cp->freecbs(cbs);
            return;
        }

        PageClrFn* pcf = reinterpret_cast<PageClrFn*>(arg);

        #ifdef __PGDEBUG__
        cout << "Write back cb called for gid: " << p->idx.gid << " bid: " << p->idx.bid << " finish_indicator: " << (finish_indicator ? *finish_indicator : -9999) << endl;
        #endif
        
        if(dcp->return_code) {
            pcf->cp->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
            // write error
            // TODO process error
            if(finish_indicator) {
                *finish_indicator = -1;
            }
            // p->status = cacheStruct::ERROR;
            p->release();
            pcf->cp->freecbs(cbs);
            return;
        }
        // after write back, the cache should be invalid
        p->status = cacheStruct::INVALID;
        if(finish_indicator) {
            *finish_indicator = 1;
        }
        p->release();
        pcf->cp->freecbs(cbs);
    };
        

    // lock the cache
    CTemplateGuard g(*p);
    if (g.returnCode() != 0) {
        // if lock fail
        // TODO process error
        cp->m_log.log_error("cache status invalid before write back, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
        // to indicate error
        if(!finish_indicator) {
            // TODO
            p->release();
            return;
        }

        zptr = this->cp->alloczptr(1);
        if(!zptr) {
            cp->m_log.log_error("can't malloc small block for error indication, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            p->release();
            return;
        }


        dpfs_engine_cb_struct* cbs = this->cp->alloccbs();
        if(!cbs) {
            cp->m_log.log_error("can't malloc dpfs_engine_cb_struct, no memory, write back fail!, gid: %llu, bid: %llu\n", p->idx.gid, p->idx.bid);
            return;
        }

        cbs->m_arg = this;
        // call back function for disk write
        cbs->m_cb = [cbs, finish_indicator](void* arg, const dpfs_compeletion* dcp) {

            PageClrFn* pcf = reinterpret_cast<PageClrFn*>(arg);
            
            if(dcp->return_code) {
                pcf->cp->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
                // write error
                // TODO process error
                if(finish_indicator) {
                    *finish_indicator = -1;
                }

                pcf->cp->freecbs(cbs);
                return;
            }
            // after write back, the cache is invalid
            if(finish_indicator) {
                *finish_indicator = 1;
            }

            pcf->cp->freecbs(cbs);
        };
        

        // write to 0 to refresh the indicator
        rc = cp->m_engine_list[p->idx.gid]->write(0, zptr, 1, cbs);
        if(rc < 0) {
            cp->m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            p->release();
        }
        return;
    }
    this->cp->m_currentSizeInByte -= p->len * dpfs_lba_size;

    // some problem, need to considerate call back method
    if(p->dirty || finish_indicator != nullptr) {

        // change status to writing
        p->status = cacheStruct::INVALID;


        // if write is successfully called, the unlock and release will be in callback function
        rc = cp->m_engine_list[p->idx.gid]->write(p->idx.bid, zptr, p->len, cbs);
        if(rc < 0) {
            cp->m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            // write error
            // !-!QUESTION!-!
            // TODO
            p->release();
            return;
        }

    } else {
        p->release();
    }

done:
    
    return;

errReturn:

    return;
}

/*
void PageClrFn::flush(cacheStruct*& p) {
    int rc = 0;
    // find disk group by gid, find block on disk by bid, write p, block number = blkNum
    // p->p must by alloc by engine_list->zmalloc

    void* zptr = p->zptr;
    dpfs_engine_cb_struct* cbs = this->cp->alloccbs(); //new dpfs_engine_cb_struct;
    if(!cbs) {
        cp->m_log.log_error("can't malloc dpfs_engine_cb_struct, no memory, write back fail!, gid: %llu, bid: %llu\n", p->idx.gid, p->idx.bid);
        return;
    }

    // some problem, need to considerate call back method
    if(p->dirty) { 

        // change status to writing
        rc = p->lock();
        if(rc) {

            cp->m_log.log_error("cache status invalid before write back, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            return;
        }
        p->status = cacheStruct::WRITING;
        p->unlock();

        cbs->m_arg = this;

        // call back function for disk write
        cbs->m_cb = [&p, cbs](void* arg, const dpfs_compeletion* dcp) {

            PageClrFn* pcf = reinterpret_cast<PageClrFn*>(arg);
            
            if(dcp->return_code) {
                pcf->cp->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
                // write error
                // TODO process error
                p->status = cacheStruct::ERROR;
                pcf->cp->freecbs(cbs);
                return;
            }
            p->status = cacheStruct::VALID;
            p->dirty = 0;
            pcf->cp->freecbs(cbs);
        };

        rc = cp->m_engine_list[p->idx.gid]->write(p->idx.bid, zptr, p->len, cbs);
        if(rc < 0) {

            cp->m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            // write error
            // TODO

            return;
        }
    }

}
*/
