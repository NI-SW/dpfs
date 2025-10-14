#include <collect/page.hpp>

// max block len, manage memory block alloced by dpfsEngine::zmalloc
const size_t maxBlockLen = 20;
// ?
const size_t maxMemBlockLimit = 20;

CPage::CPage(std::vector<dpfsEngine*>& engine_list, size_t cacheSize, logrecord& log) : m_engine_list(engine_list), m_cache(cacheSize, this), m_log(log) {
    // the length larger then 10 need Special Processing
    m_zptrList.resize(maxBlockLen);
    m_zptrLock.resize(maxBlockLen);
    m_exit = 0;
    m_hitCount = 0;
    m_getCount = 0;
}

CPage::~CPage() {
    m_exit = 1;
}

cacheStruct* CPage::get(const bidx& idx, size_t len) {
    if(idx.gid >= m_engine_list.size()) {
        return nullptr;
    }

    int rc = 0;
    // cache block struct
    cacheStruct* cs = nullptr;
    // dpfsEngine memory pointer

    void* zptr = nullptr;
    dpfs_engine_cb_struct cbs;

    CDpfsCache<bidx, cacheStruct*, PageClrFn>::cacheIter* ptr = m_cache.getCache(idx);
    if(!ptr) {
        // zptr = nullptr;
        // alloc zptr
        if(len < maxBlockLen) {
            if(!m_zptrList[len].empty()) {
                // acquire a memory block from mem list
                m_zptrLock[len].lock();
                zptr = m_zptrList[len].front();
                m_zptrList[len].pop_front();
                m_zptrLock[len].unlock();
            } else {
                zptr = m_engine_list[idx.gid]->zmalloc(dpfs_lba_size * len);
                if(!zptr) {
                    // can't malloc mem block, reutrn error
                    m_log.log_error("Can't malloc block, size = %llu Bytes\n", dpfs_lba_size * len);
                    goto errReturn;
                }
            }
        } else { 
            // big block
            zptr = m_engine_list[idx.gid]->zmalloc(dpfs_lba_size * len);
            if(!zptr) {
                // can't malloc big block, reutrn error
                m_log.log_error("Can't malloc large block, size = %llu Bytes\n", dpfs_lba_size * len);
                goto errReturn;
            }
        }

        // async read, if read completed, it will change to true.
        volatile bool complete = false;
        // use lambda to define callback func
        cbs.m_cb = [&complete](void* arg, const dpfs_compeletion* dcp) {

            complete = true;
            if(dcp->return_code) {
                CPage* pge = static_cast<CPage*>(arg);
                if(!pge) {
                    return;
                }
                pge->m_log.log_error("write fail code: %d, msg: %s\n", dcp->return_code, dcp->errMsg);
            }
        };
        cbs.m_arg = this;

        // read from disk
        rc = m_engine_list[idx.gid]->read(idx.bid, zptr, len, &cbs);
        if(rc < 0) {
            m_log.log_error("read from disk err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            goto errReturn;
        }

        // alloc cache block struct 
        // cs = nullptr;
        if(!m_cacheStructMemList.empty()) {
            m_csmLock.lock();
            cs = m_cacheStructMemList.front();
            m_cacheStructMemList.pop_front();
            m_csmLock.unlock();
        } else {
            cs = new cacheStruct;
            if(!cs) {
                m_log.log_error("can't malloc cacheStruct, no memory\n");
                goto errReturn;
            }
        }

        cs->idx = idx;
        cs->len = len;
        cs->zptr = zptr;

        // insert to cache
        rc = m_cache.insertCache(cs->idx, cs);
        if(rc) {
            m_log.log_error("insert to cache err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            goto errReturn;
        }

        while(!complete) {
            // wait for async read complete.
        }
        ++m_getCount;
        return cs;
    }
    ++m_getCount;
    ++m_hitCount;
    return ptr->cache;

errReturn:
    if(cs) {
        m_csmLock.lock();
        m_cacheStructMemList.push_back(cs);
        m_csmLock.unlock();
    }
    if(zptr) {
        if(len < maxBlockLen) {
            m_zptrLock[len].lock();
            m_zptrList[len].push_back(zptr);
            m_zptrLock[len].unlock();
        } else {
            m_engine_list[idx.gid]->zfree(zptr);
        }
        zptr = nullptr;
    }
    return nullptr;
}

int CPage::put(bidx idx, void* zptr, size_t len, bool wb) {
    if(idx.gid >= m_engine_list.size()) {
        return -ERANGE;
    }

    int rc = 0;
    cacheStruct* cs = nullptr;
    dpfs_engine_cb_struct cbs;
    volatile bool cb_completed = false;
    CDpfsCache<bidx, cacheStruct*, PageClrFn>::cacheIter* cptr = nullptr;

    // if write back immediate
    if(wb) {
        cbs.m_arg = this;

        // call back function for disk write
        cbs.m_cb = [&cb_completed](void* arg, const dpfs_compeletion* dcp) {
            PageClrFn* pcf = reinterpret_cast<PageClrFn*>(arg);
            cb_completed = true;
            if(dcp->return_code) {
                pcf->cp->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
                // write error
                // TODO process error

            }
        };
        // write to disk
        rc = m_engine_list[idx.gid]->write(idx.bid, zptr, len, &cbs);
        if(rc < 0) {
            m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            // write error
            // TODO

            goto errReturn;
        }
    } else {
        cb_completed = true;
    }

    // update cache
    cptr = m_cache.getCache(idx);
    // if this idx is not loaded on cache
    if(!cptr) {
        // get a cache struct object.
        if(!m_cacheStructMemList.empty()) {
            // get from list
            m_csmLock.lock();
            cs = m_cacheStructMemList.front();
            m_cacheStructMemList.pop_front();
            m_csmLock.unlock();
        } else {
            // malloc one
            cs = new cacheStruct;
            if(!cs) {
                m_log.log_error("can't malloc cacheStruct, no memory\n");
                goto errReturn;
            }
        }

        cs->zptr = zptr;
        cs->idx = idx;
        cs->len = len;

        // if immediately write back
        if(wb) {
            cs->dirty = false;
        } else {
            cs->dirty = true;
        }

        // insert the cache to lru system
        rc = m_cache.insertCache(idx, cs);
        // if immediately write back


    } else {
        // if loaded on cache, update it
        // recycle the memory
        m_zptrLock[cptr->cache->len].lock();
        m_zptrList[cptr->cache->len].push_back(cptr->cache->zptr);
        m_zptrLock[cptr->cache->len].unlock();
        
        // write lock
        cptr->cache->rwLock.lock();

        // update the cache
        cptr->cache->zptr = zptr;
        cptr->cache->len = len;
        // cptr->cache->idx = idx;

        // if immediately write back
        if(wb) {
            cptr->cache->dirty = false;
        } else {
            cptr->cache->dirty = true;
        }
        
        cptr->cache->rwLock.unlock();
        // rc = m_cache.loadCache(idx, cptr->cache);
    }


    while(!cb_completed) {
        // wait for write back complete
    }


    return 0;

errReturn:
    if(cs) {
        m_csmLock.lock();
        m_cacheStructMemList.push_back(cs);
        m_csmLock.unlock();
    }
    return rc;
}

int CPage::writeBack(cacheStruct *cache) {
    // TODO 

    return 0;
}

int CPage::flush() {
    // TODO 

    return 0;
}

void* CPage::cacheMalloc(size_t sz) {
    void* zptr = nullptr;
    if(sz < maxBlockLen) {
        if(m_zptrList[sz].empty()) {
            return m_engine_list[0]->zmalloc(sz);
        }

        m_zptrLock[sz].lock();
        if(m_zptrList[sz].empty()) {
            m_zptrLock[sz].unlock();
            return m_engine_list[0]->zmalloc(sz);
        }
        zptr = m_zptrList[sz].front();
        m_zptrList[sz].pop_front();
        m_zptrLock[sz].unlock();

    } else {
        zptr = m_engine_list[0]->zmalloc(sz);
    }
    return zptr;
}

PageClrFn::PageClrFn(void* clrFnArg) : cp(reinterpret_cast<CPage*>(clrFnArg)) {

}

void PageClrFn::operator()(cacheStruct*& p) {
    int rc = 0;
    // find disk group by gid, find block on disk by bid, write p, block number = blkNum
    // p->p must by alloc by engine_list->zmalloc

    size_t zLen = 0;
    uint64_t zgid = p->idx.gid;
    void* zptr = p->zptr;
    volatile bool cb_completed = false;
    dpfs_engine_cb_struct cbs;

    // some problem, need to considerate call back method
    if(p->dirty) {
        cbs.m_arg = this;

        // call back function for disk write
        cbs.m_cb = [&cb_completed](void* arg, const dpfs_compeletion* dcp) {
            PageClrFn* pcf = reinterpret_cast<PageClrFn*>(arg);
            cb_completed = true;
            if(dcp->return_code) {
                pcf->cp->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
                // write error
                // TODO process error

            }
        };

        rc = cp->m_engine_list[p->idx.gid]->write(p->idx.bid, zptr, p->len, &cbs);
        if(rc < 0) {
            cp->m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            // write error
            // TODO
        }
    } else {
        cb_completed = true;
    }

    // free cacheStruct, recycle the memory
    p->zptr = nullptr;
    cp->m_csmLock.lock();
    cp->m_cacheStructMemList.push_back(p);
    cp->m_csmLock.unlock();
    zLen = p->len;
    p = nullptr;
    

    while(!cb_completed) {
        // wait for call back complete.
        
    }


    // free zptr(alloced by dpfsEngine::zmalloc)
    if(zLen < maxBlockLen) {
        cp->m_zptrLock[zLen].lock();
        cp->m_zptrList[zLen].push_back(zptr);
        cp->m_zptrLock[zLen].unlock();
    } else {
        cp->m_engine_list[zgid]->zfree(zptr);
    }



}

void PageClrFn::flush(cacheStruct*& p) {
    int rc = 0;
    // find disk group by gid, find block on disk by bid, write p, block number = blkNum
    // p->p must by alloc by engine_list->zmalloc

    void* zptr = p->zptr;
    volatile bool cb_completed = false;
    dpfs_engine_cb_struct cbs;

    // some problem, need to considerate call back method
    if(p->dirty) { 
        cbs.m_arg = this;

        // call back function for disk write
        cbs.m_cb = [&cb_completed](void* arg, const dpfs_compeletion* dcp) {
            PageClrFn* pcf = reinterpret_cast<PageClrFn*>(arg);
            cb_completed = true;
            if(dcp->return_code) {
                pcf->cp->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
                // write error
                // TODO process error

            }
        };

        rc = cp->m_engine_list[p->idx.gid]->write(p->idx.bid, zptr, p->len, &cbs);
        if(rc < 0) {
            cp->m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            // write error
            // TODO
        }
    } else {
        cb_completed = true;
    }
    

    while(!cb_completed) {
        // wait for call back complete.
        
    }

}
