#include <collect/page.hpp>

// max block len, manage memory block alloced by dpfsEngine::zmalloc
const size_t maxBlockLen = 20;
// ?
const size_t maxMemBlockLimit = 20;

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

int CPage::get(cacheStruct*& cptr, const bidx& idx, size_t len) {
    if(idx.gid >= m_engine_list.size()) {
        return -ERANGE;
    }
    cptr = nullptr;

    int rc = 0;

    // cacheStruct* cs;
    // dpfsEngine memory pointer
    void* zptr = nullptr;
    // io call back struct
    dpfs_engine_cb_struct* cbs = new dpfs_engine_cb_struct;

    CDpfsCache<bidx, cacheStruct*, PageClrFn>::cacheIter* ptr = m_cache.getCache(idx);
    if(!ptr) {
        // alloc zptr
        zptr = alloczptr(dpfs_lba_size * len);
        if(!zptr) {
            // can't malloc big block, reutrn error
            m_log.log_error("Can't malloc large block, size = %llu Bytes\n", dpfs_lba_size * len);
            goto errReturn;
        }



        // alloc cache block struct 
        cptr = alloccs();
        if(!cptr) {
            m_log.log_error("can't malloc cacheStruct, no memory\n");
            goto errReturn;
        }

        // use lambda to define callback func
        cbs->m_cb = [&cptr](void* arg, const dpfs_compeletion* dcp) {
            if(dcp->return_code) {
                CPage* pge = static_cast<CPage*>(arg);
                if(!pge) {
                    return;
                }
                cptr->status = cacheStruct::ERROR;
                pge->m_log.log_error("write fail code: %d, msg: %s\n", dcp->return_code, dcp->errMsg);
                return;
            }
            cptr->status = cacheStruct::VALID;
        };
        cbs->m_arg = this;

        cptr->status = cacheStruct::READING;
        // read from disk
        rc = m_engine_list[idx.gid]->read(idx.bid, zptr, len, cbs);
        if(rc < 0) {
            m_log.log_error("read from disk err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            goto errReturn;
        }


        cptr->idx = idx;
        cptr->len = len;
        cptr->zptr = zptr;
        // one for lru, one for user
        cptr->refs += 2;

        // insert to cache
        rc = m_cache.insertCache(cptr->idx, cptr);
        if(rc) {
            m_log.log_error("insert to cache err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            goto errReturn;
        }

        ++m_getCount;
        
        // std::chrono::milliseconds ns = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock().now().time_since_epoch());
        // while(!complete) {
        //     // wait for async read complete.
        // }
        // std::chrono::milliseconds ns1 = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock().now().time_since_epoch());
        // m_log.log_inf("wait time : %llu ms\n", (ns1 - ns).count());
        
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

int CPage::put(bidx idx, void* zptr, size_t len, bool wb) {
    if(idx.gid >= m_engine_list.size()) {
        return -ERANGE;
    }

    int rc = 0;
    cacheStruct* cs = nullptr;
    dpfs_engine_cb_struct* cbs = new dpfs_engine_cb_struct;

    CDpfsCache<bidx, cacheStruct*, PageClrFn>::cacheIter* cptr = nullptr;



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
        

        // insert the cache to lru system
        rc = m_cache.insertCache(idx, cs);
        if(rc) {
            m_log.log_error("insert to cache err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            --cs->refs;
            goto errReturn;
        }

    } else {
        // if loaded on cache, update it
        // recycle the memory
        freezptr(cptr->cache->zptr, cptr->cache->len);
        
        // write lock
        cptr->cache->rwLock.lock();

        // update the cache
        cptr->cache->zptr = zptr;
        cptr->cache->len = len;
        cptr->cache->dirty = 1;
        
        cptr->cache->rwLock.unlock();
        // rc = m_cache.loadCache(idx, cptr->cache);
    }


    // if write back immediate
    if(wb) {
        cbs->m_arg = this;
        // call back function for disk write
        cbs->m_cb = [&cptr](void* arg, const dpfs_compeletion* dcp) {
            cptr->cache->rwLock.unlock_shared();
            if(dcp->return_code) {
                cptr->cache->status = cacheStruct::ERROR;
                PageClrFn* pcf = reinterpret_cast<PageClrFn*>(arg);
                pcf->cp->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
                // write error
                // TODO process error

                cptr->cache->status = cacheStruct::ERROR;
                return;
            }
            cptr->cache->status = cacheStruct::VALID;
            cptr->cache->dirty = 0;
        };

        // read lock 
        cptr->cache->rwLock.lock_shared();

        // write to disk
        rc = m_engine_list[idx.gid]->write(idx.bid, zptr, len, cbs);
        if(rc < 0) {
            m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            // write error
            // TODO

            // if error, unlock immediately, else unlock in callback
            cptr->cache->rwLock.unlock_shared();
            goto errReturn;
        }
    } else {
        cptr->cache->dirty = 1;
    }

    return 0;

errReturn:
    if(cs) {
        freecs(cs);
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

// for class outside call
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

void CPage::freecs(cacheStruct* cs) {
    if(!cs) {
        return;
    }
    cs->zptr = nullptr;
    cs->dirty = 0;
    cs->status = cacheStruct::INVALID;
    m_csmLock.lock();
    m_cacheStructMemList.push_back(cs);
    m_csmLock.unlock();
}

cacheStruct* CPage::alloccs() {
    cacheStruct* cs = nullptr;
    if(!m_cacheStructMemList.empty()) {
        m_csmLock.lock();
        cs = m_cacheStructMemList.front();
        m_cacheStructMemList.pop_front();
        m_csmLock.unlock();
    } else {
        cs = new cacheStruct(this);
        if(!cs) {
            return nullptr;
        }
    }
    return cs;
}

void CPage::freezptr(void* zptr, size_t sz) {
    if(!zptr) {
        return;
    }
    if(sz < maxBlockLen) {
        m_zptrLock[sz].lock();
        m_zptrList[sz].push_back(zptr);
        m_zptrLock[sz].unlock();
    } else {
        m_engine_list[0]->zfree(zptr);
    }
}

void* CPage::alloczptr(size_t sz) {
    void* zptr = nullptr;
    if(sz < maxBlockLen) {
        if(!m_zptrList[sz].empty()) {
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
    } else {
        zptr = m_engine_list[0]->zmalloc(sz);
    }
    return zptr;
}

void CPage::freecbs(const dpfs_engine_cb_struct* cbs) {
    if(!cbs) {
        return;
    }
}


dpfs_engine_cb_struct* CPage::alloccbs() {
    dpfs_engine_cb_struct* cbs = nullptr;
    if(!m_cbMemList.empty()) {
        m_cbmLock.lock();
        if(m_cbMemList.empty()) {
            m_cbmLock.unlock();
            return new dpfs_engine_cb_struct;
        }
        cbs = m_cbMemList.front();
        m_cbMemList.pop_front();
        m_cbmLock.unlock();
    } else {
        cbs = new dpfs_engine_cb_struct;
        if(!cbs) {
            return nullptr;
        }
    }
    return cbs;
}

PageClrFn::PageClrFn(void* clrFnArg) : cp(reinterpret_cast<CPage*>(clrFnArg)) {

}

// write back only
void PageClrFn::operator()(cacheStruct*& p) {
    int rc = 0;
    // find disk group by gid, find block on disk by bid, write p, block number = blkNum
    // p->p must by alloc by engine_list->zmalloc

    void* zptr = p->zptr;

    dpfs_engine_cb_struct* cbs = new dpfs_engine_cb_struct;

    // some problem, need to considerate call back method
    if(p->dirty) {
        cbs->m_arg = this;

        // call back function for disk write
        cbs->m_cb = [&p](void* arg, const dpfs_compeletion* dcp) {
            p->rwLock.unlock_shared();
            PageClrFn* pcf = reinterpret_cast<PageClrFn*>(arg);

            if(dcp->return_code) {
                pcf->cp->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
                // write error
                // TODO process error
                
                // p->status = cacheStruct::ERROR;
                p->release();
                return;
            }
            p->release();
        };

        p->rwLock.lock_shared();
        rc = cp->m_engine_list[p->idx.gid]->write(p->idx.bid, zptr, p->len, cbs);
        if(rc < 0) {
            cp->m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            // write error
            // !-!QUESTION!-!
            // TODO

            p->rwLock.unlock_shared();
            return;
        }
    }


}

void PageClrFn::flush(cacheStruct*& p) {
    int rc = 0;
    // find disk group by gid, find block on disk by bid, write p, block number = blkNum
    // p->p must by alloc by engine_list->zmalloc

    void* zptr = p->zptr;
    dpfs_engine_cb_struct* cbs = new dpfs_engine_cb_struct;

    // some problem, need to considerate call back method
    if(p->dirty) { 
        cbs->m_arg = this;

        // call back function for disk write
        cbs->m_cb = [&p](void* arg, const dpfs_compeletion* dcp) {
            p->rwLock.unlock_shared();
            
            if(dcp->return_code) {
                PageClrFn* pcf = reinterpret_cast<PageClrFn*>(arg);
                pcf->cp->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
                // write error
                // TODO process error
                p->status = cacheStruct::ERROR;
                return;
            }
            p->dirty = 0;
        };

        p->rwLock.lock_shared();
        rc = cp->m_engine_list[p->idx.gid]->write(p->idx.bid, zptr, p->len, cbs);
        if(rc < 0) {
            cp->m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            // write error
            // TODO

            p->rwLock.unlock_shared();
            return;
        }
    }

}
