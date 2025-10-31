#include <collect/page.hpp>

// max block len, manage memory block alloced by dpfsEngine::zmalloc
const size_t maxBlockLen = 20;
// ?
const size_t maxMemBlockLimit = 20;

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
    cptr = nullptr;

    int rc = 0;

    // cacheStruct* cs;
    // dpfsEngine memory pointer
    void* zptr = nullptr;
    bool updateCache = false;

    CDpfsCache<bidx, cacheStruct*, PageClrFn>::cacheIter* ptr = m_cache.getCache(idx);
    if(ptr) {
        m_log.log_inf("found cache in LRU, gid=%llu bid=%llu len=%llu\n", idx.gid, idx.bid, ptr->cache->len);

        if(ptr->cache->len < len) {
            updateCache = true;

        }
    }

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

        // cbvar* cbv = reinterpret_cast<cbvar*>((char*)cbs + sizeof(dpfs_engine_cb_struct));
        // cbv->cptr = cptr;
        // cbv->cbs = cbs;
        // cbv->page = this;
        // cbs->m_acb = page_get_cb;
        // cbs->m_arg = cbv;

        // use lambda to define callback func
        cbs->m_cb = [&cptr, cbs](void* arg, const dpfs_compeletion* dcp) {

            CPage* pge = static_cast<CPage*>(arg);
            if(!pge) {
                // free(cbs);
                delete cbs;
                return;
            }

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
            goto errReturn;
        }



        if(updateCache) {
            cptr->refs += 1;
        } else {
            cptr->idx = idx;
            // one for lru, one for user
            cptr->refs += 2;
            // insert to cache
            rc = m_cache.insertCache(cptr->idx, cptr);
            if(rc) {
                m_log.log_error("insert to cache err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
                goto errReturn;
            }
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

int CPage::put(const bidx& idx, void* zptr, int* finish_indicator, size_t len, bool wb) {
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
    }


    // if write back immediate
    if(wb) {
        cptr = m_cache.getCache(idx);
        if(!cptr) {
            m_log.log_error("can't find cache before write back, gid=%llu bid=%llu len=%llu\n", idx.gid, idx.bid, len);
            rc = -ENOENT;
            goto errReturn;
        }

        rc = cptr->cache->lock();
        if(rc) {
            // TODO process error

            m_log.log_error("cache status invalid before write back, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            goto errReturn;
        }
        cptr->cache->status = cacheStruct::WRITING;
        cptr->cache->unlock();

        if(finish_indicator) {
            dpfs_engine_cb_struct* cbs = alloccbs();// new dpfs_engine_cb_struct;
            if(!cbs) {
                m_log.log_error("can't malloc dpfs_engine_cb_struct, no memory\n");
                rc = -ENOMEM;
                goto errReturn;
            }

            cbs->m_arg = this;
            // call back function for disk write
            cbs->m_cb = [cptr, cbs, finish_indicator](void* arg, const dpfs_compeletion* dcp) {
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

            rc = m_engine_list[idx.gid]->write(idx.bid, zptr, len, cbs);
        } else {
            rc = m_engine_list[idx.gid]->write(idx.bid, zptr, len);
        }

        if(rc < 0) {
            // if error, unlock immediately, else unlock in callback
            m_log.log_error("write to disk err, gid=%llu bid=%llu len=%llu rc = %d\n", idx.gid, idx.bid, len, rc);
            // write error
            // TODO
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

// int CPage::writeBack(cacheStruct *cache) {
//     // TODO 
//     return 0;
// }

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

}

// write back only
void PageClrFn::operator()(cacheStruct*& p) {
    if(p->idx.gid != 0) {
        // write is only allowed on gid 0
        p->release();
        return;
    }

    int rc = 0;
    // find disk group by gid, find block on disk by bid, write p, block number = blkNum
    // p->p must by alloc by engine_list->zmalloc

    void* zptr = p->zptr;



    // some problem, need to considerate call back method
    if(p->dirty) {

        // change status to writing
        rc = p->lock();
        if(rc) {
            // TODO process error
            cp->m_log.log_error("cache status invalid before write back, gid=%llu bid=%llu len=%llu rc = %d\n", p->idx.gid, p->idx.bid, p->len, rc);
            return;
        }
        p->status = cacheStruct::WRITING;
        p->unlock();

        dpfs_engine_cb_struct* cbs = this->cp->alloccbs(); //new dpfs_engine_cb_struct;
        if(!cbs) {
            cp->m_log.log_error("can't malloc dpfs_engine_cb_struct, no memory, write back fail!, gid: %llu, bid: %llu\n", p->idx.gid, p->idx.bid);
            return;
        }

        cbs->m_arg = this;
        // call back function for disk write
        cbs->m_cb = [&p, cbs](void* arg, const dpfs_compeletion* dcp) {
            PageClrFn* pcf = reinterpret_cast<PageClrFn*>(arg);

            if(dcp->return_code) {
                pcf->cp->m_log.log_error("write to disk error, rc = %d, message: %s\n", dcp->return_code, dcp->errMsg);
                // write error
                // TODO process error
                
                // p->status = cacheStruct::ERROR;
                p->release();
                pcf->cp->freecbs(cbs);
                return;
            }
            p->release();
            pcf->cp->freecbs(cbs);
        };

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
