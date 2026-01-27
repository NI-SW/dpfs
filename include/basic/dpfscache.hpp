#pragma once
#include <cstddef>
#include <list>
#include <unordered_map>
#include <threadlock.hpp>
// #define __DPFS_CACHE_DEBUG__
#define __DPCACHE_DEBUG__

#ifdef __DPCACHE_DEBUG__
#include <iostream>
using namespace std;
#endif

#ifdef __DPFS_CACHE_DEBUG__
#include <string>
#include <cstring>
#include <iostream>
using namespace std;

template<class T>
class CClearFunc {
    public:
    CClearFunc(void* initArg){}
    void operator()(T& p) {
        
    }
    
    void flush(T& p) {
        
    }
};

#endif

// for quick range compare
/*
    @tparam IDX cache index
    @tparam T cache type
    @tparam CLEARFUNC function to clear cache when drop cache or flush cache
*/
#ifdef __DPFS_CACHE_DEBUG__
template<class IDX = string, class T = int, class CLEARFUNC = CClearFunc<T>>
#else 
template<class IDX, class T, class CLEARFUNC>
#endif
class CDpfsCache {
public:
    /*
        @param clrFnArg use a pointer to initialize clear function
        @note 
    */
    CDpfsCache(void* clrFnArg = nullptr) : clrfunc(clrFnArg) {
        m_cacheSize = 1024;
        m_cachesList.clear();
        m_cacheMap.clear();
    }
    /*
        @param cs cache size
    */
    CDpfsCache(size_t cs, void* clrFnArg = nullptr) : clrfunc(clrFnArg) {
        m_cacheSize = cs;
        m_cachesList.clear();
        m_cacheMap.clear();
    }

    ~CDpfsCache() {
        
        size_t sz = m_cacheMap.size();

        if(sz > 0) {
            int finish_indicator = 0;
            for (auto cacheIt = m_cacheMap.begin(); cacheIt != m_cacheMap.end(); ++cacheIt) {

                #ifdef __DPCACHE_DEBUG__
                cout << "Deleting cache idx: " << (*cacheIt).first.bid << " cache: " << (*cacheIt).second->cache->getPtr() << endl;
                cout << "size = " << sz << endl;
                #endif

                if(sz <= 1) {
                    clrfunc((*cacheIt).second->cache, &finish_indicator);
                } else {
                    clrfunc((*cacheIt).second->cache);
                }
                --sz;
            }
            
            while(finish_indicator == 0) {
                if(finish_indicator < 0) {
                    // error occurred TODO:: PROCESS ERROR
                    #ifdef __DPCACHE_DEBUG__
                    abort();
                    #endif
                    break;
                }
            }
        }
        
        for (auto& cacheIt : m_cachesList) {
            delete (cacheIter*)cacheIt;
        }

        m_cachesList.clear();

    }

    struct cacheIter {
        T cache;
        IDX idx;
    private:
        friend CDpfsCache;
        std::list<void*>::iterator selfIter;
    };

    /*
        @param idx index of cache
        @note return the cache if found, else return nullptr
    */
    CDpfsCache::cacheIter* getCache(const IDX& idx) {
        // if found
        auto cs = m_cacheMap.find(idx);
        if (cs != m_cacheMap.end()) {
            // move this cache to head
            m_lock.lock();
            m_cachesList.splice(m_cachesList.begin(), m_cachesList, cs->second->selfIter);
            // m_cachesList.erase(cs->second->selfIter);
            // m_cachesList.push_front(cs->second);
            // cs->second->selfIter = m_cachesList.begin();
            m_lock.unlock();
            return cs->second;
        }

        return nullptr;
    }

    /*
        @param idx index of the cache
        @param cache cache to insert
        @note insert cache to LRU system, if cache is inserted, update it.
    */
    int loadCache(const IDX& idx, const T& cache) {
        // insert or update cache
        cacheIter* it = getCache(idx);
        if (it) {
            it->cache = cache;
            return 0;
        }

        // if larger than max size, remove the last
        if (m_cachesList.size() >= m_cacheSize) {

            m_lock.lock();

            // reuse the mem
            it = reinterpret_cast<cacheIter*>(m_cachesList.back());
            // erase cache in map, and pop up the oldest unused cache
            m_cacheMap.erase(it->idx);
            m_cachesList.pop_back();

            m_lock.unlock();
            
            clrfunc(it->cache);
        }
        else {
            it = new cacheIter;
            if (!it) {
                return -ENOMEM;
            }
        }

        it->cache = cache;
        it->idx = idx;

        m_lock.lock();

        m_cachesList.push_front(it);
        m_cacheMap[idx] = it;
        it->selfIter = m_cachesList.begin();

        m_lock.unlock();


#ifdef __DPFS_CACHE_DEBUG__
        cout << " now cache list: " << endl;
        for (auto& it : m_cachesList) {
            cout << ((cacheIter*)it)->idx << " : " << ((cacheIter*)it)->cache << endl;
        }
#endif
        
        return 0;
    }

    /*
        @param idx index of the cache
        @param cache cache to insert
        @note insert cache to LRU system, do not check if the cache inserted.
    */
    int insertCache(const IDX& idx, const T& cache) {
        cacheIter* it = nullptr;
        // if larger than max size, remove the last one
        if (m_cachesList.size() >= m_cacheSize) {

            m_lock.lock();

            // reuse the mem
            it = reinterpret_cast<cacheIter*>(m_cachesList.back());
            // erase cache in map, and pop up the oldest unused cache
            m_cacheMap.erase(it->idx);
            m_cachesList.pop_back();
            
            m_lock.unlock();
            
            clrfunc(it->cache);
        }
        else {
            it = new cacheIter;
            if (!it) {
                return -ENOMEM;
            }
        }

        it->cache = cache;
        it->idx = idx;

        m_lock.lock();

        m_cachesList.push_front(it);
        m_cacheMap[idx] = it;
        it->selfIter = m_cachesList.begin();
        
        m_lock.unlock();


#ifdef __DPFS_CACHE_DEBUG__
        cout << " now cache list: " << endl;
        for (auto& it : m_cachesList) {
            cout << ((cacheIter*)it)->idx << " : " << ((cacheIter*)it)->cache << endl;
        }
#endif
        
        return 0;
    }

    /*
        @note flush all cache to disk immediate
        @return 0 on success, else on failure
    */
    int flush() {
        m_lock.lock();
        // for (auto cacheIt = m_cacheMap.begin(); cacheIt != m_cacheMap.end(); ++cacheIt) {
        //     clrfunc.flush((*cacheIt).second->cache);
        // }

        clrfunc.flush(&m_cacheMap);

        m_lock.unlock();
        return 0;
    }
private:
    // limit of how many caches in pool
    size_t m_cacheSize;

    /*
        cache struct list, use to record cache info and sort
        use void* as type is becaues list<cacheIter*>::iterator can't be compile with cacheIter*
    */
    std::list<void*> m_cachesList;
    // cache map, use to find cache by idx
    std::unordered_map<IDX, CDpfsCache<IDX, T, CLEARFUNC>::cacheIter*> m_cacheMap;
    CLEARFUNC clrfunc;
    CSpin m_lock;
};



#ifdef __DPFS_CACHE_DEBUG__
class myclear {
public:
    myclear() {}
    void operator()(void* p) {
        delete (int*)p;
    }
};


int test() {
        CDpfsCache<std::string, int*, myclear> test(5);

    int* a = new int;

    for (int i = 0; i < 20; ++i) {
        *a = i;
        test.loadCache((std::string)"qwe" + to_string(i), a);
        a = new int;
    }

    std::string line;
    while (1) {
        cin >> line;
        if (line == "qqq") {
            return 0;
        }
        else if (line == "load") {
            cout << "input load string:" << endl;
            cin >> line;
            cout << "input value:" << endl;
            int* val = new int;
            cin >> *val;

            test.loadCache(line, val);
        }
        a = test.getCache(line);
        if (a) {
            cout << *a << endl;
        }
        else {
            cout << "not found" << endl;
        }

    }

    return 0;
}
#endif