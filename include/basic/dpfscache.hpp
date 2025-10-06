#include <cstddef>
#include <list>
#include <unordered_map>
#include <storage/engine.hpp>
// #define __DPFS_CACHE_DEBUG__

#ifdef __DPFS_CACHE_DEBUG__
#include <string>
#include <cstring>
#include <iostream>
using namespace std;
#endif

template<class T>
class CClearFunc {
public:
    CClearFunc(){}
    void operator()(T& p) {

    }
};

template<class IDX, class T>
class CStorageSample {
public:
    CStorageSample(dpfsEngine* engine) : eng(engine) {
        data = eng->zmalloc(dpfs_lba_size * 10);
    }
    ~CStorageSample() {

    }

    // from index load target
    T* load(IDX idx) {
        eng->read();
        return nullptr;
    }

    // write back to storage
    int writeBack(IDX idx, T* cache) {
        return 0;
    }

private:
    dpfsEngine* eng;
    void* data;
};

// for quick range compare
/*
    @tparam IDX cache index
    @tparam T cache type
    @tparam CLEARFUNC function to clear cache when drop cache
*/
#ifdef __DPFS_CACHE_DEBUG__
template<class IDX = string, class T = int, class CStorage = CStorageSample<IDX, T>, class CLEARFUNC = CClearFunc<T>>
#else 
template<class IDX, class T, class CStorage = CStorageSample<IDX, T>, class CLEARFUNC = CClearFunc<T>>
#endif
class CDpfsCache {
public:
    CDpfsCache(CStorage& st) : m_storage(st){
        m_cacheSize = 1024;
        m_cachesList.clear();
        m_cacheMap.clear();
    }
    /*
        @param cs cache size
    */
    CDpfsCache(size_t cs, CStorage& st) : m_storage(st) {
        m_cacheSize = cs;
        m_cachesList.clear();
        m_cacheMap.clear();
    }

    ~CDpfsCache() {
        for (auto cacheIt = m_cacheMap.begin(); cacheIt != m_cacheMap.end(); ++cacheIt) {
            clrfunc((*cacheIt).second->cache);
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
            m_cachesList.erase(cs->second->selfIter);
            m_cachesList.push_front(cs->second);
            cs->second->selfIter = m_cachesList.begin();
            return cs->second;
        } else {
            // not found in cache, try to find on storage, and load it to cache
            //T* p = m_storage.load(idx);
            //if (p) {
            //    int rc = loadCache(idx, *p);
            //}
        }
        return nullptr;
    }

    /*
        @param idx index of the cache
        @param cache cache to insert
        @note insert cache to LRU system
    */
    int loadCache(const IDX& idx, const T& cache) {
        // insert new cache to list and map
        cacheIter* it = getCache(idx);
        if (it) {
            it->cache = cache;
            return 0;
        }

        // if larger than max size, remove the last
        if (m_cachesList.size() >= m_cacheSize) {
            // reuse the mem
            it = reinterpret_cast<cacheIter*>(m_cachesList.back());
            // erase cache in map, and pop up the oldest unused cache
            m_cacheMap.erase(reinterpret_cast<cacheIter*>(m_cachesList.back())->idx);
            clrfunc(reinterpret_cast<cacheIter*>(m_cachesList.back())->cache);
            m_cachesList.pop_back();
        }
        else {
            it = new cacheIter;
            if (!it) {
                return -ENOMEM;
            }
        }

        it->cache = cache;
        it->idx = idx;
        m_cachesList.push_front(it);
        it->selfIter = m_cachesList.begin();
        m_cacheMap[idx] = it;

#ifdef __DPFS_CACHE_DEBUG__
        cout << " now cache list: " << endl;
        for (auto& it : m_cachesList) {
            cout << ((cacheIter*)it)->idx << " : " << ((cacheIter*)it)->cache << endl;
        }
#endif
        
        return 0;
    }

private:
    size_t m_cacheSize;
    std::list<void*> m_cachesList;
    std::unordered_map<IDX, cacheIter*> m_cacheMap;
    CLEARFUNC clrfunc;
    CStorage& m_storage;
};

class myclear {
public:
    myclear() {}
    void operator()(void* p) {
        delete (int*)p;
    }
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