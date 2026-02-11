/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#pragma once
#include <atomic>
#include <mutex>

class CSpin {
public:
    CSpin() {
        m_lock.store(0);
    }

    void lock() noexcept {
        while (m_lock.exchange(1, std::memory_order_acquire) == 1);
    }

    void unlock() noexcept {
        m_lock.store(0, std::memory_order_release);
    }

    bool try_lock() noexcept {
        while (m_lock.exchange(1, std::memory_order_acquire) == 1) {
            return false;
        }
        return true;
	}

    CSpin(const CSpin& t) {
        m_lock.store(t.m_lock.load());
    }

    CSpin& operator=(const CSpin& t) {
        m_lock.store(t.m_lock.load());
        // m_lock.store(0);
        return *this;
    }
private:
    std::atomic<uint8_t> m_lock;
};

/*
    a test for atomic_flag
*/
class CSpinB {
public:
    CSpinB() {
        m_lock.clear();
    }

    void lock() noexcept {
        while (m_lock.test_and_set()) {
		};
    }

    void unlock() noexcept {
        m_lock.clear();
    }

    bool try_lock() noexcept {
        if (m_lock.test_and_set()) {
			return false;
		}
		return true;
	}
private:
    std::atomic_flag m_lock;
};

template<typename T>
class CTemplateGuard {
public:
    CTemplateGuard(T& lock) : m_lock(lock) {
        m_error_code = m_lock.lock();
    }
    ~CTemplateGuard() {
        if(m_error_code == 0) {
            m_lock.unlock();
        }
    }
    int returnCode() const noexcept {
        return m_error_code;
    }
private:
    T& m_lock;
    int m_error_code = 0;
};

template<typename T>
class CTemplateReadGuard {
public:
    CTemplateReadGuard(T& lock, bool lockRightNow = true) : m_lock(lock) {
        if (lockRightNow) {
            error_code = m_lock.read_lock();
        } else {
            error_code = -ENOLCK;
        }
    }
    ~CTemplateReadGuard() {
        if(error_code == 0) {
            m_lock.read_unlock();
        }
    }
    int returnCode() const noexcept {
        return error_code;
    }

    int lockGuard() {
        if (error_code == -ENOLCK) {
            error_code = m_lock.read_lock();
        }
        return error_code;
    }

    /*
        @note release the lock, after call this function, the guard will not hold the lock anymore, 
        and you need to call lockGuard() to acquire the lock again if you want to use the guard.
    */
    void release() {
        if (error_code == 0) {
            m_lock.read_unlock();
            error_code = -ENOLCK;
        }
    }
    
private:
    T& m_lock;
    int error_code = -ENOLCK;
};

class CMutexGuard {
public:
    CMutexGuard(std::mutex& lock) : m_lock(lock) {
        m_lock.lock();
    }
    ~CMutexGuard() {
        m_lock.unlock();
    }
private:
	CMutexGuard(const CMutexGuard &);
	CMutexGuard & operator = (const CMutexGuard &);

private:
    std::mutex& m_lock;
};

class CRecursiveGuard {
public:
    CRecursiveGuard(std::recursive_mutex& lock) : m_lock(lock) {
        m_lock.lock();
    }
    ~CRecursiveGuard() {
        m_lock.unlock();
    }
private:
    CRecursiveGuard(const CRecursiveGuard &);
    CRecursiveGuard & operator = (const CRecursiveGuard &);
private:
    std::recursive_mutex& m_lock;
};

class CSpinGuard {
public:
    CSpinGuard(CSpin& lock) : m_lock(lock) {
        m_lock.lock();
    }
    ~CSpinGuard() {
        m_lock.unlock();
    }
private:
	CSpinGuard(const CSpinGuard &);
	CSpinGuard & operator = (const CSpinGuard &);

private:
    CSpin& m_lock;
};




// now useless
// class refObj {
// public:
// 	refObj() {
// 		ref_count = new size_t;
// 		m_lock = new CSpin();
// 		objName = new std::string();
// 		*objName = "refObj";
// 		ref_count = 0;
// 	}
// 	refObj(const refObj& tgt) {
// 		tgt.m_lock->lock();
// 		objName = tgt.objName;
// 		ref_count = tgt.ref_count;
// 		m_lock = tgt.m_lock;
// 		++ref_count;
// 		tgt.m_lock->unlock();
// 	}

// 	refObj& operator=(const refObj& tgt) {
// 		m_lock->lock();
// 		if (this == &tgt) {
// 			m_lock->unlock();
// 			return *this;
// 		}

// 		if (--(*ref_count) == 0) {
// 			delete objName;
// 			delete ref_count;
// 		}
// 		m_lock->unlock();
// 		delete m_lock;


// 		tgt.m_lock->lock();
// 		objName = tgt.objName;
// 		ref_count = tgt.ref_count;
// 		m_lock = tgt.m_lock;
// 		++ref_count;
// 		tgt.m_lock->unlock();
// 		return *this;
// 	}

// 	refObj(refObj&& tgt) {
// 		objName = tgt.objName;
// 		ref_count = tgt.ref_count;
// 		m_lock = tgt.m_lock;
// 		tgt.objName = nullptr;
// 		tgt.ref_count = nullptr;
// 		tgt.m_lock = nullptr;
// 	}

// 	~refObj() {
// 		m_lock->lock();
// 		if (--(*ref_count) == 0) {
// 			delete objName;
// 			delete ref_count;
// 		}
// 		m_lock->unlock();
// 		delete m_lock;
// 	}
// private:
// 	std::string* objName;
// 	CSpin* m_lock;
// 	size_t* ref_count;
// };