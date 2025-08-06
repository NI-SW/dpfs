#pragma once
#include <atomic>
#include <mutex>

class CSpin {
public:
    CSpin() {
        m_lock.store(0);
    }

    void lock() {
        while (m_lock.exchange(1, std::memory_order_acquire) == 1);
    }

    void unlock() {
        m_lock.store(0, std::memory_order_release);
    }

    bool try_lock() {
        while (m_lock.exchange(1, std::memory_order_acquire) == 1) {
            return false;
        }
        return true;
	}

    std::atomic<uint8_t> m_lock;
};


class CSpinB {
public:
    CSpinB() {
        m_lock.clear();
    }

    void lock() {
        while (m_lock.test_and_set()) {
		};
    }

    void unlock() {
        m_lock.clear();
    }

    bool try_lock() {
        if (m_lock.test_and_set()) {
			return false;
		}
		return true;
	}
private:
    std::atomic_flag m_lock;
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
class refObj {
public:
	refObj() {
		ref_count = new size_t;
		m_lock = new CSpin();
		objName = new std::string();
		*objName = "refObj";
		ref_count = 0;
	}
	refObj(const refObj& tgt) {
		tgt.m_lock->lock();
		objName = tgt.objName;
		ref_count = tgt.ref_count;
		m_lock = tgt.m_lock;
		++ref_count;
		tgt.m_lock->unlock();
	}

	refObj& operator=(const refObj& tgt) {
		m_lock->lock();
		if (this == &tgt) {
			m_lock->unlock();
			return *this;
		}

		if (--(*ref_count) == 0) {
			delete objName;
			delete ref_count;
		}
		m_lock->unlock();
		delete m_lock;


		tgt.m_lock->lock();
		objName = tgt.objName;
		ref_count = tgt.ref_count;
		m_lock = tgt.m_lock;
		++ref_count;
		tgt.m_lock->unlock();
		return *this;
	}

	refObj(refObj&& tgt) {
		objName = tgt.objName;
		ref_count = tgt.ref_count;
		m_lock = tgt.m_lock;
		tgt.objName = nullptr;
		tgt.ref_count = nullptr;
		tgt.m_lock = nullptr;
	}

	~refObj() {
		m_lock->lock();
		if (--(*ref_count) == 0) {
			delete objName;
			delete ref_count;
		}
		m_lock->unlock();
		delete m_lock;
	}
private:
	std::string* objName;
	CSpin* m_lock;
	size_t* ref_count;
};