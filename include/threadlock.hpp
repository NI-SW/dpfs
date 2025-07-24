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

    std::atomic<char> m_lock;
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

