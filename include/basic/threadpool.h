/*
线程池
*/
#include <pthread.h>

class CSpinLock {
public:
    CSpinLock() {
        pthread_spin_init(&m_lock, 0);
    }
    ~CSpinLock() {
        pthread_spin_destroy(&m_lock);
    }
    void Lock() {
        pthread_spin_lock(&m_lock);
    }
    void Unlock() {
        pthread_spin_unlock(&m_lock);
    }
    bool TryLock() {
        return pthread_spin_trylock(&m_lock) == 0;
    }
private:
    pthread_spinlock_t m_lock;
	
};
