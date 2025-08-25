/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#pragma once
#include <dpfsnet/dpfssvr.hpp>
#include <queue>
#include <thread>
#include <list>
#include <threadlock.hpp>
#include <condition_variable>
#include <log/logbinary.h>

#ifdef _WIN64
void initWinsock();
void destroyWinsock();
#else 
void initWinsock();
void destroyWinsock();
#endif

class CDpfsTcp : public CDpfscli {
public:

    CDpfsTcp();
    virtual ~CDpfsTcp() override;

    /*
        "ip:192.168.34.12 port:20500"
    */
    virtual int connect(const char* connString) override;
    virtual int disconnect() override;
    virtual int send(const char* buffer, int size) override;
    virtual int recv(char*& buffer, int* retsize) override;
    // virtual void* bufalloc(size_t size) override;
    virtual void buffree(void* buffer) override;
    /*
        @param log_path: Path to the log file.
        @return 0 on success, else on failure.
        @note not thread safe
    */
    virtual void set_log_path(const char* log_path) override;

    /*
        @param level: Log level.
        @return 0 on success, else on failure.
        @note not thread safe
    */
    virtual void set_log_level(int level) override;

private:
    logrecord log;
    struct dpfsmsg {
        uint32_t size = 0;
        char data[];
    };
    // wait all pending data sent
    void syncSend() const;
    // clear all cache data in send and recv queue.
    void clearCache();
    void clearEnv();


    // thread to send data
    std::thread sendGuard;
    std::queue<dpfsmsg*> sendQueue;
    CSpin sendLock;
    std::queue<dpfsmsg*> sendthdQueue;
    CSpin sendthdLock;
    std::condition_variable sendCv;
    std::mutex sendMutex;



    // thread to recv data
    std::thread recvGuard;
    std::queue<dpfsmsg*> recvQueue;
    CSpin recvLock;
    // this queue only used in recv thread
    std::queue<dpfsmsg*> recvthdQueue;
    CSpin recvthdLock;

    // for client connect
    struct addrinfo *targetAddr = nullptr;
    // for server listen
    // struct addrinfo *addr = nullptr;
    friend class CDpfstcpsvr;
    // object lock
    CSpin m_lock;
    int sockfd = -1;
    volatile bool m_exit = false;
    volatile bool m_connected = false;
    volatile bool m_reject = false;

};
