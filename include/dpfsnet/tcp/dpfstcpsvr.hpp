/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#pragma once
#include "dpfstcp.hpp"
#include <queue>
#include <thread>
#include <list>
#include <threadlock.hpp>

/*
    @note TCP server class for handling multiple client connections.
    This class is responsible for listening to incoming connections, managing client threads,
    and cleaning up resources when the server is stopped.
*/
class CDpfstcpsvr : public CDpfssvr {
public:
    CDpfstcpsvr();
    virtual ~CDpfstcpsvr() override;

    
    /*
        @param serverString: Server listening string for the network connection.
        @param cb: Callback function to be called when a new connection is established.
        @return 0 on success, else on failure.
        @example "ip:0.0.0.0 port:20500"
    */
    virtual int listen(const char* serverString, listenCallback cb, void* cb_arg) override;

    /*
        @return: 0 on success, else on failure.
        @note: close all connections and stop the server, will wait for all threads to finish. for disconnect all client and stop server.
    */
    virtual int stop() override;

    /*
        @param log_path: Path to the log file.
        @return 0 on success, else on failure.
    */
    virtual void set_log_path(const char* log_path) override;

    /*
        @param level: Log level.
        @return 0 on success, else on failure.
    */
    virtual void set_log_level(int level) override;

    /*
        @return name of the object
    */
    virtual const char* name() const override { 
        return "DPFS_TCP_SERVER"; 
    }

    virtual bool is_listening() const override {
        return (m_listening && (sockfd != -1));
    }
    
private:
    logrecord log;
    volatile bool m_exit = false;
    int sockfd = -1;
    int listenQueue = 16;
    volatile bool m_listening = false;
    struct addrinfo *localAddr = nullptr;
    std::thread listenGuard;

    struct dpfsconn {
        dpfsconn() { }

        ~dpfsconn() {
            if (thd.joinable()) {
                thd.join();
            }
        }

        CDpfsTcp cli;
        std::thread thd;
        // position in client list
        std::list<dpfsconn*>::iterator it;
        uint32_t ip;
        uint16_t port;
        // char ipStr[16] { 0 };
        // char portStr[16] { 0 };
    };

    std::list<dpfsconn*> clients;
    CSpin clientLock;
    std::queue<dpfsconn*> destroyQueue;
    CSpin destroyLock;
    std::thread destroyGuard;
    std::recursive_mutex m_lock;



};
