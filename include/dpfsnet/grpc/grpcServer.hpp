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
#include <log/logbinary.h>


/*
    @note TCP server class for handling multiple client connections.
    This class is responsible for listening to incoming connections, managing client threads,
    and cleaning up resources when the server is stopped.
*/
class CDpfsGrpcsvr : public CDpfssvr {
public:
    CDpfsGrpcsvr();
    virtual ~CDpfsGrpcsvr() override;

    
    /*
        @param serverString: Server listening string for the network connection.
        @param cb: Callback function to be called when a new connection is established.
        @return 0 on success, else on failure.
        @example "ip:0.0.0.0 port:20500"
    */
    virtual int listen(const char* serverString, listenCallback cb, void* cb_arg) override;

    virtual int listen(const char* serverString, void* service) override;

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
        return "DPFS_GRPC_SERVER"; 
    }

    virtual bool is_listening() const override {
        return listening;
    }
    
private:

    std::recursive_mutex m_lock;
    bool m_exit = false;
    bool listening = false;
    std::thread grpcServerThread;
    logrecord log;
    
    void* grpcServer = nullptr;
    // grpcStop stop_cb = nullptr;
};
