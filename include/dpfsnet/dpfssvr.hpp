/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#pragma once
#include <dpfsnet/dpfscli.hpp>

using listenCallback = void (*)(CDpfscli& cli, void* cb_arg);

class CDpfssvr {
public:
    CDpfssvr() = default;
    virtual ~CDpfssvr() = default;

    
    /*
        @param server_string: Server listening string for the network connection.
        @param cb: Callback function to be called when a new connection is established.
        @return 0 on success, else on failure.
        @example "ip:0.0.0.0 port:20500"(for tcp protocol)
    */
    virtual int listen(const char* server_string, listenCallback cb, void* cb_arg) = 0;

    /*
        @note close all connections and stop the server.
        @return 0 on success, else on failure.[p0]
    */
    virtual int stop() = 0;

    /*
        @param log_path: Path to the log file.
        @return 0 on success, else on failure.
    */
    virtual void set_log_path(const char* log_path) = 0;

    /*
        @param level: Log level.
        @return 0 on success, else on failure.
    */
    virtual void set_log_level(int level) = 0;

    /*
        @return name of the object
    */
    virtual const char* name() const = 0;
    
};

CDpfssvr* newServer(const char* net_type);