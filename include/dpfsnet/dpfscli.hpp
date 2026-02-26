/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#pragma once
#include <cstddef>
#include <cerrno>

class CDpfscli {
public:
    CDpfscli() = default;
    virtual ~CDpfscli() = default;

    /*
        @return true if connected, false not connected or connection lost.
    */
    virtual bool is_connected() const = 0;

    /*
        @param conn_tring: Connection string for the network connection.
        @return 0 on success, else on failure.
        @note: keepalive should automatically.
        @example "ip:192.168.1.1 port:20500"
    */
    virtual int connect(const char* conn_tring) = 0;

    /*
        @param fd: File descriptor of the network connection.
        @return 0 on success, else on failure.
        @note disconnect from the network connection.
    */
    virtual int disconnect() = 0;

    /*
        @note send data over the network connection.
        @param data: Pointer to the data to be sent.
        @param size: Size of the data in bytes.
        @return 0 on success, else -1 on failure.
    */
    virtual int send(const void* buffer, int size) = 0;

    // /*
    //     @param data: Pointer to the data to be sent.
    //     @param size: Size of the data in bytes.
    //     @return Number of bytes sent on success, else on failure.
    //     @note data will be freed after sending.
    // */
    // virtual int send_without_copy(char* data, size_t size) = 0;

    /*
        @note receive data from the network connection. the buffer retrived must be freed by buffree manually.
        @param buffer: 
        @param retsize: Size of the buffer in bytes.
        @return 0 on success, else on failure.
    */
    virtual int recv(void** buffer, int* retsize) = 0;

    /*
        @note allocate a buffer for read or receive data.
        @param size: Size of the buffer in bytes.
        @return Pointer to the allocated buffer, or nullptr on failure.
    */
    // virtual void* bufalloc(size_t size) = 0;

    /*
        @note free a buffer allocated by bufalloc.
        @param buf: Pointer to the buffer to be freed.
    */
    virtual void buffree(void* buffer) = 0;

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

    /*
        @note send test message to server, for test purpose.
    */
    virtual int sendTestMsg() {
        return -ENOTSUP;
    }
};


CDpfscli* newClient(const char* net_type);