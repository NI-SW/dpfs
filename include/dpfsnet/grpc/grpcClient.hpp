// /*  DPFS-License-Identifier: Apache-2.0 license
//  *  Copyright (C) 2025 LBR.
//  *  All rights reserved.
//  */

// #pragma once
// #include <dpfsnet/dpfssvr.hpp>
// #include <queue>
// #include <thread>
// #include <list>
// #include <threadlock.hpp>
// #include <condition_variable>
// #include <log/logbinary.h>

// #include <grpcpp/grpcpp.h>



// class CDpfsGrpc : public CDpfscli {
// public:

//     CDpfsGrpc();
//     virtual ~CDpfsGrpc() override;
//     virtual bool is_connected() const noexcept override;
//     /*
//         "ip:192.168.34.12 port:20500"
//     */
//     virtual int connect(const char* connString) override;
//     virtual int disconnect() override;
//     virtual int send(const void* buffer, int size) override;
//     virtual int recv(void** buffer, int* retsize) override;
//     // virtual void* bufalloc(size_t size) override;
//     virtual void buffree(void* buffer) override;
//     /*
//         @param log_path: Path to the log file.
//         @return 0 on success, else on failure.
//         @note not thread safe
//     */
//     virtual void set_log_path(const char* log_path) override;

//     /*
//         @param level: Log level.
//         @return 0 on success, else on failure.
//         @note not thread safe
//     */
//     virtual void set_log_level(int level) override;
    
//     /*
//         @return name of the object
//     */
//     virtual const char* name() const noexcept override { 
//         return "DPFS_GRPC_CLIENT"; 
//     }

//     virtual int sendTestMsg() override;

// private:
    

//     bool m_connected = false;
//     std::recursive_mutex m_lock;

// };
