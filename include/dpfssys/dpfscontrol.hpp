#pragma once
#include <basic/dpfsconst.hpp>
#include <threadlock.hpp>


class CControlsvc {
public:

    CControlsvc() {
        m_exe[(int)dpfsipc::DPFS_IPC_CONNECT]    = &CControlsvc::connect;
        m_exe[(int)dpfsipc::DPFS_IPC_DISCONNECT] = &CControlsvc::disconnect;
        m_exe[(int)dpfsipc::DPFS_IPC_FOODTRACE]  = &CControlsvc::foodtrace;
    }

    inline const char* name() const { 
        return "DPFS_CONTROL_SVC"; 
    }
    
    dpfs_rsp* process_request(const dpfs_cmd* cmd);

private:
    using msgCallback = dpfs_rsp* (CControlsvc::*)(const dpfs_cmd* cmd);
    /*
        @note handle client connect request
        @return 0 on success, else on failure
    */
    dpfs_rsp* connect(const dpfs_cmd* cmd);

    /*
        @note handle client disconnect request
        @return 0 on success, else on failure
    */
    dpfs_rsp* disconnect(const dpfs_cmd* cmd);

    /*
        @note handle foodtrace request
        @return 0 on success, else on failure
    */
    dpfs_rsp* foodtrace(const dpfs_cmd* cmd);

    std::recursive_mutex m_lock;
    msgCallback m_exe[(int)dpfsipc::DPFS_IPC_MAX];
};