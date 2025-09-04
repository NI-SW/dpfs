#include <dpfssys/dpfscontrol.hpp>
#include <cstring>
static dpfs_rsp invalid_rsp = {dpfsrsp::DPFS_RSP_INVALID, 0, {}};
static dpfs_rsp notsupport_rsp = {dpfsrsp::DPFS_RSP_NOTSUPPORT, 0, {}};
static dpfs_rsp systembusy_rsp = {dpfsrsp::DPFS_RSP_SYSTEMBUSY, 0, {}};

dpfs_rsp* CControlsvc::process_request(const dpfs_cmd* cmd) {

    if(cmd->cmd >= dpfsipc::DPFS_IPC_MAX) {
        return &invalid_rsp; // Invalid command
    }
    if(!m_exe[(uint32_t)cmd->cmd]) {
        return &notsupport_rsp; // No handler for this command
    }
    return (this->*m_exe[(uint32_t)cmd->cmd])(cmd);
}

dpfs_rsp* CControlsvc::connect(const dpfs_cmd* cmd) {
    CRecursiveGuard guard(m_lock);
    // Handle client connect request
    if(cmd->size != sizeof(ipc_connect)) {
        return &invalid_rsp; // Invalid parameters
    }
    const ipc_connect* param = (const ipc_connect*)cmd->data;
    if(memcmp(param->user, "root", 4) != 0) {
        return &invalid_rsp; // Invalid user
    }
    if(memcmp(param->password, "123456", 6) != 0) {
        return &invalid_rsp; // Invalid password
    }

    // check version
    // if((*(uint32_t*)param->version) != (*(uint32_t*)dpfsVersion)) {
    //     return &invalid_rsp; // Version mismatch
    // }


    dpfs_rsp* rsp = (dpfs_rsp*)malloc(sizeof(dpfs_rsp) + sizeof(ipc_connect_rsp));
    if(!rsp) {
        return &systembusy_rsp; // Memory allocation failure
    }
    rsp->rsp = dpfsrsp::DPFS_RSP_CONNECT;
    rsp->size = sizeof(ipc_connect_rsp);
    ipc_connect_rsp* connRsp = (ipc_connect_rsp*)rsp->data;
    connRsp->retcode = 0; // Success
    connRsp->serverEndian = B_END; // Server endian
    memcpy(connRsp->version, dpfsVersion, versionSize);

    // for test
    memcpy(connRsp->authToken, "dummy_token", 12);

    return rsp;
}


dpfs_rsp* CControlsvc::disconnect(const dpfs_cmd* cmd) {
    return nullptr;
}


dpfs_rsp* CControlsvc::foodtrace(const dpfs_cmd* cmd) {
    return nullptr;
}