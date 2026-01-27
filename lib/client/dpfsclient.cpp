#include <dpfsclient/dpfsclient.hpp>
#include <basic/dpfsconst.hpp>
#include <cstring>
#include <stdexcept>


CDpfsSysCli::CDpfsSysCli(const char* netType) {
    m_netType.assign(netType);
    m_cli = newClient(netType);
    if(!m_cli) {
        throw std::runtime_error("Failed to create dpfs client with network type: " + m_netType);
    }
    log.set_log_path("dpfsClient.log");
}

CDpfsSysCli::~CDpfsSysCli() {
    if(m_cli) {
        delete m_cli;
        m_cli = nullptr;
    }
}

/*
    @param connStr connection string, format: "ip:0.0.0.0 port:20500 user:root passwd:123456"(if tcp).
    @return 0 if success.
    @note connect with user and password, and authToken will be retrived if success.
*/
int CDpfsSysCli::connect(const char* connStr) {
    if(!m_cli) {
        return -EIO;
    }
    int rc = 0;
    dpfs_rsp* rsp = nullptr;
    ipc_connect_rsp* connRsp = nullptr;

    // construct command
    dpfs_cmd* cmd = (dpfs_cmd*)malloc(sizeof(dpfs_cmd) + sizeof(ipc_connect));
    if(!cmd) {
        rc = -ENOMEM;
        return rc;
    }
    cmd->cmd = dpfsipc::DPFS_IPC_CONNECT;
    cmd->size = sizeof(ipc_connect);

    ipc_connect* param = (ipc_connect*)cmd->data;
    memcpy(param->version, dpfsVersion, versionSize);
    rc = parse_string(connStr, "user", param->user, sizeof(param->user));
    if(rc) {
        goto error;
    }
    rc = parse_string(connStr, "passwd", param->password, sizeof(param->password));
    if(rc) {
        goto error;
    }


    rc = m_cli->connect(connStr);
    if(rc) {
        return rc;
    }

    // use little-endian to sent and receive connect ipc
    if(B_END) {
        cmd_edn_cvt(cmd);
    }

    log.log_debug("Connecting to server with %s\n", connStr);
    rc = m_cli->send(cmd, sizeof(cmd) + sizeof(ipc_connect));
    if(rc) {
        m_cli->disconnect();
        return rc;
    }

    // wait for response
    log.log_debug("Waiting for response from server...\n");
    rc = m_cli->recv((void**)&rsp, nullptr);
    if(rc) {
        log.log_error("Failed to receive response from server: %d\n", rc);
        m_cli->disconnect();
        return rc;
    }
    log.log_debug("Received response from server. rsp: %s\n", dpfsrspStr[(uint32_t)rsp->rsp]);


    if(B_END) {
        rsp_edn_cvt(rsp);
    }

    if(rsp->rsp != dpfsrsp::DPFS_RSP_CONNECT) {
        rc = -(int)rsp->rsp;
        log.log_error("Failed to connect to server, rsp=%u\n", (uint32_t)rsp->rsp);
        m_cli->buffree(rsp);
        goto error;
    }

    connRsp = (ipc_connect_rsp*)rsp->data;
    if(connRsp->retcode != 0) {
        rc = -connRsp->retcode;
        // message is null terminated
        log.log_error("%s", connRsp->message);
        m_cli->buffree(rsp);
        goto error;
    }
    // get server endian and auth token
    serverEndian = connRsp->serverEndian;
    memcpy(authToken, connRsp->authToken, sizeof(authToken));
    
    log.log_debug("Connected to server, version: %u.%u.%u.%u, authToken: %s, serverEndian: %s\n",
        (uint8_t)connRsp->version[0], (uint8_t)connRsp->version[1],
        (uint8_t)connRsp->version[2], (uint8_t)connRsp->version[3],
        authToken,
        serverEndian ? "big-endian" : "little-endian");
    m_cli->buffree(rsp);
    return 0;

    error:
    if(cmd) {
        free(cmd);
        cmd = nullptr;
    }
    if(m_cli->is_connected()) {
        m_cli->disconnect();
    }

    return rc;

}

/*
    @return 0 if success.
*/
int CDpfsSysCli::disconnect() {
    if(!m_cli) {
        return -EIO;
    }
    return m_cli->disconnect();
}

bool CDpfsSysCli::is_connected() const {
    if(!m_cli) {
        return false;
    }
    return m_cli->is_connected();
}

int CDpfsSysCli::execute(const char* execStr) {
    if(!m_cli || !is_connected()) {
        return -EIO;
    }

    int rc = 0;
    dpfs_rsp* rsp = nullptr;
    ipc_connect_rsp* connRsp = nullptr;

    // construct command
    dpfs_cmd* cmd = (dpfs_cmd*)malloc(sizeof(dpfs_cmd));
    if(!cmd) {
        rc = -ENOMEM;
        return rc;
    }
    cmd->cmd = dpfsipc::DPFS_IPC_CONNECT;
    cmd->size = sizeof(ipc_connect);





    // use little-endian to sent and receive connect ipc
    if(B_END) {
        cmd_edn_cvt(cmd);
    }



    // wait for response
    log.log_debug("Waiting for response from server...\n");
    rc = m_cli->recv((void**)&rsp, nullptr);
    if(rc) {
        log.log_error("Failed to receive response from server: %d\n", rc);
        m_cli->disconnect();
        return rc;
    }
    log.log_debug("Received response from server. rsp: %s\n", dpfsrspStr[(uint32_t)rsp->rsp]);


    if(B_END) {
        rsp_edn_cvt(rsp);
    }

    if(rsp->rsp != dpfsrsp::DPFS_RSP_CONNECT) {
        rc = -(int)rsp->rsp;
        log.log_error("Failed to connect to server, rsp=%u\n", (uint32_t)rsp->rsp);
        m_cli->buffree(rsp);
        goto error;
    }

    connRsp = (ipc_connect_rsp*)rsp->data;
    if(connRsp->retcode != 0) {
        rc = -connRsp->retcode;
        // message is null terminated
        log.log_error("%s", connRsp->message);
        m_cli->buffree(rsp);
        goto error;
    }
    // get server endian and auth token
    serverEndian = connRsp->serverEndian;
    memcpy(authToken, connRsp->authToken, sizeof(authToken));
    
    log.log_debug("Connected to server, version: %u.%u.%u.%u, authToken: %s, serverEndian: %s\n",
        (uint8_t)connRsp->version[0], (uint8_t)connRsp->version[1],
        (uint8_t)connRsp->version[2], (uint8_t)connRsp->version[3],
        authToken,
        serverEndian ? "big-endian" : "little-endian");
    m_cli->buffree(rsp);
    return 0;

    error:
    if(cmd) {
        free(cmd);
        cmd = nullptr;
    }
    if(m_cli->is_connected()) {
        m_cli->disconnect();
    }

    return rc;

}