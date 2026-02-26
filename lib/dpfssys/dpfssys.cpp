
#include <dpfssys/dpfssys.hpp>
#include <dpfssys/dpfsdata.hpp>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <fstream>
#include <string>

#include <grpcpp/grpcpp.h>
#include <proto/sysrpc.grpc.pb.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
// using dpfsgrpc::exesql;
// using dpfsgrpc::getRowReply;
// using dpfsgrpc::Greeter;

class sysCtlServiceImpl final : public dpfsgrpc::sysCtl::Service {

    dpfsSystem* system = nullptr;

    Status login(ServerContext* context, const dpfsgrpc::loginReq* request, dpfsgrpc::loginReply* response) override {
        // Implement your login logic here
        // For demonstration, we will just set a dummy user handle
        // response->set_userid(12345);
        // response->set_username(request->username());
        int rc = 0;
        std::string usrName = request->username();
        std::string password = request->password();

        // check the user authentication.
        
        auto& sysUsrAuth = system->dataService->m_sysSchema->sysusers;
        KEY_T userName(const_cast<char*>(usrName.data()), usrName.size(), sysUsrAuth.m_cmpTyps);
    
        CTemplateReadGuard guard(*sysUsrAuth.m_cltInfoCache);
        if (guard.returnCode() != 0) {
            response->set_msg("Failed to acquire read lock on sysusers cache");
            response->set_husr(0);
            response->set_rc(-EAGAIN);
            return Status::OK;
        }
        
        CCollection::collectionStruct sysUserCs(sysUsrAuth.m_cltInfoCache->getPtr(), sysUsrAuth.m_cltInfoCache->getLen() * dpfs_lba_size);
        
        CItem rowData(sysUserCs.m_cols);
        rc = sysUsrAuth.getRow(userName, &rowData);
        if (rc != 0) {
            response->set_msg("Authentication failed: user not found");
            response->set_husr(0);
            response->set_rc(-ENOENT);
            return Status::OK;
        }

        CValue pwd = rowData.beginIter[4];
        size_t pwdLen = strnlen(pwd.data, pwd.len); // ensure the password is null-terminated
        if (pwdLen != password.size()) {
            response->set_msg("Authentication failed: incorrect password: received length does not match expected length rcvLen = " + std::to_string(password.size()) + ", expectedLen = " + std::to_string(pwd.len));
            response->set_husr(0);
            response->set_rc(-EACCES);
            return Status::OK;
        }

        if (memcmp(pwd.data, password.data(), pwdLen) != 0) {
            response->set_msg("Authentication failed: incorrect password for user " + usrName + ": passwd = " + std::string(pwd.data, pwd.len));
            response->set_husr(0);
            response->set_rc(-EACCES);
            return Status::OK;
        }

        // unlock
        guard.release();



        CUser usr;
        usr.userid = *(int32_t*)rowData.beginIter[5].data;
        usr.username = usrName;
        usr.dbprivilege = static_cast<dbPrivilege>(*(uint8_t*)rowData.beginIter[3].data);
        usr.currentSchema = "root"; // for simplicity, set current schema to root, can be modified later when implementing schema management

        // seconds since epoch
        usr.lastActiveTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch());
        usr.logOff = false;

        system->m_usrCacheLock.lock();
        // find next not used handle
        while (system->m_userCache.find(system->m_usrHandleCount) != system->m_userCache.end()) {
            ++system->m_usrHandleCount;
        }
        response->set_husr(system->m_usrHandleCount);

        system->m_userCache.emplace(system->m_usrHandleCount, std::move(usr));
        ++system->m_usrHandleCount;
        if (system->m_userCache.size() > 1024) {
            system->log.log_notic("trim user cache, remove invalid login client\n");

            auto iter = system->m_userCache.begin();
            for (; iter != system->m_userCache.end();) {
                if (iter->second.logOff || 
                    (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch() - iter->second.lastActiveTime)).count() > 3600 /* 1 hour */) {
                    system->log.log_notic("remove inactive user, handle: %d, username: %s\n", iter->first, iter->second.username.c_str());
                    iter = system->m_userCache.erase(iter);
                } else {
                    ++iter;
                }
            }
            if (system->m_usrHandleCount > 100000000) {
                system->m_usrHandleCount = 0;
            }

        }
        system->m_usrCacheLock.unlock();


        response->set_msg("Login successful for user: " + usrName);
        response->set_rc(0);

        return Status::OK;
    }

    Status logoff(ServerContext* context, const dpfsgrpc::logoffReq* request, dpfsgrpc::operateReply* response) override {
        // Implement your logoff logic here
        // For demonstration, we will just set a dummy response

        int32_t husr = request->husr();
        system->m_usrCacheLock.lock();
        auto iter = system->m_userCache.find(husr);
        if (iter != system->m_userCache.end()) {
            iter->second.logOff = true;
            system->log.log_notic("User logged off, handle: %d, username: %s\n", iter->first, iter->second.username.c_str());
            system->m_userCache.erase(iter);
        }
        system->m_usrCacheLock.unlock();

        response->set_msg("Logoff successful");
        response->set_rc(0);
        return Status::OK;
    }

public:
    sysCtlServiceImpl(void* arg) : Service() {
        system = static_cast<dpfsSystem*>(arg);
    }
};

// void RunServer(void* arg) {
//   std::string server_address("0.0.0.0:50051");
//   sysCtlServiceImpl* service = static_cast<sysCtlServiceImpl*>(arg);
//   ServerBuilder builder;
//   builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
//   builder.RegisterService(service);
//   std::unique_ptr<Server> server(builder.BuildAndStart());
//   server->Wait();
// }



dpfs_rsp error_rsp = {dpfsrsp::DPFS_RSP_SYSTEMERROR, 0, {}};

static void ctrlSvc(CDpfscli& cli, void* cb_arg) {
    dpfsSystem* dsys = (dpfsSystem*)cb_arg;

    void* reqPtr = nullptr;
    int retsize = 0;
    int rc = 0;
    // process connect ipc
    do {
        rc = cli.recv(&reqPtr, &retsize);
        if(rc == 0) {
            break;
        }
        dsys->log.log_debug("Waiting for connect command from client...\n");
    } while(rc == -ENODATA);
    dpfs_cmd* cmd = (dpfs_cmd*)reqPtr;
    if(B_END) {
        cmd_edn_cvt(cmd);
    }

    if(cmd->cmd != dpfsipc::DPFS_IPC_CONNECT && retsize != (sizeof(dpfs_cmd) + sizeof(ipc_connect))) {
        cli.send(&error_rsp, sizeof(dpfs_rsp));
        dsys->log.log_notic("illegal command: %u\n", (uint32_t)cmd->cmd);
        // std::this_thread::sleep_for(std::chrono::milliseconds(500));
        cli.buffree(reqPtr);
        cli.disconnect();
        return;
    }
    dsys->log.log_debug("Received request: %s\n", dpfsipcStr[(uint32_t)cmd->cmd]);

    dpfs_rsp* rsp = dsys->controlService.process_request(cmd);
    if(!rsp) {
        cli.send(&error_rsp, sizeof(dpfs_rsp));
        dsys->log.log_error("Failed to process request: %s\n", dpfsipcStr[(uint32_t)cmd->cmd]);
        // std::this_thread::sleep_for(std::chrono::milliseconds(500));
        cli.disconnect();
        return;
    }

    uint32_t rspSize = sizeof(dpfs_rsp) + rsp->size; 
    if(B_END) {
        rsp_edn_cvt(rsp);
    }
    cli.send(rsp, rspSize);
    cli.buffree(reqPtr);
    dsys->log.log_notic("Client connected.\n");
    
    // after connect, server do not check endian, client do it.
    // process control service requests here
    while(cli.is_connected()) {

        
        // receive a request
        rc = cli.recv(&reqPtr, &retsize);
        if(rc) {
            goto error;
        }

        // convert from network byte order
        cmd = (dpfs_cmd*)reqPtr;

        if(!is_valid_ipc(cmd->cmd)) {
            cli.send(&error_rsp, sizeof(dpfs_rsp));
            dsys->log.log_notic("illegal command: %u\n", (uint32_t)cmd->cmd);
            cli.buffree(reqPtr);
            continue;
        }

        // process the request and get a response
        // use little-endian to receive and send ipc
        dsys->log.log_debug("Received request: %s\n", dpfsipcStr[(uint32_t)cmd->cmd]);
        rsp = dsys->controlService.process_request(cmd);
        if(!rsp) {
            cli.send(&error_rsp, sizeof(dpfs_rsp));
            dsys->log.log_error("Failed to process request: %s\n", dpfsipcStr[(uint32_t)cmd->cmd]);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            cli.disconnect();
            break;
        }

        // convert to network byte order and send the response
        dsys->log.log_debug("Processed request: %s\n", dpfsipcStr[(uint32_t)cmd->cmd]);
        cli.send(rsp, sizeof(dpfs_rsp) + rsp->size);
        dsys->log.log_debug("Sent response: %u\n", (uint32_t)rsp->rsp);
        cli.buffree(reqPtr);

        // if received a disconnect command, close the connection
        if(cmd->cmd == dpfsipc::DPFS_IPC_DISCONNECT) {
            // disconnect command, close the connection
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            cli.disconnect();
            break;
        }

        continue;
        error:
        if(rc == -ENODATA) {
            continue;
        }
        break;
    }
    dsys->log.log_notic("Client disconnected.\n");
    return;
}

static void dataSvc(CDpfscli& cli, void* cb_arg) {
    // Handle data service requests here
    // dpfsSystem* sys = (dpfsSystem*)cb_arg;
    // sys->cleanup();
}

static void repSvc(CDpfscli& cli, void* cb_arg) {

}


dpfsSystem::dpfsSystem(const char* cfile) : m_modules(log) {
    CRecursiveGuard guard(m_lock);
    conf_file = cfile;
    int rc = init();
    if(rc != 0) {
        throw std::runtime_error("Failed to initialize dpfsSystem with configuration file: " + conf_file);
    }
}

dpfsSystem::~dpfsSystem() {
    CRecursiveGuard guard(m_lock);
    stop();
    cleanup();

};

int dpfsSystem::cleanup() {
    // Cleanup the system, detach devices, stop services, etc.
    CRecursiveGuard guard(m_lock);
    
    if(ctrlSvr) {
        ctrlSvr->stop();
        delete ctrlSvr;
        ctrlSvr = nullptr;
    }
    if(dataSvr) {
        dataSvr->stop();
        delete dataSvr;
        dataSvr = nullptr;
    }
    if(repSvr) {
        repSvr->stop();
        delete repSvr;
        repSvr = nullptr;
    }

    if(dataService) {
        delete dataService;
        dataService = nullptr;
    }

    if(!engine_list.empty()) {
        for(auto& eng : engine_list) {
            delete eng;
        }
    }
    engine_list.clear();

    return 0; // Return 0 on success
}

int dpfsSystem::start() {
    CRecursiveGuard guard(m_lock);
    if(m_start) {
        return 0; // Already started
    }
    int rc = 0;
    if(ctrlSvr) {
        // 
        if (ctrlSvr->name() == std::string("DPFS_GRPC_SERVER")) {
            ctlSvrImpHandle = new sysCtlServiceImpl(this);
            if (!ctlSvrImpHandle) {
                log.log_error("Failed to create control service implementation\n");
                return -ENOMEM;
            }
            ctrlSvr->listen(controlSvrStr.c_str(), ctlSvrImpHandle);
            log.log_inf("Control server started with gRPC on %s\n", controlSvrStr.c_str());
        } else {
            rc = ctrlSvr->listen(controlSvrStr.c_str(), ctrlSvc, this);
            if(rc != 0) {
                log.log_error("Failed to start control server: %d\n", rc);
                return rc;
            }
        }
    }

    // maybe deprecate in the future, use ctrlSvr for all services, and use different command to distinguish different services, TODO;
    if(dataSvr && 0) {
        rc = dataSvr->listen(dataSvrStr.c_str(), dataSvc, nullptr);
        if(rc != 0) {
            log.log_error("Failed to start data server: %d\n", rc);
            return rc;
        }
    }

    // not use right now, TODO;
    if(repSvr && 0) {
        rc = repSvr->listen(replicationSvrStr.c_str(), repSvc, nullptr);
        if(rc != 0) {
            log.log_error("Failed to start replication server: %d\n", rc);
            return rc;
        }
    }

    m_start = true;
    m_exit = false;
    return 0; // Return 0 on success
}

int dpfsSystem::stop() {
    CRecursiveGuard guard(m_lock);
    if(!m_start) {
        return 0; // Already stopped
    }
    int rc = 0;
    if(ctrlSvr) {
        rc = ctrlSvr->stop();
        if(rc != 0) {
            log.log_error("Failed to stop control server: %d\n", rc);
            return rc;
        }
        delete static_cast<sysCtlServiceImpl*>(ctlSvrImpHandle);
        ctlSvrImpHandle = nullptr;
    }
    if(dataSvr) {
        rc = dataSvr->stop();
        if(rc != 0) {
            log.log_error("Failed to stop data server: %d\n", rc);
            return rc;
        }
    }
    if(repSvr) {
        rc = repSvr->stop();
        if(rc != 0) {
            log.log_error("Failed to stop replication server: %d\n", rc);
            return rc;
        }
    }

    m_start = false;
    return 0; // Return 0 on success
}

int dpfsSystem::init() {
    // Initialize the system, load configurations, etc.
    CRecursiveGuard guard(m_lock);

    int rc = 0;
    if(!conf_file.empty()) {
        // Load configuration from file
        rc = readConfig();
        if(rc) {
            return rc; // Return error if configuration loading fails
        }
    }

    // Initialize members
    if((int)ctrlSvrType < 0 || ctrlSvrType >= dpfsnetType::MAX) {
        rc = -EINVAL;
        goto errinit; // Return error if control server type is invalid
    }
    ctrlSvr = newServer(dpfsnetTypeStr[(int)ctrlSvrType]);
    if(!ctrlSvr) {
        rc = -ENOMEM;
        goto errinit; // Return error if server creation fails
    }

    if((int)dataSvrType < 0 || dataSvrType >= dpfsnetType::MAX) {
        rc = -EINVAL;
        goto errinit; // Return error if data server type is invalid
    }
    dataSvr = newServer(dpfsnetTypeStr[(int)dataSvrType]);
    if(!dataSvr) {
        rc = -ENOMEM;
        goto errinit; // Return error if server creation fails
    }

    if((int)repSvrType < 0 || repSvrType >= dpfsnetType::MAX) {
        rc = -EINVAL;
        goto errinit; // Return error if replication server type is invalid
    }
    repSvr = newServer(dpfsnetTypeStr[(int)repSvrType]);
    if(!repSvr) {
        rc = -ENOMEM;
        goto errinit; // Return error if server creation fails
    }

    dataService = new CDatasvc(engine_list, 4096, log); // 4K block cache size
    if(!dataService) {
        rc = -ENOMEM;
        goto errinit; // Return error if data service creation fails
    }

    // use default configurations if no file is provided
    return 0; // Return 0 on success

errinit:

    cleanup();
    return rc;
    

}

int dpfsSystem::initDataSvc() {
    CRecursiveGuard guard(m_lock);

    if(!dataService) {
        return -EINVAL;
    }

    int rc = dataService->init();
    if(rc != 0) {
        return rc; // Return error if initialization fails
    }
    return 0; // Return 0 on success
}

int dpfsSystem::readConfig() {
    // Read configuration from the file
    rapidjson::Document doc;

    std::fstream f;
    f.open(conf_file, std::ios::in);
    if(!f.is_open()) {
        return -EIO; // Return error if file cannot be opened
    }
    char buffer[65536];
    f.read(buffer, sizeof(buffer));
    f.close();


    if(doc.Parse(buffer).HasParseError()) {
        return -EINVAL; // Return error if parsing fails
    }
    // Load the JSON configuration file into the document
    // Parse the JSON and populate the system's configurations

    if(doc.HasMember("logpath") && doc["logpath"].IsString()) {
        log.set_log_path(doc["logpath"].GetString());
    } else {
        log.set_log_path("/tmp/dpfs.log"); // Default log path
    }

    log.log_inf("Configuration loaded from %s\n", conf_file.c_str());

    if(doc.HasMember("asynclog") && doc["asynclog"].IsBool()) {
        bool async = doc["asynclog"].GetBool();
        log.log_inf("Setting asynchronous logging to %s\n", async ? "true" : "false");
        log.set_async_mode(async);
    } else {
        log.log_inf("Asynchronous logging not specified, defaulting to false\n");
        log.set_async_mode(false); // Default to synchronous logging
    }
    

    if(doc.HasMember("net")) {
        const rapidjson::Value& net = doc["net"];
        if(net.HasMember("tcp")) {
            const rapidjson::Value& tcp = net["tcp"];
            if(tcp.HasMember("data_service") && tcp["data_service"].IsString()) {
                dataSvrStr = tcp["data_service"].GetString();
                dataSvrType = dpfsnetType::TCP;
            }
            if(tcp.HasMember("control_service") && tcp["control_service"].IsString()) {
                controlSvrStr = tcp["control_service"].GetString();
                ctrlSvrType = dpfsnetType::TCP;
            }
            if(tcp.HasMember("replication_service") && tcp["replication_service"].IsString()) {
                replicationSvrStr = tcp["replication_service"].GetString();
                repSvrType = dpfsnetType::TCP;
            }
        } else if (net.HasMember("grpc")) {
            const rapidjson::Value& grpc = net["grpc"];
            if(grpc.HasMember("data_service") && grpc["data_service"].IsString()) {
                dataSvrStr = grpc["data_service"].GetString();
                dataSvrType = dpfsnetType::GRPC;
            }
            if(grpc.HasMember("control_service") && grpc["control_service"].IsString()) {
                controlSvrStr = grpc["control_service"].GetString();
                ctrlSvrType = dpfsnetType::GRPC;
            }
            if(grpc.HasMember("replication_service") && grpc["replication_service"].IsString()) {
                replicationSvrStr = grpc["replication_service"].GetString();
                repSvrType = dpfsnetType::GRPC;
            }
        }
        log.log_inf("Data service: %s\n", dataSvrStr.c_str());
        log.log_inf("Control service: %s\n", controlSvrStr.c_str());
        log.log_inf("Replication service: %s\n", replicationSvrStr.c_str());
    }


    // add engine for system
    if(doc.HasMember("engines")) {
        const rapidjson::Value& engines = doc["engines"];

        rapidjson::GenericMemberIterator<true, rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<>> it = engines.MemberBegin();
        rapidjson::GenericMemberIterator<true, rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<>> itend = engines.MemberEnd();

        // for each engine type
        for (;it != itend; ++it) {
            std::string engine_type = (*it).name.GetString();
            const rapidjson::Value& eng = (*it).value;
            if(eng.IsArray() && !eng.Empty()) {
                log.log_inf("Adding engine type: %s\n", engine_type.c_str());

                // new engine, type is defined by json
                dpfsEngine* dpfseng = newEngine(engine_type);
                if(!dpfseng) {
                    return -ENXIO;
                }
                for(rapidjson::SizeType j = 0; j < eng.Size(); ++j) {
                    std::string engine_desc = eng[j].GetString();
                    log.log_inf("Adding engine: %s\n", engine_desc.c_str());
                    dpfseng->set_logdir(log.get_log_path() + engine_desc);
                    dpfseng->set_async_mode(false);
                    int rc = dpfseng->attach_device(engine_desc);
                    if(rc != 0) {
                        log.log_error("Failed to attach device %s: %d\n", engine_desc.c_str(), rc);
                        delete dpfseng;
                        return rc;
                    } else {
                        log.log_inf("Successfully attached device %s\n", engine_desc.c_str());
                    }
                }
                engine_list.emplace_back(dpfseng);
            }
        }
    }


    

    return 0; // Return 0 on success
}



/*

            "trtype:tcp adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1",
            "trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50658 subnqn:nqn.2016-06.io.spdk:cnode1",
*/
