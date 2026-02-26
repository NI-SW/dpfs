#include <storage/engine.hpp>
#include <dpfsnet/dpfssvr.hpp>
#include <log/logbinary.h>
#include <dpfsmodule/module.hpp>
#include "dpfscontrol.hpp"
#include <dpfssys/user.hpp>

class CDatasvc;
class CReplicatesvc;

class dpfsSystem final {
public:
    /*
        @param cfile: configuration file path
        @note this constructor will create a new dpfs system with the given configuration file
    */
    dpfsSystem(const char* cfile);

    ~dpfsSystem();

    // dpfs API

    /*
        @return name of the object
    */
    inline const char* name() const { return "DPFS_SYSTEM"; }

    /*
        @note start all services
        @return 0 on success, else on failure
    */
    int start();

    /*
        @note stop all services
        @return 0 on success, else on failure
    */
    int stop();


// private:

    // below shoud be private, but now keep it public for simplicity

    /*
        @note read configuration from the file and initialize the system, only called in constructor
        @return 0 on success, else on failure
    */
    int init();

    /*
        @note initialize data services.
        @return 0 on success, else on failure
    */
    int initDataSvc();
    
    /*
        read configuration from the file
        @return 0 on success, else on failure
    */
    int readConfig();

    /*
        @note reload configuration from the file. some services may not support warm reload, need to stop and start again
        @return 0 on success, else on failure
    */
    int reloadConfig();

    /*
        @note cleanup the system, detach all devices, stop and release all services, function will be called in destructor
        @return 0 on success, else on failure
    */
    int cleanup();
    
    // net server string
    // for client data I/O
    std::string dataSvrStr = "0.0.0.0:20500";
    dpfsnetType dataSvrType = dpfsnetType::TCP;
    // for remote control and management
    std::string controlSvrStr = "0.0.0.0:20501";
    dpfsnetType ctrlSvrType = dpfsnetType::TCP;
    // for data replication between dpfs nodes
    std::string replicationSvrStr = "0.0.0.0:20502";
    dpfsnetType repSvrType = dpfsnetType::TCP;

    void* ctlSvrImpHandle = nullptr;
    CDpfssvr* ctrlSvr = nullptr;
    CDpfssvr* dataSvr = nullptr;
    CDpfssvr* repSvr = nullptr;

    // control services only one instance
    CControlsvc controlService;

    CDatasvc* dataService = nullptr;
    CReplicatesvc* repService = nullptr;


    // specified by configuration file
    std::vector<dpfsEngine*> engine_list; // storage engines list
    // CDatasvc data_svc;
    logrecord log;
    std::string conf_file = "config.json";
    std::recursive_mutex m_lock;
    volatile bool m_start = false;
    volatile bool m_exit = false;
    CModuleMan m_modules;

    CSpin m_usrCacheLock;
    int32_t m_usrHandleCount = 0;
    std::unordered_map<int32_t, CUser> m_userCache; // user cache for authentication, key is the hash of the username
};


