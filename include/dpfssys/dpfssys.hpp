#include <storage/engine.hpp>
#include <log/logbinary.h>
#include "dpfsconst.hpp"

class CDatasvc;
class CControlsvc;
class CReplicatesvc;

class dpfsSystem {
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

    
// private:

    // below shoud be private, but now keep it public for simplicity
    int init();
    /*
        read configuration from the file
        @return 0 on success, else on failure
    */
    int readConfig();
    
    // net server string
    // for client data I/O
    std::string dataSvrStr = "0.0.0.0:20500";
    // for remote control and management
    std::string controlSvrStr = "0.0.0.0:20501";
    // for data replication between dpfs nodes
    std::string replicationSvrStr = "0.0.0.0:20502";

    // specified by configuration file
    std::vector<dpfsEngine*> engine_list; // storage engines list
    // CDatasvc data_svc;
    logrecord log;
    std::string conf_file;

};


