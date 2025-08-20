#include <storage/engine.hpp>
#include <log/logbinary.h>
#include "sysconst.hpp"

class dpfsSystem {
public:
    /*
        @param cfile: configuration file path
        @note this constructor will create a new dpfs system with the given configuration file
    */
    dpfsSystem(const char* cfile);

    ~dpfsSystem();

    // dpfs API




    // below shoud be private, but now keep it public for simplicity
    int init();
    /*
        read configuration from the file
        @return 0 on success, else on failure
    */
    int readConfig();
    
    // client connect this port
    dpfssysPort dataport = dpfssysPort::DATAPORT;
    // not use, reserved.
    dpfssysPort controlport = dpfssysPort::CONTROLPORT;
    // replication port, reserved.
    dpfssysPort replicationport = dpfssysPort::REPLICATIONPORT;
    // specified by configuration file
    std::vector<dpfsEngine*> engine_list; // storage engines list

    logrecord log;
    std::string conf_file;
};
