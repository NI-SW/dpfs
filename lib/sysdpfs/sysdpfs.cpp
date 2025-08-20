
#include <sysdpfs/sysdpfs.hpp>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <fstream>


dpfsSystem::dpfsSystem(const char* cfile) {
    conf_file = cfile;
    int rc = init();
    if(rc != 0) {
        throw std::runtime_error("Failed to initialize dpfsSystem with configuration file: " + conf_file);
    }
    
}

dpfsSystem::~dpfsSystem() {
    if(!engine_list.empty()) {
        for(auto& eng : engine_list) {
            delete eng;
        }
    }
    engine_list.clear();
};

int dpfsSystem::init() {
    // Initialize the system, load configurations, etc.
    if(!conf_file.empty()) {
        // Load configuration from file
        if(readConfig()) {
            return -1; // Return error if configuration loading fails
        }
    }
    // use default configurations if no file is provided
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
        return -1; // Return error if parsing fails
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
    


    if(doc.HasMember("dataport") && doc["dataport"].IsInt()) {
        dataport = static_cast<dpfssysPort>(doc["dataport"].GetInt());
    }
    if(doc.HasMember("controlport") && doc["controlport"].IsInt()) {
        controlport = static_cast<dpfssysPort>(doc["controlport"].GetInt());
    }
    if(doc.HasMember("replicationport") && doc["replicationport"].IsInt()) {
        replicationport = static_cast<dpfssysPort>(doc["replicationport"].GetInt());
    }

    
    log.log_inf("Data port: %d\n", static_cast<int>(dataport));
    log.log_inf("Control port: %d\n", static_cast<int>(controlport));
    log.log_inf("Replication port: %d\n", static_cast<int>(replicationport));

    // add engine for system
    if(doc.HasMember("engines")) {
        const rapidjson::Value& engines = doc["engines"];

        rapidjson::GenericMemberIterator it = engines.MemberBegin();
        rapidjson::GenericMemberIterator itend = engines.MemberEnd();

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
                    dpfseng->set_async_mode(true);
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
