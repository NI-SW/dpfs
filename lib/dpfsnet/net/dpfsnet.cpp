#include <dpfsnet/dpfssvr.hpp>
#include <string>

// defination in dpfstcp.cpp (-ldpfs_tcp)
void* newTcpClient();
// defination in dpfstcpsvr.cpp (-ldpfs_tcp)
void* newTcpServer();

CDpfscli* newClient(const char* engine_type) {
    std::string net_type = engine_type;
    if(net_type == "tcp") {
        return reinterpret_cast<CDpfscli*>(newTcpClient());
    } else {
        return nullptr; // or throw an exception
    }
}

CDpfssvr* newServer(const char* engine_type) {
    std::string net_type = engine_type;
    if(net_type == "tcp") {
        return reinterpret_cast<CDpfssvr*>(newTcpServer());
    } else {
        return nullptr; // or throw an exception
    }
}