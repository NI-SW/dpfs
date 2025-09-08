#include <dpfsnet/dpfssvr.hpp>
#include <string>
#include <unordered_map>
// defination in dpfstcp.cpp (-ldpfs_tcp)
void* newTcpClient();
// defination in dpfstcpsvr.cpp (-ldpfs_tcp)
void* newTcpServer();

std::unordered_map<std::string, void* (*)()> clientTypes {{"tcp", newTcpClient}};
std::unordered_map<std::string, void* (*)()> serverTypes {{"tcp", newTcpServer}};


CDpfscli* newClient(std::string& client_type) {
    if(clientTypes.find(client_type) != clientTypes.end()) {
        return reinterpret_cast<CDpfscli*>(clientTypes[client_type]());
    } else {
        return nullptr; // or throw an exception
    }

    return nullptr;
}

CDpfssvr* newServer(std::string& server_type) {
    if(clientTypes.find(server_type) != clientTypes.end()) {
        return reinterpret_cast<CDpfssvr*>(clientTypes[server_type]());
    } else {
        return nullptr; // or throw an exception
    }

    return nullptr;
}