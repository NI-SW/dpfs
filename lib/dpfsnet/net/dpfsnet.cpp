#include <dpfsnet/dpfssvr.hpp>
#include <string>
#include <unordered_map>
// defination in dpfstcp.cpp (-ldpfs_tcp)
extern void* newTcpClient();
// defination in dpfstcpsvr.cpp (-ldpfs_tcp)
extern void* newTcpServer();

// -ldpfs_grpc
// extern void* newGrpcClient();
extern void* newGrpcServer();

std::unordered_map<std::string, void* (*)()> clientTypes {{"tcp", newTcpClient}, {"grpc", nullptr}};
std::unordered_map<std::string, void* (*)()> serverTypes {{"tcp", newTcpServer}, {"grpc", newGrpcServer}};
    

CDpfscli* newClient(const std::string& client_type) {
    if(clientTypes.find(client_type) != clientTypes.end()) {
        return reinterpret_cast<CDpfscli*>(clientTypes[client_type]());
    } else {
        return nullptr; // or throw an exception
    }

    return nullptr;
}

CDpfscli* newClient(const char* client_type) {
    return newClient(std::string(client_type));
}

CDpfssvr* newServer(const std::string& server_type) {
    if(serverTypes.find(server_type) != serverTypes.end()) {
        return reinterpret_cast<CDpfssvr*>(serverTypes[server_type]());
    } else {
        return nullptr; // or throw an exception
    }

    return nullptr;
}

CDpfssvr* newServer(const char* server_type) {
    return newServer(std::string(server_type));
}