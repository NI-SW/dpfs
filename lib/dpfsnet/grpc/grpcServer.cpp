#include <dpfsnet/grpc/grpcServer.hpp>
#include <grpcpp/grpcpp.h>
#include <thread>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

CDpfsGrpcsvr::CDpfsGrpcsvr() {

}

CDpfsGrpcsvr::~CDpfsGrpcsvr() {
    m_exit = true;
    if (grpcServerThread.joinable()) {
        grpcServerThread.join();
    }

}

    
/*
    @param serverString: Server listening string for the network connection.
    @param cb: Callback function to be called when a new connection is established.
    @return 0 on success, else on failure.
    @example "ip:0.0.0.0 port:20500"
*/
int CDpfsGrpcsvr::listen(const char* serverString, listenCallback cb, void* cb_arg) {
    return -ENOTSUP;    
}

int CDpfsGrpcsvr::listen(const char* serverString, void* service) {
    
    std::lock_guard<std::recursive_mutex> lock(m_lock);

    if (listening) {
        return -EISCONN; // Already listening
    }

    int rc = 0;
    size_t i = 0;
    char addr[256]{0};
    char port[16]{0};


    std::string connectionString = serverString;
    size_t ipPos = connectionString.find("ip:");
    size_t portPos = connectionString.find("port:");

    if (ipPos == std::string::npos || portPos == std::string::npos) {
        return -EADDRNOTAVAIL; // Invalid connection string
    }


    for(ipPos += 3; ipPos < connectionString.length() && connectionString[ipPos] != ' '; ++ipPos) {
        if (connectionString[ipPos] == '\\' || connectionString[ipPos] == '/') {
            return -EADDRNOTAVAIL; // Invalid connection string
        }
        addr[i++] = connectionString[ipPos];
        if(i >= sizeof(addr) - 1) {
            return -EADDRNOTAVAIL; // Address too long
        }
    }

    addr[i] = '\0'; // Null-terminate the address string            
    i = 0;
    for(portPos += 5; portPos < connectionString.length() && connectionString[portPos] != ' '; ++portPos) {
        if (connectionString[portPos] < 0x30 || connectionString[portPos] > 0x39) {
            return -EADDRNOTAVAIL; // Invalid connection string
        }
        port[i++] = connectionString[portPos];
        if(i >= sizeof(port) - 1) {
            return -EADDRNOTAVAIL; // Port too long
        }
    }
    port[i] = '\0'; // Null-terminate the port string

    
    std::string server_address = std::string(addr) + ":" + std::string(port);
    
    volatile bool started = false;
    
    grpc::Service* svc = static_cast<grpc::Service*>(service);

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(svc);
    std::unique_ptr<Server> server(builder.BuildAndStart());


    grpcServerThread = std::thread([this, &started, svr = std::move(server)]() mutable {
        // std::string svraddr = std::move(server_address);
        // GreeterServiceImpl service;
        // ServerBuilder builder;
        // builder.AddListeningPort(svraddr, grpc::InsecureServerCredentials());
        // builder.RegisterService(&service);
        // std::unique_ptr<Server> server(builder.BuildAndStart());
        // started = true;
        // server->Wait();
        grpcServer = svr.get();
        started = true;
        svr->Wait();
    });

    while(!started) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // this->stop_cb = stop_cb;
    listening = true;
    return 0;
}

/*
    @return: 0 on success, else on failure.
    @note: close all connections and stop the server, will wait for all threads to finish. for disconnect all client and stop server.
*/
int CDpfsGrpcsvr::stop() {

    if (grpcServer) {
        Server* server = static_cast<Server*>(grpcServer);
        server->Shutdown();
        grpcServer = nullptr;
    }

    if (grpcServerThread.joinable()) {
        grpcServerThread.join();
    }
    listening = false;
    return 0;
}

/*
    @param log_path: Path to the log file.
    @return 0 on success, else on failure.
*/
void CDpfsGrpcsvr::set_log_path(const char* log_path) {

}

/*
    @param level: Log level.
    @return 0 on success, else on failure.
*/
void CDpfsGrpcsvr::set_log_level(int level) {

}

void* newGrpcServer() {
    return new CDpfsGrpcsvr();
}