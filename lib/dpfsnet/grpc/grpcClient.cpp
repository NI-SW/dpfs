// this file deprecate, maybe remove in the future, grpc client will be defined in client folder.

// #include <dpfsnet/grpc/grpcClient.hpp>
// #include <grpcpp/grpcpp.h>
// // #include "net.grpc.pb.h"

// using grpc::Server;
// using grpc::ServerBuilder;
// using grpc::ServerContext;
// using grpc::Status;
// // using dpfsgrpc::HelloRequest;
// // using dpfsgrpc::HelloReply;
// // using dpfsgrpc::Greeter;


// // class GreeterClient {
// // public:
// //     GreeterClient(std::shared_ptr<grpc::Channel> channel) : stub_(Greeter::NewStub(channel)) {

// //     }

// //     std::string SayHello(const std::string& user) {
// //         HelloRequest request;
// //         request.set_name(user);
// //         HelloReply reply;
// //         grpc::ClientContext context;

// //         grpc::Status status = stub_->SayHello(&context, request, &reply);
// //         return status.ok() ? reply.message() : "RPC failed";
// //     }

// // private:
// //     std::unique_ptr<Greeter::Stub> stub_;
// // };


// CDpfsGrpc::CDpfsGrpc() {

// }

// CDpfsGrpc::~CDpfsGrpc() {

// }

// bool CDpfsGrpc::is_connected() const noexcept{

//     return m_connected;
// }

// /*
//     "ip:192.168.34.12 port:20500"
// */
// int CDpfsGrpc::connect(const char* connString) {

//     std::lock_guard<std::recursive_mutex> lock(m_lock);

//     int rc = 0;
//     size_t i = 0;
//     char addr[256]{0};
//     char port[16]{0};


//     std::string connectionString = connString;
//     size_t ipPos = connectionString.find("ip:");
//     size_t portPos = connectionString.find("port:");

//     if (ipPos == std::string::npos || portPos == std::string::npos) {
//         return -EADDRNOTAVAIL; // Invalid connection string
//     }


//     for(ipPos += 3; ipPos < connectionString.length() && connectionString[ipPos] != ' '; ++ipPos) {
//         if (connectionString[ipPos] == '\\' || connectionString[ipPos] == '/') {
//             return -EADDRNOTAVAIL; // Invalid connection string
//         }
//         addr[i++] = connectionString[ipPos];
//         if(i >= sizeof(addr) - 1) {
//             return -EADDRNOTAVAIL; // Address too long
//         }
//     }

//     addr[i] = '\0'; // Null-terminate the address string            
//     i = 0;
//     for(portPos += 5; portPos < connectionString.length() && connectionString[portPos] != ' '; ++portPos) {
//         if (connectionString[portPos] < 0x30 || connectionString[portPos] > 0x39) {
//             return -EADDRNOTAVAIL; // Invalid connection string
//         }
//         port[i++] = connectionString[portPos];
//         if(i >= sizeof(port) - 1) {
//             return -EADDRNOTAVAIL; // Port too long
//         }
//     }
//     port[i] = '\0'; // Null-terminate the port string

    
//     std::string server_address = std::string(addr) + ":" + std::string(port);

//     // m_connHandle = new GreeterClient(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
//     // if (!m_connHandle) {
//     //     rc = -ENOMEM; // Memory allocation error
//     //     goto connerror;
//     // }




//     m_connected = true;
//     return 0;
// connerror:
//     m_connected = false;
//     return rc; // Connection error

//     return 0;
// }

// int CDpfsGrpc::disconnect() {
//     if (!m_connected) {
//         return 0; // Not connected
//     }
//     m_connected = false;
//     delete m_connHandle;
//     m_connHandle = nullptr;
//     return 0;
// }

// int CDpfsGrpc::send(const void* buffer, int size) {
//     return 0;
// }

// int CDpfsGrpc::recv(void** buffer, int* retsize) {
//     return 0;
// }

// void CDpfsGrpc::buffree(void* buffer) {

// }
// /*
//     @param log_path: Path to the log file.
//     @return 0 on success, else on failure.
//     @note not thread safe
// */
// void CDpfsGrpc::set_log_path(const char* log_path) {

// }

// /*
//     @param level: Log level.
//     @return 0 on success, else on failure.
//     @note not thread safe
// */
// void CDpfsGrpc::set_log_level(int level) {

// }

// void* newGrpcClient() {
//     return new CDpfsGrpc();
// }

// // int CDpfsGrpc::sendTestMsg() {
// //     std::string user("world");
// //     std::string reply = m_connHandle->SayHello(user);
// //     printf("Greeter received: %s\n", reply.c_str());
// //     return 0;
// // }