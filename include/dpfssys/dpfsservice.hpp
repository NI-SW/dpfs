#pragma once

#include <dpfssys/dpfssys.hpp>
#include <dpfssys/dpfsdata.hpp>
#include <string>
#include <grpcpp/grpcpp.h>
#include <proto/sysrpc.grpc.pb.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

class sysCtlServiceImpl final : public dpfsgrpc::sysCtl::Service {

    dpfsSystem* system = nullptr;

    // login to the system, return a user handle for subsequent operations, the user handle is an integer that uniquely identifies the user session
    Status login(ServerContext* context, const dpfsgrpc::loginReq* request, dpfsgrpc::loginReply* response) override;
    Status logoff(ServerContext* context, const dpfsgrpc::logoffReq* request, dpfsgrpc::operateReply* response) override;

    

public:
    sysCtlServiceImpl(void* arg) : Service() {
        system = static_cast<dpfsSystem*>(arg);
    }
};