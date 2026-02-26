#include <cstdint>
#include <grpcpp/grpcpp.h>
#include <proto/sysrpc.grpc.pb.h>


class CGrpcCli {
public:
    CGrpcCli(std::shared_ptr<grpc::ChannelInterface> channel) : _stub(dpfsgrpc::sysCtl::NewStub(channel)) {
        if (!channel) {
            throw std::invalid_argument("Channel cannot be null");
        }
        if (!_stub) {
            throw std::runtime_error("Failed to create gRPC stub");
        }
    }
    ~CGrpcCli() = default;

    /*
        @param user the user name to login
        @param password the password to login
        @return 0 if login success, otherwise return the error code, and set msg to the error message
        @note login service
    */
    int login(const std::string& user, const std::string& password);
    int logoff();
    int execSQL(const std::string& sql);



    std::string msg;
private:
    std::unique_ptr<dpfsgrpc::sysCtl::Stub> _stub;
    int32_t husr = 0;
};