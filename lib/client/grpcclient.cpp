#include <grpcpp/grpcpp.h>

#include <dpfsclient/grpcclient.hpp>

int CGrpcCli::login(const std::string& user, const std::string& password) {
    dpfsgrpc::loginReq request;
    request.set_username(user);
    request.set_password(password);
    dpfsgrpc::loginReply reply;
    grpc::ClientContext context;
    
    grpc::Status status = _stub->login(&context, request, &reply);

    if (!status.ok()) {
        msg = "RPC failed: " + status.error_message();
        husr = 0;
        return status.error_code();
    }

    if (reply.rc() == 0) {
        msg = "Login success: " + reply.msg();
    } else {
        msg = "Login failed: " + reply.msg();
        msg = reply.msg(); 
        return reply.rc();
    }

    husr = reply.husr();
    
    return 0;
}

int CGrpcCli::logoff() {
    dpfsgrpc::logoffReq request;
    request.set_husr(husr);
    dpfsgrpc::operateReply reply;
    grpc::ClientContext context;

    grpc::Status status = _stub->logoff(&context, request, &reply);

    if (!status.ok()) {
        msg = "RPC failed: " + status.error_message();
        return status.error_code();
    }

    if (reply.rc() == 0) {
        msg = "Logoff success: " + reply.msg();
    } else {
        msg = "Logoff failed: " + reply.msg();
        return reply.rc();
    }

    husr = 0; // Reset the user handle after logoff
    return 0;
}

int CGrpcCli::execSQL(const std::string& sql) {
    dpfsgrpc::exesql request;
    request.set_husr(husr);
    request.set_sql(sql);
    dpfsgrpc::operateReply reply;
    grpc::ClientContext context;

    grpc::Status status = _stub->execSQL(&context, request, &reply);

    if (!status.ok()) {
        msg = "RPC failed: " + status.error_message();
        return status.error_code();
    }

    if (reply.rc() == 0) {
        msg = "SQL execution success: " + reply.msg();
    } else {
        msg = "SQL execution failed: " + reply.msg();
        return reply.rc();
    }

    return 0;
}

int CGrpcCli::getTableHandle(const std::string& schema, const std::string& table) {
    dpfsgrpc::getTableRequest request;
    request.set_husr(husr);
    request.set_schemaname(schema);
    request.set_tablename(table);
  
    dpfsgrpc::getTableReply reply;
    grpc::ClientContext context;

    grpc::Status status = _stub->getTableHandle(&context, request, &reply);

    if (!status.ok()) {
        msg = "RPC failed: " + status.error_message();
        return status.error_code();
    }

    if (reply.rc() == 0) {
        msg = "Get table handle success: " + reply.msg();
        memcpy(htab, reply.tablehandle().data(), sizeof(htab));
    } else {
        msg = "Get table handle failed: " + reply.msg();
        return reply.rc();
    }

    std::cout << "Received table handle: ";
    for (size_t i = 0; i < sizeof(htab); ++i) {
        printf("%02x", htab[i]);
    }
    std::cout << std::endl;
    return 0;
}
