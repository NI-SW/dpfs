#include <grpcpp/grpcpp.h>

#include <dpfsclient/grpcclient.hpp>


CGrpcCli::~CGrpcCli() {
    if (husr != 0) {
        logoff();
    }
    tabStructInfo.clear();
}


int CGrpcCli::login(const std::string& user, const std::string& password) {
    dpfsgrpc::LoginReq request;
    request.set_username(user);
    request.set_password(password);
    dpfsgrpc::LoginReply reply;
    grpc::ClientContext context;
    
    grpc::Status status = _stub->Login(&context, request, &reply);

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
    dpfsgrpc::LogoffReq request;
    request.set_husr(husr);
    dpfsgrpc::OperateReply reply;
    grpc::ClientContext context;

    grpc::Status status = _stub->Logoff(&context, request, &reply);

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
    dpfsgrpc::Exesql request;
    request.set_husr(husr);
    request.set_sql(sql);
    dpfsgrpc::OperateReply reply;
    grpc::ClientContext context;

    grpc::Status status = _stub->ExecSQL(&context, request, &reply);

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
    dpfsgrpc::GetTableRequest request;
    request.set_husr(husr);
    request.set_schema_name(schema);
    request.set_table_name(table);
  
    memset(htab, 0, sizeof(htab));
    tabStructInfo.clear();

    dpfsgrpc::GetTableReply reply;
    grpc::ClientContext context;

    grpc::Status status = _stub->GetTableHandle(&context, request, &reply);

    if (!status.ok()) {
        msg = "RPC failed: " + status.error_message();
        return status.error_code();
    }

    if (reply.rc() != 0) {
        msg = "Get table handle failed: " + reply.msg();
        return reply.rc();
    }

    std::cout << "Received table handle: ";
    for (size_t i = 0; i < sizeof(htab); ++i) {
        printf("%02x", htab[i]);
    }
    std::cout << std::endl;

    msg = "Get table handle success: " + reply.msg();
    memcpy(htab, reply.table_handle().data(), sizeof(htab));
    tabStructInfo = reply.table_infos();

    return 0;
}

int CGrpcCli::releaseTableHandle() {    
    memset(htab, 0, sizeof(htab));
    tabStructInfo.clear();
    msg = "Table handle released successfully.";
    return 0;
}


int CGrpcCli::getIdxIter(const std::vector<std::string>& idxCol, const std::vector<std::string>& idxVals, IDXHANDLE* hidx) {
    
    if (husr == -1) {
        msg = "User not logged in.";
        return -EINVAL; // Not logged in
    }

    uint8_t emptyHandle[16] = { 0 };

    if (memcmp(htab, emptyHandle, sizeof(emptyHandle)) == 0) {
        msg = "Table handle is not set.";
        return -EINVAL; // Table handle not set
    }

    if (hidx == nullptr) {
        msg = "Output index handle pointer cannot be null.";
        return -EINVAL; // Invalid output parameter
    }

    if (idxCol.size() != idxVals.size()) {
        msg = "The size of index columns and index values must be the same.";
        return -EINVAL; // Invalid input
    }

    dpfsgrpc::GetIdxIterReq request;
    request.set_husr(husr);
    request.set_table_handle((void*)htab, sizeof(htab));

    for (int i = 0; i < idxCol.size(); ++i) {
        request.add_col_names(idxCol[i]);
        // request.set_col_names(idxCol[i]);
        request.add_key_vals(idxVals[i]);
    }

    
  
    dpfsgrpc::GetIdxIterReply reply;
    grpc::ClientContext context;

    grpc::Status status = _stub->GetIdxIter(&context, request, &reply);
    if (!status.ok()) {
        msg = "RPC failed: " + status.error_message();
        return status.error_code();
    }

    if (reply.rc() != 0) {
        msg = "Get index iterator failed: " + reply.msg();
        return reply.rc();
    }

    
    idxHandles.emplace(idxHandleCount, tableResultCache(reply.hidx(), tabStructInfo));
    *hidx = idxHandleCount;
    ++idxHandleCount;
    msg = "Get index iterator success: " + reply.msg();
    return 0;
}

int CGrpcCli::releaseIdxIter(const IDXHANDLE& hidx) {
    auto it = idxHandles.find(hidx);
    if (it == idxHandles.end()) {
        msg = "Invalid index handle.";
        return -EINVAL;
    }
    SERVER_IDXHANDLE serverHidx = it->second.serverIdxHandle;
    idxHandles.erase(it);

    dpfsgrpc::ReleaseIdxIterReq request;
    request.set_husr(husr);
    request.set_hidx(serverHidx);

    dpfsgrpc::OperateReply reply;
    grpc::ClientContext context;

    grpc::Status status = _stub->ReleaseIdxIter(&context, request, &reply);
    if (!status.ok()) {
        msg = "RPC failed: " + status.error_message();
        return status.error_code();
    }

    if (reply.rc() != 0) {
        msg = "Release index iterator failed: " + reply.msg();
        return reply.rc();
    }

    msg = "Release index iterator success.";
    return 0;
}

int CGrpcCli::getRowByIdxIter(const IDXHANDLE& hidx, int colPos, std::string& value) {
    if (husr == -1) {
        msg = "User not logged in.";
        return -EINVAL; // Not logged in
    }

    auto it = idxHandles.find(hidx);
    if (it == idxHandles.end()) {
        msg = "Invalid index handle.";
        return -EINVAL; // Invalid index handle
    }

    auto& rs = it->second;

    // TODO
    // if (rs.currentRowPos < 0 || rs.currentRowPos >= rs.item.size()) {
    //     // Need to fetch next batch of rows from server
    //     dpfsgrpc::FetchIdxIterReq request;
    //     request.set_husr(husr);
    //     request.set_hidx(rs.serverIdxHandle);
    //     request.set_batch_size(DEFAULT_FETCH_ROW_NUMBER);

    //     dpfsgrpc::FetchIdxIterReply reply;
    //     grpc::ClientContext context;

    //     grpc::Status status = _stub->FetchIdxIter(&context, request, &reply);
    //     if (!status.ok()) {
    //         msg = "RPC failed: " + status.error_message();
    //         return status.error_code();
    //     }

    //     if (reply.rc() != 0) {
    //         msg = "Fetch index iterator failed: " + reply.msg();
    //         return reply.rc();
    //     }

    //     // Clear current items and load new batch
    //     rs.item.clear();
    //     for (const auto& row : reply.rows()) {
    //         rs.item.addRow(row.data());
    //     }
    //     rs.currentRowPos = 0; // Reset position to the first row of the new batch
    // }

    // if (colPos < 0 || colPos >= rs.item.getColNum()) {
    //     msg = "Invalid column position.";
    //     return -EINVAL;
    // }

    // value = rs.item.getColValue(rs.currentRowPos, colPos);
    // rs.currentRowPos++; // Move to the next row for the next call

    return 0;
}

