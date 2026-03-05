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

    #ifdef __DEBUG_GRPCCLIENT__
    std::cout << "Received table handle: ";
    printMemory(reply.table_handle().data(), reply.table_handle().size());
    std::cout << std::endl;
    #endif

    msg = "Get table handle success: " + reply.msg();
    memcpy(htab, reply.table_handle().data(), sizeof(htab));

    tabStructInfo = reply.table_infos();

    // std::cout << "Received table structure info: " << tabStructInfo << std::endl;   
    std::cout << "Table structure info size: " << tabStructInfo.size() << std::endl;

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

    auto it = idxHandles.find(idxHandleCount);
    cout << "vec Size = " << it->second.item.cols.size() << " addr = " << &it->second.item << endl;

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

    if (rs.currentRowPos >= rs.item.size()) {
        msg = "No more rows available in the current batch.";
        return -ENOENT; // No more rows
    }

    CValue val = rs.item.getValue(colPos);
    if (val.data == nullptr || val.len == 0) {
        msg = "Value is null or empty.";
        return -ENOENT; // No value
    }

    value.assign(val.data, val.len);

    return 0;
}

int CGrpcCli::fetchNextRow(const IDXHANDLE& hidx) {

    int rc = 0;
    if (husr == -1) {
        msg = "User not logged in.";
        return -EINVAL; // Not logged in
    }

    if (hidx == -1) {
        msg = "Index handle cannot be null.";
        return -EINVAL; // Invalid input
    }

    auto it = idxHandles.find(hidx);
    if (it == idxHandles.end()) {
        msg = "Invalid index handle.";
        return -EINVAL; // Invalid index handle
    }

    cout << "vecsize = " << idxHandles.begin()->second.item.size() << endl;

    auto& rs = it->second;
    rs.currentRowPos++;
    if (rs.currentRowPos >= rs.item.size()) {
        // If we've reached the end of the current batch, fetch the next batch
        rc = fetchNextRowSets(hidx);
        if (rc != 0) {
            // msg = "Failed to fetch next row sets: " + std::to_string(rc);
            return rc;
        }
        rs.item.resetScan();
        rs.currentRowPos = 0; // Reset the current row position for the new batch
    } else {
        // msg = "Fetched next row from current batch successfully.";
        rs.item.nextRow();
    }

    return 0;
}

int CGrpcCli::fetchNextRowSets(const IDXHANDLE& hidx) {
    if (husr == -1) {
        msg = "User not logged in.";
        return -EINVAL; // Not logged in
    }
    int rc = 0;

    auto it = idxHandles.find(hidx);
    if (it == idxHandles.end()) {
        msg = "Invalid index handle.";
        return -EINVAL; // Invalid index handle
    }

    auto& rs = it->second;

    dpfsgrpc::FetchNextRowSetsReq request;
    request.set_husr(husr);
    request.set_hidx(rs.serverIdxHandle);
    request.set_acquire_row_number(this->fetch_row_number);

    dpfsgrpc::FetchNextRowSetsReply reply;
    grpc::ClientContext context;

    grpc::Status status = _stub->FetchNextRowSets(&context, request, &reply);
    if (!status.ok()) {
        msg = "RPC failed: " + status.error_message();
        return status.error_code();
    }

    if (reply.rc() != 0) {
        msg = "Fetch next row sets failed: " + reply.msg();
        return reply.rc();
    }



    rs.item.resetScan();
    // Update the cache with the new data
    for(auto& data : reply.data()) {
        rs.item.assignOneRow(data.data(), data.size());
        rs.item.nextRow();
    }


    rs.currentRowPos = 0; // Reset the current row position for the new batch

    // msg = "Fetch next row sets success: " + reply.msg();
    return 0;
}