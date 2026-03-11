#include <grpcpp/grpcpp.h>

#include <dpfsclient/grpcclient.hpp>

// TODO
int toInt(dpfs_datatype_t src_type, const char* src, size_t len, std::string& dest) {
    return -ENOTSUP;
}

int toDouble(dpfs_datatype_t src_type, const char* src, size_t len) {
   
    return -ENOTSUP;
}

int toString(dpfs_datatype_t src_type, const char* src, size_t len, std::string& dest) {
    return -ENOTSUP;
}

int toBinary(dpfs_datatype_t src_type, const char* src, size_t len, std::string& dest) {
    return -ENOTSUP;
}

static int dataConvert(const char* src, size_t len, dpfs_datatype_t src_type, std::string& dest, dpfs_ctype_t tgt_type) {

    return -ENOTSUP;
    // TODO type convertor
    switch (tgt_type)
    {
    case dpfs_ctype_t::TYPE_INT:
    case dpfs_ctype_t::TYPE_BIGINT:
        toInt(src_type, src, len, dest);
        break;
    case dpfs_ctype_t::TYPE_DOUBLE:
        break;
    case dpfs_ctype_t::TYPE_STRING: 
        dest.assign(src, len);
        break;
    case dpfs_ctype_t::TYPE_BINARY:
        dest.assign(src, len);
        break;
    default:
        break;
    }
    
    return 0;
}

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

int CGrpcCli::getIdxIter(const std::vector<std::string>& idxCol, const std::vector<std::string>& idxVals, IDXHANDLE& hidx) {
    
    if (husr == -1) {
        msg = "User not logged in.";
        return -EINVAL; // Not logged in
    }

    uint8_t emptyHandle[16] = { 0 };

    if (memcmp(htab, emptyHandle, sizeof(emptyHandle)) == 0) {
        msg = "Table handle is not set.";
        return -EINVAL; // Table handle not set
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

    
    idxHandles.emplace(idxHandleCount, std::move(tableResultCache(reply.hidx(), tabStructInfo)));

    auto it = idxHandles.find(idxHandleCount);
    cout << "column Size = " << it->second.item.m_dataLen.size() << " addr = " << &it->second.item << endl;

    hidx = idxHandleCount;
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

int CGrpcCli::getDataByIdxIter(const IDXHANDLE& hidx, int colPos, std::string& value, dpfs_ctype_t type) {
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

    if (rs.currentRowPos >= rs.currentBatchRowCount) {
        msg = "No more rows available in the current batch.";
        return -ENODATA; // No more rows
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

    cout << "curser pos = " << idxHandles.begin()->second.item.curPos() << endl;

    auto& rs = it->second;
    ++rs.currentRowPos;

    if (rs.currentRowPos >= rs.currentBatchRowCount) {
        if (rs.fetch2End) {
            msg = "No more rows to fetch from server.";
            return -ENODATA;
        }

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
        rc = rs.item.nextRow();
        if (rc != 0) {
            msg = "Failed to move to next row in current batch: " + std::to_string(rc);
            return rc;
        }
    }

    return 0;
}

const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM>& CGrpcCli::getColInfo(const IDXHANDLE& hidx) {
    auto it = idxHandles.find(hidx);
    if (it == idxHandles.end()) {
        throw std::invalid_argument("Invalid index handle.");
    }
    return it->second.cs.m_cols;
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

    if (reply.rc() == ENODATA) {
        rs.fetch2End = true;
    } else if (reply.rc() != 0) {
        msg = "Fetch next row sets failed: " + reply.msg();
        return reply.rc();
    }



    rs.item.resetScan();
    // Update the cache with the new data
    for(auto& data : reply.data()) {
        rs.item.assignOneRow(data.data(), data.size());
        rs.item.nextRow();
    }

    rs.currentBatchRowCount = reply.data_size();
    rs.currentRowPos = 0; // Reset the current row position for the new batch

    // msg = "Fetch next row sets success: " + reply.msg();
    return 0;
}

int CGrpcCli::createTracablePro(const std::string& schema_name, const std::string& structure_name, 
    const std::map<std::string, std::string>& base_info, const std::vector<std::string>& ingredient_names, int32_t total_production_num, std::string& traceCodePrefix) {
    
    if (husr == -1) {
        msg = "User not logged in.";
        return -EINVAL; // Not logged in
    }

    dpfsgrpc::CreateTracableProReq request;
    request.set_husr(husr);
    request.set_schema_name(schema_name);
    request.set_structure_name(structure_name);
    for (const auto& pair : base_info) {
        (*request.mutable_base_info())[pair.first] = pair.second;
    }
    for (const auto& ingredient : ingredient_names) {
        request.add_ingredient_names(ingredient);
    }
    request.set_total_production_num(total_production_num);

    dpfsgrpc::CreateTracableProReply reply;
    grpc::ClientContext context;
    grpc::Status status = _stub->CreateTracablePro(&context, request, &reply);
    if (!status.ok()) {
        msg = "RPC failed: " + status.error_message();
        return status.error_code();
    }
    if (reply.rc() != 0) {
        msg = "Create traceable production failed: " + reply.msg();
        return reply.rc();
    }
    msg = "Create traceable production success: " + reply.msg();
    traceCodePrefix.assign(reply.trace_code_prefix().data(), reply.trace_code_prefix().size());
    return 0;

}

int CGrpcCli::traceBack(const std::string& trace_code, std::string& trace_result, bool show_detail) {
    if (husr == -1) {
        msg = "User not logged in.";
        return -EINVAL; // Not logged in
    }

    if (trace_code.size() != sizeof(bidx) + sizeof(int32_t)) {
        msg = "Invalid trace code format.";
        return -EINVAL; // Invalid trace code format
    }

    dpfsgrpc::TraceBackReq request;
    request.set_husr(husr);
    request.set_trace_code(trace_code);
    request.set_show_detail(show_detail);
    dpfsgrpc::TraceBackReply reply;
    grpc::ClientContext context;
    grpc::Status status = _stub->TraceBack(&context, request, &reply);
    if (!status.ok()) {
        msg = "RPC failed: " + status.error_message();
        return status.error_code();
    }
    if (reply.rc() != 0) {
        msg = "Trace back failed: " + reply.msg();
        return reply.rc();
    }
    msg = "Trace back success: " + reply.msg();

    trace_result.assign(reply.trace_back_result().data(), reply.trace_back_result().size());
    return 0;
}

int CGrpcCli::makeTrade(
    const std::string& schema_name, 
    const std::string& structure_name, 
    int64_t jyid, 
    int32_t start_uid, 
    int32_t num,
    const std::string& mfmc, 
    const std::string& mfdz, 
    const std::string& mflx, 
    const std::string& ffmc, 
    const std::string& ffdz, 
    const std::string& fflx, 
    const std::string& wlxx, 
    const std::string& other_info,
    const std::string& fsje) {

    if (husr == -1) {
        msg = "User not logged in.";
        return -EINVAL; // Not logged in
    }

    dpfsgrpc::MakeTradeReq request;
    request.set_husr(husr);
    request.set_schema_name(schema_name);
    request.set_structure_name(structure_name);
    request.set_jyid(jyid);
    request.set_start_uid(start_uid);
    request.set_num(num);
    request.set_mfmc(mfmc);
    request.set_mfdz(mfdz);
    request.set_mflx(mflx);
    request.set_ffmc(ffmc);
    request.set_ffdz(ffdz);
    request.set_fflx(fflx);
    request.set_wlxx(wlxx);
    request.set_others(other_info); 
    request.set_fsje(fsje);

    dpfsgrpc::OperateReply reply;
    grpc::ClientContext context;
    grpc::Status status = _stub->MakeTrade(&context, request, &reply);
    if (!status.ok()) {
        msg = "RPC failed: " + status.error_message();
        return status.error_code();
    }
    if (reply.rc() != 0) {
        msg = "Make trade failed: " + reply.msg();
        return reply.rc();
    }
    msg = "Make trade success: " + reply.msg();
    return 0;
}




