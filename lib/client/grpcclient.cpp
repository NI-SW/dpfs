#include <grpcpp/grpcpp.h>
#include <dpfsclient/grpcclient.hpp>
#include <basic/dpfsconst.hpp>

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

 size_t CGrpcCli::getTotalRowCount(const IDXHANDLE& hidx) {
    auto it = idxHandles.find(hidx);
    if (it == idxHandles.end()) {
        throw std::invalid_argument("Invalid index handle.");
    }
    return it->second.cs.ds->m_rowCount;
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
    const std::map<std::string, std::string>& base_info, const std::map<std::string, std::string>& ingredient_infos, int32_t total_production_num, std::string& traceCodePrefix) {
    
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
    for (const auto& ingredient : ingredient_infos) {
        (*request.mutable_ingredient_infos())[ingredient.first] = ingredient.second;
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

int CGrpcCli::DropTracablePro(const std::string& schema_name, const std::string& structure_name) {
    if (husr == -1) {
        msg = "User not logged in.";
        return -EINVAL;
    }

    if (schema_name.empty() || structure_name.empty()) {
        msg = "schema_name and structure_name must not be empty.";
        return -EINVAL;
    }

    const std::string spxxbTableName = structure_name + "_SPXXB";
    const std::string plkzbTableName = structure_name + "_PLKZB";
    const std::string spjybTableName = structure_name + "_SPJYB";
    const std::string spkzbTableName = structure_name + "_SPKZB";

    // Drop all traceability tables created by createTracablePro.
    const std::string dropSqlList[] = {
        "drop table " + schema_name + "." + spxxbTableName,
        "drop table " + schema_name + "." + plkzbTableName,
        "drop table " + schema_name + "." + spjybTableName,
        "drop table " + schema_name + "." + spkzbTableName
    };

    for (const auto& sql : dropSqlList) {
        int rc = execSQL(sql);
        if (rc != 0) {
            msg = "Drop traceable production failed, SQL: " + sql + ", rc=" + std::to_string(rc) + ", detail: " + msg;
            return rc;
        }
    }

    msg = "Drop traceable production success: " + schema_name + "." + structure_name;
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

inline int parseResultLine(const std::string& resultStr, size_t& pos, std::unordered_map<std::string, std::string>& resultMap) {
    // TODO parse the result string into structured data

    size_t sepPos = pos;
    while (sepPos < resultStr.size() && resultStr[sepPos] != ':' && resultStr[sepPos] != '\n') {
        ++sepPos;
    }
    
    std::string key = resultStr.substr(pos, sepPos - pos).c_str();

    pos = sepPos + 1;

    

    while(pos < resultStr.size() && (resultStr[pos] == ' ' || pos == '\0')) {
        if (resultStr[pos] == '\n') {
            ++pos;
            resultMap[key] = ""; // Key with empty value
            return 0; // Invalid format, value is missing
        }
        ++pos; // Skip spaces
    }

    size_t nPos = resultStr.find('\n', pos);
    if (nPos == std::string::npos) {
        resultMap[key] = resultStr.substr(pos).c_str();
        pos = resultStr.size(); // Move position to the end of the string
        return 0;
    }

    resultMap[key] = resultStr.substr(pos, nPos - pos).c_str();
    pos = nPos + 1; // Move position to the start of the next line

    return 0;
}

// process one trade block
inline int parseTradeBlock(const std::string& resultStr, size_t& pos, std::vector<CGrpcCli::CResult::CTradeInfo>& resultMap) {
    // for each deep 

    /* line number = JYB_COLNUMBER
DEEP: 89
JYID: 11
SPJYQSID: 11
SPJYSL: 900
MFMC: TESTNAME
MFDZ: TESTADDRESS
MFLX: TESTPHONE
FFMC: TESTFNAME
FFDZ: TESTFADDRESS
FFLX: TESTFPHONE
LOGISTICS_INFO: TEST上海虹桥冷链运输车牌1234567至北京市大兴区
OTHER_INFO: TEST交易日期:2026-03-10
FSJE: 50.2010
    */
    
    int rc = 0;

    resultMap.emplace_back();
    for (int i = 0; i < JYB_COLNUMBER; ++i) { // Assuming JYB_COLNUMBER lines per trade block
        rc = parseResultLine(resultStr, pos, resultMap.back().trade_info);
        if (rc != 0) {
            return rc;
        }
    }

    return 0;
}

// process one ingredient block
inline int parseIngreBlock(const std::string& resultStr, size_t& pos, std::vector<CGrpcCli::CResult::CIngredientInfo>& resultMap) {
    // for each deep 

    /* line number = INGREDIENT_COLNUMBER
INGREDIENTINFOBEGIN: 1
Ingredient Name: 白糖
Ingredient Percentage: 75.00
Ingredient Trace Code: 0000000000000000ce0400000000000000000000
Ingredient Name: 食用油
Ingredient Percentage: 10.00
Ingredient Trace Code: 00000000000000000a0400000000000000000000
Ingredient Name: 食盐
Ingredient Percentage: 15.00
Ingredient Trace Code: 00000000000000007f0400000000000000000000
INGREDIENTINFOEND: 1
    */
    
    int rc = 0;

    resultMap.emplace_back();
    for (int i = 0; i < INGREDIENT_COLNUMBER; ++i) { // Assuming INGREDIENT_COLNUMBER lines per ingredient block
        rc = parseResultLine(resultStr, pos, resultMap.back().ingredient_info);
        if (rc != 0) {
            return rc;
        }
    }

    return 0;
}

int CGrpcCli::parseTraceResult(const std::string& trace_result, CResult& result) {

    size_t parsePos = 0, nextPos = 0;
    size_t endPos = 0, nextEndPos = 0;
    const std::string& str = trace_result;
    // parse base info
    nextPos = str.find(BASEINFOBEGIN, parsePos);
    nextEndPos = str.find(BASEINFOEND, nextPos);
    if (nextPos != std::string::npos && nextEndPos != std::string::npos && nextPos < nextEndPos) {
        parsePos = nextPos;
        endPos = nextEndPos;
        parsePos += sizeof(BASEINFOBEGIN) - 1;

        while(parsePos < endPos) {
            int rc = parseResultLine(str, parsePos, result.base_info);
            if (rc != 0) {
                msg = "Failed to parse base info line.";
                return rc; // Failed to parse base info line
            }
        }
    }

    // parse product info
    nextPos = str.find(PRODUCTINFOBEGIN, parsePos);
    nextEndPos = str.find(PRODUCTINFOEND, nextPos);
    if (nextPos != std::string::npos && nextEndPos != std::string::npos && nextPos < nextEndPos) {
        parsePos = nextPos;
        endPos = nextEndPos;

        parsePos += sizeof(PRODUCTINFOBEGIN) - 1;
        while(parsePos < endPos) {
            int rc = parseResultLine(str, parsePos, result.base_info);
            if (rc != 0) {
                msg = "Failed to parse product info line.";
                return rc; // Failed to parse product info line
            }
        }
    } 


    // parse trade info
    nextPos = str.find(TRADEBEGIN, parsePos);
    nextEndPos = str.find(TRADEEND, nextPos);
    if (nextPos != std::string::npos && nextEndPos != std::string::npos && nextPos < nextEndPos) {
        parsePos = nextPos;
        endPos = nextEndPos;
        parsePos += sizeof(TRADEBEGIN) - 1;

        while(parsePos < endPos) {
            int rc = parseTradeBlock(str, parsePos, result.trade_info);
            if (rc != 0) {
                msg = "Failed to parse trade info line.";
                return rc; // Failed to parse trade info line
            }
        }
    }


    // parse ingredient info
    nextPos = str.find(INGREDIENTINFOBEGIN, parsePos);
    nextEndPos = str.find(INGREDIENTINFOEND, nextPos);
    if (nextPos != std::string::npos && nextEndPos != std::string::npos && nextPos < nextEndPos) {
        parsePos = nextPos;
        endPos = nextEndPos;

        parsePos += sizeof(INGREDIENTINFOBEGIN) - 1;

        while(parsePos < endPos) {
            int rc = parseIngreBlock(str, parsePos, result.ingredient_info);
            if (rc != 0) {
                msg = "Failed to parse ingredient info line.";
                return rc; // Failed to parse ingredient info line
            }
        }
    }

    return 0;
}


