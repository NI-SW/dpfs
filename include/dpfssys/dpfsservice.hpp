#pragma once
#include <dpfssys/dpfssys.hpp>
#include <dpfssys/dpfsdata.hpp>
#include <string>
#include <grpcpp/grpcpp.h>
#include <proto/sysrpc.grpc.pb.h>

// constexpr char BASEINFOBEGIN[] = "BASEINFOBEGIN: 1\n";
// constexpr char BASEINFOEND[] = "BASEINFOEND: 1\n";
// constexpr char PRODUCTINFOBEGIN[] = "PRODUCTINFOBEGIN: 1\n";
// constexpr char PRODUCTINFOEND[] = "PRODUCTINFOEND: 1\n";
// constexpr char TRADEBEGIN[] = "TRADEBEGIN: 1\n";
// constexpr char TRADEEND[] = "TRADEEND: 1\n";
// constexpr char INGREDIENTINFOBEGIN[] = "INGREDIENTINFOBEGIN: 1\n";
// constexpr char INGREDIENTINFOEND[] = "INGREDIENTINFOEND: 1\n";

// #define __DEBUG_GRPCSERVICE__

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

class sysCtlServiceImpl final : public dpfsgrpc::SysCtl::Service {

    dpfsSystem* system = nullptr;

    int getUserInfo(int32_t husr, CUser*& user) noexcept;
    /*
        @param htab: table handle, which is the pointer to the table info in CCollection::m_cltInfoCache, and the client can directly use CCollection::collectionStruct to load the table info
        @param tab: output parameter, the table info will be loaded to this parameter
        @return 0 on success, else on failure
        @note maybe not necessary.
    */
    //int getTableInfo(int32_t htab, CCollection& tab) noexcept;

    // login to the system, return a user handle for subsequent operations, the user handle is an integer that uniquely identifies the user session
    Status Login(ServerContext* context, const dpfsgrpc::LoginReq* request, dpfsgrpc::LoginReply* response) override;
    Status Logoff(ServerContext* context, const dpfsgrpc::LogoffReq* request, dpfsgrpc::OperateReply* response) override;
    Status ExecSQL(ServerContext* context, const dpfsgrpc::Exesql* request, dpfsgrpc::OperateReply* response) override;
    Status GetTableHandle(ServerContext* context, const dpfsgrpc::GetTableRequest* request, dpfsgrpc::GetTableReply* response) override;
    // DOING ::: 
    
    // TODO :: not support
    // 传入表句柄和一行数据，插入到表中
    Status InsertTable(ServerContext* context, const dpfsgrpc::InsertRequest* request, dpfsgrpc::OperateReply* response) override;
    // 传入主键，返回一行数据
    Status GetRow(ServerContext* context, const dpfsgrpc::GetRowRequest* request, dpfsgrpc::GetRowReply* response) override;
    
    // DOING ::
    Status GetIdxIter(ServerContext* context, const dpfsgrpc::GetIdxIterReq* request, dpfsgrpc::GetIdxIterReply* response) override;
    Status ReleaseIdxIter(ServerContext* context, const dpfsgrpc::ReleaseIdxIterReq* request, dpfsgrpc::OperateReply* response) override;
    Status FetchNextRowSets(ServerContext* context, const dpfsgrpc::FetchNextRowSetsReq* request, dpfsgrpc::FetchNextRowSetsReply* response) override;

    Status CreateTracablePro(ServerContext* context, const dpfsgrpc::CreateTracableProReq* request, dpfsgrpc::CreateTracableProReply* response) override;
    Status DropTracablePro(ServerContext* context, const dpfsgrpc::DropTracableProReq* request, dpfsgrpc::OperateReply* response) override;

    Status TraceBack(ServerContext* context, const dpfsgrpc::TraceBackReq* request, dpfsgrpc::TraceBackReply* response) override;

    Status MakeTrade(ServerContext* context, const dpfsgrpc::MakeTradeReq* request, dpfsgrpc::OperateReply* response) override;

public:
    sysCtlServiceImpl(void* arg) : Service() {
        system = static_cast<dpfsSystem*>(arg);
    }
private:

    bidx LinkIngredient(const std::string& schemaName, const std::string& structureName);
    int GetTraceTradeDetail(const bidx &bidx, int64_t traceId, std::string& result) noexcept;
    int GetIngredientInfo(const bidx &bidx, int64_t traceId, std::string& result) noexcept;
};