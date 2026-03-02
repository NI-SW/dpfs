#include <cstdint>
#include <grpcpp/grpcpp.h>
#include <proto/sysrpc.grpc.pb.h>
#include <collect/collect.hpp>


#define IDXHANDLE int32_t
#define SERVER_IDXHANDLE int32_t
#define DEFAULT_FETCH_ROW_NUMBER 50

class CGrpcCli {
public:
    CGrpcCli(std::shared_ptr<grpc::ChannelInterface> channel) : _stub(dpfsgrpc::SysCtl::NewStub(channel)) {
        if (!channel) {
            throw std::invalid_argument("Channel cannot be null");
        }
        if (!_stub) {
            throw std::runtime_error("Failed to create gRPC stub");
        }
    }
    ~CGrpcCli();

    /*
        @param user the user name to login
        @param password the password to login
        @return 0 if login success, otherwise return the error code, and set msg to the error message
        @note login service
    */
    int login(const std::string& user, const std::string& password);
    int logoff();
    int execSQL(const std::string& sql);

    /*
        @param schema the schema name of the table
        @param table the table name to get handle
        @return 0 if get handle success, otherwise return the error code, and set msg to the error message
        @note get table handle service, the handle is used for subsequent data I/O operations on this table, such as insert, query, etc.
        Each client can only have one table handle at a time.
    */
    int getTableHandle(const std::string& schema, const std::string& table);

    /*
        @return 0 if success.
        @note release table handle service.
    */
    int releaseTableHandle();

    /*
        @param idxCol the index column name to get iterator
        @param idxVals the index values to get iterator, the size of idxVals should be equal to the number of columns in the index, and the order of values should be the same as the order of columns in the index
        @return 0 if get iterator success, otherwise return the error code, and set msg to the error message
        @note get index iterator service, the iterator can be used for subsequent data I/O operations on this table, such as query, etc.
    */
    int getIdxIter(const std::vector<std::string>& idxCol, const std::vector<std::string>& idxVals, IDXHANDLE* hidx);
    int releaseIdxIter(const IDXHANDLE& hidx);

    int getRowByIdxIter(const IDXHANDLE& hidx, int colPos, std::string& value);

    std::string msg;
private:
    std::unique_ptr<dpfsgrpc::SysCtl::Stub> _stub;
    int32_t husr = -1;

    uint8_t htab[16] { 0 };
    // use string to store binary table structure info.
    std::string tabStructInfo = "";
    // uint8_t* tabStructInfoPtr = nullptr;

    struct tableResultCache {
        tableResultCache(
            SERVER_IDXHANDLE serverIdxHandle,
            const std::string& tabStructInfo, 
            size_t maxRowNumber = DEFAULT_FETCH_ROW_NUMBER
        ) : serverIdxHandle(serverIdxHandle),
        cs(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(tabStructInfo.data())), tabStructInfo.size()),
        item(cs.m_cols, maxRowNumber) {

        }

        tableResultCache(const tableResultCache& other) = delete;
        tableResultCache(tableResultCache&& other) noexcept :
        serverIdxHandle(other.serverIdxHandle),
        cs(other.cs),
        item(std::move(other.item)) {

        }

        ~tableResultCache() = default;

        SERVER_IDXHANDLE serverIdxHandle;
        const CCollection::collectionStruct cs;
        CItem item;
        int currentRowPos = -1;
    };

    // key: index handle on local, value: handle on server
    std::unordered_map<IDXHANDLE, tableResultCache> idxHandles;
    IDXHANDLE idxHandleCount = 0;

};