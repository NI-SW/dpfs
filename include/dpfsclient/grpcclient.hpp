#include <cstdint>
#include <grpcpp/grpcpp.h>
#include <proto/sysrpc.grpc.pb.h>
#include <collect/collect.hpp>
#include <mysql_decimal/my_decimal.h>

#define __DEBUG_GRPCCLIENT__

#ifdef __DEBUG_GRPCCLIENT__
#include <dpfsdebug.hpp>
#endif

#define IDXHANDLE int32_t
#define SERVER_IDXHANDLE int32_t

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
    int getIdxIter(const std::vector<std::string>& idxCol, const std::vector<std::string>& idxVals, IDXHANDLE& hidx);
    
    /*
        @param hidx the index iterator handle returned by getIdxIter
        @return 0 if release success, otherwise return the error code, and set msg to the error message
        @note you must release idxhandle after using it.
    */
    int releaseIdxIter(const IDXHANDLE& hidx);

    /*
        @param hidx the index iterator handle returned by getIdxIter
        @param colPos the column position to get value, start from 0
        @param value the value of the column at current row, will return binary data in string format.
        @return 0 if get value success, otherwise return the error code, and set msg to the error message
    */
    int getDataByIdxIter(const IDXHANDLE& hidx, int colPos, std::string& value, dpfs_ctype_t type = dpfs_ctype_t::TYPE_BINARY);
    // fetch next row, maybe trigger the server to fetch next batch of rows if the current batch is all fetched.
    int fetchNextRow(const IDXHANDLE& hidx);

    const CFixLenVec<CColumn, uint8_t, MAX_COL_NUM>& getColInfo(const IDXHANDLE& hidx);
    
    std::string msg;
    
private:
    // fetch next batch of rows.
    int fetchNextRowSets(const IDXHANDLE& hidx);


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
        m_tabStructInfo(tabStructInfo.data(), tabStructInfo.size()),
        cs(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(m_tabStructInfo.data())), m_tabStructInfo.size()),
        item(cs.m_cols, maxRowNumber) {
            currentRowPos = 0;
            currentBatchRowCount = 0;
        }

        tableResultCache(const tableResultCache& other) = delete;

        tableResultCache(tableResultCache&& other) noexcept :
        serverIdxHandle(other.serverIdxHandle),
        m_tabStructInfo(std::move(other.m_tabStructInfo)),
        cs(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(m_tabStructInfo.data())), m_tabStructInfo.size()),
        item(cs.m_cols, other.item.maxSize()) {
            currentRowPos = other.currentRowPos;
            currentBatchRowCount = other.currentBatchRowCount;
        }
        // item(cs.m_cols, other.item.maxSize()) {
        //     currentRowPos = other.currentRowPos;
        // }

        ~tableResultCache() = default;

        SERVER_IDXHANDLE serverIdxHandle;
        std::string m_tabStructInfo;
        const CCollection::collectionStruct cs;
        CItem item;
        // no more data to fetch from server
        bool fetch2End = false;
        int currentRowPos = -1;
        int currentBatchRowCount = 0;
    };

    // key: index handle on local, value: handle on server
    std::unordered_map<IDXHANDLE, tableResultCache> idxHandles;
    IDXHANDLE idxHandleCount = 0;

    int fetch_row_number = DEFAULT_FETCH_ROW_NUMBER;
};