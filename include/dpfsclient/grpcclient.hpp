#include <cstdint>
#include <grpcpp/grpcpp.h>
#include <proto/sysrpc.grpc.pb.h>
#include <collect/collect.hpp>
#include <mysql_decimal/my_decimal.h>

// #define __DEBUG_GRPCCLIENT__

#ifdef __DEBUG_GRPCCLIENT__
#include <dpfsdebug.hpp>
#endif

#define IDXHANDLE int32_t
#define SERVER_IDXHANDLE int32_t

class CGrpcCli {
public:
    CGrpcCli(std::shared_ptr<grpc::ChannelInterface> channel) {
        if (!channel) {
            throw std::invalid_argument("Channel cannot be null");
        }

        _stub = dpfsgrpc::SysCtl::NewStub(channel);
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

    /*
        @param schema_name schema name of the traceable production set.
        @param structure_name name of the traceable production.
        @param map<string, string> base_info = 4; // 需要包含特定列名和对应的值，例如：SPKZB表中的bidx列和具体的bidx值
        @param repeated string ingredient_names = 5; // 需要追溯的原料控制表名
    */
    int createTracablePro(const std::string& schema_name, const std::string& structure_name, 
        const std::map<std::string, std::string>& base_info, const std::map<std::string, std::string>& ingredient_infos, int32_t total_production_num, std::string& traceCodePrefix);

    /*
        @param trace_code the code to identify the table of production's base info and production unique id.
        @param trace_result the trace result, which contains the production base info, trace info and ingredient info.
        @return 0 if trace back success, otherwise return the error code, and set msg
    */
    int traceBack(const std::string& trace_code, std::string& trace_result, bool show_detail = false);

    struct CResult {
        /*
        three part:
        | baseInfo | tradeInfo | IngredientInfo |
        */

        void clear() {
            base_info.clear();
            trade_info.clear();
            ingredient_info.clear();
        }

        struct CTradeInfo {
            // std::string name;
            std::unordered_map<std::string, std::string> trade_info;
        };

        struct CIngredientInfo {
            // std::string name;
            std::unordered_map<std::string, std::string> ingredient_info;
        };

        void print() const {
            std::cout << "Base Info:" << std::endl;
            for (const auto& [key, value] : base_info) {
                std::cout << key << ": " << value << std::endl;
            }

            std::cout << "\nTrade Info:" << std::endl;
            for (const auto& trade : trade_info) {
                for (const auto& [key, value] : trade.trade_info) {
                    std::cout << key << ": " << value << std::endl;
                }
                std::cout << "-----------------" << std::endl;
            }

            std::cout << "\nIngredient Info:" << std::endl;
            for (const auto& ingredient : ingredient_info) {
                for (const auto& [key, value] : ingredient.ingredient_info) {
                    std::cout << key << ": " << value << std::endl;
                }
                std::cout << "-----------------" << std::endl;
            }
        }

        // base info of the production.
        std::unordered_map<std::string, std::string> base_info;
        // trade info of one product
        std::vector<CTradeInfo> trade_info;
        // ingredient info of production.
        std::vector<CIngredientInfo> ingredient_info;
    };

    /*
        
    */
    int parseTraceResult(const std::string& trace_result, CResult& result);

    /*
  string schema_name = 2;
  string structure_name = 3;
  int64 jyid = 4;
  int32 start_uid = 5; // 交易商品起始uid
  int32 num = 6; // 交易商品数量
  // 买方名称
  string mfmc = 7;
  // 买方地址
  string mfdz = 8;
  // 买方联系方式
  string mflx = 9;
  // 卖方名称
  string ffmc = 10;
  // 卖方地址
  string ffdz = 11;
  // 卖方联系方式
  string fflx = 12;
  // 物流信息
  string wlxx = 13;

  // 其他信息,暂不处理
  // repeated string others = 13;
    */
    int makeTrade(const std::string& schema_name, const std::string& structure_name, int64_t jyid, int32_t start_uid, int32_t num,
        const std::string& mfmc, const std::string& mfdz, const std::string& mflx, const std::string& ffmc, const std::string& ffdz, 
        const std::string& fflx, const std::string& wlxx, const std::string& other_info, const std::string& fsje);

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