#include <dpfssys/dpfsservice.hpp>
#include <dpfssys/dpfsdata.hpp>
#include <parser/dpfsparser.hpp>
#include <mysql_decimal/my_decimal.h>

constexpr char SPXXBDEF[] = "(name char(32) NOT NULL PRIMARY KEY, value char(128) NOT NULL)";
constexpr char PLKZBDEF[] = "(name char(64) NOT NULL PRIMARY KEY, bidx binary(16) NOT NULL)";
constexpr char SPJYBDEF[] = "(\
                JYID BIGINT not null primary key, \
                SPJYQSID INT NOT NULL, \
                SPJYSL INT NOT NULL, \
                MFMC CHAR(64) NOT NULL, \
                MFDZ CHAR(64) NOT NULL, \
                MFLX CHAR(32) NOT NULL, \
                FFMC CHAR(64) NOT NULL, \
                FFDZ CHAR(64) NOT NULL, \
                FFLX CHAR(32) NOT NULL, \
                PREV_JYID BIGINT NOT NULL, \
                LOGISTICS_INFO CHAR(255) NOT NULL, \
                OTHER_INFO CHAR(255) NOT NULL, \
                FSJE DECIMAL(16, 4))";

constexpr char SPKZBDEF[] = "(\
            uid int not null primary key, \
            ctime bigint not null, \
            cstate bool not null, \
            ccount int not null, \
            ltrade bigint not null \
            )";

int sysCtlServiceImpl::getUserInfo(int32_t husr, CUser*& user) noexcept {
    system->m_usrCacheLock.lock();
    auto iter = system->m_userCache.find(husr);
    if (iter != system->m_userCache.end()) {
        user = &iter->second;
    } else {
        system->m_usrCacheLock.unlock();
        return -ENOENT; // user handle not found
    }
    user->lastActiveTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch());
    system->m_usrCacheLock.unlock();
    
    return 0;
}

Status sysCtlServiceImpl::Login(ServerContext* context, const dpfsgrpc::LoginReq* request, dpfsgrpc::LoginReply* response) {
    // Implement your login logic here
    // For demonstration, we will just set a dummy user handle
    // response->set_userid(12345);
    // response->set_username(request->username());
    int rc = 0;
    std::string usrName = request->username();
    std::string password = request->password();
    system->log.log_notic("Login attempt from user: %s\n", usrName.c_str());

    // cout << "Login attempt from user: " << usrName << endl;
    // check the user authentication.
    
    const auto& sysUsr = system->dataService->m_sysSchema->sysusers;
    KEY_T userName(const_cast<char*>(usrName.data()), usrName.size(), sysUsr.m_cmpTyps);

    cacheLocker cl(sysUsr.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard guard(cl);
    if (guard.returnCode() != 0) {
        system->log.log_error("Failed to acquire read lock on sysusers cache, rc=%d\n", guard.returnCode());

        response->set_msg("Failed to acquire read lock on sysusers cache");
        response->set_husr(0);
        response->set_rc(-EAGAIN);
        return Status::OK;
    }
    
    CCollection::collectionStruct sysUserCs(sysUsr.m_cltInfoCache->getPtr(), sysUsr.m_cltInfoCache->getLen() * dpfs_lba_size);
    
    CItem rowData(sysUserCs.m_cols);


    // release the lock before get row, since get row will acquire the lock again, and the lock is not recursive, if not release, will cause deadlock
    guard.release();

    rc = sysUsr.getRow(userName, &rowData);
    if (rc != 0) {
        response->set_msg("Authentication failed: user not found");
        response->set_husr(0);
        response->set_rc(-ENOENT);
        return Status::OK;
    }


    CValue pwd = rowData.beginIter[4];
    size_t pwdLen = strnlen(pwd.data, pwd.len); // ensure the password is null-terminated
    if (pwdLen != password.size()) {
        system->log.log_notic("Authentication failed for user: %s, incorrect password\n", usrName.c_str());
        response->set_msg("Authentication failed: incorrect password: received length does not match expected length rcvLen = " + std::to_string(password.size()) + ", expectedLen = " + std::to_string(pwd.len));
        response->set_husr(0);
        response->set_rc(-EACCES);
        return Status::OK;
    }

    if (memcmp(pwd.data, password.data(), pwdLen) != 0) {
        system->log.log_notic("Authentication failed for user: %s, incorrect password\n", usrName.c_str());
        response->set_msg("Authentication failed: incorrect password for user " + usrName + ": passwd = " + std::string(pwd.data, pwd.len));
        response->set_husr(0);
        response->set_rc(-EACCES);
        return Status::OK;
    }



    CUser usr;
    usr.userid = *(int32_t*)rowData.beginIter[5].data;
    usr.username = usrName;
    usr.dbprivilege = static_cast<dbPrivilege>(*(uint8_t*)rowData.beginIter[3].data);
    usr.currentSchema = "root"; // for simplicity, set current schema to root, can be modified later when implementing schema management

    // seconds since epoch
    usr.lastActiveTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch());
    usr.logOff = false;

    system->m_usrCacheLock.lock();
    // find next not used handle
    while (system->m_userCache.find(system->m_usrHandleCount) != system->m_userCache.end()) {
        ++system->m_usrHandleCount;
    }
    response->set_husr(system->m_usrHandleCount);

    system->m_userCache.emplace(system->m_usrHandleCount, std::move(usr));
    ++system->m_usrHandleCount;
    if (system->m_userCache.size() > 1024) {
        system->log.log_notic("trim user cache, remove invalid login client\n");

        auto iter = system->m_userCache.begin();
        for (; iter != system->m_userCache.end();) {
            if (iter->second.logOff || 
                (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch() - iter->second.lastActiveTime)).count() > 3600 /* 1 hour */) {
                system->log.log_notic("remove inactive user, handle: %d, username: %s\n", iter->first, iter->second.username.c_str());
                iter = system->m_userCache.erase(iter);
            } else {
                ++iter;
            }
        }
        if (system->m_usrHandleCount > 100000000) {
            system->m_usrHandleCount = 0;
        }

    }
    system->m_usrCacheLock.unlock();

    system->log.log_notic("User logged in, handle: %d, username: %s\n", response->husr(), usrName.c_str());

    response->set_msg("Login successful for user: " + usrName);
    response->set_rc(0);

    return Status::OK;
}


Status sysCtlServiceImpl::Logoff(ServerContext* context, const dpfsgrpc::LogoffReq* request, dpfsgrpc::OperateReply* response) {
    // Implement your logoff logic here
    // For demonstration, we will just set a dummy response

    int32_t husr = request->husr();
    system->m_usrCacheLock.lock();
    auto iter = system->m_userCache.find(husr);
    if (iter != system->m_userCache.end()) {
        iter->second.logOff = true;
        system->log.log_notic("User logged off, handle: %d, username: %s\n", iter->first, iter->second.username.c_str());
        system->m_userCache.erase(iter);
    }
    system->m_usrCacheLock.unlock();

    response->set_msg("Logoff successful");
    response->set_rc(0);
    return Status::OK;
}


Status sysCtlServiceImpl::ExecSQL(ServerContext* context, const dpfsgrpc::Exesql* request, dpfsgrpc::OperateReply* response) {

    int32_t husr = request->husr();
    const std::string& sql = request->sql();

    // get user info from cache
    
    CUser* puser = nullptr;
    int rc = getUserInfo(husr, puser);
    if (rc != 0) {
        response->set_msg("Failed to get user info for handle: " + std::to_string(husr));
        response->set_rc(rc);
        return Status::OK;
    }

    CUser& usr = *puser;

    
    CParser parser(usr, *system->dataService);

    rc = parser(sql);
    if (rc != 0) {
        response->set_msg("Failed to parse and build execution plan for SQL: " + sql);
        response->set_rc(rc);
    }
    CPlanHandle out(system->dataService->m_page, system->dataService->m_diskMan);

    rc = parser.buildPlan(sql, out);
    if (rc != 0) {
        response->set_msg("Failed to build execution plan for SQL: " + sql);
        response->set_rc(rc);
        return Status::OK;
    } 
    
    
    response->set_msg("SQL command completed successfully.");
    response->set_rc(0);
    return Status::OK;
}

Status sysCtlServiceImpl::GetTableHandle(ServerContext* context, const dpfsgrpc::GetTableRequest* request, dpfsgrpc::GetTableReply* response) {

    int husr = request->husr();
    CUser* pusr = nullptr;
    int rc = getUserInfo(husr, pusr);
    if (rc != 0) {
        response->set_msg("Failed to get user info for handle: " + std::to_string(husr));
        response->set_rc(rc);
        
        return Status::OK;
    }
    
    // TODO ::
    const std::string& schemaName = request->schema_name();
    const std::string& tableName = request->table_name();


    rc = system->dataService->checkExist(schemaName, tableName);
    if (rc != -EEXIST) {
        response->set_msg("Table does not exist: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }

    rc = system->dataService->checkPrivilege(*pusr, schemaName, tableName, tbPrivilege::TBPRIVILEGE_SELECT);
    if (rc != 0) {
        response->set_msg("User does not have access privilege on table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }

    // from systables get table info
    bidx tableBidx;
    rc = system->dataService->getTableBidx(schemaName, tableName, tableBidx);
    if (rc != 0) {
        response->set_msg("Failed to get table bidx for table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }

    // If we get here, the user has select privilege on the table
    // get table info from system schema
    



    CCollection c(system->dataService->m_diskMan, system->dataService->m_page);
    rc = c.loadFrom(tableBidx, false);
    if (rc != 0) {
        response->set_msg("Failed to load table collection for table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }
    
    rc = c.m_cltInfoCache->read_lock();
    if (rc != 0) {
        response->set_msg("Failed to acquire lock on collection info cache for table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }
    response->set_table_infos(c.m_cltInfoCache->getPtr(), c.m_cltInfoCache->getLen() * dpfs_lba_size);  
    
    response->set_table_handle((void*)&tableBidx, sizeof(tableBidx));
    c.m_cltInfoCache->read_unlock();
    response->set_rc(0);
    
    std::cout << "User " << pusr->username << " get table handle for " << schemaName << "." << tableName << ", bidx: (" << tableBidx.gid << ", " << tableBidx.bid << ")\n";

    return Status::OK;
}

// 传入表句柄和一行数据，插入到表中
Status sysCtlServiceImpl::InsertTable(ServerContext* context, const dpfsgrpc::InsertRequest* request, dpfsgrpc::OperateReply* response) {
    return Status::OK;
}
// 传入主键，返回一行数据
Status sysCtlServiceImpl::GetRow(ServerContext* context, const dpfsgrpc::GetRowRequest* request, dpfsgrpc::GetRowReply* response) {
    return Status::OK;
}

/*


client -> getidxiter -> acquire and idxhandle
server -> getidxiter -> acquire idxIter and return idxHandle


*/
Status sysCtlServiceImpl::GetIdxIter(ServerContext* context, const dpfsgrpc::GetIdxIterReq* request, dpfsgrpc::GetIdxIterReply* response) {

    int rc = 0;
    int husr = request->husr();
    CUser* puser = nullptr;
    rc = getUserInfo(husr, puser);
    if (rc != 0) {
        response->set_msg("Failed to get user info for handle: " + std::to_string(husr));
        response->set_rc(rc);
        return Status::OK;
    }
    CUser& usr = *puser;

    std::vector<std::string> colNames;
    // maybe bad memory sequence
    std::vector<CValue> keyVals;
    keyVals.resize(request->key_vals_size());

    for (int i = 0; i < request->col_names_size(); ++i) {
        colNames.push_back(std::move(request->col_names(i)));
    }

    for (int i = 0; i < request->key_vals_size(); ++i) {
        keyVals[i].resetData(request->key_vals(i).data(), request->key_vals(i).size());
    }

    bidx tabIdx = *(bidx*)request->table_handle().data();
    CCollection c(system->dataService->m_diskMan, system->dataService->m_page);
    c.loadFrom(tabIdx);

    // init iter
    CCollection::CIdxIter iter;
    
    rc = c.getIdxIter(colNames, keyVals, iter);
    if (rc != 0) {
        system->log.log_error("Failed to get index iterator for table: %s.%s, rc=%d\n", std::to_string(tabIdx.gid).c_str(), std::to_string(tabIdx.bid).c_str(), rc);
        response->set_msg("Failed to get index iterator for table: " + std::to_string(tabIdx.gid) + "." + std::to_string(tabIdx.bid));
        response->set_rc(rc);
        return Status::OK;
    }

    usr.idxIterMap.emplace(usr.idxHandleCount, make_pair(std::move(c), std::move(iter)));

    response->set_hidx(usr.idxHandleCount);
    ++usr.idxHandleCount;
    #ifdef __DEBUG_GRPCSERVICE__
    response->set_msg("Get index iterator successfully for table: " + std::to_string(tabIdx.gid) + "." + std::to_string(tabIdx.bid));
    system->log.log_debug("Get index iterator successfully for table: %s.%s, handle count: %d\n", std::to_string(tabIdx.gid).c_str(), std::to_string(tabIdx.bid).c_str(), usr.idxHandleCount);
    #endif
    response->set_rc(0);

    

    return Status::OK;
}

Status sysCtlServiceImpl::ReleaseIdxIter(ServerContext* context, const dpfsgrpc::ReleaseIdxIterReq* request, dpfsgrpc::OperateReply* response) {
    int husr = request->husr();
    int hidx = request->hidx();

    CUser* puser = nullptr;
    int rc = getUserInfo(husr, puser);
    if (rc != 0) {
        response->set_msg("Failed to get user info for handle: " + std::to_string(husr));
        response->set_rc(rc);
        return Status::OK;
    }
    CUser& usr = *puser;

    auto iter = usr.idxIterMap.find(hidx);
    if (iter == usr.idxIterMap.end()) {
        response->set_msg("Index iterator handle not found: " + std::to_string(hidx));
        response->set_rc(-ENOENT);
        return Status::OK;
    }

    usr.idxIterMap.erase(iter);

    response->set_msg("Index iterator released successfully for handle: " + std::to_string(hidx));
    response->set_rc(0);
    return Status::OK;
}

Status sysCtlServiceImpl::FetchNextRowSets(ServerContext* context, const dpfsgrpc::FetchNextRowSetsReq* request, dpfsgrpc::FetchNextRowSetsReply* response) {

    int husr = request->husr();
    int hidx = request->hidx();
    int acquireRowNumber = request->acquire_row_number();



    CUser* puser = nullptr;
    int rc = getUserInfo(husr, puser);
    if (rc != 0) {
        response->set_msg("Failed to get user info for handle: " + std::to_string(husr));
        response->set_rc(rc);
        return Status::OK;
    }
    CUser& usr = *puser;

    system->log.log_debug("FetchNextRowSets called for user: %s, index iterator handle: %d, acquire row number: %d\n", usr.username.c_str(), hidx, acquireRowNumber);   

    auto iter = usr.idxIterMap.find(hidx);
    if (iter == usr.idxIterMap.end()) {
        system->log.log_error("Index iterator handle not found: %d for user: %s\n", hidx, usr.username.c_str());
        response->set_msg("Index iterator handle not found: " + std::to_string(hidx));
        response->set_rc(-ENOENT);
        return Status::OK;
    }

    CCollection& collection = iter->second.first;
    CCollection::CIdxIter& idxIter = iter->second.second;

    cacheLocker cl(collection.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard guard(cl);
    if (guard.returnCode() != 0) {
        system->log.log_error("Failed to acquire read lock on collection info cache for index iterator handle: %d, rc=%d\n", hidx, guard.returnCode());
        response->set_msg("Failed to acquire read lock on collection info cache for index iterator handle: " + std::to_string(hidx));
        response->set_rc(-EAGAIN);
        return Status::OK;
    }

    CCollection::collectionStruct cs(collection.m_cltInfoCache->getPtr(), collection.m_cltInfoCache->getLen() * dpfs_lba_size);

    
    CItem rowData(cs.m_cols, 1);
    
    system->log.log_debug("Start fetching rows for index iterator handle: %d for user: %s\n", hidx, usr.username.c_str());

    for(int i = 0; i < acquireRowNumber; ++i) {
        system->log.log_debug("acq row number = %d for index iterator handle: %d for user: %s\n", i, hidx, usr.username.c_str());
        // get one row from disk
        rc = collection.getByIndexIter(idxIter, rowData);
        if (rc != 0) {
            if (rc == -EAGAIN) {
                system->log.log_notic("No more rows to fetch for index iterator handle: %d for user: %s\n", hidx, usr.username.c_str());
                response->set_msg("No more rows to fetch for index iterator handle: " + std::to_string(hidx));
                response->set_rc(0);
                return Status::OK;
            }
            system->log.log_notic("No more rows to fetch for index iterator handle: %d for user: %s\n", hidx, usr.username.c_str());
            response->set_rc(rc);
            response->set_msg("Failed to fetch next row sets for index iterator handle: " + std::to_string(hidx) + ", rc=" + std::to_string(rc));
            return Status::OK;
        }



        system->log.log_debug("rc = %d for fetching row number = %d for index iterator handle: %d for user: %s\n", rc, i, hidx, usr.username.c_str());

        printMemory(rowData.getPtr(), rowData.getRowLen());
        
        response->add_data(rowData.getPtr(), rowData.getRowLen());

        // switch to next row
        rc = ++idxIter;
        if (rc == -ENOENT) {
            system->log.log_debug("No more rows to fetch for index iterator handle: %d for user: %s\n", hidx, usr.username.c_str());
            response->set_msg("No more rows to fetch for index iterator handle: " + std::to_string(hidx));
            response->set_rc(ENODATA);
            return Status::OK;
        } else if (rc != 0) {
            system->log.log_error("Failed to move index iterator to next position for handle: %d for user: %s, rc=%d\n", hidx, usr.username.c_str(), rc);
            response->set_msg("Failed to move index iterator to next position for handle: " + std::to_string(hidx) + ", rc=" + std::to_string(rc));
            response->set_rc(rc);
            return Status::OK;
        }
    }
    
    response->set_rc(rc);
    response->set_msg("Fetched " + std::to_string(response->data_size()) + " rows for index iterator handle: " + std::to_string(hidx));
    return Status::OK;
}

Status sysCtlServiceImpl::CreateTracablePro(ServerContext* context, const dpfsgrpc::CreateTracableProReq* request, dpfsgrpc::CreateTracableProReply* response) {
/*
example:
create table manager.apple_PLKZB (
// 物料_prodinfo bidx, 物料表硬盘地址
name char(32) not null primary key,   // 物料名称
bidx binary(16) NOT NULL
);

create table manager.apple_SPKZB(
uid int not null primary key,   // 唯一标识一件商品
ctime bigint not null,          // 上一次查验时间
cstate bool not null,           // 查验商品的状态，有效或失效
ccount int not null             // 查验次数
)                         


// table that log the transaction
// 商品交易表
create table manager.apple_SPJYB (
JYID BIGINT not null primary key,    // 交易id
SPJYQSID INT NOT NULL,               // 交易起始产品id
SPJYSL INT NOT NULL,                 // 交易数量
MFMC  CHAR(64) NOT NULL,             // 买方名称
MFDZ  CHAR(64) NOT NULL              // 买方地址
MFLX  CHAR(32) NOT NULL,             // 买方联系方式
FFMC  CHAR(64) NOT NULL,             // 卖方名称
FFDZ  CHAR(64) NOT NULL              // 卖方地址
FFLX  CHAR(32) NOT NULL,             // 卖方联系方式
PREV_JYID BIGINT NOT NULL,           // 上一笔交易的id
LOGISTICS_INFO CHAR(255) NOT NULL,   // 物流信息
OTHER_INFO CHAR(255) NOT NULL,       // 其他信息
FSJE  DECIMAL(16, 4)                 // 发生金额 
)

// contain base info of a production
// 商品信息表
create table manager.apple_SPXXB(key char(32), value char(128));
insert into manager.apple_SPXXB values
('商品名称', '苹果'), 
('品牌', '洛川苹果'), 
('保质期', '30'),
('质检报告', 'https://mytest.com/jsbg.png'),
('PLKZB', {gid, bid}),
('SPKZB', {gid, bid}),
('SPJYB', {gid, bid})
(其它自定义信息)
*/

/*
create table manager.apple_PLKZB
create table manager.apple_SPKZB               
create table manager.apple_SPJYB
create table manager.apple_SPXXB
*/

    std::string schema = request->schema_name();
    std::string productionName = request->structure_name();
    int32_t pNumber = request->total_production_num();

    const int tables = 4;

    std::string spxxbTableName = productionName + "_SPXXB"; // 商品信息表
    std::string plkzbTableName = productionName + "_PLKZB"; // 配料控制表
    std::string spkzbTableName = productionName + "_SPKZB"; // 商品控制表
    std::string spjybTableName = productionName + "_SPJYB"; // 商品交易表

    int32_t husr = request->husr();

    // get user info from cache
    
    CUser* puser = nullptr;
    int rc = getUserInfo(husr, puser);
    if (rc != 0) {
        response->set_msg("Failed to get user info for handle: " + std::to_string(husr));
        response->set_rc(rc);
        return Status::OK;
    }

    CUser& usr = *puser;

    // size_t ingredientCount = request->ingredient_info_size();
    bidx ingredientBidx;
    bidx spkzbBidx;

    CCollection spxxb(system->dataService->m_diskMan, system->dataService->m_page);
    CItem* itm = nullptr;
    CPointerGuard pg(itm);

    CParser parser(usr, *system->dataService);
    for (int i = 0; i < tables; ++i) {
        std::string sql = "";
        if (i == 0) {
            // create table manager.apple_SPXXB(key char(32), value char(128));
            // insert into manager.apple_SPXXB values
            // ('PLKZB', {gid, bid}),
            // ('SPKZB', {gid, bid}),
            // ('SPJYB', {gid, bid})
            // ('商品名称', '苹果'), 
            // ('品牌', '洛川苹果'), 
            // ('保质期', '30'),
            // ('质检报告', 'https://mytest.com/jsbg.png'),
            // (其它自定义信息)
            sql = "create table " + schema + "." + spxxbTableName + SPXXBDEF;
        } else if (i == 1) {
            // create table manager.apple_PLKZB (
            // bidx binary(16) NOT NULL PRIMARY KEY
            // );
            sql = "create table " + schema + "." + plkzbTableName + PLKZBDEF;
        } else if (i == 2) {
            // create table manager.apple_SPJYB (
            // JYID BIGINT not null primary key,    // 交易id
            // SPJYQSID INT NOT NULL,               // 交易起始产品id
            // SPJYSL INT NOT NULL,                 // 交易数量
            // MFMC  CHAR(64) NOT NULL,             // 买方名称
            // MFDZ  CHAR(64) NOT NULL              // 买方地址
            // MFLX  CHAR(32) NOT NULL,             // 买方联系方式
            // FFMC  CHAR(64) NOT NULL,             // 卖方名称
            // FFDZ  CHAR(64) NOT NULL              // 卖方地址
            // FFLX  CHAR(32) NOT NULL,             // 卖方联系方式
            // PREV_JYID BIGINT NOT NULL,           // 上一笔交易的id
            // LOGISTICS_INFO CHAR(255) NOT NULL,   // 物流信息
            // OTHER_INFO CHAR(255) NOT NULL,       // 其他信息
            // FSJE  DECIMAL(16, 4)                 // 发生金额
            // )
            sql = "create table " + schema + "." + spjybTableName + SPJYBDEF;
        } else if (i == 3) {
            // create table manager.apple_SPKZB(
            // uid int not null primary key,   // 唯一标识一件商品
            // ctime bigint not null,          // 上一次查验时间
            // cstate bool not null,           // 查验商品的状态，有效或失效
            // ccount int not null             // 查验次数
            // ltrade bigint not null,         // 上一笔交易id
            // )         
            sql = "create table " + schema + "." + spkzbTableName + SPKZBDEF;
        }
            
        rc = parser(sql);
        if (rc != 0) {
            response->set_msg("Failed to parse and build execution plan for SQL: " + sql);
            response->set_rc(rc);
            return Status::OK;
        }
        CPlanHandle out(system->dataService->m_page, system->dataService->m_diskMan);

        rc = parser.buildPlan(sql, out);
        if (rc != 0) {
            // TODO :: undo previous created tables if any
            response->set_msg("Failed to build execution plan for SQL: " + sql);
            response->set_rc(rc);
            return Status::OK;
        } 

        if (i == 0) {
            // get bid of spxxb
            rc = spxxb.loadFrom(out.plan.planObjects[0].collectionBidx, true);
            if (rc != 0) {
                response->set_msg("Failed to load collection for table: " + schema + "." + spxxbTableName);
                response->set_rc(rc);
                return Status::OK;
            }
            
            response->set_trace_code_prefix((char*)&out.plan.planObjects[0].collectionBidx, sizeof(bidx));

            cacheLocker cl(spxxb.m_cltInfoCache, system->dataService->m_page);
            CTemplateReadGuard guard(cl);
            if (guard.returnCode() != 0) {
                system->log.log_error("Failed to acquire read lock on collection info cache for table: %s.%s, rc=%d\n", schema.c_str(), spxxbTableName.c_str(), guard.returnCode());
                response->set_msg("Failed to acquire read lock on collection info cache for table: " + schema + "." + spxxbTableName);
                response->set_rc(-EAGAIN);
                return Status::OK;
            }

            CCollection::collectionStruct cs(spxxb.m_cltInfoCache->getPtr(), spxxb.m_cltInfoCache->getLen() * dpfs_lba_size);
            itm = new CItem(cs.m_cols, request->base_info().size() + 3);
            

        } else if (i == 1) {
            // get bid of plkzb
            bidx tidx = out.plan.planObjects[0].collectionBidx;
            ingredientBidx = tidx;
            CValue val;
            // first data, no need to switch to next row
            val.resetData("PLKZB", 6);
            itm->updateValue(0, val);
            val.resetData(&tidx, sizeof(tidx));
            itm->updateValue(1, val);
            
        } else if (i == 2) {
            // get bid of spjyb
            bidx tidx = out.plan.planObjects[0].collectionBidx;
            CValue val;
            itm->nextRow();
            val.resetData("SPJYB", 6);
            itm->updateValue(0, val);
            val.resetData(&tidx, sizeof(tidx));
            itm->updateValue(1, val);
        } else if (i == 3) {
            // get bid of spkzb
            bidx tidx = out.plan.planObjects[0].collectionBidx;
            spkzbBidx = tidx;
            CValue val;
            itm->nextRow();
            val.resetData("SPKZB", 6);
            itm->updateValue(0, val);
            val.resetData(&tidx, sizeof(tidx));
            itm->updateValue(1, val);
        }
    }

    CValue val;
    // init info table
    for (const auto& it : request->base_info()) {

        #ifdef __DEBUG_GRPCSERVICE__
        system->log.log_debug("Inserting base info into SPXXB table, key: %s, value: %s\n", it.first.c_str(), it.second.c_str());
        #endif

        // Initialize each table based on the base info
        val.resetData(it.first.data(), it.first.size());
        itm->nextRow();
        itm->updateValue(0, val);
        val.resetData(it.second.data(), it.second.size());
        itm->updateValue(1, val);

    }

    rc = spxxb.addItem(*itm);
    if (rc != 0) {
        // TODO :: UNDO previous inserted base info if any
        response->set_msg("Failed to insert base info into SPXXB table");
        response->set_rc(rc);
        return Status::OK;
    }
    
    // init ingredient table
    CCollection ingredient(system->dataService->m_diskMan, system->dataService->m_page);
    rc = ingredient.loadFrom(ingredientBidx, true);
    if (rc != 0) {
        response->set_msg("Failed to load collection for ingredient table");
        response->set_rc(rc);
        return Status::OK;
    }
    cacheLocker cl(ingredient.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard ingGuard(cl);
    if (ingGuard.returnCode() != 0) {
        system->log.log_error("Failed to acquire read lock on collection info cache for ingredient table, rc=%d\n", ingGuard.returnCode());
        response->set_msg("Failed to acquire read lock on collection info cache for ingredient table");
        response->set_rc(-EAGAIN);
        return Status::OK;
    }
    CCollection::collectionStruct cs(ingredient.m_cltInfoCache->getPtr(), ingredient.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem itmIngre(cs.m_cols, request->ingredient_names().size());
    ingGuard.release();

    // init ingradient table
    bool theFirst = true;
    for (const auto& it : request->ingredient_names()) {
        val.resetData(it.data(), it.size());
        
        if (!theFirst) {
            itmIngre.nextRow();
        } else {
            theFirst = false;
        }

        itmIngre.updateValue(0, val);
        
        #ifdef __DEBUG_GRPCSERVICE__
        system->log.log_debug("Inserting ingredient name into ingredient table, name: %s\n", it.c_str());
        #endif

        // search in system table. find the bidx of the ingredient table, and insert the ingredient names into the table
        // test data
        bidx testidx = {0, 7890};
        val.resetData(&testidx, sizeof(testidx));
        itmIngre.updateValue(1, val);
    }

    rc = ingredient.addItem(itmIngre);
    if (rc != 0) {
        response->set_msg("Failed to insert ingredient info into ingredient table");
        response->set_rc(rc);
        return Status::OK;
    }

    // init product control table
    CCollection spzkb(system->dataService->m_diskMan, system->dataService->m_page);
    rc = spzkb.loadFrom(spkzbBidx, true);
    if (rc != 0) {
        response->set_msg("Failed to load collection for product control table");
        response->set_rc(rc);
        return Status::OK;
    }

    cacheLocker clSpkzb(spzkb.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard spkzbGuard(clSpkzb);
    if (spkzbGuard.returnCode() != 0) {
        system->log.log_error("Failed to acquire read lock on collection info cache for product control table, rc=%d\n", spkzbGuard.returnCode());
        response->set_msg("Failed to acquire read lock on collection info cache for product control table");
        response->set_rc(-EAGAIN);
        return Status::OK;
    }
    CCollection::collectionStruct spzkbCs(spzkb.m_cltInfoCache->getPtr(), spzkb.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem itmSpkzb(spzkbCs.m_cols);
    spkzbGuard.release();

    /*
        uid int not null primary key, \
        ctime bigint not null, \
        cstate bool not null, \
        ccount int not null, \
        ltrade bigint not null, \
        )";
    */
    int64_t initTime = 0;
    bool initCheckState = true;
    int32_t initCheckCount = 0;
    int64_t initLTrade = -1; // means no trade yet
    itmSpkzb.updateValue(1, &initTime, sizeof(initTime));
    itmSpkzb.updateValue(2, &initCheckState, sizeof(initCheckState));
    itmSpkzb.updateValue(3, &initCheckCount, sizeof(initCheckCount));
    itmSpkzb.updateValue(4, &initLTrade, sizeof(initLTrade));

    for (int i = 0; i < pNumber; ++i) {
        itmSpkzb.updateValue(0, &i, sizeof(i));
        rc = spzkb.addItem(itmSpkzb);
        if (rc != 0) {
            response->set_msg("Failed to insert initial data into product control table");
            response->set_rc(rc);
            return Status::OK;
        }
    }
    
    // insert the traceable info into systracetable;
    
    cacheLocker clTraceable(system->dataService->m_sysSchema->systraceables.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard traceableGuard(clTraceable);
    if (traceableGuard.returnCode() != 0) {
        system->log.log_error("Failed to acquire read lock on collection info cache for systraceables table, rc=%d\n", traceableGuard.returnCode());
        response->set_msg("Failed to acquire read lock on collection info cache for systraceables table");
        response->set_rc(-EAGAIN);
        return Status::OK;
    }
    CCollection::collectionStruct traceableCs(system->dataService->m_sysSchema->systraceables.m_cltInfoCache->getPtr(), system->dataService->m_sysSchema->systraceables.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem itmTraceable(traceableCs.m_cols);
    traceableGuard.release();
/*
"ROOT",               dpfs_datatype_t::TYPE_BINARY,    16,  0, cf::NOT_NULL | cf::PRIMARY_KEY);
"NAME",               dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL);                  
"SCHEMA",             dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL);                  
*/
    rc = itmTraceable.updateValue(0, &spxxb.m_collectionBid, sizeof(spxxb.m_collectionBid)); if (rc < 0) { system->log.log_error("Fail to update systab\n"); response->set_rc(-EAGAIN); return Status::OK; }
    rc = itmTraceable.updateValue(1, productionName.data(), productionName.size());          if (rc < 0) { system->log.log_error("Fail to update systab\n"); response->set_rc(-EAGAIN); return Status::OK; }
    rc = itmTraceable.updateValue(2, schema.data(), schema.size());                          if (rc < 0) { system->log.log_error("Fail to update systab\n"); response->set_rc(-EAGAIN); return Status::OK; }

    rc = system->dataService->m_sysSchema->systraceables.addItem(itmTraceable);
    if (rc != 0) {
        system->log.log_error("Failed to insert traceable info into systracetable, rc=%d\n", rc);
        response->set_msg("Failed to insert traceable info into systracetable");
        response->set_rc(rc);
        return Status::OK;
    }

    response->set_msg("command completed successfully.");
    response->set_rc(0);
    return Status::OK;

}

Status sysCtlServiceImpl::TraceBack(ServerContext* context, const dpfsgrpc::TraceBackReq* request, dpfsgrpc::TraceBackReply* response) {

    /*
        trackcoide = |BIDX|productionID|
        productionTable => productionID => last tradeId
        productionTable => gradientId
        productionTable => other infos
        last tracId => prev TradId
        BIDX -> table collection -> get row by pk -> get row data -> return to client
    */
    int32_t husr = request->husr();
    bidx spxxbBidx = *(bidx*)request->trace_code().data();
    int32_t productionId = *(int32_t*)(request->trace_code().data() + sizeof(bidx));
    std::string result = "";
    int rc = 0;

    // check if the trace Table exists.
    cacheLocker clTraceable(system->dataService->m_sysSchema->systraceables.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard traceableGuard(clTraceable);
    if (traceableGuard.returnCode() != 0) {
        system->log.log_error("Failed to acquire read lock on collection info cache for systraceables table, rc=%d\n", traceableGuard.returnCode());
        response->set_msg("Failed to acquire read lock on collection info cache for systraceables table");
        response->set_rc(-EAGAIN);
        return Status::OK;
    }
    CCollection::collectionStruct traceableCs(system->dataService->m_sysSchema->systraceables.m_cltInfoCache->getPtr(), system->dataService->m_sysSchema->systraceables.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem itmTraceable(traceableCs.m_cols);
    traceableGuard.release();

    KEY_T tcb(&spxxbBidx, sizeof(bidx), system->dataService->m_sysSchema->systraceables.m_cmpTyps);
    rc = system->dataService->m_sysSchema->systraceables.getRow(tcb, &itmTraceable);
    if (rc != 0) {
        system->log.log_error("Trace code not found in systraceables table, bidx: (%d, %d), rc=%d\n", spxxbBidx.gid, spxxbBidx.bid, rc);
        response->set_msg("Trace code not found in systraceables table, bidx: (" + std::to_string(spxxbBidx.gid) + ", " + std::to_string(spxxbBidx.bid) + ")");
        response->set_rc(rc);
        return Status::OK;
    }

    
    system->log.log_debug("TraceBack called for user handle: %d, trace code: (bidx: (%d, %d), productionId: %d)\n", husr, spxxbBidx.gid, spxxbBidx.bid, productionId);
    CCollection spxxb(system->dataService->m_diskMan, system->dataService->m_page);
    rc = spxxb.loadFrom(spxxbBidx, true);
    if (rc != 0) {
        system->log.log_error("Failed to load collection for trace back, bidx: (%d, %d), rc=%d\n", spxxbBidx.gid, spxxbBidx.bid, rc);
        response->set_msg("Failed to load collection for trace back, bidx: (" + std::to_string(spxxbBidx.gid) + ", " + std::to_string(spxxbBidx.bid) + ")");
        response->set_rc(rc);
        return Status::OK;
    }

    cacheLocker cl(spxxb.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard guard(cl);
    if (guard.returnCode() != 0) {
        system->log.log_error("Failed to acquire read lock on collection info cache for trace back, bidx: (%d, %d), rc=%d\n", spxxbBidx.gid, spxxbBidx.bid, guard.returnCode());
        response->set_msg("Failed to acquire read lock on collection info cache for trace back, bidx: (" + std::to_string(spxxbBidx.gid) + ", " + std::to_string(spxxbBidx.bid) + ")");
        response->set_rc(-EAGAIN);
        return Status::OK;
    }

    CCollection::collectionStruct cs(spxxb.m_cltInfoCache->getPtr(), spxxb.m_cltInfoCache->getLen() * dpfs_lba_size);

    CItem itm(cs.m_cols);
    
    guard.release();

    CCollection::CIdxIter colliter;
    rc = spxxb.getScanIter(colliter);
    if (rc != 0) {
        system->log.log_error("Failed to get scan iterator for trace back, bidx: (%d, %d), rc=%d\n", spxxbBidx.gid, spxxbBidx.bid, rc);
        response->set_msg("Failed to get scan iterator for trace back, bidx: (" + std::to_string(spxxbBidx.gid) + ", " + std::to_string(spxxbBidx.bid) + ")");
        response->set_rc(rc);
        return Status::OK;
    }

    // 交易控制表的查询操作在循环后执行
    bidx spjybBidx {0, 0};
    bidx plkzbBidx {0, 0};
    bidx spkzbBidx {0, 0};
    while (1) {
        rc = spxxb.getByScanIter(colliter, itm);
        if (rc != 0) {
            system->log.log_error("Failed to get row by scan iterator for trace back, bidx: (%d, %d), rc=%d\n", spxxbBidx.gid, spxxbBidx.bid, rc);
            response->set_msg("Failed to get row by scan iterator for trace back, bidx: (" + std::to_string(spxxbBidx.gid) + ", " + std::to_string(spxxbBidx.bid) + ")");
            response->set_rc(rc);
            return Status::OK;
        }

        CValue name = itm.getValue(0);
        CValue value = itm.getValue(1);

        std::string nameStr(name.data, name.len);
        // std::string valueStr(value.data, value.len);
        
        // TODO:: 
        if (memcmp(name.data, "SPKZB", 6) == 0) {
            // get spkzb bidx
            memcpy(&spkzbBidx, value.data, sizeof(bidx));
            system->log.log_debug("Found SPKZB in SPXXB table for trace back, spkzb bidx: (%d, %d)\n", spkzbBidx.gid, spkzbBidx.bid);

            // 查询商品信息，返回查验状态

            // break;
        } else if (memcmp(name.data, "SPJYB", 6) == 0) {
            // get spjyb bidx
            memcpy(&spjybBidx, value.data, sizeof(bidx));
            system->log.log_debug("Found SPJYB in SPXXB table for trace back, spjyb bidx: (%d, %d)\n", spjybBidx.gid, spjybBidx.bid);
            // 查询交易信息，返回交易记录， 在此处不执行。查找交易记录的执行必须在查找SPKZB后。

        } else if (memcmp(name.data, "PLKZB", 6) == 0) {
            // get plkzb bidx
            memcpy(&plkzbBidx, value.data, sizeof(bidx));
            system->log.log_debug("Found PLKZB in SPXXB table for trace back, plkzb bidx: (%d, %d)\n", plkzbBidx.gid, plkzbBidx.bid);
        } else {
            std::string valueStr(value.data);
            system->log.log_debug("Found base info in SPXXB table for trace back, key: %s, value: %s\n", nameStr.c_str(), valueStr.c_str());
            result += nameStr + ": " + valueStr + "\n";
        }
        
        rc = ++colliter;
        if (rc == -ENOENT) {
            system->log.log_debug("Scan iterator reached end of collection for trace back, bidx: (%d, %d)\n", spxxbBidx.gid, spxxbBidx.bid);
            break;
        } else if (rc != 0) {
            system->log.log_error("Failed to move scan iterator to next position for trace back, bidx: (%d, %d), rc=%d\n", spxxbBidx.gid, spxxbBidx.bid, rc);
            response->set_msg("Failed to move scan iterator to next position for trace back, bidx: (" + std::to_string(spxxbBidx.gid) + ", " + std::to_string(spxxbBidx.bid) + ")");
            response->set_rc(rc);
            return Status::OK;
        }
    }

    if (spkzbBidx.bid == 0 || plkzbBidx.bid == 0 || spjybBidx.bid == 0) {
        system->log.log_error("Failed to find necessary table bidx in SPXXB for trace back, SPKZB bidx: (%d, %d), PLKZB bidx: (%d, %d), SPJYB bidx: (%d, %d)\n", spkzbBidx.gid, spkzbBidx.bid, plkzbBidx.gid, plkzbBidx.bid, spjybBidx.gid, spjybBidx.bid);
        response->set_msg("Failed to find necessary table bidx in SPXXB for trace back, SPKZB bidx: (" + std::to_string(spkzbBidx.gid) + ", " + std::to_string(spkzbBidx.bid) + "), PLKZB bidx: (" + std::to_string(plkzbBidx.gid) + ", " + std::to_string(plkzbBidx.bid) + "), SPJYB bidx: (" + std::to_string(spjybBidx.gid) + ", " + std::to_string(spjybBidx.bid) + ")");
        response->set_rc(-ENOENT);
        return Status::OK;
    }

    system->log.log_debug("get bidx for tables \nSPKZB: (%d, %d), SPJYB: (%d, %d), PLKZB: (%d, %d)\n", spkzbBidx.gid, spkzbBidx.bid, spjybBidx.gid, spjybBidx.bid, plkzbBidx.gid, plkzbBidx.bid);

    // TODO::

    // 查找商品控制表中的记录
    CCollection spkzb(system->dataService->m_diskMan, system->dataService->m_page);
    rc = spkzb.loadFrom(spkzbBidx, true);
    if (rc != 0) {
        system->log.log_error("Failed to load collection for product control table in trace back, bidx: (%d, %d), rc=%d\n", spkzbBidx.gid, spkzbBidx.bid, rc);
        response->set_msg("Failed to load collection for product control table in trace back, bidx: (" + std::to_string(spkzbBidx.gid) + ", " + std::to_string(spkzbBidx.bid) + ")");
        response->set_rc(rc);
        return Status::OK;
    }

    cacheLocker clspkzb(spkzb.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard gspkzb(clspkzb);
    if (gspkzb.returnCode() != 0) {
        system->log.log_error("Failed to acquire read lock on collection info cache for trace back, bidx: (%d, %d), rc=%d\n", spkzbBidx.gid, spkzbBidx.bid, gspkzb.returnCode());
        response->set_msg("Failed to acquire read lock on collection info cache for trace back, bidx: (" + std::to_string(spkzbBidx.gid) + ", " + std::to_string(spkzbBidx.bid) + ")");
        response->set_rc(-EAGAIN);
        return Status::OK;
    }
    CCollection::collectionStruct cskzb(spkzb.m_cltInfoCache->getPtr(), spkzb.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem itmkzb(cskzb.m_cols);
    gspkzb.release();


    KEY_T kspkzb(&productionId, sizeof(productionId), spkzb.m_cmpTyps);

    rc = spkzb.getRow(kspkzb, &itmkzb);
    if (rc != 0) {
        system->log.log_error("Failed to get row by primary key for product control table in trace back, bidx: (%d, %d), rc=%d\n", spkzbBidx.gid, spkzbBidx.bid, rc);
        response->set_msg("Failed to get row by primary key for product control table in trace back, bidx: (" + std::to_string(spkzbBidx.gid) + ", " + std::to_string(spkzbBidx.bid) + ")");
        response->set_rc(rc);
        return Status::OK;
    }



    /*
        uid int not null primary key, \
        ctime bigint not null, \
        cstate bool not null, \
        ccount int not null, \
        ltrade bigint not null, \
        )";
    */
    result += "uid/产品编号:         " + std::to_string(productionId) + "\n";
    result += "ctime/上一次校验时间  " + std::to_string(*(int64_t*)itmkzb.getValue(1).data) + "\n";
    result += "cstate/校验状态:      " + std::to_string(*(bool*)itmkzb.getValue(2).data) + "\n";
    result += "ccount/校验次数:      " + std::to_string(*(int32_t*)itmkzb.getValue(3).data) + "\n";
    int64_t lastTradeId = *(int64_t*)itmkzb.getValue(4).data;

    time_t lt = time(nullptr);

    int32_t ccount = *(int32_t*)itmkzb.getValue(3).data;
    ccount += 1;
    itmkzb.updateValue(1, &lt, sizeof(int64_t));
    itmkzb.updateValue(3, &ccount, sizeof(int32_t));
    // update checkout status
    rc = spkzb.updateRow(kspkzb, &itmkzb);
    if (rc != 0) {
        system->log.log_error("Failed to update row for product control table in trace back, bidx: (%d, %d), rc=%d\n", spkzbBidx.gid, spkzbBidx.bid, rc);
        response->set_msg("Failed to update row for product control table in trace back, bidx: (" + std::to_string(spkzbBidx.gid) + ", " + std::to_string(spkzbBidx.bid) + ")");
        response->set_rc(rc);
        return Status::OK;
    }

    if (lastTradeId == -1) {
        result += "lastTradeId: trade info not found\n";
    } else {
        result += "lastTradeId: " + std::to_string(lastTradeId) + "\n";

        if (request->show_detail()) {
            // 根据商品控制表中交易id的记录，查找交易链
            rc = GetTraceTradeDetail(spjybBidx, lastTradeId, result);
            if (rc != 0) {
                system->log.log_error("Failed to get trace trade detail for trace back, bidx: (%d, %d), tradeId: %d, rc=%d\n", spjybBidx.gid, spjybBidx.bid, lastTradeId, rc);
                response->set_msg("Failed to get trace trade detail for trace back, bidx: (" + std::to_string(spjybBidx.gid) + ", " + std::to_string(spjybBidx.bid) + "), tradeId: " + std::to_string(lastTradeId));
                response->set_rc(rc);
                return Status::OK;
            }
        }
    }

    // 查找配料表中的记录
    rc = GetIngredientInfo(plkzbBidx, productionId, result);
    if (rc != 0) {
        system->log.log_error("Failed to get ingredient info for trace back, bidx: (%d, %d), productionId: %d, rc=%d\n", plkzbBidx.gid, plkzbBidx.bid, productionId, rc);
        response->set_msg("Failed to get ingredient info for trace back, bidx: (" + std::to_string(plkzbBidx.gid) + ", " + std::to_string(plkzbBidx.bid) + "), productionId: " + std::to_string(productionId));
        response->set_rc(rc);
        return Status::OK;
    }

    response->set_trace_back_result(result);
    response->set_msg("Trace back completed successfully.");
    response->set_rc(0);

    return Status::OK;
}

int sysCtlServiceImpl::GetTraceTradeDetail(const bidx &bidx, int64_t tradeId, std::string& result) noexcept {
    int rc = 0;
    int fsjeCvtLen = 0;
    int fsjeCvtScale = 0;
    CCollection spjyb(system->dataService->m_diskMan, system->dataService->m_page);
    rc = spjyb.loadFrom(bidx, true);
    if (rc != 0) {
        system->log.log_error("Failed to load collection for trade record table in trace back, bidx: (%d, %d), rc=%d\n", bidx.gid, bidx.bid, rc);
        return -1;
    }


    cacheLocker clspjyb(spjyb.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard gspjyb(clspjyb);
    if (gspjyb.returnCode() != 0) {
        return gspjyb.returnCode();
    }

    CCollection::collectionStruct csjyb(spjyb.m_cltInfoCache->getPtr(), spjyb.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem itmjyb(csjyb.m_cols);
    fsjeCvtLen = csjyb.m_cols[12].getDds().genLen;
    fsjeCvtScale = csjyb.m_cols[12].getScale();
    gspjyb.release();

    char kData[sizeof(tradeId)];
    KEY_T kjyb(kData, sizeof(tradeId), spjyb.m_cmpTyps);
    memcpy(kData, &tradeId, sizeof(tradeId));

    int32_t traceDeep = 0;

    while ((*(int64_t*)kjyb.data) >= 0) {
        rc = spjyb.getRow(kjyb, &itmjyb);
        if (rc != 0) {
            if (rc == -ENOENT) {
                system->log.log_debug("No more trade records found for trace back, lastTradeId: %lld\n", (*(int64_t*)kjyb.data));
                break;
            }
            return rc;
        }
        /*
            JYID BIGINT not null primary key, 
            SPJYQSID INT NOT NULL, 
            SPJYSL INT NOT NULL, 
            MFMC CHAR(64) NOT NULL, 
            MFDZ CHAR(64) NOT NULL, 
            MFLX CHAR(32) NOT NULL, 
            FFMC CHAR(64) NOT NULL, 
            FFDZ CHAR(64) NOT NULL, 
            FFLX CHAR(32) NOT NULL, 
            PREV_JYID BIGINT NOT NULL, 
            LOGISTICS_INFO CHAR(255) NOT NULL, 
            OTHER_INFO CHAR(255) NOT NULL
        */
        CValue JYID = itmjyb.getValue(0);
        CValue SPJYQSID = itmjyb.getValue(1);
        CValue SPJYSL = itmjyb.getValue(2);
        CValue MFMC = itmjyb.getValue(3);
        CValue MFDZ = itmjyb.getValue(4);
        CValue MFLX = itmjyb.getValue(5);
        CValue FFMC = itmjyb.getValue(6);
        CValue FFDZ = itmjyb.getValue(7);
        CValue FFLX = itmjyb.getValue(8);
        CValue PREV_JYID = itmjyb.getValue(9);
        CValue LOGISTICS_INFO = itmjyb.getValue(10);
        CValue OTHER_INFO = itmjyb.getValue(11);
        CValue FSJE = itmjyb.getValue(12);

        result += "trade deep : " + std::to_string(traceDeep) + "\n";
        result += "JYID: " + std::to_string(*(int64_t*)JYID.data) + "\n";
        result += "SPJYQSID: " + std::to_string(*(int32_t*)SPJYQSID.data) + "\n";
        result += "SPJYSL: " + std::to_string(*(int32_t*)SPJYSL.data) + "\n";
        result += "MFMC: " + std::string(MFMC.data, MFMC.len) + "\n";
        result += "MFDZ: " + std::string(MFDZ.data, MFDZ.len) + "\n";
        result += "MFLX: " + std::string(MFLX.data, MFLX.len) + "\n";
        result += "FFMC: " + std::string(FFMC.data, FFMC.len) + "\n";
        result += "FFDZ: " + std::string(FFDZ.data, FFDZ.len) + "\n";
        result += "FFLX: " + std::string(FFLX.data, FFLX.len) + "\n";
        result += "LOGISTICS_INFO: " + std::string(LOGISTICS_INFO.data, LOGISTICS_INFO.len) + "\n";
        result += "OTHER_INFO: " + std::string(OTHER_INFO.data, OTHER_INFO.len) + "\n";

        my_decimal dec;
        rc = binary2my_decimal(0, (unsigned char*)FSJE.data, &dec, fsjeCvtLen, fsjeCvtScale);
        if (rc != 0) {
            result += "FSJE: \"error! convert fail.\"\n";
        } else {
            String myDecStr;
            rc = my_decimal2string(0, &dec, &myDecStr);
            if (rc != 0) {
                result += "FSJE: \"error! convert fail.\"\n";
            } else {
                result += "FSJE: " + std::string(myDecStr.ptr()) + "\n";
            }
        }

        system->log.log_debug("Trace trade detail for trace back, tradeId: %lld, JYID: %lld, SPJYQSID: %d, SPJYSL: %d\n", tradeId, *(int64_t*)JYID.data, *(int32_t*)SPJYQSID.data, *(int32_t*)SPJYSL.data);
        system->log.log_debug("PREV_JYID for trace back: %d\n", *(int64_t*)PREV_JYID.data);
        memcpy(kjyb.data, PREV_JYID.data, sizeof(tradeId));
        traceDeep++;
    }

    return 0;
}

int sysCtlServiceImpl::GetIngredientInfo(const bidx &bidx, int64_t traceId, std::string& result) noexcept {
    int rc = 0;
    CCollection plkzb(system->dataService->m_diskMan, system->dataService->m_page);
    rc = plkzb.loadFrom(bidx, true);
    if (rc != 0) {
        system->log.log_error("Failed to load collection for trade record table in trace back, bidx: (%d, %d), rc=%d\n", bidx.gid, bidx.bid, rc);
        return -1;
    }


    cacheLocker clplkzb(plkzb.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard gplkzb(clplkzb);
    if (gplkzb.returnCode() != 0) {
        return gplkzb.returnCode();
    }

    CCollection::collectionStruct csplkzb(plkzb.m_cltInfoCache->getPtr(), plkzb.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem itmplkzb(csplkzb.m_cols);
    gplkzb.release();

    // scan the table
    CCollection::CIdxIter scanIter;
    rc = plkzb.getScanIter(scanIter);
    if (rc != 0) {
        system->log.log_error("Failed to get scan iterator for ingredient table in trace back, bidx: (%d, %d), rc=%d\n", bidx.gid, bidx.bid, rc);
        return rc;
    }

    
    while (1) {
        rc = plkzb.getByScanIter(scanIter, itmplkzb);
        if (rc != 0) {
            if (rc == -ENOENT) {
                system->log.log_debug("No more ingredient records found for trace back, bidx: (%d, %d)\n", bidx.gid, bidx.bid);
                break;
            }
            return rc;
        }

        CValue name = itmplkzb.getValue(0);
        CValue idx = itmplkzb.getValue(1);
/*
        name char(32) not null primary key,   // 物料名称
        bidx binary(16) NOT NULL
*/
        result += "Ingredient Name: " + std::string(name.data) + "\n";


        // |SPXXB BIDX(16B)|PRODUCTION ID(4B)|
        // constrict ingredient trace code for ingredient production 0.
        // if you need to trace the ingredient, you need to convert the hex string to binary and use it as the trace code to call TraceBack API.
        std::string hexTraceCode = toHexString(reinterpret_cast<uint8_t*>(idx.data), idx.len) + "00000000"; // append production id 0

        result += "Ingredient Trace Code: " + hexTraceCode + "\n";

        rc = ++scanIter;
        if (rc == -ENOENT) {
            break;
        } else if (rc != 0) {
            return rc;
        }
    }

    return 0;
}

Status sysCtlServiceImpl::MakeTrade(ServerContext* context, const dpfsgrpc::MakeTradeReq* request, dpfsgrpc::OperateReply* response) {

    int32_t husr = request->husr();
    
    CUser* puser = nullptr;
    int rc = getUserInfo(husr, puser);
    if (rc != 0) {
        response->set_msg("Failed to get user info for handle: " + std::to_string(husr));
        response->set_rc(rc);
        return Status::OK;
    }
    CUser& usr = *puser;
    

    const std::string& schemaName = request->schema_name();
    std::string tableName = request->structure_name() + "_SPXXB";


    rc = system->dataService->checkExist(schemaName, tableName);
    if (rc != -EEXIST) {
        response->set_msg("Table does not exist: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }

    rc = system->dataService->checkPrivilege(usr, schemaName, tableName, tbPrivilege::TBPRIVILEGE_SELECT);
    if (rc != 0) {
        response->set_msg("User does not have access privilege on table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }

    // from systables get table info
    bidx spxxbBidx;
    rc = system->dataService->getTableBidx(schemaName, tableName, spxxbBidx);
    if (rc != 0) {
        response->set_msg("Failed to get table bidx for table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }

    bidx spjybBidx = {0, 0};
    bidx spkzbBidx = {0, 0};

    // get splzn and spjyb from spxxb
    CCollection spxxb(system->dataService->m_diskMan, system->dataService->m_page);
    rc = spxxb.loadFrom(spxxbBidx, true);
    if (rc != 0) {
        response->set_msg("Failed to load collection for table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }

    // create table manager.apple_SPXXB(key char(32), value char(128));
    // insert into manager.apple_SPXXB values
    // ('PLKZB', {gid, bid}),
    // ('SPKZB', {gid, bid}),
    // ('SPJYB', {gid, bid})
    // ('商品名称', '苹果'), 
    // ('品牌', '洛川苹果'), 
    // ('保质期', '30'),
    // ('质检报告', 'https://mytest.com/jsbg.png'),
    // (其它自定义信息)
    cacheLocker spxxbcl(spxxb.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard spxxbGuard(spxxbcl);
    if (spxxbGuard.returnCode() != 0) {
        system->log.log_error("Failed to acquire read lock on collection info cache for table: %s.%s, rc=%d\n", schemaName.c_str(), tableName.c_str(), spxxbGuard.returnCode());
        response->set_msg("Failed to acquire read lock on collection info cache for table: " + schemaName + "." + tableName);
        response->set_rc(-EAGAIN);
        return Status::OK;
    }
    CCollection::collectionStruct spxxbCs(spxxb.m_cltInfoCache->getPtr(), spxxb.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem spxxbItm(spxxbCs.m_cols);
    spxxbGuard.release();

    char jybName[] = "SPJYB";
    KEY_T kjyb(jybName, sizeof(jybName), spxxb.m_cmpTyps);

    rc = spxxb.getRow(kjyb, &spxxbItm);
    if (rc != 0) {
        response->set_msg("Failed to get SPJYB row from SPXXB table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }
    spjybBidx = *(bidx*)spxxbItm.getValue(1).data;
    
    char kzbName[] = "SPKZB";
    KEY_T kkzb(kzbName, sizeof(kzbName), spxxb.m_cmpTyps);
    rc = spxxb.getRow(kkzb, &spxxbItm);
    if (rc != 0) {
        response->set_msg("Failed to get SPKZB row from SPXXB table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }
    spkzbBidx = *(bidx*)spxxbItm.getValue(1).data;

    /*
        bidx spjybBidx = {0, 0};
        bidx spkzbBidx = {0, 0};
    */

    if (spjybBidx.bid == 0 || spkzbBidx.bid == 0) {
        response->set_msg("Failed to get necessary table bidx from SPXXB table: " + schemaName + "." + tableName);
        response->set_rc(-ENOENT);
        return Status::OK;
    }

    #ifdef __DEBUG_GRPCSERVICE__
    system->log.log_debug("Got necessary table bidx from SPXXB table for MakeTrade, SPJYB bidx: (%d, %d), SPKZB bidx: (%d, %d)\n", spjybBidx.gid, spjybBidx.bid, spkzbBidx.gid, spkzbBidx.bid);
    #endif

    int32_t startUid = request->start_uid();
    int32_t num = request->num();

    // 根据商品控制表，找到对应商品的最后一笔交易ID，在交易表中插入一笔新的交易记录，更新商品控制表中的最后一笔交易ID.
    // 对于一批序号连续的商品来说，他们的最后一笔交易应属于同一笔交易

    /*
example:
PID         0 1 2 3 4 5 6 7 8 9 
lastID      1 1 1 1 2 2 2 3 3 4
nextTrade   5 5 5 6 7 7 7 8 8 9

    对于3号商品，由于被交易ID为1的交易买走，他可以参与交易序号为5的ID或任意其它交易，但是他不应该和商品4，5，6出现在同一笔交易中，所以他不应该参与交易ID为7的交易
    */

    CCollection spkzb(system->dataService->m_diskMan, system->dataService->m_page);
    rc = spkzb.loadFrom(spkzbBidx, true);
    if (rc != 0) {
        response->set_msg("Failed to load collection for product control table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }

    cacheLocker spkzbcl(spkzb.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard spkzbGuard(spkzbcl);
    if (spkzbGuard.returnCode() != 0) {
        system->log.log_error("Failed to acquire read lock on collection info cache for product control table, bidx: (%d, %d), rc=%d\n", spkzbBidx.gid, spkzbBidx.bid, spkzbGuard.returnCode());
        response->set_msg("Failed to acquire read lock on collection info cache for product control table: " + schemaName + "." + tableName);
        response->set_rc(-EAGAIN);
        return Status::OK;
    }
    CCollection::collectionStruct spkzbCs(spkzb.m_cltInfoCache->getPtr(), spkzb.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem itmspkzb(spkzbCs.m_cols);
    spkzbGuard.release();
    


    // create table manager.apple_SPKZB(
    // uid int not null primary key,   // 唯一标识一件商品
    // ctime bigint not null,          // 上一次查验时间
    // cstate bool not null,           // 查验商品的状态，有效或失效
    // ccount int not null             // 查验次数
    // ltrade bigint not null,         // 上一笔交易id
    // )

    int32_t startid = request->start_uid();
    int32_t limit = request->num();
    int32_t cvtLen = 0;
    int32_t cvtScale = 0;

    CCollection::CIdxIter spkzbIter;
    std::vector<std::string> cols;
    std::vector<CValue> uidByte;
    cols.emplace_back("uid");
    uidByte.emplace_back();
    uidByte[0].resetData(&startid, sizeof(startid));

    rc = spkzb.getIdxIter(cols, uidByte, spkzbIter);
    if (rc != 0) {
        system->log.log_error("Failed to get index iterator for product control table in trace back, bidx: (%d, %d), rc=%d\n", spkzbBidx.gid, spkzbBidx.bid, rc);
        response->set_msg("Failed to get index iterator for product control table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }

    rc = spkzb.getByIndexIter(spkzbIter, itmspkzb);
    if (rc != 0) {
        system->log.log_error("Failed to get row by index iterator for product control table in trace back, bidx: (%d, %d), rc=%d\n", spkzbBidx.gid, spkzbBidx.bid, rc);
        response->set_msg("Failed to get row by index iterator for product control table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }

    int64_t ltrade = *(int64_t*)itmspkzb.getValue(4).data;

    // use ltrade as prev_tradeId
    CCollection spjyb(system->dataService->m_diskMan, system->dataService->m_page);
    rc = spjyb.loadFrom(spjybBidx, true);
    if (rc != 0) {
        response->set_msg("Failed to load collection for trade record table: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }

    cacheLocker spjybcl(spjyb.m_cltInfoCache, system->dataService->m_page);
    CTemplateReadGuard spjybGuard(spjybcl);
    if (spjybGuard.returnCode() != 0) {
        system->log.log_error("Failed to acquire read lock on collection info cache for trade record table, bidx: (%d, %d), rc=%d\n", spjybBidx.gid, spjybBidx.bid, spjybGuard.returnCode());
        response->set_msg("Failed to acquire read lock on collection info cache for trade record table: " + schemaName + "." + tableName);
        response->set_rc(-EAGAIN);
        return Status::OK;
    }

    CCollection::collectionStruct spjybCs(spjyb.m_cltInfoCache->getPtr(), spjyb.m_cltInfoCache->getLen() * dpfs_lba_size);

    CItem itmspjyb(spjybCs.m_cols);
    cvtLen = spjybCs.m_cols[12].getDds().genLen;
    cvtScale = spjybCs.m_cols[12].getScale();
    spjybGuard.release();
    
    // create table manager.apple_SPJYB (
    // JYID BIGINT not null primary key,    // 交易id          0 
    // SPJYQSID INT NOT NULL,               // 交易起始产品id  1
    // SPJYSL INT NOT NULL,                 // 交易数量        2 
    // MFMC  CHAR(64) NOT NULL,             // 买方名称        3 
    // MFDZ  CHAR(64) NOT NULL              // 买方地址        4 
    // MFLX  CHAR(32) NOT NULL,             // 买方联系方式    5 
    // FFMC  CHAR(64) NOT NULL,             // 卖方名称        6 
    // FFDZ  CHAR(64) NOT NULL              // 卖方地址        7
    // FFLX  CHAR(32) NOT NULL,             // 卖方联系方式    8 
    // PREV_JYID BIGINT NOT NULL,           // 上一笔交易的id  9 
    // LOGISTICS_INFO CHAR(255) NOT NULL,   // 物流信息        10 
    // OTHER_INFO CHAR(255) NOT NULL,       // 其他信息        11 
    // FSJE DECIMAL(16, 4)                  // 发生金额        12 
    // )
    // get the first item's lastTradeId, update the trade infos, then update the productions.

    int64_t jyid = request->jyid();
    int32_t begin = request->start_uid();
    limit = request->num();

    my_decimal fsjeDecimal;
    const char* end = request->fsje().c_str() + request->fsje().size();
    rc = str2my_decimal(0, request->fsje().c_str(), &fsjeDecimal, &end);
    char decBinary[128];
    int binLen = 0;
    rc = my_decimal2binary(0, &fsjeDecimal, (uint8_t*)decBinary, cvtLen, cvtScale);
    if (rc != 0) {
        system->log.log_error("Failed to convert decimal to binary for trade record table in MakeTrade, rc=%d\n", rc);
        response->set_msg("Failed to convert decimal to binary for trade record table in MakeTrade");
        response->set_rc(rc);
        return Status::OK;
    }
    binLen = my_decimal_get_binary_size(cvtLen, cvtScale);

    rc = itmspjyb.updateValue(0, &jyid, sizeof(jyid));                                          if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(1, &begin, sizeof(begin));                                        if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(2, &limit, sizeof(limit));                                        if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(3, request->mfmc().data(), request->mfmc().size());               if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(4, request->mfdz().data(), request->mfdz().size());               if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(5, request->mflx().data(), request->mflx().size());               if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(6, request->ffmc().data(), request->ffmc().size());               if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(7, request->ffdz().data(), request->ffdz().size());               if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(8, request->fflx().data(), request->fflx().size());               if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(9, &ltrade, sizeof(ltrade));                                      if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(10, request->wlxx().data(), request->wlxx().size());              if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(11, request->others().data(), request->others().size());          if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }
    rc = itmspjyb.updateValue(12, decBinary, binLen);                                           if (rc < 0) { system->log.log_error("Failed to add item"); response->set_rc(rc); return Status::OK; }

    #ifdef __DEBUG_GRPCSERVICE__
    system->log.log_debug("prev trade id = {%lld, %lld}", 0, ltrade)
    #endif

    rc = spjyb.addItem(itmspjyb);
    if (rc != 0) {
        system->log.log_error("Failed to add item to trade record table for new trade, bidx: (%d, %d), rc=%d\n", spjybBidx.gid, spjybBidx.bid, rc);
        response->set_msg("Failed to add item to trade record table for new trade: " + schemaName + "." + tableName);
        response->set_rc(rc);
        return Status::OK;
    }


    spjyb.m_btreeIndex->printTree();


    int64_t tradeId = jyid;
    // update spkzb for the products in this trade, set their last trade id to the new trade id.

    while(limit) {
        rc = itmspkzb.updateValue(4, &tradeId, sizeof(tradeId)); 
        if (rc < 0) { 
            system->log.log_error("Failed to update item for product control table for new trade, bidx: (%d, %d), rc=%d\n", spkzbBidx.gid, spkzbBidx.bid, rc); 
            response->set_msg("Failed to update item for product control table for new trade: " + schemaName + "." + tableName); 
            response->set_rc(rc); 
            return Status::OK; 
        }

        rc = spkzb.updateByIter(spkzbIter, itmspkzb, 4);
        if (rc != 0) {
            system->log.log_error("Failed to update row by index iterator for product control table for new trade, bidx: (%d, %d), rc=%d\n", spkzbBidx.gid, spkzbBidx.bid, rc);
            response->set_msg("Failed to update row by index iterator for product control table for new trade: " + schemaName + "." + tableName);
            response->set_rc(rc);
            return Status::OK;
        }
        

        --limit;
        rc = ++spkzbIter;
        if (rc != 0) {
            if (rc == -ENOENT) {
                system->log.log_debug("No more products found for updating trade info in trace back, bidx: (%d, %d)\n", spkzbBidx.gid, spkzbBidx.bid);
                break;
            }
            system->log.log_error("Failed to move index iterator to next position for product control table in trace back, bidx: (%d, %d), rc=%d\n", spkzbBidx.gid, spkzbBidx.bid, rc);
            response->set_msg("Failed to move index iterator to next position for product control table: " + schemaName + "." + tableName);
            response->set_rc(rc);
            return Status::OK;
        }
    }


    response->set_rc(0);
    response->set_msg("Trade created successfully with trade ID: " + std::to_string(tradeId));



    return Status::OK;
}


