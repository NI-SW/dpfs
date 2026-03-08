#include <dpfssys/dpfsservice.hpp>
#include <dpfssys/dpfsdata.hpp>
#include <parser/dpfsparser.hpp>


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

Status sysCtlServiceImpl::CreateTracablePro(ServerContext* context, const dpfsgrpc::CreateTracableProReq* request, dpfsgrpc::OperateReply* response) {
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
OTHER_INFO CHAR(255) NOT NULL        // 其他信息
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
            sql = "create table " + schema + "." + spxxbTableName + "(name char(32) NOT NULL PRIMARY KEY, value char(128) NOT NULL)";
        } else if (i == 1) {
            // create table manager.apple_PLKZB (
            // bidx binary(16) NOT NULL PRIMARY KEY
            // );
            sql = "create table " + schema + "." + plkzbTableName + "(name char(64) NOT NULL PRIMARY KEY, bidx binary(16) NOT NULL)";
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
            // OTHER_INFO CHAR(255) NOT NULL        // 其他信息
            // )
            sql = "create table " + schema + "." + spjybTableName + "(\
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
                OTHER_INFO CHAR(255) NOT NULL)";
        } else if (i == 3) {
            // create table manager.apple_SPKZB(
            // uid int not null primary key,   // 唯一标识一件商品
            // ctime bigint not null,          // 上一次查验时间
            // cstate bool not null,           // 查验商品的状态，有效或失效
            // ccount int not null             // 查验次数
            // ltrade bigint not null,         // 上一笔交易id
            // )         
            sql = "create table " + schema + "." + spkzbTableName + "(\
            uid int not null primary key, \
            ctime bigint not null, \
            cstate bool not null, \
            ccount int not null)";
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
        response->set_msg("Failed to insert base info into SPXXB table");
        response->set_rc(rc);
        return Status::OK;
    }
    
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
        val.resetData("TEST", 4);
        itmIngre.updateValue(1, val);

    }
    
    rc = ingredient.addItem(itmIngre);
    if (rc != 0) {
        response->set_msg("Failed to insert ingredient info into ingredient table");
        response->set_rc(rc);
        return Status::OK;
    }


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
    

    response->set_msg("SQL command completed successfully.");
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
    
    system->log.log_debug("TraceBack called for user handle: %d, trace code: (bidx: (%d, %d), productionId: %d)\n", husr, spxxbBidx.gid, spxxbBidx.bid, productionId);
    CCollection spxxb(system->dataService->m_diskMan, system->dataService->m_page);
    int rc = spxxb.loadFrom(spxxbBidx, true);
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
        if (memcmp(nameStr.c_str(), "SPKZB", 6) == 0) {
            // get spkzb bidx
            memcpy(&spkzbBidx, value.data, sizeof(bidx));
            system->log.log_debug("Found SPKZB in SPXXB table for trace back, spkzb bidx: (%d, %d)\n", spkzbBidx.gid, spkzbBidx.bid);

            // 查询商品信息，返回查验状态

            // break;
        } else if (memcmp(nameStr.c_str(), "SPJYB", 6) == 0) {
            // get spjyb bidx
            memcpy(&spjybBidx, value.data, sizeof(bidx));
            system->log.log_debug("Found SPJYB in SPXXB table for trace back, spjyb bidx: (%d, %d)\n", spjybBidx.gid, spjybBidx.bid);
            // 查询交易信息，返回交易记录， 在此处不执行。查找交易记录的执行必须在查找SPKZB后。

        } else if (memcmp(nameStr.c_str(), "PLKZB", 6) == 0) {
            // get plkzb bidx
            memcpy(&plkzbBidx, value.data, sizeof(bidx));
            system->log.log_debug("Found PLKZB in SPXXB table for trace back, plkzb bidx: (%d, %d)\n", plkzbBidx.gid, plkzbBidx.bid);
        } else {
            std::string valueStr(value.data, value.len);
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

    system->log.log_debug("get bidx for tables \nSPKZB: (%d, %d), SPJYB: (%d, %d), PLKZB: (%d, %d)\n", spkzbBidx.gid, spkzbBidx.bid, spjybBidx.gid, spjybBidx.bid, plkzbBidx.gid, plkzbBidx.bid);

    // TODO::

    // 查找商品控制表中的记录

    // 查找配料表中的记录

    // 根据商品控制表中交易id的记录，查找交易链



    response->set_trace_back_result(result);
    response->set_msg("Trace back completed successfully.");
    response->set_rc(0);

    return Status::OK;
}





