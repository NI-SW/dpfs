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

        rc = ++idxIter;
        if (rc == -ENOENT) {
            system->log.log_debug("No more rows to fetch for index iterator handle: %d for user: %s\n", hidx, usr.username.c_str());
            response->set_msg("No more rows to fetch for index iterator handle: " + std::to_string(hidx));
            response->set_rc(0);
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
