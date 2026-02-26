#include <dpfssys/dpfsservice.hpp>



Status sysCtlServiceImpl::login(ServerContext* context, const dpfsgrpc::loginReq* request, dpfsgrpc::loginReply* response) {
    // Implement your login logic here
    // For demonstration, we will just set a dummy user handle
    // response->set_userid(12345);
    // response->set_username(request->username());
    int rc = 0;
    std::string usrName = request->username();
    std::string password = request->password();

    // check the user authentication.
    
    auto& sysUsrAuth = system->dataService->m_sysSchema->sysusers;
    KEY_T userName(const_cast<char*>(usrName.data()), usrName.size(), sysUsrAuth.m_cmpTyps);

    CTemplateReadGuard guard(*sysUsrAuth.m_cltInfoCache);
    if (guard.returnCode() != 0) {
        response->set_msg("Failed to acquire read lock on sysusers cache");
        response->set_husr(0);
        response->set_rc(-EAGAIN);
        return Status::OK;
    }
    
    CCollection::collectionStruct sysUserCs(sysUsrAuth.m_cltInfoCache->getPtr(), sysUsrAuth.m_cltInfoCache->getLen() * dpfs_lba_size);
    
    CItem rowData(sysUserCs.m_cols);
    rc = sysUsrAuth.getRow(userName, &rowData);
    if (rc != 0) {
        response->set_msg("Authentication failed: user not found");
        response->set_husr(0);
        response->set_rc(-ENOENT);
        return Status::OK;
    }

    CValue pwd = rowData.beginIter[4];
    size_t pwdLen = strnlen(pwd.data, pwd.len); // ensure the password is null-terminated
    if (pwdLen != password.size()) {
        response->set_msg("Authentication failed: incorrect password: received length does not match expected length rcvLen = " + std::to_string(password.size()) + ", expectedLen = " + std::to_string(pwd.len));
        response->set_husr(0);
        response->set_rc(-EACCES);
        return Status::OK;
    }

    if (memcmp(pwd.data, password.data(), pwdLen) != 0) {
        response->set_msg("Authentication failed: incorrect password for user " + usrName + ": passwd = " + std::string(pwd.data, pwd.len));
        response->set_husr(0);
        response->set_rc(-EACCES);
        return Status::OK;
    }

    // unlock
    guard.release();



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


    response->set_msg("Login successful for user: " + usrName);
    response->set_rc(0);

    return Status::OK;
}


Status sysCtlServiceImpl::logoff(ServerContext* context, const dpfsgrpc::logoffReq* request, dpfsgrpc::operateReply* response) {
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