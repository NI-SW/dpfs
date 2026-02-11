#include <dpfssys/dpfsdata.hpp>
#include <basic/dpfsconst.hpp>
#include <dpfsdebug.hpp>
/*
    basic system table :
    "sysdpfs" : "sysproducts", "systables", "syscolumns", "sysindexes"

    "user defined products" : ... user tables


    
*/

#include <ctime>
#include <sstream>
#include <iomanip>
std::string getCurrentTimestamp() {
    // 获取当前时间点
    auto now = std::chrono::system_clock::now();
    // 转换为time_t以用于localtime
    auto now_c = std::chrono::system_clock::to_time_t(now);
    // 转换为tm结构，这是ctime库需要的格式
    // unsafe 
    // std::tm* ptm = std::localtime(&now_c);

	// thread safe
	std::tm tm;
    #ifdef __linux__
    localtime_r(&now_c, &tm);
    #elif _WIN32
    localtime_s(&tm, &now_c);
    #endif

    // 使用stringstream来格式化输出
    std::stringstream ss;
    ss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

// node id to determine which nvmfhost belongs to this instance 
extern uint64_t nodeId;
bidx sysBidx = {nodeId, 16};

int CDatasvc::init() {
    int rc = 0;


    // TODO
    rc = m_sysSchema->init();
    if(rc != 0) {
        goto errReturn;
    }


    // sysdpfs->addCollection("sysproducts");
    // sysdpfs->addCollection("systables");
    // sysdpfs->addCollection("syscolumns");
    // sysdpfs->addCollection("sysindexes");




    return 0;
errReturn:
    return rc;
}

int CDatasvc::load() {
    // load include node id
    int rc = 0;
    // TODO
    rc = m_sysSchema->load();
    if(rc != 0) {
        goto errReturn;
    }
    return 0;

errReturn:

    return rc;
}

int CDatasvc::createTable(const CUser& usr, const std::string& schema, CCollection& coll) {
    // TODO : write the collection info to the storage, including the collection struct and some meta info, for example, the next auto increment id for this table
    int rc = 0;
    
    bool perm = false;

    // check authority, if user has privilege to create table in this schema, currently just check if user is system user or not
    if (usr.dbprivilege >= dbPrivilege::DBPRIVILEGE_CONTROL) {
        perm = true;
    }

    if (!perm) {
        // check schema authority, if user has privilege to create table in this schema, currently just check if user is the owner of the schema or not
        if (usr.username == schema) {
            perm = true;
        }
        rc = checkPrivilege(usr, schema, "", tbPrivilege::TBPRIVILEGE_FULL);
        if (rc != 0) {
            return rc;
        }
    }

    // check if table already exists
    CCollection& sysTables = m_sysSchema->systables;

    rc = checkExist(schema, coll.getName());
    if (rc != 0) {
        return rc;
    }

    // new table's locker
    cacheLocker collLocker(coll.m_cltInfoCache, coll.m_page);
    CTemplateReadGuard collGuard(collLocker);
    if (collGuard.returnCode() != 0) {
        m_log.log_error("Failed to lock collection struct for writing, rc=%d\n", rc);
        return collGuard.returnCode();
    }
    CCollection::collectionStruct collCs(coll.m_cltInfoCache->getPtr(), coll.m_cltInfoCache->getLen() * dpfs_lba_size);
    
    // lock systab
    cacheLocker systabLocker(sysTables.m_cltInfoCache, sysTables.m_page);
    rc = systabLocker.read_lock();
    if (rc != 0) {
        m_log.log_error("Failed to lock systables for reading, rc=%d\n", rc);
        return rc;
    }
    CCollection::collectionStruct syscs(sysTables.m_cltInfoCache->getPtr(), sysTables.m_cltInfoCache->getLen() * dpfs_lba_size);
    // insert into systables
    CItem item(syscs.m_cols);
    systabLocker.read_unlock();

    /*
        "TABLE_SCHEMA", dpfs_datatype_t::TYPE_CHAR,      64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);
        "TABLE_NAME",   dpfs_datatype_t::TYPE_CHAR,      64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);
        "CREATE_TIME",  dpfs_datatype_t::TYPE_TIMESTAMP, 64, 0, cf::NOT_NULL);                  
        "KEYCOLUMNS",   dpfs_datatype_t::TYPE_INT,       4,  0, cf::NOT_NULL);                  
        "TABLE_ID",     dpfs_datatype_t::TYPE_INT,       4,  0, cf::NOT_NULL | cf::UNIQUE);     
        "ROOT",         dpfs_datatype_t::TYPE_BINARY,    16, 0, cf::NOT_NULL);                  
    */
    std::string nowTime = getCurrentTimestamp();
    
    item.updateValue(0, schema.c_str(), schema.size());
    item.updateValue(1, coll.getName().c_str(), coll.getName().size());
    item.updateValue(2, nowTime.c_str(), nowTime.size());
    item.updateValue(3, &collCs.ds->m_pkColNum, sizeof(collCs.ds->m_pkColNum));
    item.updateValue(4, &collCs.ds->m_ccid, sizeof(collCs.ds->m_ccid));
    item.updateValue(5, &coll.m_collectionBid, sizeof(coll.m_collectionBid));




    rc = sysTables.addItem(item);
    if (rc != 0) {
        coll.destroy();
        // m_diskMan.bfree(saveBid.bid, MAX_COLLECTION_INFO_LBA_SIZE);
        m_log.log_error("Failed to add new table info to systables, rc=%d\n", rc);
        return rc;
    }

    // update SYSINDEXES
    if (collCs.m_indexInfos.size() > 0) {
        // TODO : add index info to SYSINDEXES
    }

    // update SYSTABCONSTRAINTS

    // update SYSCOLUMNS


    collGuard.release();
    // table struct and meta info save to storage(successfully created)
    rc = coll.save();
    if (rc != 0) {
        // m_diskMan.bfree(saveBid.bid, MAX_COLLECTION_INFO_LBA_SIZE);
        m_log.log_error("Failed to save new collection to storage, rc=%d\n", rc);
        return rc;
    }

    return 0;
}



int CDatasvc::checkPrivilege(const CUser& usr, const std::string& schema, const std::string& table, tbPrivilege allocPriv) const {

    if (usr.dbprivilege >= dbPrivilege::DBPRIVILEGE_FATAL) {
        // if user has control privilege, grant full privilege
        return 0;
    }
    if (usr.username == schema) {
        // if user is the owner of the schema, grant full privilege
        return 0;
    }

    int rc = 0;

    // check in sysSchema
    
    // get schema info in sysSchema->m_schemaInfo
    CCollection& sysAuth = m_sysSchema->sysauths;

    char tmpk[MAXKEYLEN];
    KEY_T key(tmpk, 0, sysAuth.m_cmpTyps);


    // 64 from systab.cpp:initAuthTab
    memset(tmpk, 0, 64 * 3);

    // input key word
    memcpy(key.data, usr.username.data(), usr.username.size());
    key.len += 64;
    memcpy(key.data + key.len, schema.data(), schema.size());
    key.len += 64;
    memcpy(key.data + key.len, table.data(), table.size());
    key.len += 64;

    cacheLocker authLocker(sysAuth.m_cltInfoCache, sysAuth.m_page);
    rc = authLocker.read_lock();
    if (rc != 0) {
        m_log.log_error("Failed to lock sysauths for reading, rc=%d\n", rc);
        return rc;
    }
    CCollection::collectionStruct authCs(sysAuth.m_cltInfoCache->getPtr(), sysAuth.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem idxItem(authCs.m_cols);

    authLocker.read_unlock();

    rc = sysAuth.getRow(key, &idxItem);
    if (rc != 0) {
        return rc;
    }
/*
    "USER_NAME",         dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL | cf::PRIMARY_KEY);
    "TABLE_SCHEMA",      dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL | cf::PRIMARY_KEY);
    "TABLE_NAME",        dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL | cf::PRIMARY_KEY);
    "USER_ID",           dpfs_datatype_t::TYPE_INT,       4,   0, cf::NOT_NULL | cf::UNIQUE);     
    "TBPRIVILEGE",       dpfs_datatype_t::TYPE_BINARY,    1,   0, cf::NOT_NULL);                  
*/
    CValue privVal = idxItem.getValue(4);
    if (privVal.len != 1) {
        return -EINVAL;
    }

    if (!(privVal.data[0] & static_cast<uint8_t>(allocPriv))) {
        return -EPERM;
    }

    return 0;

}

int CDatasvc::checkExist(const std::string& schema, const std::string& table) const {
    int rc = 0;

    // check in sysSchema
    
    // get schema info in sysSchema->m_schemaInfo
    CCollection& sysTables = m_sysSchema->systables;

    char tmpk[MAXKEYLEN];
    KEY_T key(tmpk, 0, sysTables.m_cmpTyps);

    memset(tmpk, 0, 64 * 2);
    // input key word
    memcpy(key.data, schema.data(), schema.size());
    key.len += 64;
    memcpy(key.data + key.len, table.data(), table.size());
    key.len += 64;


    cacheLocker systabLocker(sysTables.m_cltInfoCache, sysTables.m_page);
    rc = systabLocker.read_lock();
    if (rc != 0) {
        m_log.log_error("Failed to lock systables for reading, rc=%d\n", rc);
        return rc;
    }

    CCollection::collectionStruct systabcs(sysTables.m_cltInfoCache->getPtr(), sysTables.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem idxItem(systabcs.m_cols);
    systabLocker.read_unlock();

    rc = sysTables.getRow(key, &idxItem);
    if (rc != 0) {
        if (rc == -ENOENT) {
            // table not exist, return 0
            return 0;
        }
        return rc;
    }

    // if found the table, return -EEXIST to indicate the table already exists
    return -EEXIST;

}



