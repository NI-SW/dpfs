#include <dpfssys/systab.hpp>

// node id to determine which nvmfhost belongs to this instance 
extern uint64_t nodeId;
using cf = CColumn::constraint_flags;

/*
    systab init sequence:
    1. boottab
    2. tabletab
    3. coltab
    4. constab  
    5. indextab
    6. usertab
    7. schematab
    8. authtab
*/
int CSysSchemas::init() {
    int rc = 0;
    size_t baseBid = 16;
    // bidx sysBidx = {nodeId, baseBid};
    bidx tmpbid = {nodeId, baseBid};
    char version[4] = {dpfsVersion[0] + 0x30, dpfsVersion[1] + 0x30, dpfsVersion[2] + 0x30, dpfsVersion[3] + 0x30};
    
    rc = initBootTab(tmpbid); if (rc != 0) { return -ENOMEM; }

    cacheLocker cl(systemboot.m_cltInfoCache, systemboot.m_page);
    rc = cl.read_lock(); if (rc != 0) { return rc; }
    CCollection::collectionStruct sysbootcs(systemboot.m_cltInfoCache->getPtr(), systemboot.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem itm(sysbootcs.m_cols, 16);
    cl.read_unlock();
 
    // VERSION
    rc = itm.updateValue(0, "VERSION", sizeof("VERSION"));              if (rc < 0) { goto errReturn; }
    rc = itm.updateValue(1, version, sizeof(version));                  if (rc < 0) { goto errReturn; }

    // CODESET      
    rc = itm.nextRow();                                                 if (rc != 0) { goto errReturn; }
    rc = itm.updateValue(0, "CODESET", sizeof("CODESET"));              if (rc < 0) { goto errReturn; }
    rc = itm.updateValue(1, "UTF-8", sizeof("UTF-8"));                  if (rc < 0) { goto errReturn; }

    // DPFS_NODE_ID
    rc = itm.nextRow();                                                 if (rc != 0) { goto errReturn; }
    rc = itm.updateValue(0, "DPFS_NODE_ID", sizeof("DPFS_NODE_ID"));    if (rc < 0) { goto errReturn; }
    rc = itm.updateValue(1, &nodeId, sizeof(nodeId));                   if (rc < 0) { goto errReturn; }

    // SYSTABLES ROOT BIDX
    tmpbid.bid += MAX_COLLECTION_INFO_LBA_SIZE;
    rc = initTableTab(tmpbid);                                           if (rc != 0) { goto errReturn; }
    rc = itm.nextRow();                                                 if (rc != 0) { goto errReturn; }
    rc = itm.updateValue(0, "SYSTABLESRT", sizeof("SYSTABLESRT"));      if (rc < 0) { goto errReturn; }
    rc = itm.updateValue(1, &tmpbid, sizeof(tmpbid));                   if (rc < 0) { goto errReturn; }

    // SYSCOLUMNS ROOT BIDX
    tmpbid.bid += MAX_COLLECTION_INFO_LBA_SIZE;
    rc = initColTab(tmpbid);                                             if (rc != 0) { goto errReturn; }
    rc = itm.nextRow();                                                 if (rc != 0) { goto errReturn; }
    rc = itm.updateValue(0, "SYSCOLUMNSRT", sizeof("SYSCOLUMNSRT"));    if (rc < 0) { goto errReturn; }
    rc = itm.updateValue(1, &tmpbid, sizeof(tmpbid));                   if (rc < 0) { goto errReturn; }
    
    // SYSCONSTRAINTS ROOT BIDX
    tmpbid.bid += MAX_COLLECTION_INFO_LBA_SIZE;
    rc = initConTab(tmpbid);                                             if (rc != 0) { goto errReturn; }
    rc = itm.nextRow();                                                 if (rc != 0) { goto errReturn; }
    rc = itm.updateValue(0, "SYSCONSTRT", sizeof("SYSCONSTRT"));        if (rc < 0) { goto errReturn; }
    rc = itm.updateValue(1, &tmpbid, sizeof(tmpbid));                   if (rc < 0) { goto errReturn; }
    
    // SYSINDEXES ROOT BIDX
    tmpbid.bid += MAX_COLLECTION_INFO_LBA_SIZE;
    rc = initIdxTab(tmpbid);                                             if (rc != 0) { goto errReturn; }
    rc = itm.nextRow();                                                 if (rc != 0) { goto errReturn; }
    rc = itm.updateValue(0, "SYSINDEXRT", sizeof("SYSINDEXRT"));        if (rc < 0) { goto errReturn; }
    rc = itm.updateValue(1, &tmpbid, sizeof(tmpbid));                   if (rc < 0) { goto errReturn; }

    // SYSUSERS ROOT BIDX
    tmpbid.bid += MAX_COLLECTION_INFO_LBA_SIZE;
    rc = initUserTab(tmpbid);                                            if (rc != 0) { goto errReturn; }
    rc = itm.nextRow();                                                 if (rc != 0) { goto errReturn; }
    rc = itm.updateValue(0, "SYSUSERSRT", sizeof("SYSUSERSRT"));        if (rc < 0) { goto errReturn; }
    rc = itm.updateValue(1, &tmpbid, sizeof(tmpbid));                   if (rc < 0) { goto errReturn; }

    // SYSSCHEMAS ROOT BIDX
    tmpbid.bid += MAX_COLLECTION_INFO_LBA_SIZE;
    rc = initSchemaTab(tmpbid);                                          if (rc != 0) { goto errReturn; }
    rc = itm.nextRow();                                                 if (rc != 0) { goto errReturn; }
    rc = itm.updateValue(0, "SYSSCHEMASRT", sizeof("SYSSCHEMASRT"));    if (rc < 0) { goto errReturn; }
    rc = itm.updateValue(1, &tmpbid, sizeof(tmpbid));                   if (rc < 0) { goto errReturn; }

    // SYSAUTHS ROOT BIDX
    tmpbid.bid += MAX_COLLECTION_INFO_LBA_SIZE;
    rc = initAuthTab(tmpbid);                                           if (rc != 0) { goto errReturn; }
    rc = itm.nextRow();                                                if (rc != 0) { goto errReturn; }
    rc = itm.updateValue(0, "SYSAUTHRT", sizeof("SYSAUTHRT"));         if (rc < 0) { goto errReturn; }
    rc = itm.updateValue(1, &tmpbid, sizeof(tmpbid));                  if (rc < 0) { goto errReturn; }

    // itm.resetScan();
    // systemboot.addItem(*itm); if (rc < 0) { goto errReturn; }

    #ifdef __DPFSSYS_SYSTAB_DEBUG__
    cout << "System Boot Info Inserted:" << endl;
    #endif
    rc = systemboot.addItem(itm); if (rc != 0) { std::cout << "error message: " << systemboot.message << " code : " << rc << std::endl; goto errReturn; }




    return 0;
errReturn:

    return rc;
}

int CSysSchemas::load() {
    // TODO
/*
    systab init sequence:
    1. boottab
    2. tabletab
    3. coltab
    4. constab  
    5. indextab
    6. usertab
    7. schematab
    8. authtab
*/
    int rc = 0;
    bidx sysBidx = {nodeId, 16};
    rc = systemboot.loadFrom(sysBidx);         sysBidx.bid += MAX_COLLECTION_INFO_LBA_SIZE; if (rc != 0) { return rc; }
    rc = systables.loadFrom(sysBidx);          sysBidx.bid += MAX_COLLECTION_INFO_LBA_SIZE; if (rc != 0) { return rc; }
    rc = syscolumns.loadFrom(sysBidx);         sysBidx.bid += MAX_COLLECTION_INFO_LBA_SIZE; if (rc != 0) { return rc; }
    rc = sysconstraints.loadFrom(sysBidx);     sysBidx.bid += MAX_COLLECTION_INFO_LBA_SIZE; if (rc != 0) { return rc; }
    rc = sysindexes.loadFrom(sysBidx);         sysBidx.bid += MAX_COLLECTION_INFO_LBA_SIZE; if (rc != 0) { return rc; }
    rc = sysusers.loadFrom(sysBidx);           sysBidx.bid += MAX_COLLECTION_INFO_LBA_SIZE; if (rc != 0) { return rc; }
    rc = sysschemas.loadFrom(sysBidx);         sysBidx.bid += MAX_COLLECTION_INFO_LBA_SIZE; if (rc != 0) { return rc; }
    rc = sysauths.loadFrom(sysBidx);                                                        if (rc != 0) { return rc; }     


    return 0;
}

int CSysSchemas::initBootTab(const bidx& sysBidx) {
    int rc = 0;
    CCollectionInitStruct initstruct;
    initstruct.id = 0;
    initstruct.name = "SYSTEM";
    initstruct.m_perms.perm.m_systab = 1;
    initstruct.m_perms.perm.m_ddl = 0;
    initstruct.m_perms.perm.m_select = 1;
    initstruct.m_perms.perm.m_insertable = 1;
    initstruct.m_perms.perm.m_updatable = 1;
    initstruct.m_perms.perm.m_deletable = 1;
    rc = systemboot.initialize(initstruct, sysBidx);                                                          if (rc != 0) { goto errReturn; }
    rc = systemboot.addCol("KEY", dpfs_datatype_t::TYPE_CHAR, 64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);         if (rc != 0) { goto errReturn; }
    rc = systemboot.addCol("VALUE", dpfs_datatype_t::TYPE_BINARY, 64, 0, cf::NOT_NULL);                       if (rc != 0) { goto errReturn; }
    rc = systemboot.initBPlusTreeIndex();                                                                     if (rc != 0) { goto errReturn; }
    rc = systemboot.save();                                                                                   if (rc != 0) { goto errReturn; }
    
    return 0;
errReturn:
    if(systemboot.m_btreeIndex) {
        delete systemboot.m_btreeIndex;
        systemboot.m_btreeIndex = nullptr;
    }
    systemboot.clearCols();
    if(systemboot.m_btreeIndex) {
        delete systemboot.m_btreeIndex;
        systemboot.m_btreeIndex = nullptr;
    }
    return rc;
}

int CSysSchemas::initTableTab(const bidx& sysBidx) {
    int rc = 0;
    CCollectionInitStruct initstruct;
    initstruct.id = 0;
    initstruct.name = "SYSTABLES";
    initstruct.m_perms.perm.m_systab = 1;
    initstruct.m_perms.perm.m_ddl = 0;
    initstruct.m_perms.perm.m_select = 1;
    initstruct.m_perms.perm.m_insertable = 1;
    initstruct.m_perms.perm.m_updatable = 1;
    initstruct.m_perms.perm.m_deletable = 1;
    rc = systables.initialize(initstruct, sysBidx);                                                                 if (rc != 0) { goto errReturn; }
    rc = systables.addCol("TABLE_SCHEMA", dpfs_datatype_t::TYPE_CHAR,      64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);  if (rc != 0) { goto errReturn; }
    rc = systables.addCol("TABLE_NAME",   dpfs_datatype_t::TYPE_CHAR,      64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);  if (rc != 0) { goto errReturn; }
    rc = systables.addCol("CREATE_TIME",  dpfs_datatype_t::TYPE_TIMESTAMP, 64, 0, cf::NOT_NULL);                    if (rc != 0) { goto errReturn; }
    // rc = systables.addCol("COLCOUNT",     dpfs_datatype_t::TYPE_INT,       4,  0, cf::NOT_NULL);                    if (rc != 0) { goto errReturn; }
    // this column indicates how many primary key columns in the table
    rc = systables.addCol("KEYCOLUMNS",   dpfs_datatype_t::TYPE_INT,       4,  0, cf::NOT_NULL);                    if (rc != 0) { goto errReturn; }
    rc = systables.addCol("TABLE_ID",     dpfs_datatype_t::TYPE_INT,       4,  0, cf::NOT_NULL | cf::UNIQUE);       if (rc != 0) { goto errReturn; }
    rc = systables.addCol("ROOT",         dpfs_datatype_t::TYPE_BINARY,    16, 0, cf::NOT_NULL);                    if (rc != 0) { goto errReturn; }
    rc = systables.initBPlusTreeIndex();                                                                            if (rc != 0) { goto errReturn; }
    rc = systables.save();                                                                                 if (rc != 0) { goto errReturn; }
    
    return 0;
errReturn:
    if(systables.m_btreeIndex) {
        delete systables.m_btreeIndex;
        systables.m_btreeIndex = nullptr;
    }
    systables.clearCols();
    if(systables.m_btreeIndex) {
        delete systables.m_btreeIndex;
        systables.m_btreeIndex = nullptr;
    }
    return rc;
}

int CSysSchemas::initColTab(const bidx& sysBidx) {
    int rc = 0;
    CCollectionInitStruct initstruct;
    initstruct.id = 0;
    initstruct.name = "SYSCOLUMNS";
    initstruct.m_perms.perm.m_systab = 1;
    initstruct.m_perms.perm.m_ddl = 0;
    initstruct.m_perms.perm.m_select = 1;
    initstruct.m_perms.perm.m_insertable = 1;
    initstruct.m_perms.perm.m_updatable = 1;
    initstruct.m_perms.perm.m_deletable = 1;
    rc = syscolumns.initialize(initstruct, sysBidx);                                                                  if (rc != 0) { goto errReturn; }
    rc = syscolumns.addCol("TABLE_SCHEMA",      dpfs_datatype_t::TYPE_CHAR, 64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    rc = syscolumns.addCol("TABLE_NAME",        dpfs_datatype_t::TYPE_CHAR, 64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    rc = syscolumns.addCol("COLUMN_NAME",       dpfs_datatype_t::TYPE_CHAR, 64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    rc = syscolumns.addCol("ORDINAL_POSITION",  dpfs_datatype_t::TYPE_INT,  4,  0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = syscolumns.addCol("IS_NULLABLE",       dpfs_datatype_t::TYPE_CHAR, 1,  0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = syscolumns.addCol("DATA_TYPE",         dpfs_datatype_t::TYPE_CHAR, 64, 0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = syscolumns.addCol("COLUMN_DEFAULT",    dpfs_datatype_t::TYPE_CHAR, 64, 0);                                   if (rc != 0) { goto errReturn; }
    rc = syscolumns.initBPlusTreeIndex();                                                                             if (rc != 0) { goto errReturn; }
    rc = syscolumns.save();                                                                                           if (rc != 0) { goto errReturn; }
    
    return 0;
errReturn:
    if(syscolumns.m_btreeIndex) {
        delete syscolumns.m_btreeIndex;
        syscolumns.m_btreeIndex = nullptr;
    }
    syscolumns.clearCols();
    // if(syscolumns.m_collectionStruct) {
    //     if(!syscolumns.m_cltInfoCache) {
    //         m_page.freezptr(syscolumns.m_collectionStruct->data, MAX_CLT_INFO_LBA_LEN);
    //     } else {
    //         syscolumns.m_cltInfoCache->release();
    //         syscolumns.m_cltInfoCache = nullptr;
    //     }
    //     delete syscolumns.m_collectionStruct;
    //     syscolumns.m_collectionStruct = nullptr;
    // }
    return rc;
}

int CSysSchemas::initConTab(const bidx& sysBidx) {
    int rc = 0;
    
    CCollectionInitStruct initstruct;
    initstruct.id = 0;
    initstruct.name = "SYSTABCONSTRAINTS";
    initstruct.m_perms.perm.m_systab = 1;
    initstruct.m_perms.perm.m_ddl = 0;
    initstruct.m_perms.perm.m_select = 1;
    initstruct.m_perms.perm.m_insertable = 1;
    initstruct.m_perms.perm.m_updatable = 1;
    initstruct.m_perms.perm.m_deletable = 1;
    rc = sysconstraints.initialize(initstruct, sysBidx);                                                                  if (rc != 0) { goto errReturn; }
    rc = sysconstraints.addCol("TABLE_SCHEMA",      dpfs_datatype_t::TYPE_CHAR, 64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    rc = sysconstraints.addCol("TABLE_NAME",        dpfs_datatype_t::TYPE_CHAR, 64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    rc = sysconstraints.addCol("CONSTRAINT_NAME",   dpfs_datatype_t::TYPE_CHAR, 64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    rc = sysconstraints.addCol("CONTENT",           dpfs_datatype_t::TYPE_CHAR, 64, 0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysconstraints.initBPlusTreeIndex();                                                                             if (rc != 0) { goto errReturn; }
    rc = sysconstraints.save();                                                                                           if (rc != 0) { goto errReturn; }
    
    return 0;
errReturn:
    if(sysconstraints.m_btreeIndex) {
        delete sysconstraints.m_btreeIndex;
        sysconstraints.m_btreeIndex = nullptr;
    }
    return rc;
}

int CSysSchemas::initIdxTab(const bidx& sysBidx) {
    int rc = 0;
    CCollectionInitStruct initstruct;
    initstruct.id = 0;
    initstruct.name = "SYSINDEXES";
    initstruct.m_perms.perm.m_systab = 1;
    initstruct.m_perms.perm.m_ddl = 0;
    initstruct.m_perms.perm.m_select = 1;
    initstruct.m_perms.perm.m_insertable = 1;
    initstruct.m_perms.perm.m_updatable = 1;
    initstruct.m_perms.perm.m_deletable = 1;
    rc = sysindexes.initialize(initstruct, sysBidx);                                                                        if (rc != 0) { goto errReturn; }
    rc = sysindexes.addCol("TABLE_SCHEMA",      dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    rc = sysindexes.addCol("TABLE_NAME",        dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    rc = sysindexes.addCol("INDEX_NAME",        dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    // rc = sysindexes.addCol("COLNAMES",          dpfs_datatype_t::TYPE_CHAR,      128, 0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysindexes.addCol("COLCOUNT",          dpfs_datatype_t::TYPE_INT,       4,   0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysindexes.addCol("CREATE_TIME",       dpfs_datatype_t::TYPE_TIMESTAMP, 10,  0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysindexes.addCol("LASTUSED",          dpfs_datatype_t::TYPE_TIMESTAMP, 10,  0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysindexes.addCol("ROOT",              dpfs_datatype_t::TYPE_BINARY,    16,  0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysindexes.initBPlusTreeIndex();                                                                                   if (rc != 0) { goto errReturn; }
    rc = sysindexes.save();                                                                                                 if (rc != 0) { goto errReturn; }
    
    return 0;
errReturn:
    if(sysindexes.m_btreeIndex) {
        delete sysindexes.m_btreeIndex;
        sysindexes.m_btreeIndex = nullptr;
    }
    sysindexes.clearCols();
    return rc;
}

int CSysSchemas::initUserTab(const bidx& sysBidx) {
    int rc = 0;
    CCollectionInitStruct initstruct;
    initstruct.id = 0;
    initstruct.name = "SYSUSERS";
    initstruct.m_perms.perm.m_systab = 1;
    initstruct.m_perms.perm.m_ddl = 0;
    initstruct.m_perms.perm.m_select = 1;
    initstruct.m_perms.perm.m_insertable = 1;
    initstruct.m_perms.perm.m_updatable = 1;
    initstruct.m_perms.perm.m_deletable = 1;
    rc = sysusers.initialize(initstruct, sysBidx);                                                                        if (rc != 0) { goto errReturn; }
    rc = sysusers.addCol("USER_NAME",         dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    // privilege to access or control all object of database
    rc = sysusers.addCol("DBPRIVILEGE",       dpfs_datatype_t::TYPE_BINARY,    1,   0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysusers.addCol("CREATE_TIME",       dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysusers.addCol("LAST_LOGIN",        dpfs_datatype_t::TYPE_TIMESTAMP, 10,  0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysusers.addCol("PASSWORD",          dpfs_datatype_t::TYPE_BINARY,    128, 0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysusers.addCol("USERID",            dpfs_datatype_t::TYPE_INT,       4,   0, cf::NOT_NULL | cf::UNIQUE);        if (rc != 0) { goto errReturn; }
    rc = sysusers.initBPlusTreeIndex();                                                                                   if (rc != 0) { goto errReturn; }
    rc = sysusers.save();                                                                                                 if (rc != 0) { goto errReturn; }

    // insert a root user with all privileges
    {
        CTemplateReadGuard tg(*sysusers.m_cltInfoCache);
        CCollection::collectionStruct sysuserscs(sysusers.m_cltInfoCache->getPtr(), sysusers.m_cltInfoCache->getLen() * dpfs_lba_size);

        CItem useritm(sysuserscs.m_cols);
        uint8_t dbprivilege = 4;
        std::string tmstmp = getCurrentTimestamp();
        // user = root
        // default passwd = root
        rc = useritm.updateValue(0, "root", sizeof("root"));                if (rc < 0) { goto errReturn; }
        rc = useritm.updateValue(1, &dbprivilege, sizeof(dbprivilege));     if (rc < 0) { goto errReturn; }
        rc = useritm.updateValue(2, tmstmp.c_str(), tmstmp.length() + 1);   if (rc < 0) { goto errReturn; }
        rc = useritm.updateValue(3, tmstmp.c_str(), tmstmp.length() + 1);   if (rc < 0) { goto errReturn; }
        rc = useritm.updateValue(4, "root", sizeof("root"));                if (rc < 0) { goto errReturn; }
        int32_t userid = 1;
        rc = useritm.updateValue(5, &userid, sizeof(userid));               if (rc < 0) { goto errReturn; }
        // unlock
        tg.release();

        rc = sysusers.addItem(useritm); if (rc != 0) { goto errReturn; }
    }

    return 0;
errReturn:
    if(sysusers.m_btreeIndex) {
        delete sysusers.m_btreeIndex;
        sysusers.m_btreeIndex = nullptr;
    }
    return rc;
}

int CSysSchemas::initSchemaTab(const bidx& sysBidx) {
    int rc = 0;
    CCollectionInitStruct initstruct;
    initstruct.id = 0;
    initstruct.name = "SYSSCHEMAS";
    initstruct.m_perms.perm.m_systab = 1;
    initstruct.m_perms.perm.m_ddl = 0;
    initstruct.m_perms.perm.m_select = 1;
    initstruct.m_perms.perm.m_insertable = 1;
    initstruct.m_perms.perm.m_updatable = 1;
    initstruct.m_perms.perm.m_deletable = 1;
    rc = sysschemas.initialize(initstruct, sysBidx);                                                                        if (rc != 0) { goto errReturn; }
    // product name
    rc = sysschemas.addCol("SCHEMA_NAME",       dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    // privilege to access or control all object of database
    // rc = sysschemas.addCol("TABLE_COUNT",       dpfs_datatype_t::TYPE_BINARY,    1,   0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysschemas.addCol("CREATE_TIME",       dpfs_datatype_t::TYPE_TIMESTAMP, 10,  0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    // disk groupid + disk logic block address
    // rc = sysschemas.addCol("ROOT",              dpfs_datatype_t::TYPE_BINARY,    16,  0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysschemas.initBPlusTreeIndex();                                                                                   if (rc != 0) { goto errReturn; }
    rc = sysschemas.save();                                                                                        if (rc != 0) { goto errReturn; }


    // insert a default schema
    return 0;
errReturn:
    if(sysschemas.m_btreeIndex) {
        delete sysschemas.m_btreeIndex;
        sysschemas.m_btreeIndex = nullptr;
    }
    sysschemas.clearCols();
    return rc;
}

int CSysSchemas::initAuthTab(const bidx& sysBidx) {
    int rc = 0;
    CCollectionInitStruct initstruct;
    initstruct.id = 0;
    initstruct.name = "SYSAUTHS";
    initstruct.m_perms.perm.m_systab = 1;
    initstruct.m_perms.perm.m_ddl = 0;
    initstruct.m_perms.perm.m_select = 1;
    initstruct.m_perms.perm.m_insertable = 1;
    initstruct.m_perms.perm.m_updatable = 1;
    initstruct.m_perms.perm.m_deletable = 1;
    rc = sysauths.initialize(initstruct, sysBidx);                                                                        if (rc != 0) { goto errReturn; }

    // grant ... TODO:: finish grant clause
    // 为尽量保证前缀命中，ID不作为第一主键列
    rc = sysauths.addCol("USER_NAME",         dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    rc = sysauths.addCol("TABLE_SCHEMA",      dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    rc = sysauths.addCol("TABLE_NAME",        dpfs_datatype_t::TYPE_CHAR,      64,  0, cf::NOT_NULL | cf::PRIMARY_KEY);   if (rc != 0) { goto errReturn; }
    rc = sysauths.addCol("USER_ID",           dpfs_datatype_t::TYPE_INT,       4,   0, cf::NOT_NULL | cf::UNIQUE);        if (rc != 0) { goto errReturn; }
    // privilege to access or control all object of database
    rc = sysauths.addCol("TBPRIVILEGE",       dpfs_datatype_t::TYPE_BINARY,    1,   0, cf::NOT_NULL);                     if (rc != 0) { goto errReturn; }
    rc = sysauths.initBPlusTreeIndex();                                                                                   if (rc != 0) { goto errReturn; }
    rc = sysauths.save();                                                                                                 if (rc != 0) { goto errReturn; }

    
    return 0;
errReturn:
    if(sysauths.m_btreeIndex) {
        delete sysauths.m_btreeIndex;
        sysauths.m_btreeIndex = nullptr;
    }
    sysauths.clearCols();
    return rc;
}