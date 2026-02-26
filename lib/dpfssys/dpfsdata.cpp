#include <dpfssys/dpfsdata.hpp>
#include <basic/dpfsconst.hpp>
#include <dpfsdebug.hpp>
#include <mysql_decimal/my_decimal.h>
/*
    basic system table :
    "sysdpfs" : "sysproducts", "systables", "syscolumns", "sysindexes"

    "user defined products" : ... user tables


    
*/

#include <ctime>
#include <sstream>
#include <iomanip>

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

int CDatasvc::createTable(const CUser& usr, const std::string& schema, CCollection& coll, CPlanHandle& out) {
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

    m_sysSchema->cidLock.lock();
    collCs.ds->m_ccid = m_sysSchema->tableccid;
    ++m_sysSchema->tableccid;
    m_sysSchema->cidLock.unlock();

    rc = item.updateValue(0, schema.c_str(), schema.size());                               if (rc <= 0) { m_log.log_error("Failed to update SYSTABLES value, rc=%d\n", rc); return rc; }
    rc = item.updateValue(1, coll.getName().c_str(), coll.getName().size());               if (rc <= 0) { m_log.log_error("Failed to update SYSTABLES value, rc=%d\n", rc); return rc; }
    rc = item.updateValue(2, nowTime.c_str(), nowTime.size());                             if (rc <= 0) { m_log.log_error("Failed to update SYSTABLES value, rc=%d\n", rc); return rc; }
    rc = item.updateValue(3, &collCs.ds->m_pkColNum, sizeof(collCs.ds->m_pkColNum));       if (rc <= 0) { m_log.log_error("Failed to update SYSTABLES value, rc=%d\n", rc); return rc; }
    rc = item.updateValue(4, &collCs.ds->m_ccid, sizeof(collCs.ds->m_ccid));               if (rc <= 0) { m_log.log_error("Failed to update SYSTABLES value, rc=%d\n", rc); return rc; }
    rc = item.updateValue(5, &coll.m_collectionBid, sizeof(coll.m_collectionBid));         if (rc <= 0) { m_log.log_error("Failed to update SYSTABLES value, rc=%d\n", rc); return rc; }




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
            m_log.log_error("Table %s.%s does not exist.\n", schema.c_str(), table.c_str());
            // table not exist, return 0
            return 0;
        }
        return rc;
    }

    // if found the table, return -EEXIST to indicate the table already exists
    return -EEXIST;

}

// TODO MAKE A HANDLE, return a handle of the plan, and execute the plan in executor with the handle, and release the handle after execution
int CDatasvc::createInsertPlan(const std::string& osql, const std::string& schema, const std::string& table, const std::vector<std::string>& colNames, CPlanHandle& out) {
    CPlan plan;
    plan.originalSQL = osql;
    CPlanObjects insertObj;

    // search plan in cache
    auto cachePlan = m_planCache.getCache(osql);
    if (cachePlan) {
        out.plan = cachePlan->cache;
        out.type = planType::Insert;
        return 0;
    }

    
    // get collection bidx, search in systables
    int rc = 0;
    CCollection& sysTables = m_sysSchema->systables;

    // 0 "TABLE_SCHEMA", dpfs_datatype_t::TYPE_CHAR,      64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);
    // 1 "TABLE_NAME",   dpfs_datatype_t::TYPE_CHAR,      64, 0, cf::NOT_NULL | cf::PRIMARY_KEY);
    // 2 "CREATE_TIME",  dpfs_datatype_t::TYPE_TIMESTAMP, 64, 0, cf::NOT_NULL);                  
    // 3 "KEYCOLUMNS",   dpfs_datatype_t::TYPE_INT,       4,  0, cf::NOT_NULL);                  
    // 4 "TABLE_ID",     dpfs_datatype_t::TYPE_INT,       4,  0, cf::NOT_NULL | cf::UNIQUE);     
    // 5 "ROOT",         dpfs_datatype_t::TYPE_BINARY,    16, 0, cf::NOT_NULL);                  

    char tmpk[MAXKEYLEN];
    KEY_T key(tmpk, 0, sysTables.m_cmpTyps);
    memset(tmpk, 0, 64 * 2);
    // input key word
    memcpy(key.data, schema.data(), schema.size());
    key.len += 64;
    memcpy(key.data + key.len, table.data(), table.size());
    key.len += 64;

    // init item receiver
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
            m_log.log_notic("Table %s.%s does not exist.\n", schema.c_str(), table.c_str());
            return -ENOENT;
        }
        return rc;
    }

    CValue rootVal = idxItem.getValue(5);
    if (rootVal.len != sizeof(bidx)) {
        return -EINVAL;
    }

    const bidx* collBid = reinterpret_cast<bidx*>(rootVal.data);

    

    CCollection coll(m_diskMan, m_page);

    rc = coll.loadFrom(*collBid, false);
    if (rc != 0) {
        m_log.log_error("Failed to load collection for building insert plan, rc=%d\n", rc);
        return rc;
    }

    cacheLocker collLocker(coll.m_cltInfoCache, coll.m_page);
    CTemplateReadGuard collGuard(collLocker);
    if (collGuard.returnCode() != 0) {
        m_log.log_error("Failed to lock collection struct for reading, rc=%d\n", collGuard.returnCode());
        return collGuard.returnCode();
    }
    CCollection::collectionStruct collCs(coll.m_cltInfoCache->getPtr(), coll.m_cltInfoCache->getLen() * dpfs_lba_size);

    // init cplan's col pos
    if (colNames.size()) {
        for(int i = 0; i < colNames.size(); ++i) {
            for(int j = 0; j < collCs.m_cols.size(); ++j) {
                if (collCs.m_cols[j].getName() == colNames[i]) {
                    insertObj.colSequences.emplace_back(j);
                    break;
                }
            }
        }
    } else {
        for(int i = 0; i < collCs.m_cols.size(); ++i) {
            insertObj.colSequences.emplace_back(i);
        }
    }
    collGuard.release();
    insertObj.collectionBidx = *collBid;

    plan.planObjects.emplace_back(std::move(insertObj));
    out.plan = plan;
    rc = m_planCache.insertCache(osql, std::move(plan));
    if (rc != 0) {
        m_log.log_error("Failed to insert plan into plan cache, rc=%d\n", rc);
        return rc;
    }

    out.type = planType::Insert;

    return 0;


}

/*
    @param pl : the insert plan, including the collection bidx and the column sequence to insert
    @param collcs : the collection struct of the target collection, used to get the column type for value conversion
    @param valVecs : the values to insert, each inner vector is a row, each pair in the inner vector is a column value with its type.
    @return 0 on success, less than 0 on failure, larger than 0 indicates warning.
*/
static int convertValueType(const CPlan& pl, const CCollection::collectionStruct& collcs, std::vector<std::vector<std::pair<dpfs_datatype_t, CValue>>>& valVecs) {
    

    // TODO
    int rc = 0;
    const auto& cols = collcs.m_cols;

    // for each row
    for (int i = 0; i < valVecs.size(); ++i) {
        // for each column
        for (int j = 0; j < valVecs[i].size(); ++j) {
            auto& val = valVecs[i][j];
            if (j >= pl.planObjects[0].colSequences.size()) {
                return -EINVAL;
            }
            uint32_t colPos = pl.planObjects[0].colSequences[j];
            if (colPos >= cols.size()) {
                return -EINVAL;
            }
            auto& col = cols[colPos];
            // convert val to col type
            // TODO : currently just support int and char type, and the conversion is very simple, need to consider more complex type and conversion in the future
            if (col.getType() == val.first && val.first != dpfs_datatype_t::TYPE_DECIMAL) {
                continue;
            } else {
                if (col.getType() == dpfs_datatype_t::TYPE_INT && val.first == dpfs_datatype_t::TYPE_BIGINT) {
                    // convert bigint to int
                    int64_t* bigVal = reinterpret_cast<int64_t*>(val.second.data);
                    if (*bigVal > INT32_MAX || *bigVal < INT32_MIN) {
                        return -EINVAL;
                    }
                    int32_t intVal = static_cast<int32_t>(*bigVal);
                    val.second.resetData(reinterpret_cast<char*>(&intVal), sizeof(int32_t));
                    val.first = col.getType();

                } else if ((col.getType() == dpfs_datatype_t::TYPE_CHAR || col.getType() == dpfs_datatype_t::TYPE_VARCHAR) && val.first == dpfs_datatype_t::TYPE_BIGINT) {
                    // convert int to char
                    int64_t intVal = 0;
                    std::memcpy(&intVal, val.second.data, sizeof(int64_t));
                    std::string strVal = std::to_string(intVal);
                    val.second.resetData(strVal.c_str(), strVal.size());
                    val.first = col.getType();
                } else if (val.first == dpfs_datatype_t::TYPE_DECIMAL) {
                    // process decimal or float/double
                    uint8_t len = static_cast<uint8_t>(val.second.data[0]);
                    uint8_t scale = static_cast<uint8_t>(val.second.data[1]);
                    if (len > 68 || scale > len) {
                        return -EINVAL;
                    }
                    my_decimal decVal;
                    // larger than 0 means warning, for example, return 1 if the decimal value is truncated due to the precision and scale of the target column.
                    rc = binary2my_decimal(0, reinterpret_cast<uchar*>(val.second.data + 2), &decVal, len, scale);
                    if (rc != 0) {
                        return rc;
                    }

                    if (col.getType() == dpfs_datatype_t::TYPE_DECIMAL) {

                        // convert to target decimal type
                        len = col.getDds().genLen;
                        scale = static_cast<uint8_t>(col.getScale());

                        uint8_t decbin[64];
                        rc = my_decimal2binary(0, &decVal, decbin, len, scale);
                        if (rc != 0) {
                            if (rc == 2) {
                                return -E2BIG;
                            }
                            return rc;
                        }
                        len = my_decimal_get_binary_size(len, scale);
                        val.second.resetData(reinterpret_cast<char*>(decbin), len);

                    } else if (col.getType() == dpfs_datatype_t::TYPE_FLOAT) {
                        // convert decimal to float/double, currently just convert to double, and the conversion is very simple, need to consider more complex conversion in the future

                        double doubleVal = 0.0;
                        rc = my_decimal2double(0, &decVal, &doubleVal);
                        if (rc != 0) {
                            return rc;
                        }
                        
                        float floatVal = static_cast<float>(doubleVal);
                        val.second.resetData(reinterpret_cast<char*>(&floatVal), sizeof(float));

                    } else if (col.getType() == dpfs_datatype_t::TYPE_DOUBLE) {
                        // convert decimal to double

                        double doubleVal = 0.0;
                        rc = my_decimal2double(0, &decVal, &doubleVal);
                        if (rc != 0) {
                            return rc;
                        }
                        val.second.resetData(reinterpret_cast<char*>(&doubleVal), sizeof(double));
                    } else {
                        return -EINVAL;
                    }
                } 
            }
        }
    }


    return 0;
}

int CDatasvc::planInsert(const CPlan& pl, std::vector<std::vector<std::pair<dpfs_datatype_t, CValue>>>* valVecs) {
    
    if (!valVecs) {
        return -EINVAL;
    }
    int rc = 0;
    const bidx& cbid = pl.planObjects[0].collectionBidx; // get collection bidx

    CCollection coll(m_diskMan, m_page);

    rc = coll.loadFrom(cbid);
    if (rc != 0) {
        m_log.log_error("Failed to load collection for executing insert plan, rc=%d\n", rc);
        return rc;
    }

    // TODO

    // init item receiver
    cacheLocker tabLocker(coll.m_cltInfoCache, coll.m_page);
    CTemplateReadGuard tabReadGuard(tabLocker);
    rc = tabReadGuard.returnCode();
    if (rc != 0) {
        m_log.log_error("Failed to lock coll for reading, rc=%d\n", rc);
        return rc;  
    }
    CCollection::collectionStruct collcs(coll.m_cltInfoCache->getPtr(), coll.m_cltInfoCache->getLen() * dpfs_lba_size);
    CItem insertItem(collcs.m_cols, valVecs->size());




    rc = convertValueType(pl, collcs, *valVecs);
    if (rc < 0) {
        m_log.log_error("Failed to convert value type, rc=%d\n", rc);
        return rc;
    }
    if (rc == E_DEC_TRUNCATED) {
        m_log.log_notic("Value truncated during type conversion, rc=%d\n", rc);
    } else if (rc == E_DEC_OVERFLOW) {
        m_log.log_notic("Value overflow during type conversion, rc=%d\n", rc);
        return rc;
    }
    
    // for each row
    for (int i = 0; i < valVecs->size(); ++i) {
        
        if (i > 0) {
            rc = insertItem.nextRow();
            if (rc != 0) {
                m_log.log_error("Failed to prepare item for inserting, rc=%d\n", rc);
                return rc;
            }
        }

        const auto& row = (*valVecs)[i];
        // for each column 
        for (int j = 0; j < row.size(); ++j) {
            auto colseq = pl.planObjects[0].colSequences[j]; // get the column position in the collection struct, which is used to get the column type for value conversion
            const auto& val = row[j];
            rc = insertItem.updateValue(colseq, val.second.data, val.second.len);
            if (rc < 0) {
                m_log.log_error("Failed to update item value, rc=%d\n", rc);
                return rc;
            }
        }

    }

    tabReadGuard.release();

    rc = coll.addItem(insertItem); // add a new item to the collection, and update the item struct to the latest one, which is used to update the value in the following steps
    if (rc != 0) {
        m_log.log_error("Failed to add item to collection, rc=%d\n", rc);
        return rc;
    }

    return 0;
 }