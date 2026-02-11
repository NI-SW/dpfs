#pragma once
#include <collect/diskman.hpp>
#include <collect/product.hpp>
#include <basic/dpfscache.hpp>
#include <dpfssys/systab.hpp>
#include <vector>
#include <dpfssys/user.hpp>

/*
    data service class
    1.process ddl and dml commands
    2.handle client data I/O requests

    data write mode: little endian, if host is big endian, need to convert
*/
class CDatasvc {
public:
    CDatasvc(std::vector<dpfsEngine*>& engine_list, size_t cacheSize, logrecord& log) : m_diskMan(nullptr), m_page(engine_list, cacheSize, log) {
        m_diskMan.m_page = &m_page;
        m_sysSchema = new CSysSchemas(m_diskMan, m_page);
    }
    ~CDatasvc() {
        if(m_sysSchema) {
            delete m_sysSchema;
            m_sysSchema = nullptr;
        }
    }
    /*
        @return 0 on success, else on failure
        @note init the data service, create super block, system tables and some necessary structures on disk
    */
    int init();

    /*
        @return 0 on success, else on failure
        @note load super block to boot dpfs system.
    */
    int load();

    // /*
    //     @return 0 on success, else on failure
    //     @note build the sql execution plan from the parse result, but do not execute the plan
    // */
    // int buildExecPlan();

    /*
        @param usr : user info for privilege check
        @param schema : schema name to operate on
        @param table : table name to operate on, empty string for schema level privilege check
        @param allocPriv : required privilege for the operation
        @return 0 on success, -EPERM if user has no privilege to create table in this schema, else on failure
        @note check if user has privilege to operate in this schema.
    */
    int checkPrivilege(const CUser& usr, const std::string& schema, const std::string& table, tbPrivilege allocPriv) const;

    /*
        @param schema: schema name
        @param table: table name
        @return 0 if table not exist, -EEXIST if table already exists, else on failure
        @note check if the given table exists in the given schema
    */
    int checkExist(const std::string& schema, const std::string& table) const;

    /*
        @param coll: collection to create
        @return 0 on success, else on failure
        @note create a new collection, and write the collection info to the storage. update the system tables accordingly
    */
    int createTable(const CUser& usr, const std::string& schema, CCollection& coll);


    // engines managed by this service
    CDiskMan m_diskMan;
    CPage m_page;
    logrecord m_log;
    // system collection
    CSysSchemas* m_sysSchema = nullptr;

    // // product list, should storage on disk but not vector
    // std::vector<std::pair<bidx, std::string>> pdl;
    // // product cache, when trigger a query, 
    // CDpfsCache<bidx, CProduct*> pdCache;

};