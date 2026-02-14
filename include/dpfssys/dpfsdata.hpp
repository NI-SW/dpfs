#pragma once
#include <collect/diskman.hpp>
#include <collect/product.hpp>
#include <basic/dpfscache.hpp>
#include <dpfssys/systab.hpp>
#include <vector>
#include <dpfssys/user.hpp>
#include <dpfssys/plan.hpp>
#include <log/loglocate.h>
constexpr size_t MAX_CACHED_SQL = 100;

class planClrFn {
public:
    // PageClrFn(void* clrFnArg) : engine_list(*static_cast<std::vector<dpfsEngine*>*>(clrFnArg)) {}
    planClrFn(void* clrFnArg) {

    }
    ~planClrFn() {

    }

    void operator()(CPlan& p, int* finish_indicator = nullptr) {
        if (finish_indicator) {
            *finish_indicator = 1;
        }
    }
    void flush(const std::list<void*>& cacheList) {
        return;

        // for(auto& it : cacheList) {
        //     CPlan& p = reinterpret_cast<CDpfsCache<std::string, CPlan, planClrFn>::cacheIter*>(it)->cache;
        //     // do something for CPlan?
        // }
        // return;
    }

};

/*
    data service class
    1.process ddl and dml commands
    2.handle client data I/O requests

    data write mode: little endian, if host is big endian, need to convert
*/
class CDatasvc {
public:
    CDatasvc(std::vector<dpfsEngine*>& engine_list, size_t cacheSize, logrecord& log) : m_diskMan(nullptr), m_page(engine_list, cacheSize, log), m_planCache(MAX_CACHED_SQL, nullptr) {
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
        @param coll: collection to create
        @return 0 on success, else on failure
        @note create a new collection, and write the collection info to the storage. update the system tables accordingly
    */
    int createTable(const CUser& usr, const std::string& schema, CCollection& coll, CPlanHandle& out);

    /*
        @param osql: original sql string
        @param schema: schema name
        @param table: table name
        @param colNames: column names to insert
        @return 0 on success, else on failure
        @note drop the given collection from the storage, and update the system tables accordingly
    */
    int createInsertPlan(const std::string& osql, const std::string& schema, const std::string& table, const std::vector<std::string>& colNames, CPlanHandle& out);
   
    /*
        @param pl: execution plan for insert operation
        @param valVec: vector of values to insert, each inner vector is a row of values
        @return 0 on success, else on failure
        @note execute the insert plan, insert the given values into the target collection
    */
    int planInsert(const CPlan& pl, std::vector<std::vector<CValue>>* valVecs);
private:
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

public:
    // engines managed by this service
    CDiskMan m_diskMan;
    CPage m_page;
    mutable logrecord m_log;
    // system collection
    CSysSchemas* m_sysSchema = nullptr;

    // <original sql, execution plan> cache 100 plans on default.
    CDpfsCache<std::string, CPlan, planClrFn> m_planCache;

    // // product list, should storage on disk but not vector
    // std::vector<std::pair<bidx, std::string>> pdl;
    // // product cache, when trigger a query, 
    // CDpfsCache<bidx, CProduct*> pdCache;

};