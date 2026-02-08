#pragma once
#include <collect/diskman.hpp>
#include <collect/product.hpp>
#include <basic/dpfscache.hpp>
#include <dpfssys/systab.hpp>
#include <vector>

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