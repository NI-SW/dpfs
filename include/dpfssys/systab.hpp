#include <collect/page.hpp>
#include <collect/product.hpp>
#include <collect/bp.hpp>


#define __DPFSSYS_SYSTAB_DEBUG__

#ifdef __DPFSSYS_SYSTAB_DEBUG__
#include <dpfsdebug.hpp>
#endif


class CSysSchemas {
public:
    CSysSchemas(CDiskMan& diskman, CPage& page) : 
    m_diskman(diskman), 
    m_page(page), 
    systemboot(m_diskman, m_page),
    systables(m_diskman, m_page),
    syscolumns(m_diskman, m_page),
    sysconstraints(m_diskman, m_page),
    sysindexes(m_diskman, m_page),
    sysusers(m_diskman, m_page),
    sysschemas(m_diskman, m_page),
    sysauths(m_diskman, m_page) {
        
    };
    ~CSysSchemas() {};
    

    /*
        @note init the system tables, create necessary system collections on disk
        @return 0 on success, else on failure
        @warning this function should be called only once when the database is created, if is data partitioned, should get nodeId first
    */
    int init();

    /*
        @note load the system table from the storage engine, and load the collections
        @return 0 on success, else on failure
    */
    int load();
     
    /*
        @note read the system table from the storage engine, and load the collections
        @return 0 on success, else on failure
    */
    bool readSuper() { return true; }

    /*
    
    */
    int initBootTab(const bidx& sysBidx);
    int initTableTab(const bidx& sysBidx);
    int initColTab(const bidx& sysBidx);
    int initConTab(const bidx& sysBidx);
    int initIdxTab(const bidx& sysBidx);
    int initUserTab(const bidx& sysBidx);
    int initSchemaTab(const bidx& sysBidx);
    int initAuthTab(const bidx& sysBidx);

    CDiskMan& m_diskman;
    CPage& m_page;
    // tables that always exist in memory
    CCollection systemboot;
    CCollection systables;
    CCollection syscolumns;
    CCollection sysconstraints;
    CCollection sysindexes;
    // privilege control
    CCollection sysusers;
    // schema control
    CCollection sysschemas;
    // authentication control
    CCollection sysauths;

};


/*
systemboot table:
KEY                 VALUE

('VERSION',         '0001'      ) 
('CODESET',         'UTF-8'     ) 
('DPFS_NODE_ID',    '0'         )

<tableNames          root position>
('SYSPRODUCTIDX',     '12'    )
('SYSTABLESIDX',     '16'    )
('SYSCOLUMNSIDX',    '20'    )
('SYSINDEXESIDX',      '24'    )
*/