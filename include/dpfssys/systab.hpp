#include <collect/page.hpp>
#include <collect/product.hpp>
#include <collect/bp.hpp>


class CSysSchemas {
public:
    CSysSchemas(CDiskMan& diskman, CPage& page) : m_diskman(diskman), m_page(page)
    , systemboot(m_diskman, m_page) {
        
    };
    ~CSysSchemas() {};
    std::vector<CCollection*> collections; // system collection list

    CCollection systemboot;

    int init() {
        // initialize system boot collection
        CCollectionInitStruct initstruct;
        initstruct.id = 0;
        initstruct.name = "systemboot";
        initstruct.m_perms.perm.m_systab = 1;
        initstruct.m_perms.perm.m_ddl = 0;
        initstruct.m_perms.perm.m_select = 1;
        initstruct.m_perms.perm.m_insertable = 0;
        initstruct.m_perms.perm.m_updatable = 0;
        initstruct.m_perms.perm.m_deletable = 0;
        systemboot.initialize(initstruct);
        systemboot.addCol("KEY", dpfs_datatype_t::TYPE_CHAR, 64);
        systemboot.addCol("VALUE", dpfs_datatype_t::TYPE_CHAR, 64);
        systemboot.initBPlusTreeIndex();
        systemboot.saveTo({0, 1});

        const auto& cols = systemboot.m_collectionStruct->m_cols; 
        
        CItem* itm = CItem::newItem(systemboot.m_collectionStruct->m_cols);
        itm->updateValue(0, "CODESET", sizeof("CODESET"));
        itm->updateValue(1, "UTF-8", sizeof("UTF-8"));
                
        systemboot.addItem(*itm);
        
        systemboot.addItem(*itm);

        collections.push_back(&systemboot);
        return true;
    }

    /*
        @note read the system table from the storage engine, and load the collections
        @return 0 on success, else on failure
    */
    bool readSuper() { return true; }

    bool load() { return true; }
    CDiskMan& m_diskman;
    CPage& m_page;
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