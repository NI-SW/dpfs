#include <dpfssys/dpfsdata.hpp>
#include <basic/dpfsconst.hpp>
#include <dpfsdebug.hpp>
/*
    basic system table :
    "sysdpfs" : "sysproducts", "systables", "syscolumns", "sysindexes"

    "user defined products" : ... user tables


    
*/

// node id to determine which nvmfhost belongs to this instance 
extern uint64_t nodeId;

int CDatasvc::init() {
    int rc = 0;
    // TODO
    bidx sysBidx = {nodeId, 0};
    std::string data = "";
    CProduct* sysdpfs = new CProduct(m_page, m_diskMan, m_log);
    CItem* itm = nullptr;
    CValue* val;
    char version[4] = {dpfsVersion[0] + 0x30, dpfsVersion[1] + 0x30, dpfsVersion[2] + 0x30, dpfsVersion[3] + 0x30};

    if(!sysdpfs) {
        rc = -ENOMEM;
        goto errReturn;
    }

    sysdpfs->pid = {nodeId, 0};
    sysBidx.bid = m_diskMan.balloc(1024); // allocate 1024 lbas for sysdpfs
    if(sysBidx.bid == 0) {
        rc = -ENOSPC;
        goto errReturn;
    }
    
    sysdpfs->fixedInfo.addCol("KEY", dpfs_datatype_t::TYPE_CHAR, 32);
    sysdpfs->fixedInfo.addCol("VALUE", dpfs_datatype_t::TYPE_CHAR, 32);

    itm = CItem::newItems(sysdpfs->fixedInfo.m_cols, 10);
    if(!itm) {
        rc = -ENOMEM;
        goto errReturn;
    }

    val = (CValue*)malloc(sizeof(CValue) + 64);
    if(!val) {
        rc = -ENOMEM;
        goto errReturn;
    }
    // set system version

    val->setData("VERSION", sizeof("VERSION"));
    rc = itm->updateValue(0, val); 


    
    val->setData(version, sizeof(version));
    rc = itm->updateValue(1, val); 

    printMemory(itm->data, 128);
    printf("\n");
    itm->nextRow();

    printMemory(itm->data, 128);
    printf("\n");
    

    // set system codeset
    val->setData("CODESET", sizeof("CODESET"));
    itm->updateValue(0, val);

    val->setData("UTF-8", sizeof("UTF-8"));
    itm->updateValue(1, val); 

    itm->nextRow();

    printMemory(itm->data, 128);
    printf("\n");


    // set system node id
    val->setData("DPFS_NODE_ID", sizeof("DPFS_NODE_ID"));
    itm->updateValue(0, val);
    
    data = ull2str(nodeId);
    val->setData(data.c_str(), data.size());
    itm->updateValue(1, val);

	printMemory(itm->data, 256);
    printf("\n");

    
    sysdpfs->fixedInfo.addItem(*itm);


    sysdpfs->addCollection("sysproducts");
    sysdpfs->addCollection("systables");
    sysdpfs->addCollection("syscolumns");
    sysdpfs->addCollection("sysindexes");




    return 0;
errReturn:
    if(itm) {
        CItem::delItem(itm);
    }
    if(sysdpfs) {
        delete sysdpfs;
    }
    return rc;
}

int CDatasvc::load() {
    // load include node id
    int rc = 0;
    // TODO

    return 0;

errReturn:

    return rc;
}