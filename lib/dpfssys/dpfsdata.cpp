#include <dpfssys/dpfsdata.hpp>

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
    CProduct* sysdpfs = new CProduct();
    CItem* tim = nullptr;
    CValue* val;

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
    
    sysdpfs->fixedInfo.addCol("VERSION", dpfs_datatype_t::TYPE_BIGINT);
    sysdpfs->fixedInfo.addCol("CODESET", dpfs_datatype_t::TYPE_CHAR, 16);
    sysdpfs->fixedInfo.addCol("DPFS_NODE_ID", dpfs_datatype_t::TYPE_BIGINT);

    tim = CItem::newItem(sysdpfs->fixedInfo.m_cols);
    if(!tim) {
        rc = -ENOMEM;
        goto errReturn;
    }

    val = (CValue*)malloc(sizeof(CValue) + 128);
    if(!val) {
        rc = -ENOMEM;
        goto errReturn;
    }
    // set system version
    val->len = 8;
    *(int64_t*)val->data = 1;
    tim->updateValue(0, val); // version

    // set system codeset
    val->len = 16;
    memcpy(val->data, "UTF-8", 6);
    tim->updateValue(1, val); // codeset

    // set system node id
    val->len = 8;
    *(int64_t*)val->data = nodeId;
    tim->updateValue(2, val); // node id


    
    // memcpy(tim->getValue(0)->data, "wuhudasima", sizeof("wuhudasima")); );
    
    
    sysdpfs->fixedInfo.addItem(*tim);

    sysdpfs->addCollection("sysproducts");
    sysdpfs->addCollection("systables");
    sysdpfs->addCollection("syscolumns");
    sysdpfs->addCollection("sysindexes");




    return 0;
errReturn:
    if(tim) {
        CItem::delItem(tim);
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