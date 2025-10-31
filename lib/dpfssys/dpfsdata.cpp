#include <dpfssys/dpfsdata.hpp>

/*
    basic system table :
    "sysdpfs" : "sysproducts", "systables", "syscolumns", "sysindexes"

    "user defined products" : ... user tables


    
*/

// node id to determine which nvmfhost belongs to this instance 
uint64_t nodeId = 0;

int CDatasvc::init() {
    int rc = 0;
    // TODO
    bidx sysBidx = {nodeId, 0};
    CProduct* sysdpfs = new CProduct();
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
    sysdpfs;
    sysdpfs->fixedInfo.addCol("DPFS_NODE_ID", dpfs_datatype_t::TYPE_BIGINT);
    sysdpfs->fixedInfo.addCol("PRODUCT_ID", dpfs_datatype_t::TYPE_BIGINT);
    sysdpfs->fixedInfo.addCol("PRODUCT_NAME", dpfs_datatype_t::TYPE_CHAR, 128);




    return 0;
errReturn:

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