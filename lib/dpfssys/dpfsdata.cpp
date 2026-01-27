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