// #include <spdk/stdinc.h>
#include <spdk/nvme.h>
// #include <spdk_internal/nvme_util.h>
#include <spdk/vmd.h>
#include <spdk/nvme_zns.h>
#include <spdk/env.h>
#include <spdk/string.h>
#include <spdk/log.h>
#include <string>
#include <threadlock.hpp>
#include <iostream>

// for each disk host
// this class will become context for each disk host

#define DATA_BUFFER_STRING "Hello world!"

class CNvmfhost {
public:
    CNvmfhost() = delete;
    CNvmfhost(std::string trid_str);
    ~CNvmfhost();

// private:
    spdk_nvme_transport_id trid;
};


CNvmfhost::CNvmfhost(std::string trid_str) {
    if (spdk_nvme_transport_id_parse(&trid, trid_str.c_str()) != 0) {
        fprintf(stderr, "Error parsing transport address\n");
        exit(1);
    }


    char test[1024];
    memcpy(test, DATA_BUFFER_STRING, strlen(DATA_BUFFER_STRING) + 1);

    std::cout << "CNvmfhost created with trid: " << trid_str << std::endl;
    std::cout << "Test data: " << test << std::endl;
    
    
    
    
}

CNvmfhost::~CNvmfhost() {

}


void nvmf_remove_cb(void *cb_ctx, struct spdk_nvme_ctrlr *ctrlr) {

}