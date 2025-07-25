
#include <threadlock.hpp>
#include <iostream>
#include <list>
#include <vector>
#include <spdk/nvme.h>

// for each disk host
// this class will become context for each disk host

#define DATA_BUFFER_STRING "Hello world!"

/*
 * 对于一个host下的所有nvme设备，使用一个类来管理，对外提供的接口将所有硬盘抽象为一个块空间，块大小为4KB
 * trid_str: 传入的trid字符串列表, 例:
 * 'trtype:tcp  adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1'
 * 'trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1'
 * 'trtype:pcie traddr:0000.1b.00.0'
*/

struct ctrlr_entry {
	struct spdk_nvme_ctrlr		*ctrlr;
	// TAILQ_ENTRY(ctrlr_entry)	link;
	char				name[1024];
};

struct ns_entry {
	struct spdk_nvme_ctrlr	*ctrlr;
	struct spdk_nvme_ns	*ns;
	// TAILQ_ENTRY(ns_entry)	link;
	struct spdk_nvme_qpair	*qpair;
};

class CNvmfhost {
public:
    CNvmfhost() = delete;
    CNvmfhost(const std::string& trid_str);
    CNvmfhost(const std::vector<std::string>& trid_strs);
    ~CNvmfhost();
	void cleanup();
	void register_ns(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns *ns);
	void hello_world();

// private:
    std::pair<spdk_nvme_transport_id*, bool> controls; 
    std::vector<spdk_nvme_transport_id*> trids;
	std::list<ctrlr_entry*> controllers;
	std::list<ns_entry*> namespaces;
    int nvmf_attach();
};
