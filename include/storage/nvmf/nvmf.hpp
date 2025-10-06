/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#include <threadlock.hpp>
#include <storage/engine.hpp>
#include <log/logbinary.h>
#include <iostream>
#include <list>
#include <vector>
// #include <spdk/nvme.h>

// for each disk host
// this class will become context for each disk host


/*
 * 对于一个host下的所有nvme设备，使用一个类来管理，对外提供的接口将所有硬盘抽象为一个块空间，块大小为4KB
 * trid_str: 传入的trid字符串列表, 例:
 * 'trtype:tcp  adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1'
 * 'trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50658 subnqn:nqn.2016-06.io.spdk:cnode1'
 * 'trtype:pcie traddr:0000.1b.00.0'
*/


// nvmf device class
class CNvmfhost;
class nvmfnsDesc;
struct io_sequence;
// release 1.0 only support 1 namespace per controller
// maybe in future we can support multiple namespaces per controller
// for now, we just use a vector to store the namespaces
class nvmfDevice {
public:
	nvmfDevice(CNvmfhost& host);
	nvmfDevice(nvmfDevice&& tgt) = delete;
	// delete copy construct to prevent mulity instance conflict.
	nvmfDevice() = delete;
	nvmfDevice(nvmfDevice&) = delete;
	~nvmfDevice();

	CNvmfhost&	nfhost;
	std::string devdesc_str = "";
	struct spdk_nvme_transport_id* trid;
	struct spdk_nvme_ctrlr*	ctrlr = nullptr;
	struct spdk_nvme_qpair* qpair = nullptr;
	const struct spdk_nvme_ctrlr_data *cdata = nullptr;
	// std::vector<struct spdk_nvme_ns*> ns;
	std::vector<nvmfnsDesc*> nsfield;
	bool attached = false;

	// total logic block count of this device
	size_t lba_count = 0;
	// lba start position of this device in the nvmf host
	size_t lba_start = 0;
	// position in the device list for this Nvmf host
	size_t position = 0;

	int read(size_t lbaPos, void* pBuf, size_t lbc);
	int write(size_t lbaPos, void* pBuf, size_t lbc);

	// async mode function
	int read(size_t lbaPos, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg);
	int write(size_t lbaPos, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg);

	int lba_judge(size_t lba);

};

class nvmfnsDesc {
public:
	nvmfnsDesc(nvmfDevice& dev, struct spdk_nvme_ns* ns);
	nvmfnsDesc() = delete;
	~nvmfnsDesc();
	nvmfDevice& dev;
	struct spdk_nvme_ns* ns;
	struct io_sequence* sequence;
	
	const struct spdk_nvme_ns_data* nsdata;
	uint32_t nsid = 0;
	uint32_t sector_size = 0; // logical block size in bytes
	uint64_t size = 0; // in bytes

	size_t lba_count = 0;
	// lba start position of this namespace in the nvmf device
	size_t lba_start = 0;
	// lba bundle in a read/write i/o
	size_t lba_bundle = 0; 
	
	// position in the namespace list for this Nvmf device
	// size_t position = 0;

	int read(size_t lbaPos, void* pBuf, size_t lbc);
	int write(size_t lbaPos, void* pBuf, size_t lbc);

	int read(size_t lba_device, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg);
	int write(size_t lba_device, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg);
};

// for each disk host
// this class will become context for each disk host
class CNvmfhost : public dpfsEngine {
public:
    CNvmfhost();
	virtual ~CNvmfhost() override;
    // CNvmfhost(const std::vector<std::string>& trid_strs);

 	virtual int attach_device(const std::string& devdesc_str) override;
 	virtual int detach_device(const std::string& devdesc_str) override;
	virtual void cleanup() override;
    virtual void set_logdir(const std::string& log_path) override;
    virtual int read(size_t lbaPos, void* pBuf, size_t len) override;
    virtual int write(size_t lbaPos, void* pBuf, size_t pBufLen) override;

    virtual int read(size_t lbaPos, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg) override;
    virtual int write(size_t lbaPos, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg) override;

    virtual int flush() override;
    virtual int sync(size_t n = 0) override;
	virtual int replace_device(const std::string& trid_str, const std::string& new_trid_str) override = delete;
	virtual void set_async_mode(bool async) override;
	virtual bool async() const override;
	virtual int copy(const dpfsEngine& tgt) override;
	virtual size_t size() const override;
	virtual void* zmalloc(size_t size) const override;
	virtual void zfree(void*) const override;
	virtual const char* name() const override { return "DPFS-NVMF-ENGINE"; }

	void hello_world();
	int device_judge(size_t lba) const;


	void register_ns(nvmfDevice *dev, struct spdk_nvme_ns *ns);

	

	logrecord log;
	static volatile size_t hostCount;
	// device's block count for all nvmf target on nvmf host
	size_t block_count;

private:
    std::vector<nvmfDevice*> devices;
	std::thread nf_guard;

	bool async_mode : 1;
	bool m_exit : 1;
	bool : sizeof(bool);


	// attach all devices to the nvmf host
    int nvmf_attach(nvmfDevice* device);

	// if device is first attached, then init it
	int init_device();

	// read device info from NVMe controller
	// this function will read the device info from NVMe controller and fill the device's describe field.
	int read_device_info();

	friend class nvmfnsDesc;
	friend class nvmfDevice;
};


