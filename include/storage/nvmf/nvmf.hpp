/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#include <threadlock.hpp>
#include <storage/engine.hpp>
#include <log/logbinary.h>
#include <list>
#include <vector>
#include <condition_variable>
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

	enum qpair_use_state : uint8_t {
		QPAIR_NOT_INITED = 0,
		QPAIR_PREPARE = 1,
		QPAIR_WAIT_COMPLETE = 2,
		QPAIR_INVALID = 3,
		QPAIR_ERROR = 4,
	};

	struct qpair_status {
		qpair_status() {
		}
		qpair_status(const qpair_status& tgt) noexcept {
			m_reqs.store(tgt.m_reqs.load());
			state = tgt.state;
		}
		qpair_status& operator=(const qpair_status& tgt) noexcept {
			m_reqs.store(tgt.m_reqs.load());
			state = tgt.state;
			return *this;
		}
		~qpair_status() = default;

		std::atomic<uint16_t> m_reqs = 0;
		volatile nvmfDevice::qpair_use_state state = QPAIR_NOT_INITED;
		CSpin m_lock;
	} ;

	// instead of using a single io qpair, we use multiple io qpairs to improve performance
	std::vector<std::pair<struct spdk_nvme_qpair*, qpair_status>> ioqpairs;
	CSpin qpLock;
	uint8_t qpair_index = 0;


	// struct spdk_nvme_qpair* qpair = nullptr;
	const struct spdk_nvme_ctrlr_data *cdata = nullptr;
	// std::vector<struct spdk_nvme_ns*> ns;
	std::vector<nvmfnsDesc*> nsfield;
	volatile bool attached = false;
	volatile bool need_reattach = false;
	bool m_exit = false;
	CSpin m_processLock;
	std::thread process_complete_thd;
	// std::atomic<uint16_t> m_reqs;
	std::condition_variable m_convar;

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

	int lba_judge(size_t lba) const noexcept;

	/*
		@param reqs total request count of nvmfDevice
		@param times this io operate count
		@param max_io_que max io queue depth
	*/
	// inline int checkReqs(int times, int max_io_que) noexcept;
	inline int nextQpair() noexcept;

	int clear();


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

	// total lba count in this namespace
	size_t lba_count = 0;
	// lba start position of this namespace in the nvmf device
	size_t lba_start = 0;
	// lba bundle in a read/write i/o
	size_t lba_bundle = 0; 
	
	// position in the namespace list for this Nvmf device
	// size_t position = 0;

	// int submit_io(void *buffer, uint64_t lba, uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags);


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
    virtual int sync() override;
	virtual int replace_device(const std::string& trid_str, const std::string& new_trid_str) override = delete;
	virtual void set_async_mode(bool async) override;
	virtual bool async() const override;
	virtual int copy(const dpfsEngine& tgt) override;
	virtual size_t size() const override;
	virtual void* zmalloc(size_t size) const override;
	virtual void zfree(void*) const override;
	virtual const char* name() const override { return "DPFS-NVMF-ENGINE"; }

	void hello_world();
	int device_judge(size_t lba) const noexcept;


	void register_ns(nvmfDevice *dev, struct spdk_nvme_ns *ns);

	

	logrecord log;
	static volatile size_t hostCount;
	// device's block count for all nvmf target on nvmf host
	size_t block_count;

private:
    std::vector<nvmfDevice*> devices;
	std::thread nf_guard;

	CSpin devices_lock;
	CSpin m_lock;
	bool async_mode : 1;
	bool m_exit : 1;
	bool m_broke : 1;
	bool m_allowOperate : 1;
	bool : 4;
	

	// attach all devices to the nvmf host
    int nvmf_attach(nvmfDevice* device);
	int reattach_device(nvmfDevice* dev);
	// if device is first attached, then init it
	int init_device();

	// read device info from NVMe controller
	// this function will read the device info from NVMe controller and fill the device's describe field.
	int read_device_info();

	friend class nvmfnsDesc;
	friend class nvmfDevice;
};

// void totalsizeee() {
// 	sizeof(logrecord); 					// 320
// 	sizeof(size_t);						// 8
// 	sizeof(std::vector<nvmfDevice*>);	// 24	
// 	sizeof(CSpin);						// 1
// 	sizeof(std::thread);				// 8
// 	sizeof(CNvmfhost);					//376
// }
