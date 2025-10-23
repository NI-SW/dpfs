/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */

#include <storage/nvmf/nvmf.hpp>
#include <spdk/vmd.h>
#include <spdk/nvme.h>
#include <spdk/nvme_zns.h>
#include <spdk/env.h>
#include <spdk/string.h>
#include <spdk/log.h>
#include <mutex>
#include <thread>

#define DPFS_IO_CHECK
static const int max_qpair_io_queue = 512;
// default timeout set to 2 seconds
static const int qpair_io_wait_tm = 2000;
static std::mutex init_mutex;
static bool initialized = false;
static bool g_vmd = false;
volatile size_t CNvmfhost::hostCount = 0;
static std::thread engineGuardThread;

static bool g_spdk_first_init = true;

void* engineGuardFunction() {
	while (CNvmfhost::hostCount > 0) {
		// std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return nullptr;
}

static inline int detach_single_device(nvmfDevice* dev, logrecord& log) {
	int rc = 0;
	dev->attached = false;
	dev->m_exit = true;
	if(dev->process_complete_thd.joinable()) {
		dev->process_complete_thd.join();
	}

	if(dev->qpair) {
		while(spdk_nvme_qpair_get_num_outstanding_reqs(dev->qpair) && dev->attached) {
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		spdk_nvme_ctrlr_free_io_qpair(dev->qpair);
		dev->qpair = nullptr;
	}
	spdk_nvme_detach_ctx *detach_ctx = nullptr;
	rc = spdk_nvme_detach_async(dev->ctrlr, &detach_ctx);
	if(rc) {
		log.log_error("spdk_nvme_detach_async failed (%d)\n", rc);
		return rc;
	}
	for(auto& nsdesc : dev->nsfield) {
		delete nsdesc;
	}

	dev->nsfield.clear();

	if (detach_ctx) {
		spdk_nvme_detach_poll(detach_ctx);
	}
	return rc;


}

/*
	@param reqs total request count of nvmfDevice
	@param times this io operate count
	@param max_io_que max io queue depth
*/
// static inline int checkReqs(const std::atomic<uint16_t>& reqs, int times, int max_io_que) noexcept {
// 	std::chrono::milliseconds ms =  std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock().now().time_since_epoch());

// 	// if no more space in queue, polling for new space
// 	while(reqs >= max_io_que - times) {
// 		// polling wait for some time.
// 		if((std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock().now().time_since_epoch()) - ms).count() > qpair_io_wait_tm) {
// 			// printf("wait once\n"); // about 70 ms
// 			return -ETIMEDOUT;
// 		}
// 	}
// 	return 0;
// }

inline int nvmfDevice::checkReqs(std::atomic<uint16_t>& reqs, int times, int max_io_que) noexcept {
	std::chrono::milliseconds ms =  std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock().now().time_since_epoch());
	int rc = 0;
	// if no more space in queue, polling for new space
	while(reqs >= max_io_que - times) {
		// polling wait for some time.
		if((std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock().now().time_since_epoch()) - ms).count() > qpair_io_wait_tm) {
			// printf("wait once\n"); // about 70 ms
			return -ETIMEDOUT;
		}
	}
	return 0;
}

int nvmfDevice::clear() {
	int rc = 0;
	if(attached) {
		attached = false;
	}

	m_processLock.lock();
	
	if(qpair) {
		rc = spdk_nvme_ctrlr_free_io_qpair(qpair);
		if(rc) {
			nfhost.log.log_error("spdk_nvme_ctrlr_free_io_qpair failed (%d)\n", rc);
			
		}
		qpair = nullptr;
	}


	m_processLock.unlock();

	// free all ns
	for(auto& ns : nsfield) {
		spdk_nvme_ctrlr_detach_ns(ctrlr, ns->nsid, nullptr);
		delete ns;
	}

	nsfield.clear();
	
	
	struct spdk_nvme_detach_ctx* detach_ctx = nullptr;

	rc = spdk_nvme_detach_async(ctrlr, &detach_ctx);
	if (rc) {
		nfhost.log.log_inf("spdk_nvme_detach_async failed (%d)\n", rc);
	}
	ctrlr = nullptr;
	nfhost.log.log_inf("Controller %s detached\n", trid->traddr);


	if (detach_ctx) {
		spdk_nvme_detach_poll(detach_ctx);
	}
    

	return rc;
}

constexpr char eng_name[] = "DPFS-NVMF-ENGINE";

int initSpdk() {
	/*
	* SPDK relies on an abstraction around the local environment
	* named env that handles memory allocation and PCI device operations.
	* This library must be initialized first.
	*
	*/
	int rc = 0;
	if(g_spdk_first_init) {
		struct spdk_env_opts opts;
		
		opts.opts_size = sizeof(opts);
		spdk_env_opts_init(&opts);

		opts.name = eng_name;
		rc = spdk_env_init(&opts);
		g_spdk_first_init = false;
	} else {
		rc = spdk_env_init(nullptr);
	}
	if (rc < 0) {
		printf("Unable to initialize SPDK env\n");
		return rc;
	}

	printf("Initializing NVMe Controllers\n");
	rc = spdk_vmd_init();
	if (g_vmd && rc) {
		printf("Failed to initialize VMD."
			" Some NVMe devices can be unavailable.\n");
			return rc;
	}

	return rc;
}

struct io_sequence {
	nvmfnsDesc* 	ns;
	volatile bool   is_completed;
};

static void read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
	nvmfnsDesc* ns = (nvmfnsDesc *)arg;

	if(!ns->dev.nfhost.async()) {
		ns->sequence->is_completed = true;
	}

	/* See if an error occurred. If so, display information
	 * about it, and set completion value so that I/O
	 * caller is aware that an error occurred.
	 */
	if (spdk_nvme_cpl_is_error(completion)) {
		ns->dev.nfhost.log.log_error("I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		ns->dev.nfhost.log.log_error("Read I/O failed, aborting run\n");
		spdk_nvme_qpair_print_completion(ns->dev.qpair, (struct spdk_nvme_cpl *)completion);
		// exit(1);
	}
	

}

static void write_complete(void *arg, const struct spdk_nvme_cpl *completion) {
	nvmfnsDesc* ns = (nvmfnsDesc *)arg;
	ns->dev.nfhost.log.log_debug("write I/O completed\n");
	if(!ns->dev.nfhost.async()) {
		ns->sequence->is_completed = true;
	}
	/* See if an error occurred. If so, display information
	 * about it, and set completion value so that I/O
	 * caller is aware that an error occurred.
	 */
	if (spdk_nvme_cpl_is_error(completion)) {
		ns->dev.nfhost.log.log_error("I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		ns->dev.nfhost.log.log_error("Write I/O failed, aborting run\n");
		abort();
		spdk_nvme_qpair_print_completion(ns->dev.qpair, (struct spdk_nvme_cpl *)completion);
		// exit(1);
	}
	/*
	 * The write I/O has completed.  Free the buffer associated with
	 *  the write I/O and allocate a new zeroed buffer for reading
	 *  the data back from the NVMe namespace.
	 */
	
}

nvmfnsDesc::nvmfnsDesc(nvmfDevice& dev, struct spdk_nvme_ns* ns) : dev(dev), ns(ns) {
	nsdata = spdk_nvme_ns_get_data(ns);
	nsid = spdk_nvme_ns_get_id(ns);
	sector_size = spdk_nvme_ns_get_sector_size(ns);
	size = spdk_nvme_ns_get_size(ns);

	if(dpfs_lba_size % sector_size != 0) {
		delete this;
		throw std::runtime_error("NVMf namespace sector size is not suitable for dpfs_lba_size.\n");
	}

	lba_bundle = dpfs_lba_size / sector_size;

	// inner lba range [0, blockCount - 1]
	lba_count = size / sector_size / lba_bundle; 
	lba_start = dev.lba_count;
	sequence = new io_sequence;
	sequence->ns = this;
	sequence->is_completed = false;

}

nvmfnsDesc::~nvmfnsDesc() {
	if(sequence) {
		delete sequence;
	}
}

int nvmfnsDesc::read(size_t lba_device, void* pBuf, size_t lbc) {
	/*
	* map device lba to namespace lba
	*/
	size_t lba_ns = (lba_device - lba_start) * lba_bundle;
	size_t lba_ns_count = lba_bundle * lbc;
	dev.nfhost.log.log_debug("ns: %p qpair: %p sequence: %p Reading from LBA %zu, lba_ns %zu, lbc size %zu, lba_count: %zu\n", ns, dev.qpair, sequence, lba_device, lba_ns, lbc, lba_ns_count);
	
	dev.qpLock.lock();
	int rc = spdk_nvme_ns_cmd_read(ns, dev.qpair, pBuf, lba_ns, lba_ns_count, read_complete, this, 0);
	dev.qpLock.unlock();
	
	return rc;

}

int nvmfnsDesc::write(size_t lba_device, void* pBuf, size_t lbc) {
	size_t lba_ns = (lba_device - lba_start) * lba_bundle;
	size_t lba_ns_count = lba_bundle * lbc;
	dev.nfhost.log.log_debug("ns: %p qpair: %p sequence: %p Writing to LBA %zu, lba_ns %zu, lbc size %zu, lba_count: %zu\n", ns, dev.qpair, sequence, lba_device, lba_ns, lbc, lba_ns_count);
	
	dev.qpLock.lock();
	int rc = spdk_nvme_ns_cmd_write(ns, dev.qpair, pBuf, lba_ns, lba_ns_count, write_complete, this, 0);
	dev.qpLock.unlock();

	return rc;
}

static void async_compelete(void *arg, const struct spdk_nvme_cpl *completion) {
	dpfs_engine_cb_struct* cbs = (dpfs_engine_cb_struct*)arg;
	dpfs_compeletion dcp {0, 0};
	if(spdk_nvme_cpl_is_error(completion)) {
		dcp.errMsg = spdk_nvme_cpl_get_status_string(&completion->status);
		dcp.return_code = completion->status.sc;
	}
	cbs->m_cb(cbs->m_arg, &dcp);
	return;
}

int nvmfnsDesc::write(size_t lba_device, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg) {
	size_t lba_ns = (lba_device - lba_start) * lba_bundle;
	size_t lba_ns_count = lba_bundle * lbc;
	dev.nfhost.log.log_debug("ns: %p qpair: %p sequence: %p Writing to LBA %zu, lba_ns %zu, lbc size %zu, lba_count: %zu\n", ns, dev.qpair, sequence, lba_device, lba_ns, lbc, lba_ns_count);
	dev.qpLock.lock();
	int rc = spdk_nvme_ns_cmd_write(ns, dev.qpair, pBuf, lba_ns, lba_ns_count, async_compelete, arg, 0);
	dev.qpLock.unlock();
	return rc;
}

int nvmfnsDesc::read(size_t lba_device, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg) {
	/*
	* map device lba to namespace lba
	*/
	size_t lba_ns = (lba_device - lba_start) * lba_bundle;
	size_t lba_ns_count = lba_bundle * lbc;
	dev.nfhost.log.log_debug("ns: %p qpair: %p sequence: %p Reading from LBA %zu, lba_ns %zu, lbc size %zu, lba_count: %zu\n", ns, dev.qpair, sequence, lba_device, lba_ns, lbc, lba_ns_count);
	
	dev.qpLock.lock();
	int rc = spdk_nvme_ns_cmd_read(ns, dev.qpair, pBuf, lba_ns, lba_ns_count, async_compelete, arg, 0);
	dev.qpLock.unlock();
	return rc;


}


nvmfDevice::nvmfDevice(CNvmfhost& host) : nfhost(host) {
	attached = false;
	m_exit = false;
	trid = new spdk_nvme_transport_id;
	m_reqs.store(0);

	process_complete_thd = std::thread([this](){
		// process complete lock
		std::mutex pclck;
		std::unique_lock lk(pclck);

		// uint32_t n = 0;
		int32_t rc = 0;
		while(!m_exit) {
			while(attached && qpair) {
				m_convar.wait_for(lk, std::chrono::milliseconds(100), [this]() -> bool { return (this->m_reqs > 0); });
				
				// this->nfhost.log.log_debug("waked!\n");
				for(int i = 0; i < 1000; ++i) {
					if(m_reqs) {
						i = 0;
					}
					while(m_reqs) {
						m_processLock.lock();
						if(qpair == nullptr) {
							m_processLock.unlock();
							break;
						}
						qpLock.lock();
						rc = spdk_nvme_qpair_process_completions(qpair, m_reqs);
						qpLock.unlock();
						if(rc > 0) {
							m_reqs -= rc;
						} else if(rc == 0) {

						} else {
							// TODO process error
							nfhost.log.log_error("process completions fail! code : %d, unfinished requests : %d, reattaching device...\n", rc, m_reqs.load());
							attached = false;
							m_processLock.unlock();
							need_reattach = true;
							goto outLoop;
							// // realloc io queue pair
							// rc = spdk_nvme_ctrlr_free_io_qpair(qpair);
							// if(rc) {
							// 	nfhost.log.log_error("spdk_nvme_ctrlr_free_io_qpair() failed (%d)\n", rc);
							// }

							// qpair = nullptr;

							// spdk_nvme_io_qpair_opts qpopts;
							// spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &qpopts, sizeof(qpopts));
							// qpopts.io_queue_size = max_qpair_io_queue;
							// this->nfhost.log.log_inf("realloc qpair, get qpair opts: io_queue_size %u\n", qpopts.io_queue_size);
							
							// qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, &qpopts, sizeof(qpopts));
							// if (qpair == nullptr) {
							// 	this->nfhost.log.log_error("spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
							// }
							// m_reqs = 0;
						}
						
						m_processLock.unlock();
					}
				}
				
			}
		outLoop:
			// waiting for attach
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	});

}

nvmfDevice::~nvmfDevice() {
	attached = false;
	m_exit = true;
	if(process_complete_thd.joinable()) {
		process_complete_thd.join();
	}
	delete trid;
}

int nvmfDevice::lba_judge(size_t lba) {
	/*
	* judge lba in which namespace
	*/
	int rc = 0;
	for (auto& nsd : nsfield) {
		if(nsd->lba_start <= lba) {
			++rc;
			continue;
		}
		return rc - 1;
	}
	return rc - 1; // not found
}

int nvmfDevice::read(size_t lba_host, void* pBuf, size_t lbc) {
	/*
	* map host lba to device lba
	*/
	int rc;
	if(!attached || m_exit) {
		return -EPERM;
	}

	m_convar.notify_one();

	size_t lba_device = lba_host - lba_start;
	size_t nsPos = lba_judge(lba_device);
	nvmfnsDesc* ns;

	if(nsPos >= nsfield.size()) {
		nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());
		rc = -ERANGE;
		goto errReturn;
	}

	rc = 0;
	ns = nsfield[nsPos];
	ns->sequence->is_completed = false;
	nfhost.log.log_debug("dev: Reading from LBA %zu, lba_dev %zu, lba_count %zu, buffer size %zu\n", lba_host, lba_device, lbc, lbc * dpfs_lba_size);

	if(lba_device + lbc > ns->lba_count + ns->lba_start) {
		// cross ns write
		// next ns start lba


		if(nsPos + 1 >= nsfield.size()) {
			nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());

			goto errReturn;
		}

		size_t pBuf_next_lbc = lba_device + lbc - ns->lba_count;
		size_t lba_next_ns = ns->lba_start + ns->lba_count;
		void* next_start_pbuf = (char*)pBuf + ((lbc - pBuf_next_lbc) * dpfs_lba_size);
		lbc -= pBuf_next_lbc;

		
		nvmfnsDesc* subns;
		subns = nsfield[nsPos + 1];
		subns->sequence->is_completed = false;

		rc = checkReqs(m_reqs, 2, max_qpair_io_queue);
		if(rc) {
			nfhost.log.log_error("Failed to read from ns %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}
		
		rc = subns->read(lba_next_ns, next_start_pbuf, pBuf_next_lbc);
		if(rc != 0) {
			nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}


		rc = ns->read(lba_device, pBuf, lbc);
		if(rc != 0) {
			m_reqs += 1;
			nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos, rc);
			goto errReturn;
		}

		m_reqs += 2;

		if(!nfhost.async_mode) {
			// wait for completion
			while(!subns->sequence->is_completed) {

			}
		}

		if(!nfhost.async_mode) {
			// wait for completion
			while(!ns->sequence->is_completed) {

			}
		}

		return 2;
	}

	rc = checkReqs(m_reqs, 1, max_qpair_io_queue);
	if(rc) {
		nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
		goto errReturn;
	}

	rc = ns->read(lba_device, pBuf, lbc);
	if(rc != 0) {
		nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos, rc);
		return rc;
	}
	m_reqs += 1;
	if(!nfhost.async_mode) {
		// wait for completion
		while(!ns->sequence->is_completed) {

		}
	}

	return 1;

errReturn:

	return rc;
}

int nvmfDevice::write(size_t lba_host, void* pBuf, size_t lbc) {
	int rc;
	if(!attached || m_exit) {
		return -EPERM;
	}

	m_convar.notify_one();

	size_t lba_device = lba_host - lba_start;
	size_t nsPos = lba_judge(lba_device);
	nvmfnsDesc* ns;

	if(nsPos >= nsfield.size()) {
		nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());
		rc = -ERANGE;
		goto errReturn;
	}

	rc = 0;
	ns = nsfield[nsPos];
	ns->sequence->is_completed = false;
	nfhost.log.log_debug("dev: Writing to LBA %zu, lba_dev %zu, lba_count %zu, buffer size %zu\n", lba_host, lba_device, lbc, lbc * dpfs_lba_size);

	if(lba_device + lbc > ns->lba_count + ns->lba_start) {
		// cross ns write
		// next ns start lba

		if(nsPos + 1 >= nsfield.size()) {
			nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());
			rc = -ERANGE;
			goto errReturn;
		}
		size_t pBuf_next_lbc = lba_device + lbc - ns->lba_count;
		size_t lba_next_ns =  ns->lba_start + ns->lba_count;
		void* next_start_pbuf = (char*)pBuf + ((lbc - pBuf_next_lbc) * dpfs_lba_size);
		lbc -= pBuf_next_lbc;

		nvmfnsDesc* subns;
		subns = nsfield[nsPos + 1];
		subns->sequence->is_completed = false;
		
		rc = checkReqs(m_reqs, 2, max_qpair_io_queue);
		if(rc) {
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}

		rc = subns->write(lba_next_ns, next_start_pbuf, pBuf_next_lbc);
		if(rc != 0) {
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}


		rc = ns->write(lba_device, pBuf, lbc);
		if(rc != 0) {
			m_reqs += 1;
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
			goto errReturn;
		}

		m_reqs += 2;

		if(!nfhost.async_mode) {
			// wait for completion
			while(!subns->sequence->is_completed) {

			}
		}

		if(!nfhost.async_mode) {
			// wait for completion
			while(!ns->sequence->is_completed) {

			}
		}

		return 2;
	}

	rc = checkReqs(m_reqs, 1, max_qpair_io_queue);
	if(rc) {
		m_reqs -= 1;
		nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
		goto errReturn;
	}

	rc = ns->write(lba_device, pBuf, lbc);
	if(rc != 0) {
		nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
		goto errReturn;
	}
	m_reqs += 1;
	if(!nfhost.async_mode) {
		// wait for completion
		while(!ns->sequence->is_completed) {

		}

	}

	return 1;

errReturn:

	return rc;
}

int nvmfDevice::read(size_t lba_host, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg) {
	/*
	* map host lba to device lba
	*/
	int rc;
	if(!attached || m_exit) {
		return -EPERM;
	}

	m_convar.notify_one();

	size_t lba_device = lba_host - lba_start;
	size_t nsPos = lba_judge(lba_device);
	nvmfnsDesc* ns;

	if(nsPos >= nsfield.size()) {
		nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());
		rc = -ERANGE;
		goto errReturn;
	}

	rc = 0;
	ns = nsfield[nsPos];
	nfhost.log.log_debug("dev: Reading from LBA %zu, lba_dev %zu, lba_count %zu, buffer size %zu\n", lba_host, lba_device, lbc, lbc * dpfs_lba_size);

	if(lba_device + lbc > ns->lba_count + ns->lba_start) {
		// cross ns write
		// next ns start lba

		if(nsPos + 1 >= nsfield.size()) {
			nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());
			rc = -ERANGE;
			goto errReturn;
		}


		size_t pBuf_next_lbc = lba_device + lbc - ns->lba_count;
		size_t lba_next_ns = ns->lba_start + ns->lba_count;
		void* next_start_pbuf = (char*)pBuf + ((lbc - pBuf_next_lbc) * dpfs_lba_size);
		lbc -= pBuf_next_lbc;
		
		nvmfnsDesc* subns;
		subns = nsfield[nsPos + 1];

		rc = checkReqs(m_reqs, 2, max_qpair_io_queue);
		if(rc) {
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}

		rc = subns->read(lba_next_ns, next_start_pbuf, pBuf_next_lbc, arg);
		if(rc != 0) {
			nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}

		rc = ns->read(lba_device, pBuf, lbc, arg);
		if(rc != 0) {
			m_reqs += 1;
			nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos, rc);
			goto errReturn;
		}
		m_reqs += 2;
		return 2;
	}

	rc = checkReqs(m_reqs, 1, max_qpair_io_queue);
	if(rc) {
		nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
		goto errReturn;
	}

	rc = ns->read(lba_device, pBuf, lbc, arg);
	if(rc != 0) {
		nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos, rc);
		return rc;
	}
	m_reqs += 1;

	return 1;

errReturn:

	return rc;
}

int nvmfDevice::write(size_t lba_host, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg) {
	int rc;
	if(!attached || m_exit) {
		return -EPERM;
	}

	m_convar.notify_one();

	size_t lba_device = lba_host - lba_start;
	size_t nsPos = lba_judge(lba_device);
	nvmfnsDesc* ns;

	if(nsPos >= nsfield.size()) {
		nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());
		rc = -ERANGE;
		goto errReturn;
	}


	rc = 0;
	ns = nsfield[nsPos];
	nfhost.log.log_debug("dev: Writing to LBA %zu, lba_dev %zu, lba_count %zu, buffer size %zu\n", lba_host, lba_device, lbc, lbc * dpfs_lba_size);

	if(lba_device + lbc > ns->lba_count + ns->lba_start) {
		// cross ns write

		if(nsPos + 1 >= nsfield.size()) {
			nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());
			rc = -ERANGE;
			goto errReturn;
		}

		// next ns start lba
		size_t pBuf_next_lbc = lba_device + lbc - ns->lba_count;
		size_t lba_next_ns =  ns->lba_start + ns->lba_count;
		void* next_start_pbuf = (char*)pBuf + ((lbc - pBuf_next_lbc) * dpfs_lba_size);
		lbc -= pBuf_next_lbc;



		nvmfnsDesc* subns;
		subns = nsfield[nsPos + 1];

		rc = checkReqs(m_reqs, 2, max_qpair_io_queue);
		if(rc) {
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}

		rc = subns->write(lba_next_ns, next_start_pbuf, pBuf_next_lbc, arg);
		if(rc != 0) {
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}

		rc = ns->write(lba_device, pBuf, lbc, arg);
		if(rc != 0) {
			m_reqs += 1;
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
			goto errReturn;
		}
		m_reqs += 2;
		return 2;
	}

	rc = checkReqs(m_reqs, 1, max_qpair_io_queue);
	if(rc) {
		nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
		goto errReturn;
	}

	rc = ns->write(lba_device, pBuf, lbc, arg);
	if(rc != 0) {
		nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
		goto errReturn;
	}
	m_reqs += 1;

	return 1;
errReturn:

	return rc;

}


static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, struct spdk_nvme_ctrlr_opts *opts) {
    nvmfDevice *device = (nvmfDevice *)cb_ctx;
	CNvmfhost *nfhost = &device->nfhost;
	nfhost->log.log_inf("Attaching to %s, ops: %p\n", trid->traddr, opts);
	return true;
}

static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts) {
	uint32_t nsid;
	struct spdk_nvme_ns *ns;
	nvmfDevice* device = (nvmfDevice*)cb_ctx;
	CNvmfhost* fh = &device->nfhost;

	device->ctrlr = ctrlr;
	device->attached = true;

	// entry = (ctrlr_entry *)malloc(sizeof(struct ctrlr_entry));
	// if (entry == NULL) {
	// 	perror("ctrlr_entry malloc");
	// 	exit(1);
	// }

	

	/*
	 * spdk_nvme_ctrlr is the logical abstraction in SPDK for an NVMe
	 *  controller.  During initialization, the IDENTIFY data for the
	 *  controller is read using an NVMe admin command, and that data
	 *  can be retrieved using spdk_nvme_ctrlr_get_data() to get
	 *  detailed information on the controller.  Refer to the NVMe
	 *  specification for more details on IDENTIFY for NVMe controllers.
	 */
	device->cdata = spdk_nvme_ctrlr_get_data(ctrlr);


	/*
	 * Each controller has one or more namespaces.  An NVMe namespace is basically
	 *  equivalent to a SCSI LUN.  The controller's IDENTIFY data tells us how
	 *  many namespaces exist on the controller.  For Intel(R) P3X00 controllers,
	 *  it will just be one namespace.
	 *
	 * Note that in NVMe, namespace IDs start at 1, not 0.
	 */
	for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0; nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
		ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
		if (ns == nullptr) {
			continue;
		}

		device->nsfield.emplace_back(new nvmfnsDesc(*device, ns));
		device->lba_count += (device->nsfield.back()->size / dpfs_lba_size); // adjust 4 KB block size
		fh->register_ns(device, ns);
	}
	if(device->nsfield.empty()) {
		fh->log.log_error("No namespaces found on controller %s\n", trid->traddr);
		spdk_nvme_detach(ctrlr);
		exit(1);
	}
	fh->block_count += device->lba_count;
	fh->log.log_inf("Attached to %s opts : %p\n", trid->traddr, opts);
}

static void remove_cb(void *cb_ctx, struct spdk_nvme_ctrlr *ctrlr) {
	nvmfDevice* device = (nvmfDevice*)cb_ctx;
	CNvmfhost* fh = &device->nfhost;

	// const spdk_nvme_transport_id* trid = spdk_nvme_ctrlr_get_transport_id(ctrlr);
	
	// for(auto& dev : fh->devices) {
	// 	if(dev->trid == trid) {
	// 		fh->log.log_inf("Controller %s removed\n", dev->trid->traddr);
	// 		dev->attached = false;
	// 		// throw std::runtime_error("double attach occur!");
	// 		break;
	// 	}
	// }

    fh->log.log_notic("Controller %s removed, ctrlr : %p\n", device->devdesc_str.c_str(), ctrlr);

}

CNvmfhost::CNvmfhost() : async_mode(false) {
	init_mutex.lock();
	++hostCount;
	if(initialized) {

	} else {
		initialized = true;
		int rc = initSpdk();
		if (rc != 0) {
			log.log_fatal("Failed to initialize SPDK environment: %d\n", rc);
			// throw std::runtime_error("Failed to initialize SPDK environment");
			exit(1);
		}
		// engineGuardThread = std::thread(engineGuardFunction);
	}
	init_mutex.unlock();

	block_count = 0;
	m_exit = false;
	devices.clear();
	m_allowOperate = true;

	nf_guard = std::thread([this]() {
		int32_t rc = 0;
		// keep alive for ctrlr
		while(!m_exit) {
			devices_lock.lock();
			for(auto device : devices) {
				if(device) {
					if(device->attached) {
						rc = spdk_nvme_ctrlr_process_admin_completions(device->ctrlr);
						if(rc < 0) {
							// maybe need to record err info
							device->attached = false;
							device->clear();
							reattach_device(device);
						}
					} else if(device->need_reattach) {
						device->attached = false;
						device->clear();
						rc = reattach_device(device);

						if(!rc) {
							device->need_reattach = false;
						}
					}
				}
			}
			devices_lock.unlock();

			if(m_broke) {
				// TODO

				log.log_fatal("device or transport broken, need restart.\n");
			}

			// wait for 3 seconds
			for(int i = 0; i < 10; ++i) {
				if(m_exit) {
					return;
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(300));
			}
		}
	});
}

CNvmfhost::~CNvmfhost() {
	m_exit = true;
    // Cleanup all controllers and namespaces
	if(nf_guard.joinable())
		nf_guard.join();
		
	cleanup();
    log.log_notic("CNvmfhost destroyed\n");

	init_mutex.lock();
	--hostCount;
	if (hostCount == 0) {
		// do not release spdk resource, for other nvmf host may use it, reinit spdk env may cause crash (dpdk mem map error)

		// spdk_vmd_fini();
		// spdk_env_fini();
		// initialized = false;
		// engineGuardThread.join();
	}
	init_mutex.unlock();
}

int CNvmfhost::nvmf_attach(nvmfDevice* device) {
    if (device->attached) {
        log.log_notic("transport addresses already attached\n");
        return 1;
    }
    int rc = 0;


	rc = spdk_nvme_probe(device->trid, device, probe_cb, attach_cb, remove_cb);
	if (rc != 0) {
		log.log_error("spdk_nvme_probe failed (%d)\n", rc);
		return rc;
	}

    return rc;
}

void CNvmfhost::cleanup() {
	// struct ns_entry *ns_entry, *tmp_ns_entry;
	// struct ctrlr_entry *ctrlr_entry, *tmp_ctrlr_entry;
	if (devices.empty()) {
		log.log_notic("No devices to cleanup\n");
		return;
	}

	struct spdk_nvme_detach_ctx** detach_ctx = new spdk_nvme_detach_ctx* [devices.size()] { nullptr };
	int rc = 0;
	int i = 0;

	sync();
	// remove all namespaces and controllers
	for(auto& dev : devices) {
		if (dev->attached) {
			dev->attached = false;
			dev->m_exit = true;
			if(dev->process_complete_thd.joinable()) {
				dev->process_complete_thd.join();
			}

			if(dev->qpair) {
				spdk_nvme_ctrlr_free_io_qpair(dev->qpair);
				dev->qpair = nullptr;
			}

			rc = spdk_nvme_detach_async(dev->ctrlr, &detach_ctx[i++]);
			if (rc) {
				log.log_inf("spdk_nvme_detach_async failed (%d)\n", rc);
				continue;
			}
			for(auto& nsdesc : dev->nsfield) {
				delete nsdesc;
			}
			dev->nsfield.clear();
			log.log_inf("Controller %s detached\n", dev->trid->traddr);

			// lock for nsguard thread
			devices_lock.lock();
			delete dev;
			dev = nullptr;
			devices_lock.unlock();
		} else {
			log.log_notic("Controller %s already detached\n", dev->trid->traddr);
		}
	}

    for(int j = 0; j < i; ++j) {
        if (detach_ctx[j]) {
            spdk_nvme_detach_poll(detach_ctx[j]);
        }
    }

    delete[] detach_ctx;

	devices_lock.lock();
	devices.clear();
	devices_lock.unlock();
}

void CNvmfhost::register_ns(nvmfDevice* dev, struct spdk_nvme_ns* ns) {
	
	log.log_debug("dev : %s\nNamespace ID: %d size: %juGB = %lluB\n", dev->devdesc_str, spdk_nvme_ns_get_id(ns), spdk_nvme_ns_get_size(ns) / 1024/1024/1024, spdk_nvme_ns_get_size(ns));
	log.log_debug("nsid : %u  sector_size : %u  size: %llu\n", spdk_nvme_ns_get_id(ns), spdk_nvme_ns_get_sector_size(ns), spdk_nvme_ns_get_size(ns));

}

// static const std::vector<std::string> spdk_trans_type {"pcie", "rdma", "tcp", "unknow"};
// static const std::string& trtypeCvt(spdk_nvme_transport_type type) {
// 	if(type == SPDK_NVME_TRANSPORT_PCIE) {
// 		return spdk_trans_type[0];
// 	} else if (type == SPDK_NVME_TRANSPORT_RDMA) {
// 		return spdk_trans_type[1];
// 	} else if (type == SPDK_NVME_TRANSPORT_TCP) {
// 		return spdk_trans_type[2];
// 	}
// 	return spdk_trans_type.back();
// };

void CNvmfhost::set_logdir(const std::string& log_path) {
	log.set_log_path(log_path);
	// log.set_log_path(log_path + "nvmfhost_" + trtypeCvt(devices[0]->trid->trtype) + "_" + devices[0]->trid->traddr + ".log");
}

void CNvmfhost::set_async_mode(bool async) {
	async_mode = async;
}

bool CNvmfhost::async() const {
	return async_mode;
}

int CNvmfhost::attach_device(const std::string& devdesc_str) {
	int rc = 0;
	nvmfDevice* ndev = new nvmfDevice(*this);

	ndev->devdesc_str = devdesc_str;
	rc = spdk_nvme_transport_id_parse(ndev->trid, devdesc_str.c_str());
    if (rc != 0) {
        log.log_error("Error parsing transport address\n");
		delete ndev;
        return rc;
    }
	
    rc = nvmf_attach(ndev);
	if(rc) {
		log.log_error("Failed to attach device %s\n", devdesc_str.c_str());
		delete ndev;
		return rc;
	}

	if(devices.empty()) {
		ndev->lba_start = 0;
		ndev->position = 0;
	} else {
		ndev->lba_start = devices.back()->lba_start + devices.back()->lba_count;
		ndev->position = devices.back()->position + 1;
	}
	spdk_nvme_io_qpair_opts qpopts;
	spdk_nvme_ctrlr_get_default_io_qpair_opts(ndev->ctrlr, &qpopts, sizeof(qpopts));
	// qpopts.io_queue_size = 4096;
	qpopts.io_queue_size = max_qpair_io_queue;
	log.log_inf("get qpair opts: io_queue_size %u\n", qpopts.io_queue_size);

	
	ndev->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ndev->ctrlr, &qpopts, sizeof(qpopts));
	if (ndev->qpair == nullptr) {
		log.log_error("spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
		delete ndev;
		return -ENOMEM;
	}

	log.log_inf("Controller %s attached with qpair %p\n", ndev->trid->traddr, ndev->qpair);
	log.log_inf("qpair opts: io_queue_size %u\n", qpopts.io_queue_size);

	devices.emplace_back(ndev);
	return rc;
	// spdk_nvme_ns_is_active(ns)
}

int CNvmfhost::detach_device(const std::string& devdesc_str) {
	int rc = 0;
	for(auto dev = devices.begin(); dev != devices.end(); ++dev) {
		if((*dev)->devdesc_str == devdesc_str) {
			if((*dev)->attached) {
				(*dev)->attached = false;
				(*dev)->m_exit = true;
				if((*dev)->process_complete_thd.joinable()) {
					(*dev)->process_complete_thd.join();
				}

				if((*dev)->qpair) {
					while(spdk_nvme_qpair_get_num_outstanding_reqs((*dev)->qpair)) {
						spdk_nvme_qpair_process_completions((*dev)->qpair, 0);
						// std::this_thread::sleep_for(std::chrono::milliseconds(10));
					}
					spdk_nvme_ctrlr_free_io_qpair((*dev)->qpair);
					(*dev)->qpair = nullptr;
				}
				spdk_nvme_detach_ctx *detach_ctx = nullptr;
				rc = spdk_nvme_detach_async((*dev)->ctrlr, &detach_ctx);
				if(rc) {
					log.log_error("spdk_nvme_detach_async failed (%d)\n", rc);
					return rc;
				}
				for(auto& nsdesc : (*dev)->nsfield) {
					delete nsdesc;
				}

				(*dev)->nsfield.clear();

				if (detach_ctx) {
					spdk_nvme_detach_poll(detach_ctx);
				}


				log.log_inf("Device %s detached\n", (*dev)->devdesc_str.c_str());
			} else {
				log.log_notic("Device %s already detached\n", (*dev)->devdesc_str.c_str());
			}
			devices_lock.lock();
			delete *dev;
			devices.erase(dev);
			devices_lock.unlock();
			return rc;
		}
	}
	return rc;
}

int CNvmfhost::device_judge(size_t lba) const {
	int rc = 0;
	for(auto& dev : devices) {
		if(dev->lba_start <= lba) {
			++rc;
			continue;
		}
		return rc - 1;
	}
	return rc - 1;
}

int CNvmfhost::read(size_t lba, void* pBuf, size_t lbc) { 

	int rc = 0;
	int req_count = 0;
#ifdef DPFS_IO_CHECK
	log.log_debug("host: Reading from LBA %zu\n", lba);
	if(lba + lbc > block_count) {
		log.log_error("LBA %zu is out of range, total blocks: %zu\n", lba, block_count);
		return -EINVAL;
	}
	if(pBuf == nullptr) {
		log.log_error("Buffer pointer is null\n");
		return -EINVAL;
	}
	if(lbc == 0) {
		log.log_error("logic block is zero\n");
		return -EINVAL;
	}
#endif

	size_t devPos = device_judge(lba);
	nvmfDevice* dev = devices[devPos];

	// cross device read
	size_t op_lba_count = lbc;
	size_t lba_device = lba - dev->lba_start;
	if(lba_device + op_lba_count > dev->lba_count + dev->lba_start) {
		// next device read lb count
		size_t lba_next_dev_count = lba_device + op_lba_count - dev->lba_count;
		// next device read start lba
		size_t lba_next_dev = lba + op_lba_count - lba_next_dev_count;
		void* next_start_pbuf = (char*)pBuf + ((dev->lba_count - lba_device) * dpfs_lba_size);
		size_t pBuf_next_lbc = lbc - ((dev->lba_count - lba_device));
		lbc -= pBuf_next_lbc;

		rc = devices[devPos + 1]->read(lba_next_dev, next_start_pbuf, pBuf_next_lbc);
		if(rc < 0) {
			log.log_error("Failed to read from next device %zu, rc: %d\n", devPos + 1, rc);
			return rc;
		}
		++req_count;
	}

	rc = dev->read(lba, pBuf, lbc);
	if(rc < 0) {
		log.log_error("Failed to read from device %zu, rc: %d\n", devPos, rc);
		return rc;
	}
	

	return ++req_count;
}

int CNvmfhost::write(size_t lba, void* pBuf, size_t lbc) {
	int rc = 0;
	int req_count = 0;
#ifdef DPFS_IO_CHECK
	log.log_debug("host: Writing to LBA %zu, buffer size %zu\n", lba, lbc * dpfs_lba_size);

	if(lba + lbc > block_count) {
		log.log_error("LBA %zu is out of range, total blocks: %zu\n", lba, block_count);
		return -EINVAL;
	}
	if(pBuf == nullptr) {
		log.log_error("Buffer pointer is null\n");
		return -EINVAL;
	}
	if(lbc == 0) {
		log.log_error("logic block is zero\n");
		return -EINVAL;
	}
#endif

	size_t devPos = device_judge(lba);
	nvmfDevice* dev = devices[devPos];
	size_t op_lba_count = lbc;
	size_t lba_device = lba - dev->lba_start;
	// cross device write
	if(lba_device + op_lba_count > dev->lba_count + dev->lba_start) {
		// next device read lb count
		size_t lba_next_dev_count = lba_device + op_lba_count - dev->lba_count;
		// next device read start lba
		size_t lba_next_dev = lba + op_lba_count - lba_next_dev_count;
		void* next_start_pbuf = (char*)pBuf + ((dev->lba_count - lba_device) * dpfs_lba_size);
		size_t pBuf_next_lbc = lbc - ((dev->lba_count - lba_device));
		lbc -= pBuf_next_lbc;

		rc = devices[devPos + 1]->write(lba_next_dev, next_start_pbuf, pBuf_next_lbc);
		if(rc < 0) {
			log.log_error("Failed to write to next device %zu, rc: %d\n", devPos + 1, rc);
			return rc;
		}
		++req_count;
	}

	rc = dev->write(lba, pBuf, lbc);
	if(rc < 0) {
		log.log_error("Failed to write to device %zu, rc: %d\n", devPos, rc);
		return rc;
	}
	
	return ++req_count;
}

int CNvmfhost::read(size_t lba, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg) { 

	int rc = 0;
	int req_count = 0;
#ifdef DPFS_IO_CHECK
	log.log_debug("host: Reading from LBA %zu\n", lba);
	if(lba + lbc > block_count) {
		log.log_error("LBA %zu is out of range, total blocks: %zu\n", lba, block_count);
		return -EINVAL;
	}
	if(pBuf == nullptr) {
		log.log_error("Buffer pointer is null\n");
		return -EINVAL;
	}
	if(lbc == 0) {
		log.log_error("logic block is zero\n");
		return -EINVAL;
	}
#endif

	size_t devPos = device_judge(lba);
	nvmfDevice* dev = devices[devPos];

	// cross device read
	size_t op_lba_count = lbc;
	size_t lba_device = lba - dev->lba_start;
	if(lba_device + op_lba_count > dev->lba_count + dev->lba_start) {
		// next device read lb count
		size_t lba_next_dev_count = lba_device + op_lba_count - dev->lba_count;
		// next device read start lba
		size_t lba_next_dev = lba + op_lba_count - lba_next_dev_count;
		void* next_start_pbuf = (char*)pBuf + ((dev->lba_count - lba_device) * dpfs_lba_size);
		size_t pBuf_next_lbc = lbc - ((dev->lba_count - lba_device));
		lbc -= pBuf_next_lbc;

		rc = devices[devPos + 1]->read(lba_next_dev, next_start_pbuf, pBuf_next_lbc, arg);
		if(rc < 0) {
			log.log_error("Failed to read from next device %zu, rc: %d\n", devPos + 1, rc);
			return rc;
		}
		++req_count;
	}

	rc = dev->read(lba, pBuf, lbc, arg);
	if(rc < 0) {
		log.log_error("Failed to read from device %zu, rc: %d\n", devPos, rc);
		return rc;
	}
	

	return ++req_count;
}

int CNvmfhost::write(size_t lba, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg) {
	int rc = 0;
	int req_count = 0;
#ifdef DPFS_IO_CHECK
	log.log_debug("host: Writing to LBA %zu, buffer size %zu\n", lba, lbc * dpfs_lba_size);

	if(lba + lbc > block_count) {
		log.log_error("LBA %zu is out of range, total blocks: %zu\n", lba, block_count);
		return -EINVAL;
	}
	if(pBuf == nullptr) {
		log.log_error("Buffer pointer is null\n");
		return -EINVAL;
	}
	if(lbc == 0) {
		log.log_error("logic block is zero\n");
		return -EINVAL;
	}
#endif

	size_t devPos = device_judge(lba);
	nvmfDevice* dev = devices[devPos];
	size_t op_lba_count = lbc;
	size_t lba_device = lba - dev->lba_start;
	// cross device write
	if(lba_device + op_lba_count > dev->lba_count + dev->lba_start) {
		// next device read lb count
		size_t lba_next_dev_count = lba_device + op_lba_count - dev->lba_count;
		// next device read start lba
		size_t lba_next_dev = lba + op_lba_count - lba_next_dev_count;
		void* next_start_pbuf = (char*)pBuf + ((dev->lba_count - lba_device) * dpfs_lba_size);
		size_t pBuf_next_lbc = lbc - ((dev->lba_count - lba_device));
		lbc -= pBuf_next_lbc;

		rc = devices[devPos + 1]->write(lba_next_dev, next_start_pbuf, pBuf_next_lbc, arg);
		if(rc < 0) {
			log.log_error("Failed to write to next device %zu, rc: %d\n", devPos + 1, rc);
			return rc;
		}
		++req_count;
	}

	rc = dev->write(lba, pBuf, lbc, arg);
	if(rc < 0) {
		log.log_error("Failed to write to device %zu, rc: %d\n", devPos, rc);
		return rc;
	}
	
	return ++req_count;
}

int CNvmfhost::flush() { 
	
	return 0;
}
 
int CNvmfhost::sync() {
	if(!async_mode) {
		log.log_debug("Synchronous mode, no need to wait for completions.\n");
		return 0;
	}

	for(const auto& dev : devices) {
		if(dev->attached) {
			log.log_debug("Waiting for outstanding requests on device %s\n", dev->devdesc_str.c_str());
			while(spdk_nvme_qpair_get_num_outstanding_reqs(dev->qpair)) {
				// log.log_debug("Waiting for %d outstanding requests to complete on device %s\n", num, dev->devdesc_str.c_str());
			}
		}
	}


	return 0;
}

int CNvmfhost::copy(const dpfsEngine& tgt) {
	const CNvmfhost* nvmf_tgt = dynamic_cast<const CNvmfhost*>(&tgt);
	if(!nvmf_tgt) {
		log.log_error("Target engine is not a CNvmfhost instance.\n");
		return -1;
	}

	if(nvmf_tgt->devices.empty()) {
		log.log_error("Target engine has no devices.\n");
		return -1;
	}

	set_logdir(nvmf_tgt->log.get_log_path());

	for(auto& dev : nvmf_tgt->devices) {
		if(dev->attached) {
			int rc = attach_device(dev->devdesc_str);
			if(rc) {
				log.log_error("Failed to attach device %s\n", dev->devdesc_str.c_str());
				return rc;
			}
		} else {
			log.log_notic("Device %s is not attached, skipping.\n", dev->devdesc_str.c_str());
		}
	}

	return 0;
}

size_t CNvmfhost::size() const {
	return block_count;
}

void* CNvmfhost::zmalloc(size_t size) const {
	return (void*)spdk_zmalloc(size, dpfs_lba_size, NULL, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA);
}
void CNvmfhost::zfree(void* p) const {
	spdk_free(p);
}

int CNvmfhost::reattach_device(nvmfDevice* dev) {
	if(dev == nullptr) {
		log.log_error("Device pointer is null.\n");
		return -EINVAL;
	}
	int rc = 0;

	
    rc = nvmf_attach(dev);
	if(rc) {
		log.log_error("Failed to attach device %s\n", dev->devdesc_str.c_str());
		return rc;
	}

	// if(devices.empty()) {
	// 	ndev->lba_start = 0;
	// 	ndev->position = 0;
	// } else {
	// 	ndev->lba_start = devices.back()->lba_start + devices.back()->lba_count;
	// 	ndev->position = devices.back()->position + 1;
	// }

	// alloc qpair
	spdk_nvme_io_qpair_opts qpopts;
	spdk_nvme_ctrlr_get_default_io_qpair_opts(dev->ctrlr, &qpopts, sizeof(qpopts));
	// qpopts.io_queue_size = 4096;
	qpopts.io_queue_size = max_qpair_io_queue;
	log.log_inf("get qpair opts: io_queue_size %u\n", qpopts.io_queue_size);

	
	dev->qpair = spdk_nvme_ctrlr_alloc_io_qpair(dev->ctrlr, &qpopts, sizeof(qpopts));
	if (dev->qpair == nullptr) {
		log.log_error("spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
		return -ENOMEM;
	}

	log.log_inf("Controller %s attached with qpair %p\n", dev->trid->traddr, dev->qpair);
	log.log_inf("qpair opts: io_queue_size %u\n", qpopts.io_queue_size);

	return rc;
	// spdk_nvme_ns_is_active(ns)
}


void* newNvmf() {
    return new CNvmfhost();
}


