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
#include <stdexcept>

#define log_inf(fmt, ...) log_inf("%s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__);
#define log_notic(fmt, ...) log_notic("%s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__);
#define log_error(fmt, ...) log_error("%s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__);
#define log_fatal(fmt, ...) log_fatal("%s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__);
#define log_debug(fmt, ...) log_debug("%s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__);

#define DPFS_IO_CHECK

// 16 is almost enough for most nvmf devices
static const int max_qpair_size = 16;
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

inline int nvmfDevice::nextQpair() noexcept {
	int rc = -ENODEV;

	// find next available qpair
	size_t next_avail_qpair_index = qpair_index;

	for(size_t i = 0; i < ioqpairs.size() - 1; ++i) {

		++next_avail_qpair_index;
		if(next_avail_qpair_index >= ioqpairs.size()) {
			next_avail_qpair_index = 0;
		}

		if(ioqpairs[next_avail_qpair_index].second.state == qpair_use_state::QPAIR_PREPARE) {
			rc = 0;
			// if found a valid qpair, set now used qpair state to WAIT_COMPLETE and then switch to new qpair
			// ioqpairs[qpair_index].second.state = qpair_use_state::QPAIR_WAIT_COMPLETE;
			qpair_index = next_avail_qpair_index;
			break;
		} else if(ioqpairs[next_avail_qpair_index].second.state == qpair_use_state::QPAIR_WAIT_COMPLETE) {
			rc = -EBUSY;
			break;
		} else {
			continue;
		}
	}

	return rc;
}

int nvmfDevice::clear() {
	int rc = 0;
	if(attached) {
		attached = false;
	}

	// nfhost.log.log_inf("clr func called, Detaching controller %s\n", trid->traddr);
	m_processLock.lock();
	
	// free all ioqpair
	for(auto& qp : ioqpairs) {
		if(qp.second.state == QPAIR_INVALID || qp.second.state == QPAIR_NOT_INITED) {
			continue;
		}
		qp.second.m_lock.lock();
		qp.second.state = QPAIR_INVALID;
		rc = spdk_nvme_ctrlr_free_io_qpair(qp.first);
		if(rc) {
			nfhost.log.log_error("spdk_nvme_ctrlr_free_io_qpair failed (%d)\n", rc);
		}
		nfhost.log.log_notic("IO qpair freed %p\n", qp.first);
		qp.first = nullptr;
		qp.second.m_lock.unlock();
	}

	ioqpairs.clear();


	m_processLock.unlock();

	// free all ns
	for(auto& ns : nsfield) {
		// no need to detach ns
		// spdk_nvme_ctrlr_detach_ns(ctrlr, ns->nsid, nullptr);
		delete ns;
	}

	nsfield.clear();
	
	
	struct spdk_nvme_detach_ctx* detach_ctx = nullptr;

	// detach the device
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
		// spdk_nvme_qpair_print_completion(ns->dev.ioqpairs[ns->dev.qpair_index].first, (struct spdk_nvme_cpl *)completion);
		
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
		// abort();
		// spdk_nvme_qpair_print_completion(ns->dev.ioqpairs[ns->dev.qpair_index].first, (struct spdk_nvme_cpl *)completion);

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

int submit_io(nvmfnsDesc* ns, void *buffer, uint64_t lba, uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags, int isRead) {
	int rc = 0;
	size_t ioCount = 0;
	size_t ioBytes = dpfs_lba_size * lba_count / ns->lba_bundle;
	ioCount = ioBytes % max_io_size == 0 ? (ioBytes / max_io_size) : (ioBytes / max_io_size + 1);
	// ns->dev.nfhost.log.log_error("submit_io called lba: %llu lba_count: %u isRead: %d\n", lba, lba_count, isRead);
	// std::chrono::microseconds usb;
	// std::chrono::microseconds us;
	if(ns->dev.ioqpairs[ns->dev.qpair_index].second.state != nvmfDevice::qpair_use_state::QPAIR_PREPARE) {
		ns->dev.nfhost.log.log_error("Current qpair is not ready for new io submission, index: %d state: %d\n", ns->dev.qpair_index, ns->dev.ioqpairs[ns->dev.qpair_index].second.state);
		rc = -EBUSY;
		goto errReturn;
	}

	// lock for io use
	ns->dev.qpLock.lock();

	if(ns->dev.ioqpairs[ns->dev.qpair_index].second.state != nvmfDevice::qpair_use_state::QPAIR_PREPARE) {
		ns->dev.qpLock.unlock();
		ns->dev.nfhost.log.log_error("Current qpair is not ready for new io submission, index: %d state: %d\n", ns->dev.qpair_index,ns-> dev.ioqpairs[ns->dev.qpair_index].second.state);
		rc = -EBUSY;
		goto errReturn;
	}

	// lock for qpair use
	ns->dev.ioqpairs[ns->dev.qpair_index].second.m_lock.lock();

	if(ns->dev.ioqpairs[ns->dev.qpair_index].second.m_reqs >= max_qpair_io_queue) {
		ns->dev.ioqpairs[ns->dev.qpair_index].second.m_lock.unlock();
		ns->dev.qpLock.unlock();
		ns->dev.nfhost.log.log_error("Current qpair has no more space for new io submission, index: %d reqs: %u\n", ns->dev.qpair_index, ns->dev.ioqpairs[ns->dev.qpair_index].second.m_reqs.load());
		rc = -EBUSY;
		goto errReturn;
	}

	// relate with max_io_size in config_nvmf.json
	// ns->dev.nfhost.log.log_error("ioCount = %zu\n", ioCount);

	ns->dev.ioqpairs[ns->dev.qpair_index].second.m_reqs += ioCount;
	if(isRead) {
		rc = spdk_nvme_ns_cmd_read(ns->ns, ns->dev.ioqpairs[ns->dev.qpair_index].first, buffer, lba, lba_count, cb_fn, cb_arg, io_flags);
	} else {
		rc = spdk_nvme_ns_cmd_write(ns->ns, ns->dev.ioqpairs[ns->dev.qpair_index].first, buffer, lba, lba_count, cb_fn, cb_arg, io_flags);
	}
	if(rc) {
		ns->dev.ioqpairs[ns->dev.qpair_index].second.m_reqs -= ioCount;
		ns->dev.ioqpairs[ns->dev.qpair_index].second.m_lock.unlock();
		ns->dev.qpLock.unlock();
		goto errReturn;
	}
	
	// usb = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch());
	// us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch());
	// ns->dev.nfhost.log.log_debug("wakeup io : bef %llu us aft %llu us qpair index %d reqs %u\n", usb.count(), us.count(), ns->dev.qpair_index, ns->dev.ioqpairs[ns->dev.qpair_index].second.m_reqs.load());	

	
	if(ns->dev.ioqpairs[ns->dev.qpair_index].second.m_reqs + max_data_split_size >= max_qpair_io_queue) {

		size_t exchange_qpair_index = ns->dev.qpair_index;
		ns->dev.ioqpairs[ns->dev.qpair_index].second.state = nvmfDevice::qpair_use_state::QPAIR_WAIT_COMPLETE;
		ns->dev.ioqpairs[exchange_qpair_index].second.m_lock.unlock();
		
		do {
			rc = ns->dev.nextQpair();
			if(rc == -EBUSY) {
				// ns->dev.ioqpairs[exchange_qpair_index].second.m_lock.unlock();
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
				// ns->dev.ioqpairs[exchange_qpair_index].second.m_lock.lock();
			}
		} while (rc == -EBUSY);
		
		// ns->dev.ioqpairs[exchange_qpair_index].second.m_lock.unlock();
		
		if(rc) {
			ns->dev.qpLock.unlock();
			ns->dev.nfhost.log.log_error("Failed to switch to next qpair after io submission, rc: %d\n", rc);
			goto errReturn;
		}
		
	} else {
		ns->dev.ioqpairs[ns->dev.qpair_index].second.m_lock.unlock();
	}
	


	ns->dev.m_convar.notify_one();
	ns->dev.qpLock.unlock();
	return 0;

errReturn:
	return rc;
}

int nvmfnsDesc::read(size_t lba_device, void* pBuf, size_t lbc) {
	/*
	* map device lba to namespace lba
	*/
	size_t lba_ns = (lba_device - lba_start) * lba_bundle;
	size_t lba_ns_count = lba_bundle * lbc;
	dev.nfhost.log.log_debug("ns: %p qpair: %p sequence: %p Reading from LBA %zu, lba_ns %zu, lbc size %zu, lba_count: %zu\n", ns, dev.ioqpairs[dev.qpair_index].first, sequence, lba_device, lba_ns, lbc, lba_ns_count);
	

	// int rc = spdk_nvme_ns_cmd_read(ns, dev.qpair, pBuf, lba_ns, lba_ns_count, read_complete, this, 0);
	int rc = submit_io(this, pBuf, lba_ns, lba_ns_count, read_complete, this, 0, 1);

	
	return rc;

}

int nvmfnsDesc::write(size_t lba_device, void* pBuf, size_t lbc) {
	size_t lba_ns = (lba_device - lba_start) * lba_bundle;
	size_t lba_ns_count = lba_bundle * lbc;
	dev.nfhost.log.log_debug("ns: %p qpair: %p sequence: %p Writing to LBA %zu, lba_ns %zu, lbc size %zu, lba_count: %zu\n", ns, dev.ioqpairs[dev.qpair_index].first, sequence, lba_device, lba_ns, lbc, lba_ns_count);
	
	// dev.qpLock.lock();
	// int rc = spdk_nvme_ns_cmd_write(ns, dev.qpair, pBuf, lba_ns, lba_ns_count, write_complete, this, 0);
	// dev.qpLock.unlock();

	int rc = submit_io(this, pBuf, lba_ns, lba_ns_count, write_complete, this, 0, 0);
	return rc;
}

static void async_compelete(void *arg, const struct spdk_nvme_cpl *completion) {
	dpfs_engine_cb_struct* cbs = (dpfs_engine_cb_struct*)arg;
	dpfs_compeletion dcp {0, 0};
	if(spdk_nvme_cpl_is_error(completion)) {
		dcp.errMsg = spdk_nvme_cpl_get_status_string(&completion->status);
		dcp.return_code = completion->status.sc;
	}
	// if(cbs->m_acb) {
	// 	cbs->m_acb(cbs->m_arg, &dcp);
	// 	return;
	// }
	
	cbs->m_cb(cbs->m_arg, &dcp);
	return;
}

int nvmfnsDesc::write(size_t lba_device, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg) {
	size_t lba_ns = (lba_device - lba_start) * lba_bundle;
	size_t lba_ns_count = lba_bundle * lbc;
	dev.nfhost.log.log_debug("ns: %p qpair: %p sequence: %p Writing to LBA %zu, lba_ns %zu, lbc size %zu, lba_count: %zu\n", ns, dev.ioqpairs[dev.qpair_index].first, sequence, lba_device, lba_ns, lbc, lba_ns_count);
	// dev.qpLock.lock();
	// int rc = spdk_nvme_ns_cmd_write(ns, dev.qpair, pBuf, lba_ns, lba_ns_count, async_compelete, arg, 0);
	// dev.qpLock.unlock();


	int rc = submit_io(this, pBuf, lba_ns, lba_ns_count, async_compelete, arg, 0, 0);
	return rc;
}

int nvmfnsDesc::read(size_t lba_device, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg) {
	/*
	* map device lba to namespace lba
	*/
	size_t lba_ns = (lba_device - lba_start) * lba_bundle;
	size_t lba_ns_count = lba_bundle * lbc;
	dev.nfhost.log.log_debug("ns: %p qpair: %p sequence: %p Reading from LBA %zu, lba_ns %zu, lbc size %zu, lba_count: %zu\n", ns, dev.ioqpairs[dev.qpair_index].first, sequence, lba_device, lba_ns, lbc, lba_ns_count);
	
	// dev.qpLock.lock();
	// int rc = spdk_nvme_ns_cmd_read(ns, dev.ioqpairs[dev.qpair_index].first, pBuf, lba_ns, lba_ns_count, async_compelete, arg, 0);
	// dev.qpLock.unlock();

	int rc = submit_io(this, pBuf, lba_ns, lba_ns_count, async_compelete, arg, 0, 1);
	return rc;


}

static void device_guard(void* arg) {
	nvmfDevice* dev = (nvmfDevice*)arg;
	// process complete lock
	std::chrono::microseconds us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch());
	std::mutex pclck;
	std::unique_lock<std::mutex> lk(pclck);

	// uint32_t n = 0;
	int32_t rc = 0;
	uint8_t current_qpair_index = 0;

	while(!dev->m_exit) {
		current_qpair_index = 0;
		while(dev->attached) {

			std::atomic<int16_t>& m_reqs = dev->ioqpairs[current_qpair_index].second.m_reqs;
			volatile nvmfDevice::qpair_use_state& qp_state = dev->ioqpairs[current_qpair_index].second.state;
			CSpin& qp_lock = dev->ioqpairs[current_qpair_index].second.m_lock;
			spdk_nvme_qpair*& qpair = dev->ioqpairs[current_qpair_index].first;

			// for fast respond, when qpair not full, wait for some time then process this qpair.
			while(m_reqs || (qp_state == nvmfDevice::qpair_use_state::QPAIR_PREPARE)) {
				
				if(m_reqs <= 0 && qp_state == nvmfDevice::qpair_use_state::QPAIR_PREPARE) {
					// dev->nfhost.log.log_inf("nvmfDevice %s qpair index %d no requests, wait for new requests\n",dev->devdesc_str.c_str(), current_qpair_index);
					// continue;

					dev->m_convar.wait_for(lk, std::chrono::milliseconds(qpair_io_wait_tm));
					if(!dev->attached) {
						goto outLoop;
					}
					if(m_reqs <= 0 && qp_state == nvmfDevice::qpair_use_state::QPAIR_PREPARE) {
						continue;
					}


					// dev->m_convar.wait_until(lk, std::chrono::system_clock::now() + std::chrono::milliseconds(qpair_io_wait_tm));
					// dev->m_convar.wait(lk);

					// std::chrono::microseconds us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch());
					// dev->nfhost.log.log_inf("nvmfDevice %s qpair index %d wake up from wait at %llu us\n", dev->devdesc_str.c_str(), current_qpair_index, us.count());
					// nfhost.log.log_inf("nvmfDevice %s qpair index %d wake up from wait\n", devdesc_str.c_str(), current_qpair_index);
				}


				dev->m_processLock.lock();
				if(qpair == nullptr) {
					dev->m_processLock.unlock();
					break;
				}
				
				// size_t otstdreqs = spdk_nvme_qpair_get_num_outstanding_reqs(ioqpairs[current_qpair_index].first);
				// nfhost.log.log_inf("nvmfDevice %s qpair index %d outstanding requests : %zu, unfinished requests : %d\n", devdesc_str.c_str(), current_qpair_index, otstdreqs, m_reqs.load());
				
				// std::chrono::microseconds ms = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch());
				// lock for qpair use
				// std::this_thread::sleep_for(std::chrono::milliseconds(50));
				

				// QUESTION :: large memory copy may cause miss callback function execution?
				qp_lock.lock();
				rc = spdk_nvme_qpair_process_completions(qpair, 0);
				qp_lock.unlock();

				// std::chrono::microseconds diff =  std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()) - ms;

				if(rc > 0) {
					// dev->nfhost.log.log_inf("nvmfDevice %s qpair index %d processed %d completions, unfinished requests : %d, use time: %llu us, qpStatus: %u\n", dev->devdesc_str.c_str(), current_qpair_index, rc, m_reqs.load() - rc, diff.count(), qp_state);
					// std::chrono::microseconds us_now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch());
					// dev->nfhost.log.log_inf("nvmfDevice %s qpair index %d processed %d completions, unfinished requests : %d, time: %llu us, qpStatus: %u\n", dev->devdesc_str.c_str(), current_qpair_index, rc, m_reqs.load() - rc, us_now.count(), qp_state);
					dev->nfhost.log.log_inf("nvmfDevice %s qpair index %d processed %d completions, unfinished requests : %d, qpStatus: %u\n", 
						dev->devdesc_str.c_str(), current_qpair_index, rc, m_reqs.load() - rc, qp_state);
					m_reqs -= rc;
				} else if(rc == 0) {

					// dev->nfhost.log.log_inf("nvmfDevice %s qpair index %d processed %d completions, unfinished requests : %d, use time: %llu, qpStatus: \n", dev->devdesc_str.c_str(), current_qpair_index, rc, m_reqs.load(), 1, qp_state);
					// std::this_thread::sleep_for(std::chrono::milliseconds(50));
				} else {
					// TODO process error
					dev->nfhost.log.log_error("process completions fail! code : %d, unfinished requests : %d, reattaching device...\n", rc, m_reqs.load());
					dev->attached = false;
					dev->m_processLock.unlock();
					dev->need_reattach = true;
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
				
				dev->m_processLock.unlock();

				// if(ioqpairs[current_qpair_index].second.state == nvmfDevice::qpair_use_state::QPAIR_PREPARE) {
				// 	std::this_thread::sleep_for(std::chrono::milliseconds(15));
				// }

			}
			
			dev->nfhost.log.log_inf("nvmfDevice %s finish qpair index %d\n", dev->devdesc_str.c_str(), current_qpair_index);

			// finish all requests on this qpair, set it to PREPARE state
			dev->ioqpairs[current_qpair_index].second.state = nvmfDevice::qpair_use_state::QPAIR_PREPARE;

			// find next valid qpair which is PREPARE or WAIT_COMPLETE state
			size_t next_wait_qpair = current_qpair_index;
			// check all qpairs
			for(size_t i = 0; i < dev->ioqpairs.size(); ++i) {
				++next_wait_qpair;
				if(next_wait_qpair >= dev->ioqpairs.size()) {
					next_wait_qpair = 0;
				}
				// if found a valid qpair, switch to it
				if(dev->ioqpairs[next_wait_qpair].second.state == nvmfDevice::qpair_use_state::QPAIR_PREPARE || // no more requests
				dev->ioqpairs[next_wait_qpair].second.state == nvmfDevice::qpair_use_state::QPAIR_WAIT_COMPLETE) { // has requests to process
					current_qpair_index = next_wait_qpair;
					break;
				}
			}
			dev->nfhost.log.log_inf("nvmfDevice %s switch to qpair index %d\n", dev->devdesc_str.c_str(), current_qpair_index);

			// if not found any valid qpair, stay on current qpair
			
		}
	outLoop:
		// waiting for attach
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
}

nvmfDevice::nvmfDevice(CNvmfhost& host) : nfhost(host) {
	attached = false;
	m_exit = false;
	ioqpairs.resize(max_qpair_size, {nullptr, qpair_status()});

	trid = new spdk_nvme_transport_id;
	// m_reqs.store(0);

	process_complete_thd = std::thread(device_guard, this);

}


nvmfDevice::~nvmfDevice() {
	attached = false;
	m_exit = true;
	if(process_complete_thd.joinable()) {
		process_complete_thd.join();
	}
	delete trid;
}

int nvmfDevice::lba_judge(size_t lba) const noexcept {
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
		
		rc = subns->read(lba_next_ns, next_start_pbuf, pBuf_next_lbc);
		if(rc != 0) {
			nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}


		rc = ns->read(lba_device, pBuf, lbc);
		if(rc != 0) {
			nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos, rc);
			goto errReturn;
		}

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

	rc = ns->read(lba_device, pBuf, lbc);
	if(rc != 0) {
		nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos, rc);
		return rc;
	}

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

		rc = subns->write(lba_next_ns, next_start_pbuf, pBuf_next_lbc);
		if(rc != 0) {
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}


		rc = ns->write(lba_device, pBuf, lbc);
		if(rc != 0) {
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
			goto errReturn;
		}

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

	rc = ns->write(lba_device, pBuf, lbc);
	if(rc != 0) {
		nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
		goto errReturn;
	}

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

		rc = subns->read(lba_next_ns, next_start_pbuf, pBuf_next_lbc, arg);
		if(rc != 0) {
			nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}

		rc = ns->read(lba_device, pBuf, lbc, arg);
		if(rc != 0) {
			nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos, rc);
			goto errReturn;
		}

		return 2;
	}

	rc = ns->read(lba_device, pBuf, lbc, arg);
	if(rc != 0) {
		nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos, rc);
		return rc;
	}


	return 1;

errReturn:

	return rc;
}

int nvmfDevice::write(size_t lba_host, void* pBuf, size_t lbc, dpfs_engine_cb_struct* arg) {
	int rc;
	if(!attached || m_exit) {
		return -EPERM;
	}


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

		rc = subns->write(lba_next_ns, next_start_pbuf, pBuf_next_lbc, arg);
		if(rc != 0) {
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos + 1, rc);
			goto errReturn;
		}

		rc = ns->write(lba_device, pBuf, lbc, arg);
		if(rc != 0) {
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
			goto errReturn;
		}

		return 2;
	}

	rc = ns->write(lba_device, pBuf, lbc, arg);
	if(rc != 0) {
		nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
		goto errReturn;
	}

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
	if(!ctrlr) {
		device->ctrlr = nullptr;
		fh->log.log_error("Failed to attach to controller %s\n", trid->traddr);
		return;
	}

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

CNvmfhost::CNvmfhost() : async_mode(true) {
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
	cleanup();

	m_exit = true;
    // Cleanup all controllers and namespaces

	if(nf_guard.joinable())
		nf_guard.join();
		
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
	if(!device->attached) {
		log.log_error("Failed to attach to device %s\n", device->devdesc_str.c_str());
		return -ENODEV;
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
			dev->clear();
			
			// if(dev->ioqpairs[dev->qpair_index].first) {
			// 	spdk_nvme_ctrlr_free_io_qpair(dev->ioqpairs[dev->qpair_index].first);
			// 	dev->ioqpairs[dev->qpair_index].first = nullptr;
			// }

			// rc = spdk_nvme_detach_async(dev->ctrlr, &detach_ctx[i++]);
			// if (rc) {
			// 	log.log_inf("spdk_nvme_detach_async failed (%d)\n", rc);
			// 	continue;
			// }
			// for(auto& nsdesc : dev->nsfield) {
			// 	delete nsdesc;
			// }
			// dev->nsfield.clear();
			// log.log_inf("Controller %s detached\n", dev->trid->traddr);

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

// LOG_BINARY,
// LOG_FATAL,
// LOG_ERROR,
// LOG_NOTIC,
// LOG_INFO,
// LOG_DEBUG,

void CNvmfhost::set_loglevel(int level) {
	log.set_loglevel(logrecord::loglevel(level));
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

	size_t i = 0;
	for(; i < ndev->ioqpairs.size(); ++i) {
		ndev->ioqpairs[i].first = spdk_nvme_ctrlr_alloc_io_qpair(ndev->ctrlr, &qpopts, sizeof(qpopts));
		if(ndev->ioqpairs[i].first) {
			ndev->ioqpairs[i].second.state = nvmfDevice::qpair_use_state::QPAIR_PREPARE;
			log.log_inf("Preallocate qpair idx %d success\n", i);
		} else {
			log.log_notic("spdk_nvme_ctrlr_alloc_io_qpair() failed for preallocate idx: %d\n", i);
			ndev->ioqpairs[i].second.state = nvmfDevice::qpair_use_state::QPAIR_INVALID;
		}
	}
	ndev->qpair_index = 0;

	

	// ndev->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ndev->ctrlr, &qpopts, sizeof(qpopts));
	// if (ndev->qpair == nullptr) {
	// 	log.log_error("spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
	// 	delete ndev;
	// 	return -ENOMEM;
	// }

	log.log_inf("Controller %s attached with qpair %p\n", ndev->trid->traddr, ndev->ioqpairs[ndev->qpair_index].first);
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
				// (*dev)->attached = false;
				// (*dev)->m_exit = true;
				(*dev)->clear();

				// if((*dev)->process_complete_thd.joinable()) {
				// 	(*dev)->process_complete_thd.join();
				// }

				// free ioqpair
				// if((*dev)->ioqpairs[(*dev)->qpair_index].first) {
				// 	while(spdk_nvme_qpair_get_num_outstanding_reqs((*dev)->ioqpairs[(*dev)->qpair_index].first)) {
				// 		spdk_nvme_qpair_process_completions((*dev)->ioqpairs[(*dev)->qpair_index].first, 0);
				// 		// std::this_thread::sleep_for(std::chrono::milliseconds(10));
				// 	}
				// 	(*dev)->ioqpairs[(*dev)->qpair_index].second.state = nvmfDevice::qpair_use_state::QPAIR_INVALID;
				// 	spdk_nvme_ctrlr_free_io_qpair((*dev)->ioqpairs[(*dev)->qpair_index].first);
				// 	(*dev)->ioqpairs[(*dev)->qpair_index].first = nullptr;
				// }
				// spdk_nvme_detach_ctx *detach_ctx = nullptr;
				// rc = spdk_nvme_detach_async((*dev)->ctrlr, &detach_ctx);
				// if(rc) {
				// 	log.log_error("spdk_nvme_detach_async failed (%d)\n", rc);
				// 	return rc;
				// }
				// for(auto& nsdesc : (*dev)->nsfield) {
				// 	delete nsdesc;
				// }

				// (*dev)->nsfield.clear();

				// if (detach_ctx) {
				// 	spdk_nvme_detach_poll(detach_ctx);
				// }


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

int CNvmfhost::device_judge(size_t lba) const noexcept {
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
	if(lbc > max_data_split_size) {
		log.log_error("IO size %zu exceeds max split size %zu\n", lbc, max_data_split_size);
		return -E2BIG;
	}
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
	if(lbc > max_data_split_size) {
		log.log_error("IO size %zu exceeds max split size %zu\n", lbc, max_data_split_size);
		return -E2BIG;
	}
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
	if(lbc > max_data_split_size) {
		log.log_error("IO size %zu exceeds max split size %zu\n", lbc, max_data_split_size);
		return -E2BIG;
	}
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
	if(lbc > max_data_split_size) {
		log.log_error("IO size %zu exceeds max split size %zu\n", lbc, max_data_split_size);
		return -E2BIG;
	}
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
			while(spdk_nvme_qpair_get_num_outstanding_reqs(dev->ioqpairs[dev->qpair_index].first)) {
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

	for(size_t i = 0; i < dev->ioqpairs.size(); ++i) {
		dev->ioqpairs[i].first = spdk_nvme_ctrlr_alloc_io_qpair(dev->ctrlr, &qpopts, sizeof(qpopts));
		if (dev->ioqpairs[i].first == nullptr) {
			log.log_error("spdk_nvme_ctrlr_alloc_io_qpair() failed, index : %llu\n", i);
			rc = -ENOMEM;
			break;
		}

		log.log_inf("Controller %s attached with qpair %p\n", dev->trid->traddr, dev->ioqpairs[i].first);
		log.log_inf("qpair opts: io_queue_size %u\n", qpopts.io_queue_size);
	}


	return rc;
	// spdk_nvme_ns_is_active(ns)
}


void* newNvmf() {
    return new CNvmfhost();
}


