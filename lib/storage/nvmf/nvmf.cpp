/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2016 Intel Corporation.
 *   All rights reserved.
 */

#include <storage/nvmf/nvmf.hpp>

#include <spdk/vmd.h>
#include <spdk/nvme.h>
#include <spdk/nvme_zns.h>
#include <spdk/env.h>
#include <spdk/string.h>
#include <spdk/log.h>
#include <string>
#include <threadlock.hpp>
#include <iostream>
#include <list>
#include <vector>
#include <mutex>
#include <thread>

#define DPFS_IO_CHECK

static std::mutex init_mutex;
static bool initialized = false;
static bool g_vmd = false;
volatile size_t CNvmfhost::hostCount = 0;
static std::thread engineGuardThread;

void* engineGuardFunction() {
	while (CNvmfhost::hostCount > 0) {
		// std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return nullptr;
}

const char* eng_name = "DPFS-NVMF-Engine";

int initSpdk() {
	/*
	* SPDK relies on an abstraction around the local environment
	* named env that handles memory allocation and PCI device operations.
	* This library must be initialized first.
	*
	*/
	int rc = 0;
	struct spdk_env_opts opts;
	// CNvmfhost nfhost("trtype:tcp adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1");
	
	opts.opts_size = sizeof(opts);
	spdk_env_opts_init(&opts);

	opts.name = eng_name;
	rc = spdk_env_init(&opts);
	if (rc < 0) {
		fprintf(stderr, "Unable to initialize SPDK env\n");
		return rc;
	}

	printf("Initializing NVMe Controllers\n");
	rc = spdk_vmd_init();
	if (g_vmd && rc) {
		fprintf(stderr, "Failed to initialize VMD."
			" Some NVMe devices can be unavailable.\n");
			return rc;
	}

	return rc;
}

struct io_sequence {
	nvmfnsDesc* 	ns;
	volatile bool   is_completed;
	CSpin sequenceMutex;
};

static void read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
	nvmfnsDesc* ns = (nvmfnsDesc *)arg;

	if(!ns->dev.nfhost.async_mode) {
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
		exit(1);
	}
	

}

static void write_complete(void *arg, const struct spdk_nvme_cpl *completion) {
	nvmfnsDesc* ns = (nvmfnsDesc *)arg;

	if(!ns->dev.nfhost.async_mode) {
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
		exit(1);
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
	return spdk_nvme_ns_cmd_read(ns, dev.qpair, pBuf, lba_ns, lba_ns_count, read_complete, this, 0);

}

int nvmfnsDesc::write(size_t lba_device, void* pBuf, size_t lbc) {
	size_t lba_ns = (lba_device - lba_start) * lba_bundle;
	size_t lba_ns_count = lba_bundle * lbc;
	dev.nfhost.log.log_debug("ns: %p qpair: %p sequence: %p Writing to LBA %zu, lba_ns %zu, lbc size %zu, lba_count: %zu\n", ns, dev.qpair, sequence, lba_device, lba_ns, lbc, lba_ns_count);
	return spdk_nvme_ns_cmd_write(ns, dev.qpair, pBuf, lba_ns, lba_ns_count, write_complete, this, 0);
}

nvmfDevice::nvmfDevice(CNvmfhost& host) : nfhost(host) {
	attached = false;
	trid = new spdk_nvme_transport_id;
}

nvmfDevice::~nvmfDevice() {
	delete trid;
	attached = false;
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
	int rc = 0;
	int req_count = 0;
	size_t lba_device = lba_host - lba_start;
	size_t nsPos = nfhost.device_judge(lba_device);

	if(nsPos >= nsfield.size()) {
		nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());
		return -1;
	}
	nvmfnsDesc* ns = nsfield[nsPos];
	ns->sequence->is_completed = false;
	nfhost.log.log_debug("dev: Reading from LBA %zu, lba_dev %zu, lba_count %zu, buffer size %zu\n", lba_host, lba_device, lbc, lbc * dpfs_lba_size);

	if(lba_device + lbc > ns->lba_count) {
		// cross ns write
		// next ns start lba
		size_t pBuf_next_lbc = lba_device + lbc - ns->lba_count;
		size_t lba_next_ns = ns->lba_start + ns->lba_count;
		void* next_start_pbuf = (char*)pBuf + ((lbc - pBuf_next_lbc) * dpfs_lba_size);
		lbc -= pBuf_next_lbc;
		if(nsPos + 1 >= nsfield.size()) {
			nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());
			return -1;
		}
		
		nvmfnsDesc* subns;
		subns = nsfield[nsPos + 1];
		subns->sequence->is_completed = false;
		rc = subns->write(lba_next_ns, next_start_pbuf, pBuf_next_lbc);
		if(rc != 0) {
			nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos + 1, rc);
			return rc;
		}
		++req_count;
		if(!nfhost.async_mode) {
			// wait for completion
			do {
				spdk_nvme_qpair_process_completions(qpair, 0);
			} while(!subns->sequence->is_completed);
		}
	}
	rc = ns->read(lba_device, pBuf, lbc);
	if(rc != 0) {
		nfhost.log.log_error("Failed to read from device %zu, rc: %d\n", nsPos, rc);
		return rc;
	}
	++req_count;

	if(!nfhost.async_mode) {
		// wait for completion
		do {
			spdk_nvme_qpair_process_completions(qpair, 0);
		} while(!ns->sequence->is_completed);
	}


	return req_count;
}

int nvmfDevice::write(size_t lba_host, void* pBuf, size_t lbc) {
	int rc = 0;
	int req_count = 0;
	size_t lba_device = lba_host - lba_start;
	size_t nsPos = nfhost.device_judge(lba_device);
	
	if(nsPos >= nsfield.size()) {
		nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());
		return -1;
	}
	nvmfnsDesc* ns = nsfield[nsPos];
	ns->sequence->is_completed = false;
	nfhost.log.log_debug("dev: Writing to LBA %zu, lba_dev %zu, lba_count %zu, buffer size %zu\n", lba_host, lba_device, lbc, lbc * dpfs_lba_size);

	if(lba_device + lbc > ns->lba_count) {
		// cross ns write
		// next ns start lba
		size_t pBuf_next_lbc = lba_device + lbc - ns->lba_count;
		size_t lba_next_ns =  ns->lba_start + ns->lba_count;
		void* next_start_pbuf = (char*)pBuf + ((lbc - pBuf_next_lbc) * dpfs_lba_size);
		lbc -= pBuf_next_lbc;

		if(nsPos + 1 >= nsfield.size()) {
			nfhost.log.log_error("LBA %zu is out of range for device %s\n", lba_host, devdesc_str.c_str());
			return -1;
		}

		nvmfnsDesc* subns;
		subns = nsfield[nsPos + 1];
		subns->sequence->is_completed = false;
		rc = subns->write(lba_next_ns, next_start_pbuf, pBuf_next_lbc);
		if(rc != 0) {
			nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos + 1, rc);
			return rc;
		}
		++req_count;
		if(!nfhost.async_mode) {
			// wait for completion
			do {
				spdk_nvme_qpair_process_completions(qpair, 0);
			} while(!subns->sequence->is_completed);
			// spdk_nvme_ns_cmd_flush(subns->ns, qpair, nullptr, 0);
		}
	}
	rc = ns->write(lba_device, pBuf, lbc);
	if(rc != 0) {
		nfhost.log.log_error("Failed to write to ns %zu, rc: %d\n", nsPos, rc);
		return rc;
	}
	++req_count;
		if(!nfhost.async_mode) {
			// wait for completion
			do {
				spdk_nvme_qpair_process_completions(qpair, 0);
			} while(!ns->sequence->is_completed);
			// spdk_nvme_ns_cmd_flush(ns->ns, qpair, nullptr, 0);
		}

	return req_count;
}

static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, struct spdk_nvme_ctrlr_opts *opts) {
    nvmfDevice *device = (nvmfDevice *)cb_ctx;
	CNvmfhost *nfhost = &device->nfhost;
	nfhost->log.log_inf("Attaching to %s\n", trid->traddr);
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

	// snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

	// entry->ctrlr = ctrlr;
	// TAILQ_INSERT_TAIL(&g_controllers, entry, link);
    // fh->controllers.push_back(entry);

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
	fh->log.log_inf("Attached to %s\n", trid->traddr);
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

    fh->log.log_notic("Controller %s removed\n", device->devdesc_str.c_str());

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
	devices.clear();
}

CNvmfhost::~CNvmfhost() {
	init_mutex.lock();
	--hostCount;
	if (hostCount == 0) {
		spdk_vmd_fini();
		spdk_env_fini();
		// engineGuardThread.join();
		initialized = false;
	}
	init_mutex.unlock();
	
    // Cleanup all controllers and namespaces
	cleanup();
    log.log_notic("CNvmfhost destroyed\n");
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
			dev->attached = false;
			log.log_inf("Controller %s detached\n", dev->trid->traddr);
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
}

void CNvmfhost::register_ns(nvmfDevice* dev, struct spdk_nvme_ns* ns) {
	
	log.log_debug("  Namespace ID: %d size: %juGB = %lluB\n", spdk_nvme_ns_get_id(ns), spdk_nvme_ns_get_size(ns) / 1024/1024/1024, spdk_nvme_ns_get_size(ns));
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
	qpopts.io_queue_size = 128;
	log.log_debug("qpair opts: io_queue_size %u\n", qpopts.io_queue_size);

	
	
	ndev->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ndev->ctrlr, &qpopts, sizeof(qpopts));
	if (ndev->qpair == nullptr) {
		log.log_error("spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
		delete ndev;
		return rc;
	}

	log.log_debug("Controller %s attached with qpair %p\n", ndev->trid->traddr, ndev->qpair);
	log.log_debug("qpair opts: io_queue_size %u\n", qpopts.io_queue_size);

	devices.emplace_back(ndev);
	return rc;
	// spdk_nvme_ns_is_active(ns)
}

int CNvmfhost::detach_device(const std::string& devdesc_str) {
	int rc = 0;
	for(auto dev = devices.begin(); dev != devices.end(); ++dev) {
		if((*dev)->devdesc_str == devdesc_str) {
			if((*dev)->attached) {
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
				(*dev)->attached = false;

				if (detach_ctx) {
					spdk_nvme_detach_poll(detach_ctx);
				}


				log.log_inf("Device %s detached\n", (*dev)->devdesc_str.c_str());
			} else {
				log.log_notic("Device %s already detached\n", (*dev)->devdesc_str.c_str());
			}
			delete *dev;
			devices.erase(dev);
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
#ifdef DPFS_IO_CHECK
	log.log_debug("host: Reading from LBA %zu\n", lba);
	if(lba >= block_count) {
		log.log_error("LBA %zu is out of range, total blocks: %zu\n", lba, block_count);
		return -1;
	}
	if(pBuf == nullptr) {
		log.log_error("Buffer pointer is null\n");
		return -1;
	}
	if(lbc == 0) {
		log.log_error("logic block is zero\n");
		return -1;
	}
	
#endif

	size_t devPos = device_judge(lba);
	nvmfDevice* dev = devices[devPos];

	// cross device read
	size_t op_lba_count = lbc;
	size_t lba_device = lba - dev->lba_start;
	if(lba_device + op_lba_count > dev->lba_count) {
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

	}
	return dev->read(lba, pBuf, lbc) + rc;
}

int CNvmfhost::write(size_t lba, void* pBuf, size_t lbc) {
	int rc = 0;
#ifdef DPFS_IO_CHECK
	log.log_debug("host: Writing to LBA %zu, buffer size %zu\n", lba, lbc * dpfs_lba_size);

	if(lba >= block_count) {
		log.log_error("LBA %zu is out of range, total blocks: %zu\n", lba, block_count);
		return -1;
	}
	if(pBuf == nullptr) {
		log.log_error("Buffer pointer is null\n");
		return -1;
	}
	if(lbc == 0) {
		log.log_error("logic block is zero\n");
		return -1;
	}
#endif

	size_t devPos = device_judge(lba);
	nvmfDevice* dev = devices[devPos];
	size_t op_lba_count = lbc;
	size_t lba_device = lba - dev->lba_start;
	// cross device write
	if(lba_device + op_lba_count > dev->lba_count) {
		// next device read lb count
		size_t lba_next_dev_count = lba_device + op_lba_count - dev->lba_count;
		// next device read start lba
		size_t lba_next_dev = lba + op_lba_count - lba_next_dev_count;
		void* next_start_pbuf = (char*)pBuf + ((dev->lba_count - lba_device) * dpfs_lba_size);
		size_t pBuf_next_lbc = lbc - ((dev->lba_count - lba_device));
		lbc -= pBuf_next_lbc;

		rc = devices[devPos + 1]->write(lba_next_dev, next_start_pbuf, pBuf_next_lbc);
		if(rc < 0) {
			log.log_error("Failed to read from next device %zu, rc: %d\n", devPos + 1, rc);
			return rc;
		}
	}
	return dev->write(lba, pBuf, lbc) + rc;
}

int CNvmfhost::flush() { 
	
	return 0;
}
 
int CNvmfhost::sync(size_t n) {
	if(!async_mode) {
		log.log_debug("Synchronous mode, no need to wait for completions.\n");
		return 0;
	}
	if(n != 0) {
		for(const auto& dev : devices) {
			if(dev->attached) {
				log.log_debug("Waiting for %lu outstanding requests on device %s\n", n, dev->devdesc_str.c_str());
				while(n) { 
					n -= spdk_nvme_qpair_process_completions(dev->qpair, n);
					// int32_t comp_num = spdk_nvme_qpair_process_completions(dev->qpair, 0);
					// n -= comp_num;
					// log.log_debug("%s:%d compelete %d reqs, Waiting for %d outstanding requests to complete on device %s\n", __FILE__, __LINE__, comp_num, num - comp_num, dev->devdesc_str.c_str());
				}
			}
		}
	} else {
		for(const auto& dev : devices) {
			if(dev->attached) {
				log.log_debug("Waiting for outstanding requests on device %s\n", dev->devdesc_str.c_str());
				while(spdk_nvme_qpair_get_num_outstanding_reqs(dev->qpair)) {
					spdk_nvme_qpair_process_completions(dev->qpair, 0);

					// log.log_debug("Waiting for %d outstanding requests to complete on device %s\n", num, dev->devdesc_str.c_str());
				}
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

void* CNvmfhost::zmalloc(size_t size) const {
	return (void*)spdk_zmalloc(size, dpfs_lba_size, NULL, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA);
}
void CNvmfhost::zfree(void* p) const {
	spdk_free(p);
}






