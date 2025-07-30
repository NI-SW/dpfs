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


static std::mutex init_mutex;
static bool initialized = false;
static bool g_vmd = false;
volatile size_t CNvmfhost::hostCount = 0;
static std::thread engineGuardThread;

void* engineGuardFunction() {
	while (CNvmfhost::hostCount > 0) {
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}
	return nullptr;
}


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

	opts.name = "DPFS";
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

struct hello_world_sequence {
	nvmfDevice* dev_entry;
	char		*buf;
	unsigned        using_cmb_io;
	int		is_completed;
};

struct io_sequence {
	nvmfnsDesc* 	ns;
	bool	is_completed;
};

static void read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
	io_sequence* sequence = (io_sequence *)arg;


	/* See if an error occurred. If so, display information
	 * about it, and set completion value so that I/O
	 * caller is aware that an error occurred.
	 */
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(sequence->ns->dev.qpair, (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Read I/O failed, aborting run\n");
		exit(1);
	}
	sequence->is_completed = true;

}

static void write_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
	io_sequence* sequence = (io_sequence *)arg;

	/* See if an error occurred. If so, display information
	 * about it, and set completion value so that I/O
	 * caller is aware that an error occurred.
	 */
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(sequence->ns->dev.qpair, (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Write I/O failed, aborting run\n");
		exit(1);
	}
	/*
	 * The write I/O has completed.  Free the buffer associated with
	 *  the write I/O and allocate a new zeroed buffer for reading
	 *  the data back from the NVMe namespace.
	 */
	sequence->is_completed = true;
}


static void hello_read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
	struct hello_world_sequence *sequence = (hello_world_sequence *)arg;

	/* Assume the I/O was successful */
	sequence->is_completed = 1;
	/* See if an error occurred. If so, display information
	 * about it, and set completion value so that I/O
	 * caller is aware that an error occurred.
	 */
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(sequence->dev_entry->qpair, (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Read I/O failed, aborting run\n");
		sequence->is_completed = 2;
		exit(1);
	}

	if (strcmp(sequence->buf, DATA_BUFFER_STRING)) {
		fprintf(stderr, "Read data doesn't match write data\n");
		exit(1);
	}

	/*
	 * The read I/O has completed.  Print the contents of the
	 *  buffer, free the buffer, then mark the sequence as
	 *  completed.  This will trigger the hello_world() function
	 *  to exit its polling loop.
	 */
	printf("%s\n", sequence->buf);
	spdk_free(sequence->buf);
}

static void hello_write_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
	struct hello_world_sequence	*sequence = (hello_world_sequence *)arg;
	// struct ns_entry			*ns_entry = sequence->ns_entry;
	int				rc;

	/* See if an error occurred. If so, display information
	 * about it, and set completion value so that I/O
	 * caller is aware that an error occurred.
	 */
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(sequence->dev_entry->qpair, (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Write I/O failed, aborting run\n");
		sequence->is_completed = 2;
		exit(1);
	}
	/*
	 * The write I/O has completed.  Free the buffer associated with
	 *  the write I/O and allocate a new zeroed buffer for reading
	 *  the data back from the NVMe namespace.
	 */
	if (sequence->using_cmb_io) {
		spdk_nvme_ctrlr_unmap_cmb(sequence->dev_entry->ctrlr);
	} else {
		spdk_free(sequence->buf);
	}
	sequence->buf = (char *)spdk_zmalloc(0x1000, 0x1000, NULL, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA);

	rc = spdk_nvme_ns_cmd_read(sequence->dev_entry->nsfield[0]->ns, sequence->dev_entry->qpair, sequence->buf,
				   0, /* LBA start */
				   1, /* number of LBAs */
				   hello_read_complete, (void *)sequence, 0);
	if (rc != 0) {
		fprintf(stderr, "starting read I/O failed\n");
		exit(1);
	}
}

void CNvmfhost::hello_world() {
	// struct ns_entry			*ns_entry;
	struct hello_world_sequence	sequence;
	int				rc = 0;
	size_t			sz = 0;

    for(auto& dev : devices) {
		/*
		 * Allocate an I/O qpair that we can use to submit read/write requests
		 *  to namespaces on the controller.  NVMe controllers typically support
		 *  many qpairs per controller.  Any I/O qpair allocated for a controller
		 *  can submit I/O to any namespace on that controller.
		 *
		 * The SPDK NVMe driver provides no synchronization for qpair accesses -
		 *  the application must ensure only a single thread submits I/O to a
		 *  qpair, and that same thread must also check for completions on that
		 *  qpair.  This enables extremely efficient I/O processing by making all
		 *  I/O operations completely lockless.
		 */
		// spdk_nvme_io_qpair_opts opts;
		// spdk_nvme_ctrlr_get_default_io_qpair_opts(dev->ctrlr, &opts, sizeof(opts));

		// if(async_mode) {
		// 	opts.create_only = true; // Create the qpair without connecting it
		// } else {
		// 	opts.create_only = false; // Create and connect the qpair immediately
		// }

		// dev->qpair = spdk_nvme_ctrlr_alloc_io_qpair(dev->ctrlr, NULL, 0);
		// if (dev->qpair == NULL) {
		// 	printf("ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
		// 	return;
		// }

		/*
		 * Use spdk_dma_zmalloc to allocate a 4KB zeroed buffer.  This memory
		 * will be pinned, which is required for data buffers used for SPDK NVMe
		 * I/O operations.
		 */
		sequence.using_cmb_io = 1;
		sequence.buf = (char *)spdk_nvme_ctrlr_map_cmb(dev->ctrlr, &sz);
		if (sequence.buf == NULL || sz < 0x1000) {
			log.log_inf("map_cmb sz = %d\n", sz);
			sequence.using_cmb_io = 0;
			sequence.buf = (char *)spdk_zmalloc(0x1000, 0x1000, NULL, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA);
		}
		if (sequence.buf == NULL) {
			log.log_error("write buffer allocation failed\n");
			return;
		}
		if (sequence.using_cmb_io) {
			log.log_inf("using controller memory buffer for IO\n");
		} else {
			log.log_inf("using host memory buffer for IO\n");
		}
		sequence.is_completed = 0;
		sequence.dev_entry = dev;
		
		/*
		 * If the namespace is a Zoned Namespace, rather than a regular
		 * NVM namespace, we need to reset the first zone, before we
		 * write to it. This not needed for regular NVM namespaces.
		 */
		// if (spdk_nvme_ns_get_csi(dev->nsfield[0]->ns) == SPDK_NVME_CSI_ZNS) {
		// 	reset_zone_and_wait_for_completion(&sequence);
		// }

		/*
		 * Print DATA_BUFFER_STRING to sequence.buf. We will write this data to LBA
		 *  0 on the namespace, and then later read it back into a separate buffer
		 *  to demonstrate the full I/O path.
		 */
		snprintf(sequence.buf, 0x1000, "%s", DATA_BUFFER_STRING);

		/*
		 * Write the data buffer to LBA 0 of this namespace.  "write_complete" and
		 *  "&sequence" are specified as the completion callback function and
		 *  argument respectively.  write_complete() will be called with the
		 *  value of &sequence as a parameter when the write I/O is completed.
		 *  This allows users to potentially specify different completion
		 *  callback routines for each I/O, as well as pass a unique handle
		 *  as an argument so the application knows which I/O has completed.
		 *
		 * Note that the SPDK NVMe driver will only check for completions
		 *  when the application calls spdk_nvme_qpair_process_completions().
		 *  It is the responsibility of the application to trigger the polling
		 *  process.
		 */
		rc = spdk_nvme_ns_cmd_write(dev->nsfield[0]->ns, dev->qpair, sequence.buf,
					    0, /* LBA start */
					    1, /* number of LBAs */
					    hello_write_complete, &sequence, 0);
		if (rc != 0) {
			fprintf(stderr, "starting write I/O failed\n");
			exit(1);
		}

		/*
		 * Poll for completions.  0 here means process all available completions.
		 *  In certain usage models, the caller may specify a positive integer
		 *  instead of 0 to signify the maximum number of completions it should
		 *  process.  This function will never block - if there are no
		 *  completions pending on the specified qpair, it will return immediately.
		 *
		 * When the write I/O completes, write_complete() will submit a new I/O
		 *  to read LBA 0 into a separate buffer, specifying read_complete() as its
		 *  completion routine.  When the read I/O completes, read_complete() will
		 *  print the buffer contents and set sequence.is_completed = 1.  That will
		 *  break this loop and then exit the program.
		 */
		while (!sequence.is_completed) {
			spdk_nvme_qpair_process_completions(dev->qpair, 0);
		}

		/*
		 * Free the I/O qpair.  This typically is done when an application exits.
		 *  But SPDK does support freeing and then reallocating qpairs during
		 *  operation.  It is the responsibility of the caller to ensure all
		 *  pending I/O are completed before trying to free the qpair.
		 */
		// spdk_nvme_ctrlr_free_io_qpair(dev->qpair);
	}
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
	blockCount = size / sector_size; 
	lba_start = dev.lba_count;

}

int nvmfnsDesc::read(size_t lba_device, void* pBuf, size_t pBufLen, io_sequence* sequence) {
	/*
	* map device lba to namespace lba
	*/
	size_t lba_ns = (lba_device - lba_start) * lba_bundle;
	size_t lba_count = lba_bundle * (pBufLen % dpfs_lba_size == 0 ? pBufLen / dpfs_lba_size : pBufLen / dpfs_lba_size + 1);
	return spdk_nvme_ns_cmd_read(ns, dev.qpair, pBuf, lba_ns, lba_count, read_complete, sequence, 0);

}

int nvmfnsDesc::write(size_t lba_device, void* pBuf, size_t pBufLen, io_sequence* sequence) {
	size_t lba_ns = (lba_device - lba_start) * lba_bundle;
	size_t lba_count = lba_bundle * (pBufLen % dpfs_lba_size == 0 ? pBufLen / dpfs_lba_size : pBufLen / dpfs_lba_size + 1);
	return spdk_nvme_ns_cmd_write(ns, dev.qpair, pBuf, lba_ns, lba_count, write_complete, sequence, 0);
}

nvmfDevice::nvmfDevice(CNvmfhost& host) : nfhost(host) {
	attached = false;
	trid = new spdk_nvme_transport_id;
}

nvmfDevice::nvmfDevice(nvmfDevice&& tgt) : nfhost(tgt.nfhost) {
	trid = tgt.trid;
	attached = tgt.attached;
	lba_count = tgt.lba_count;
	nsfield.swap(tgt.nsfield);
	ctrlr = tgt.ctrlr;
	qpair = tgt.qpair;
	devdesc_str.swap(tgt.devdesc_str);
	lba_start = tgt.lba_start;
	position = tgt.position;

	tgt.trid = nullptr;
	tgt.ctrlr = nullptr;
	tgt.qpair = nullptr;

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

int nvmfDevice::read(size_t lba_host, void* pBuf, size_t pBufLen) {
	/*
	* map host lba to device lba
	*/
	size_t lba_device = lba_host - lba_start;
	io_sequence sequence;
	sequence.ns = nsfield[lba_judge(lba_device)];
	sequence.is_completed = false;
	int rc = sequence.ns->read(lba_device, pBuf, pBufLen, &sequence);
	
	while(!sequence.is_completed) {
		// wait for completion
		spdk_nvme_qpair_process_completions(qpair, 0);
	}

	return rc;
}

int nvmfDevice::write(size_t lba_host, void* pBuf, size_t pBufLen) {
	size_t lba_device = lba_host - lba_start;
	io_sequence sequence;
	sequence.ns = nsfield[lba_judge(lba_device)];
	sequence.is_completed = false;
	int rc = sequence.ns->write(lba_device, pBuf, pBufLen, &sequence);

	return rc;
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
	fh->log.log_inf("Controller %s attached\n", trid->traddr);

	// entry = (ctrlr_entry *)malloc(sizeof(struct ctrlr_entry));
	// if (entry == NULL) {
	// 	perror("ctrlr_entry malloc");
	// 	exit(1);
	// }

	printf("Attached to %s\n", trid->traddr);

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

    fh->log.log_notic("Controller removed\n");

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
		engineGuardThread = std::thread(engineGuardFunction);
	}
	init_mutex.unlock();
}

CNvmfhost::~CNvmfhost() {
	init_mutex.lock();
	--hostCount;
	if (hostCount == 0) {
		spdk_vmd_fini();
		spdk_env_fini();
		engineGuardThread.join();
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
	
	log.log_inf("  Namespace ID: %d size: %juGB = %lluB\n", spdk_nvme_ns_get_id(ns), spdk_nvme_ns_get_size(ns) / 1024/1024/1024, spdk_nvme_ns_get_size(ns));
	log.log_inf("nsid : %u  sector_size : %u  size: %llu\n", spdk_nvme_ns_get_id(ns), spdk_nvme_ns_get_sector_size(ns), spdk_nvme_ns_get_size(ns));

}

static const std::vector<std::string> spdk_trans_type {"pcie", "rdma", "tcp", "unknow"};
static const std::string& trtypeCvt(spdk_nvme_transport_type type) {
	if(type == SPDK_NVME_TRANSPORT_PCIE) {
		return spdk_trans_type[0];
	} else if (type == SPDK_NVME_TRANSPORT_RDMA) {
		return spdk_trans_type[1];
	} else if (type == SPDK_NVME_TRANSPORT_TCP) {
		return spdk_trans_type[2];
	}
	return spdk_trans_type.back();
};

void CNvmfhost::set_logdir(const std::string& log_path) {
	log.set_log_path(log_path + "nvmfhost_" + trtypeCvt(devices[0]->trid->trtype) + "_" + devices[0]->trid->traddr + ".log");
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
	} else {
		ndev->lba_start = devices.back()->lba_start + devices.back()->lba_count;
	}
	spdk_nvme_io_qpair_opts qpopts;
	spdk_nvme_ctrlr_get_default_io_qpair_opts(ndev->ctrlr, &qpopts, sizeof(qpopts));

	if(async_mode) {
		qpopts.async_mode = true;
	} else {
		qpopts.async_mode = false;
	}

	ndev->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ndev->ctrlr, &qpopts, sizeof(qpopts));
	if (ndev->qpair == nullptr) {
		log.log_error("spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
		delete ndev;
		return rc;
	}

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
						std::this_thread::sleep_for(std::chrono::milliseconds(10));
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

int CNvmfhost::device_judge(size_t lba) {
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

int CNvmfhost::read(size_t lba, void* pBuf, size_t pBufLen) { 
	return devices[device_judge(lba)]->read(lba, pBuf, pBufLen);
}
int CNvmfhost::write(size_t lba, void* pBuf, size_t pBufLen) {
	return devices[device_judge(lba)]->write(lba, pBuf, pBufLen);
}

int CNvmfhost::flush() { 
	
	return 0;
}
 
int CNvmfhost::sync() {
	for(auto& dev : devices) {
		if(dev->attached) {
			uint32_t num = spdk_nvme_qpair_get_num_outstanding_reqs(dev->qpair);
			while(num) {
				spdk_nvme_qpair_process_completions(dev->qpair, 0);
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
				log.log_debug("Waiting for %d outstanding requests to complete on device %s\n", num, dev->devdesc_str.c_str());
				num = spdk_nvme_qpair_get_num_outstanding_reqs(dev->qpair);
			}
		}
	}
	return 0;
}

int CNvmfhost::replace_device(const std::string& trid_str, const std::string& new_trid_str) {return 0;}

char* CNvmfhost::zmalloc(size_t size) {
	return (char*)spdk_zmalloc(size, dpfs_lba_size, NULL, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA);
}
void CNvmfhost::zfree(void* p) {
	spdk_free(p);
}






