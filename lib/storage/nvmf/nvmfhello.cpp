#include <storage/nvmf/nvmf.hpp>
#include <spdk/nvme.h>

#define DATA_BUFFER_STRING "Hello world!"

struct hello_world_sequence {
	nvmfDevice* dev_entry;
	char		*buf;
	unsigned        using_cmb_io;
	int		is_completed;
};

static void hello_read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
	struct hello_world_sequence *sequence = (hello_world_sequence *)arg;

	/* Assume the I/O was successful */
	sequence->is_completed = 1;
	/* See if an error occurred. If so, display information
	 * about it, and set completion value so that I/O
	 * caller is aware that an error occurred.
	 */
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(sequence->dev_entry->ioqpairs[0].first, (struct spdk_nvme_cpl *)completion);
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
		spdk_nvme_qpair_print_completion(sequence->dev_entry->ioqpairs[0].first, (struct spdk_nvme_cpl *)completion);
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

	rc = spdk_nvme_ns_cmd_read(sequence->dev_entry->nsfield[0]->ns, sequence->dev_entry->ioqpairs[0].first, sequence->buf,
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
		rc = spdk_nvme_ns_cmd_write(dev->nsfield[0]->ns, dev->ioqpairs[0].first, sequence.buf,
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
			spdk_nvme_qpair_process_completions(dev->ioqpairs[0].first, 0);
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
