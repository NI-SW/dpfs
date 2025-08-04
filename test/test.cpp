#include <storage/nvmf/nvmf.hpp>
#include <thread>
using namespace std;

int main() {

    CNvmfhost nfhost;
	nfhost.log.set_log_path("./output.log");
	// nfhost.log.set_loglevel(logrecord::LOG_DEBUG);
	nfhost.log.set_async_mode(true);

	nfhost.log.log_inf("Starting NVMF host...\n");

	nfhost.set_async_mode(true);
	
	// nfhost.log.set_loglevel(logrecord::LOG_FATAL);
	nfhost.attach_device("trtype:pcie traddr:0000.1b.00.0");
	nfhost.attach_device("trtype:pcie traddr:0000.13.00.0");
	// nfhost.attach_device("trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50658 subnqn:nqn.2016-06.io.spdk:cnode1");
	// nfhost.attach_device("trtype:tcp adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1");
	
	auto hclock = std::chrono::high_resolution_clock::now();
	auto now = std::chrono::duration_cast<std::chrono::milliseconds>(hclock.time_since_epoch()).count();

	

	
	nfhost.log.set_log_path("./output.log");

	char* test = (char*)nfhost.zmalloc(4096 * 100); // 400kB
	if(!test) {
		printf("Failed to allocate memory for test buffer\n");
		return -1;
	}
    int rc = 0;
	int reqs = 0;
    if (nfhost.devices.empty()) {
		fprintf(stderr, "no NVMe controllers found\n");
		rc = 1;
		goto exit;
	}


	// nfhost.hello_world();

	memcpy(test, "Hello World from NVMF host!", 28);
	memcpy(test + 4096, "Hello World from NVMF host2!", 29);
	memcpy(test + 4096 * 2, "Hello World from NVMF host3!", 29);
	memcpy(test + 4096 * 3, "Hello World from NVMF host4!", 29);

	printf("Write data: %s\n", test);

	now = std::chrono::duration_cast<std::chrono::milliseconds>(hclock.time_since_epoch()).count();

	for(int i = 1; i < 15000; ++i) {
		do{
			printf("count: %d\n", i);
			rc = nfhost.write(i, test, i); //10 * i + k * 100); //5242879
			if(rc < 0) {
				if(rc == -ENOMEM) {
					printf("Out of memory, try to sync %d reqs\n", reqs);
					exit(0);
				}
				printf("Write data err: %d\n", rc);
			}
			break;
		} while(1);

		if(rc > 0) {
			reqs += rc;
		} else {
			printf("Write data err: %d\n", rc);
			break;
		}
		while(reqs >= 100) {
			nfhost.sync(reqs);
			reqs = 0;
		}
		// std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	// printf("sync reqs = %d\n", reqs);
	reqs = nfhost.sync(reqs);
	printf("duration: %ld ms, reqs = %d\n", std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count() - now, reqs);


	rc = nfhost.read(5242879, test + 4096, 2);
	if(rc) {
		reqs += rc;
	} else {
		printf("Read data err: %d\n", rc);
	}
	printf("rc = %d\n", rc);
	rc = nfhost.sync(reqs);
	printf("Read data: %s\n", test + 4096);



	nfhost.zfree(test);

	// while(1) {
	// 	std::this_thread::sleep_for(std::chrono::seconds(1));
	// }


	
	// nfhost.detach_device("trtype:pcie traddr:0000.1b.00.0");

	// nfhost.log.log_inf("Hello World from NVMF host!\n");
	// nfhost.log.log_inf("lba count = %d\n", nfhost.block_count);
	// for(int i = 0; i < 5; ++i) {
	// 	nfhost.log.log_inf("Hello World from NVMF host!\n");
	// 	std::this_thread::sleep_for(std::chrono::seconds(1));
	// }

	

exit:

	fflush(stdout);
	nfhost.cleanup();
	return rc;
}