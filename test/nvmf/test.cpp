/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#include <storage/nvmf/nvmf.hpp>
#include <thread>
using namespace std;
std::thread test;
int testnfhost(void* arg = nullptr) {
	int rc = 0;
	
	auto hclock = std::chrono::high_resolution_clock::now();
	auto now = std::chrono::duration_cast<std::chrono::milliseconds>(hclock.time_since_epoch()).count();
	int reqs = 0;
	int start = 0;
	int end = 10000;

	dpfsEngine* engine = nullptr;
	if(arg) {
		engine = (dpfsEngine*)arg;
	} else {
		engine = new CNvmfhost();
	}


    // CNvmfhost nfhost;
	CNvmfhost& nfhost = dynamic_cast<CNvmfhost&>(*engine);


	if(!arg) {
		CNvmfhost* nfhost2 = new CNvmfhost();
		nfhost2->copy(nfhost);
		printf("starting nfhost2\n");
		test = thread([&nfhost2](){testnfhost(nfhost2);});

		printf("nvmfhost2 started\n");
	}




	nfhost.log.set_log_path("./output.log");
	// nfhost.log.set_loglevel(logrecord::LOG_DEBUG);
	nfhost.log.set_async_mode(true);

	nfhost.log.log_inf("Starting NVMF host...\n");

	nfhost.set_async_mode(true);



	rc = nfhost.attach_device("trtype:pcie traddr:0000.1b.00.0");  nfhost.attach_device("trtype:pcie traddr:0000.13.00.0");
	// rc = nfhost.attach_device("trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50658 subnqn:nqn.2016-06.io.spdk:cnode1");
	// rc = nfhost.attach_device("trtype:tcp adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1");
	if(rc) {
		nfhost.log.log_error("Failed to attach device\n");
		goto exit;
	}
	printf("using pcie transport\n");

	std::this_thread::sleep_for(std::chrono::seconds(1));


	

	
	nfhost.log.set_log_path("./output.log");

	char* test[100];
	for(int i = 0;i < 100; ++i) {
		test[i] = (char*)nfhost.zmalloc(dpfs_lba_size * 10); // 40kB
		if(!test[i]) {
			printf("Failed to allocate memory for test buffer\n");
			return -1;
		}
	}
	// test[0] = (char*)nfhost.zmalloc(dpfs_lba_size * 10); // 40kB








	// nfhost.hello_world();

	memcpy(test[0], "Hello World from NVMF host11!", 30);
	memcpy(test[1], "Hello World from NVMF host22!", 30);
	memcpy(test[2], "Hello World from NVMF host33!", 30);
	memcpy(test[3], "Hello World from NVMF host44!", 30);

	printf("Write data: %s\n", test[0]);
	if(arg) {
		start = 10000;
		end = 20000;
	}
	
	now = std::chrono::duration_cast<std::chrono::milliseconds>(hclock.time_since_epoch()).count();



	for(int i = start; i < end; ++i) {
		int waitCount = 0;

		do{
			if(waitCount > 2) {
				nfhost.log.log_error("Write data failed after 10 retries, exiting...\n");
				break;
			}
			if(i % 1000 == 0) {
				printf("count: %d\n", i);
			}
			rc = nfhost.write(5242879 + i * 10, test[i % 100], 10); //10 * i + k * 100); //5242879
			if(rc < 0) {
				++waitCount;
				if(rc == -ENOMEM) {
					nfhost.log.log_error("Out of memory, try to wait\n", reqs);
					continue;
				} else if(rc == -ENXIO) {
					nfhost.log.log_error("Device not ready, try to wait\n", reqs);
					break;
				}
				nfhost.log.log_error("Write data err: %d\n", rc);
			}
			break;
		} while(1);

		if(rc < 0) {
			nfhost.log.log_error("Write data err: %d\n", rc);
			break;
		}
		
		reqs += rc;
		if(reqs >= 250) {
			nfhost.sync(reqs);
			reqs = 0;
		}
		// std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	printf("sync reqs = %d\n", reqs);
	nfhost.sync(reqs);
	printf("duration: %ld ms, reqs = %d\n", std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count() - now, reqs);

	reqs = 0;

	rc = nfhost.read(10, test[5], 1);
	if(rc) {
		reqs += rc;
	} else {
		printf("Read data err: %d\n", rc);
	}
	printf("rc = %d\n", rc);
	rc = nfhost.sync(reqs);
	printf("Read data: %s\n", test[5]);


	for(int i = 0; i < 100; ++i) {
		nfhost.zfree(test[i]);
	}

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
	delete dynamic_cast<CNvmfhost*>(engine);
	return rc;
}

int main() {
	dpfsEngine* engine = nullptr; // new CNvmfhost();
	testnfhost(engine);
	std::this_thread::sleep_for(std::chrono::seconds(1));
	if(test.joinable())
		test.join();

	// testnfhost();
	return 0;
}

