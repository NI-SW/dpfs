#include <storage/nvmf/nvmf.hpp>
#include <thread>
using namespace std;

int main() {

    CNvmfhost nfhost;
	nfhost.set_async_mode(true);
	nfhost.log.set_loglevel(logrecord::LOG_DEBUG);
	// nfhost.log.set_loglevel(logrecord::LOG_FATAL);
	// nfhost.attach_device("trtype:pcie traddr:0000.1b.00.0");
	// nfhost.attach_device("trtype:pcie traddr:0000.13.00.0");
	// nfhost.attach_device("trtype:rdma adrfam:IPv4 traddr:192.168.34.12 trsvcid:50658 subnqn:nqn.2016-06.io.spdk:cnode1");
	nfhost.attach_device("trtype:tcp adrfam:IPv4 traddr:192.168.34.12 trsvcid:50659 subnqn:nqn.2016-06.io.spdk:cnode1");
	nfhost.set_logdir("./output.log");

	char* test = (char*)nfhost.zmalloc(4096 * 4);	
    int rc = 0;
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
	nfhost.write(5242879, test, 8192);

	nfhost.sync(1);

	nfhost.read(5242879, test + 4096, 4096);
	nfhost.sync(1);
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