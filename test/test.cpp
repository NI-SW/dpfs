#include <storage/nvmf/nvmf.hpp>
#include <thread>
using namespace std;

int main() {

    CNvmfhost nfhost;
	nfhost.attach_device("trtype:pcie traddr:0000.1b.00.0");
	nfhost.set_logdir("./output.log");
	nfhost.log.set_loglevel(logrecord::LOG_DEBUG);

	nfhost.set_async_mode(true);
	char* test = nfhost.zmalloc(1024);	
    int rc = 0;
    if (nfhost.devices.empty()) {
		fprintf(stderr, "no NVMe controllers found\n");
		rc = 1;
		goto exit;
	}

	printf("Initialization complete.\n");
	// nfhost.hello_world();

	memcpy(test, "Hello World from NVMF host!", 28);
	
	memcpy(test, "WUHUWANDAn", 11);
	printf("before read %s\n", test);
	nfhost.write(0, test, 1024);
	nfhost.read(0, test, 1024);
	printf("Read data: %s\n", test);
	memcpy(test, "WUHUWANDAn", 11);

	nfhost.read(0, test, 1024);
	printf("Read data: %s\n", test);
	nfhost.write(0, test, 1024);
	// nfhost.zfree(test);

	// while(1) {
	// 	std::this_thread::sleep_for(std::chrono::seconds(1));
	// }


	// std::this_thread::sleep_for(std::chrono::seconds(1));
	// nfhost.detach_device("trtype:pcie traddr:0000.1b.00.0");

	nfhost.log.log_inf("Hello World from NVMF host!\n");
	// for(int i = 0; i < 5; ++i) {
	// 	nfhost.log.log_inf("Hello World from NVMF host!\n");
	// 	std::this_thread::sleep_for(std::chrono::seconds(1));
	// }

	

exit:

	fflush(stdout);
	nfhost.cleanup();
	return rc;
}