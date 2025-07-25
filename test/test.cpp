#include <storage/nvmf/nvmf.hpp>

using namespace std;

int main() {

    CNvmfhost nfhost(vector<string>{"trtype:pcie traddr:0000.1b.00.0", "trtype:pcie traddr:0000.13.00.0"});
    int rc = 0;
    if (nfhost.controllers.empty()) {
		fprintf(stderr, "no NVMe controllers found\n");
		rc = 1;
		goto exit;
	}

	printf("Initialization complete.\n");
	nfhost.hello_world();

exit:
	
	fflush(stdout);
	nfhost.cleanup();
}