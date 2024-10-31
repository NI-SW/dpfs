#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <linux/nvme_ioctl.h>
#include <iostream>
#include <sys/ioctl.h>
using namespace std;

int main() {
    int fd = open("/dev/nvme0n2", O_RDONLY);
    if (fd < 0) {
        cout << "open fail" << endl;
        return 1;
    }
    char *buffer = new char[4096];
    if(!buffer) {
        perror("new fail");
        return -1;
    }
    memset(buffer, 0, 4096);

    struct nvme_user_io io;
    io.addr = (unsigned long long)buffer;
    io.slba = 0;
    io.nblocks = 1;
    io.opcode = 2;

    if(ioctl(fd, NVME_IOCTL_SUBMIT_IO, &io) == -1) {
        perror("fail\n");
        cout << " ioctl fail " << endl;
    }

    close(fd);
    delete[] buffer;
    cout << " ok " << endl;
    cout << buffer << endl;
    return 0;
}