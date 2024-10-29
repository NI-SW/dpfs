#include <fcntl.h>
#include <unistd.h>
#include <linux/nvme_ioctl.h>

int main() {
    int fd = open("/dev/sdb", O_RDONLY);
    if (fd < 0) {
        return 1;
    }
}