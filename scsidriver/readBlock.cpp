#include <iostream>
#include <scsi/scsi.h>
#include <scsi/sg.h>
#include <scsi/scsi_ioctl.h>
#include <string.h>
#include <fcntl.h>
#include <sys/ioctl.h>
using namespace std;


int main(int argc, char** argv) {

    int fd = open("/dev/sdb", O_RDONLY);
    if (fd < 0) {
        cout << "Error opening device" << endl;
        return 1;
    }
    
    int lba = atoi(argv[1]);
    int len = atoi(argv[2]);

    sg_io_hdr_t io_hdr;
    io_hdr.interface_id = 'S';
    io_hdr.cmd_len = 6;
    io_hdr.mx_sb_len = 32;
    io_hdr.dxfer_direction = SG_DXFER_FROM_DEV;
    io_hdr.dxfer_len = 512 * len;
    io_hdr.dxferp = new char[io_hdr.dxfer_len];
    io_hdr.cmdp = (unsigned char*)new char[io_hdr.cmd_len];
    io_hdr.sbp = (unsigned char*)new char[io_hdr.mx_sb_len];
    io_hdr.timeout = 20000;
    io_hdr.flags = 0;
    io_hdr.pack_id = 0;
    char cdb[6] = {0x08, 0, lba >> 8, lba, len, 0};
    memcpy(io_hdr.cmdp, cdb, 6);

    ioctl(fd, SG_IO, &io_hdr);

    cout << (char*)io_hdr.dxferp << endl;
    
    
    return 0;
}