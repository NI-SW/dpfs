

operator HOST                                                                  DISK HOST
.open-------|           |----thread->|-->rdma/tcp over DPDK --> NVMe(local)---->|-----> disk1 ns1
.write------|-->dskctx--|----thread->|-->rdma/tcp over DPDK --> NVMe(local)---->|-----> disk2 ns1
.read-------|           |----thread->|-->rdma/tcp over DPDK --> NVMe(local)---->|-----> disk1 ns2
       |                                                                             |
       |                                                                             |
       |-----------------------------------------------------------------------------|        

// for each host
class CNvmfhost;

// for each disk
class CNvmeDisk;

// for each ns
class nvmfnsDesc;
                                                      








