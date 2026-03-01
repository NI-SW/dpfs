DPFS
Data Partitioned Foodtrace System

spdk编译参数：
# ./configure --with-fio=../fio --with-rdma --with-shared
./configure --with-rdma --with-shared --with-fio=../fio/ --with-isal --with-isal-crypto

加载内核模块：
modprobe nvme_fabrics
modprobe nvme_tcp
modprobe nvme_rdma
modprobe nvmet
modprobe nvmet_rdma
modprobe rdma_rxe
rdma link add rxe_0 type rxe netdev ens160

ens160改成你的网卡名

初始化SPDK环境：
thirdparty/spdk/scripts/setup.sh


需求：
成本
性能

配合部分业务功能
溯源+再溯源



行式存储√
列式存储
混合存储

SPDK √
DPDK



