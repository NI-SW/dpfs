## DPFS
# Data Partitioned Foodtrace System

需要编译器支持C++17
spdk编译参数：
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

# 目前支持编译的系统
OS                        | Architecture
------------------------- | ----------------------------------
Centos stream 9           | X86_64


# 
```
基于SPDK，使用内核旁路技术访问硬盘（需要SSD设备与nvme驱动）
设置磁盘组策略访问硬盘，可以访问网络SPDK磁盘组
磁盘块管理使用CBT
页面置换策略使用LRU
系统底层共用LRU内存池
LRU内保存DMA内存（通过DMA直接访问硬盘，使用零拷贝技术修改存储系统对象）
存储引擎基于B+树，使用行式存储
SQL解析器使用tidb解析器
DECIMAL数据类型使用mysql内置类型
有限的SQL语句支持（CREATE TABLE， INSERT）
新建表要求强制包含主键列，多PRIMARY字句自动判定为组合主键
网络连接服务基于grpc，tcp直连模式未开发完成
不支持事务，数据修改直接落盘
支持创建溯源组，通过溯源组名产生交易信息
支持溯源+配料再溯源
支持交易链溯源
```



