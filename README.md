DPFS
Data Partitioned Foodtrace System
需求：
1、较低的成本
2、较高的性能
3、支持配料溯源


分布式食品溯源系统

1、优先考虑客户端功能，考虑客户需求
2、考虑服务端功能
3、考虑工作节点功能


客户端：
功能
1.连接      => dpfsClient
2.断开      => dpfsClient
3.发出请求  => dpfsDriver



行式存储√
列式存储
混合存储

SPDK √
DPDK


交易系统
1、...?








deps:
yum install kernel-headers



,
                "/usr/src/kernels/5.14.0-592.el9.x86_64/include/uapi",
                "/usr/src/kernels/5.14.0-592.el9.x86_64/arch/x86/include/generated/uapi",
                "/usr/src/kernels/5.14.0-592.el9.x86_64/tools/arch/x86/include",
                "/usr/src/kernels/5.14.0-592.el9.x86_64/arch/x86/include",
                "/usr/src/kernels/5.14.0-592.el9.x86_64/include"





2026.01.22
TODO:
创建系统表，剩余user部分
完善parser

