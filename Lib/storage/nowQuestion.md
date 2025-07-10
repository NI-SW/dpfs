Q:如何绑定硬盘与spdk的关系：

A: $SPDK_HOME/scripts/rpc.py bdev_aio_create  /dev/sdb      aio0       SCSI设备
                                              <devname>   <aioname>

Q:如何组织磁盘结构

A:
1、获取磁盘总块数（总块数可能变花，由bdev的块大小的变化而定）
2、绑定磁盘块与溯源数据的关系，加快寻址速度

Q:如何组织数据结构
CVS： "<areaId>, <productorId>, <name>, <length>, <binary data(pointer?)>"

typedef areaid_t        uint32_t
typedef producerid_t    uint32_t
typedef goodsid_t       uint64_t


struct goods {
    goodsid_t               goodsid;            // 唯一标识一类商品，用于索引溯源信息
    areaid_t                areaId;             // 标识商品地区
    producerid_t            producerId;         // 标识商品厂家
    std::vector<goodsid_t>  ingredient;  // 标识产品组成成分


};

    区id，厂商id， 商品id
    xxxx-xxxx-xxxx-xx

    need：
    需要全局锁，创建时将原有id + 1, 扩展节点时需要同步已有id， 扩展策略：
    

    创建国家地区指令
    返回创建的地区id
    areaid_t createRegion(std::string);
    
    创建厂商指令
    返回创建的厂商id
    producerid_t createProductor();
    
    创建商品类型
    返回创建的商品类型id
    goodsid_t createGoods();



设计原则：
尽量减少io次数
尽量使用存储压缩
设计冗余结构
使用id对商品进行区分及溯源，所以可以出现同名的商品，如鸡蛋，小麦等

    存储结构：
    根据spdk读取出来的bdev块数量
    
    如：spdk配置块大小为4KB，数量为65536块
    总大小为256MB，假设此时又另一块硬盘，数量为1024块，则大小为4096KB
    在网络上，此节点表现为总大小为260MB的存储节点

    使用商品ID作为索引，配料表使用配料的商品ID



A:

Q:压缩存储

A:


Q:将spdk存储引擎作为服务单独启动

A:

Q:向bdev写入数据

A:

Q:区块链劣势

A:运行成本远大于中心化交易系统，无法解决源头造假问题

Q：当前问题：如何高效存储控制表信息，如何高效索引控制表信息

A：

