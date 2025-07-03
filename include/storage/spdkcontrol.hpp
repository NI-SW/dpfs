// class to control spdk behavior
#include <thread>
#include <string>
// #include <basic/types.hpp>
// 传入spdk的参数
struct spdkctx;
// spdk启动参数
struct spdk_app_opts;



/*
流程：

1、启动spdk
2、读取设备数与设备大小
3、创建区块(Cblock)


class dbrg_t {
public:
    uint64_t areaid : 8;        // 国家
    uint64_t cityid : 12;       // 城镇
    uint64_t nodeid : 10;       // 节点
    uint64_t blockid : 10;      // 区块
    uint64_t productid : 24;    // 商品(标记在区块中，该商品的起始磁盘块号)最大16777216       32GB = 16777216 * 2KB
};
    如果长度超限，则单独存储于其它块中

    管理磁盘与块的映射
    32GB/块
    4TB磁盘可存储128个块
    1、标记块号
    

    1、硬盘数量
    2、硬盘大小
    3、硬盘类型

    多块磁盘时：
    例：
    device0: 1TB
    device1: 2TB

    1TB -> 2KB-> 536870912
    2TB -> 2KB -> 1073741824
    分别在dev0,dev1上建立若干Cblock


    通过区块号索引到对应Cblock
    std::vector<Cblock*> ;


*/
// 管理区块
// class Cblock {
// public:
//     Cblock();
//     ~Cblock();
//     uint64_t blockid;               // 区块号
//     size_t startBlock;              // 起始磁盘块号
//     size_t blockSize;               // 实际区块内数据块数量
//     static constexpr size_t CblockSize = 1 << productIdBitSize; // 默认区块内数据块数量（记录内部有多少个磁盘数据块）

// };



class CSpdkControl {
public:
    void test();
    CSpdkControl(std::string json_config_file);
    ~CSpdkControl();
    // start and stop spdk
    void start_spdk();
    void stop_spdk();
    bool active();

    // open blob storage device
    int open_blob_device();
    // write buffer to blockPos of bdev
    size_t write_blob_block(void *buf, size_t len, size_t blockPos);
    // read len bytes from blockPos of bdev
    size_t read_blob_block(void *buf, size_t len, size_t blockPos);
    

private:
    int send_spdk_msg(void(*fn)(void *));
    
private:
    std::string json_config_file;
    spdkctx* ctx;
    // SPDK线程
    std::thread my_thread;
    spdk_app_opts* app_opts;
    bool running;
    size_t blockSize;


};