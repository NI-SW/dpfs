/**
 * File: types.hpp
 * Created Time: 2025-04-29
 * Author: NI-SW (947743645@qq.com)
 */

 /*
    1. 设计存储系统接口
    2. 设计存储节点接口
    3. 设计存储区块接口
    4. 设计存储节点数据接口
    5. 设计存储区块数据接口
    6. 设计存储节点数据接口
    7. 设计存储架构
 */
#include <string>
#include <stddef.h>
#include <basic/types.hpp>

/*
    goodsid_t id;
    使用区块id与商品id定位商品在磁盘中的位置
    id.blockid;
    id.productid;

    

    uint64_t blockid : 10;      // 区块
    uint64_t productid : 24;    // 商品
    
*/

// 指定默认块大小为2KB

class CBlock {
public:
    // 首地址
    uint64_t startAddress;
    // 尾地址
    uint64_t endAddress;
};

class CBlockManager {
public:
    CBlockManager();
    ~CBlockManager();

    /*
        初始化存储系统，其行为包括：
        1. 计算存储空间大小
        2. 创建存储区块
        3. 创建存储节点数据，等待系统连接
    */
    int initStorageSystem();

    /*
        初始化存储节点，其行为包括：
        1. 计算存储空间大小
        2. 创建存储区块
        3. 创建存储节点数据，等待系统连接
    */
    int initStorageNode();

    /* 
        登记新增的存储节点，其行为包括
        1. 获取新增节点空间大小
        2. 连接存储节点
   */
    int addStorageNode();

    /*
        从存储系统中删除存储节点，其行为包括
        1. 删除存储节点数据
        2. 断开存储节点连接
    */
    int removeStorageNode();

    /*
        迁移存储节点，其行为包括
        1. 获取迁移节点空间大小
        2. 连接存储节点
        3. 将数据从原存储节点迁移到新存储节点
        4. 删除原存储节点数据
        5. 断开原存储节点连接
    */
    int migrateStorageNode();

    /*
        重组存储节点，其行为包括
        1. 清除已标记为删除的数据
        2. 对剩余数据进行紧凑操作
    */
    int reorgStorageNode(const std::string& nodeName, size_t blockSize);


    int createStorageBlock(const std::string& blockName, );


    void addBlock(const std::string& blockName);
    void removeBlock(const std::string& blockName);
    void listBlocks() const;
    static void loop();

private:
    dbrg_t recognizeId;

};