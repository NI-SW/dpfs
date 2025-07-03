/**
 * File: types.hpp
 * Created Time: 2025-04-29
 * Author: NI-SW (947743645@qq.com)
 */

#include <stdint.h>
#include <vector>
#include <string>
#include <dpendian.hpp>

// block recognize type
// 如需扩大brg_t的范围，可指定brg_t为其它类型，但需完成areaid，cityid，nodeid, blockid, productid数学运算符的重载（=,<,>,==）

// 默认使用国家+城镇进行区块划分
/*
basic:

    默认支持256个国家，每个国家4096个城镇
    支持最大节点数1024个，每个节点至多1024个区块
    |   国家|      城镇|      节点|     块|    商品|
    |      8|       12|        10|     10|     24|
    
*/
class dbrg_t;
enum dataType_t;

#if __cplusplus >= 201103L
constexpr size_t maxareaBitSize     = 8;
constexpr size_t maxcityBitSize     = 12;
constexpr size_t maxNodeBitSize     = 10;
constexpr size_t maxBlockBitSize    = 10;
// 商品末尾id大小
constexpr size_t productIdBitSize   = 24; 
//<数据类型，数据长度>
using dataStruct = std::pair<dataType_t, size_t>;                               
// <数据结构数组，表名>
using dataGroup = std::pair<std::vector<dataStruct>, std::vector<std::string>>;
using brg_t =  dbrg_t;
using goodsid_t = brg_t;   
#else
const size_t maxareaBitSize     = 8;
const size_t maxcityBitSize     = 12;
const size_t maxNodeBitSize     = 10;
const size_t maxBlockBitSize    = 10;
const size_t productIdBitSize   = 24;
typedef dbrg_t brg_t;
typedef brg_t  goodsid_t;
typedef std::pair<dataType_t, size_t> dataStruct;
typedef std::pair<std::vector<dataStruct>, std::vector<std::string>> dataGroup;
#endif

enum dataType_t {
    TYPE_NULL = 0,
    TYPE_INT8 = 1,
    TYPE_INT16 = 2,
    TYPE_INT32 = 3,
    TYPE_INT64 = 4,
    TYPE_UINT8 = 5,
    TYPE_UINT16 = 6,
    TYPE_UINT32 = 7,
    TYPE_UINT64 = 8,
    TYPE_FLOAT32 = 9,
    TYPE_FLOAT64 = 10,
    TYPE_BLOB = 11,
    TYPE_CHAR = 12,
    TYPE_VARCHAR = 13,
};

// default block recognize type
class dbrg_t {
public:
    uint64_t areaid : maxareaBitSize;       // 国家
    uint64_t cityid : maxcityBitSize;       // 城镇
    uint64_t nodeid : maxNodeBitSize;       // 节点
    uint64_t blockid : maxBlockBitSize;     // 区块
    uint64_t productid : productIdBitSize;  // 商品
    dbrg_t& operator=(const dbrg_t& other) {
        areaid = other.areaid;
        cityid = other.cityid;
        nodeid = other.nodeid;
        blockid = other.blockid;
        productid = other.productid;
        return *this;
    }
    bool operator==(const dbrg_t& other) {
        return (areaid == other.areaid && cityid == other.cityid && nodeid == other.nodeid && blockid == other.blockid && productid == other.productid);
    }

};





/*
‌冰箱存储‌：     将食品放入冰箱中保存是最常见的方法。冰箱可以延长食品的保质期，尤其是对于易腐食品如肉类、鱼类、蔬菜和水果等。冰箱的冷藏室通常保持在4°C左右，而冷冻室则在-18°C以下，适合长期保存肉类和鱼类‌。
‌冷藏柜‌：       对于商业用途或大量食品存储，冷藏柜是一个不错的选择。冷藏柜可以提供稳定的低温环境，适合保存需要冷藏的食品，如饮料、奶制品和熟食等‌。
‌冷冻保存‌：     将食品放入冰箱的冷冻室可以延长其保存时间，尤其适合肉类、鱼类和冰淇淋等需要长时间保存的食品。冷冻可以减缓微生物的生长和酶的活动，从而延长食品的保质期‌。
‌真空包装‌：     真空包装可以有效排除包装内的空气，防止氧化和微生物的生长，从而延长食品的保质期。这种方法适用于各种类型的食品，包括肉类、蔬菜和水果等‌。
‌罐装和瓶装‌：   将食品装入密封的罐子或瓶子中，可以有效隔绝空气和水分，防止氧化和微生物的生长。这种方法适用于果酱、腌制品和调味品等‌。
‌干燥保存‌：     对于一些干货如干果、坚果和谷物等，可以通过干燥保存来延长其保质期。干燥可以防止霉菌和虫害的生长，同时保持食品的营养和口感‌。
‌冷藏库‌：       对于需要大量存储的场所，如超市和餐厅，冷藏库是一个理想的选择。冷藏库可以提供更低的温度环境，适合长期保存大量食品‌。
*/

// enum storage_t {
//     REFRIGERATOR,           // 冰箱存储
//     COLD_STORAGE,           // 冷藏柜
//     FREEZING,               // 冷冻保存
//     VACUUM_PACKAGING,       // 真空包装
//     CANNED_BOTTLED,         // 罐装和瓶装
//     DRY_STORAGE,            // 干燥保存
//     COLD_STORAGE_ROOM       // 冷藏库
// };

/*
食品溯源信息主要包括以下几个方面‌：

‌源头信息采集‌：在农田阶段，农民需要记录农作物的品种、种植时间、施肥用药情况、灌溉水源等信息。
例如，在蔬菜种植过程中，详细登记所使用的农药名称、剂量、使用时间以及安全间隔期，确保农产品从源头就符合安全标准。
对于畜禽养殖，要记录畜禽的品种、养殖环境、饲料来源及添加剂使用情况等‌。

‌加工环节信息录入‌：食品加工企业需要详细记录原材料的供应商、进货批次、检验报告等信息。
在加工过程中，记录加工工艺、添加剂使用、生产设备清洗消毒情况等。
例如，面包加工企业需记录面粉、酵母、糖等原材料的来源，以及面包制作过程中的搅拌、发酵、烘焙时间与温度等工艺参数‌。

‌流通与销售信息记录‌：物流企业记录食品的运输路线、运输温度（针对冷藏食品）、装卸时间等信息。
销售终端如超市、农贸市场等记录食品的进货渠道、上架时间、销售数量等信息。通过信息化系统，超市将每一批次食品的进货来源、陈列位置以及每日销售数据进行详细记录，方便消费者查询与监管部门追溯‌。

‌消费者回溯功能‌：一些追溯系统提供了消费者回溯功能，消费者可以通过扫描产品包装上的追溯码或输入相关信息，查询食品的生产、加工和流通情况，了解食品的来龙去脉‌。

‌具体应用案例‌：以鲁花花生油为例，扫描其外包装上的二维码后，界面会呈现产品的品质保障与技术支撑信息。
首屏中央显示FA食品真实品质认证的巨型图标，这一认证由食品真实性技术国际联合研究中心发起，依托国家权威机构的技术背书。
图标下方依次排列三组权威认证文字，如“国家食品质量检验检测中心认证”“流通市场抽检检验通过”，以醒目排版强化品质信任。
产品下方的小字信息清晰列明产品种类、名称及特色，如“5S物理压榨工艺”“一级压榨”等，精准传递产品技术优势与品质特征‌。
*/




/*
block
---------------------------------------------
|goods|transportInfo|environmentInfo|foodobj|
---------------------------------------------
*/

/*
    运输信息包括：
    运输方式、运输时间、运输公司、运输路线、运输温度等
    运输方式：如冷藏、冷冻、常温等
    运输时间：如从生产厂家到商超的运输时间
    运输公司：如顺丰、京东等
    运输路线：如从生产厂家到商超的运输路线
    运输温度：如冷藏运输的温度范围
*/

class transportInfo {
public:
    transportInfo();
    transportInfo(transportInfo& ti);
    ~transportInfo();

private:
    time_t        transportTime;        // 运输时间 (8B) sizeof(time_t)
    std::string   transportMethod;      // 运输方式 (32B)
    std::string   transportCompany;     // 运输公司 (128B)
    std::string   transportTemperature; // 运输温度 (32B)
    std::string   origin;               // 运输起点 (128B)
    std::string   destination;          // 运输终点 (128B)
    std::string   others;               // 其他信息 (可变长度) default 512B 如果长度超限，则存储一个指向其它块的指针，包含（块号，块数）
};


/*
    食品生产环境信息：
    包括种植基地或养殖环境的地理位置、土壤、水质检测报告，使用的种子、种苗来源及品种信息，种植或养殖的具体地块编号、面积，农事活动记录如播种、施肥、灌溉、用药等日期和详情‌
    记录化肥、农药、兽药、饲料及其供应商信息，使用量、使用时间及频次记录，生物制剂、生长调节剂的使用情况，环境数据如温湿度、光照、CO2浓度等
*/
class environmentInfo {
public:
    environmentInfo();
    environmentInfo(environmentInfo& ei);
    ~environmentInfo();
private:

    std::string  info;          // 生产环境信息,结构可能复杂，使用json格式存储《？》
    std::string  temperature;   // 温度
    std::string  humidity;      // 湿度
    std::string  light;         // 光照
    uint32_t      batch;        // 产品批次
    std::string   others;       // 其他信息 (可变长度) default 512B 如果长度超限，则存储一个指向其它块的指针，包含（块号，块数）
};


class productobj {
public:
    productobj();

    // 检查食品对象，返回食品对象，检查次数达到一定次数时，标记被检查食品对象为失效
    productobj* check();

    // 检查食品对象，返回食品对象，但不修改检查次数
    productobj* supervisorCheck();

private:
/*

    64bit
    
    uint64_t     uniqueId : 28;         // 2.6亿
    uint64_t     checkStatus : 1;
    uint64_t     checkCount : 3;        // 


    uint64_t     uniqueId : 28;         // 2.6亿
    uint64_t     checkStatus : 1;
    uint64_t     checkCount : 3;        // 
*/
    uint32_t        uniqueId;               // 唯一标识一个商品对象      sizeof(uniqueobj_t)     4B
    uint32_t        checkStatus : 1;        // 标记商品对象是否失效
    uint32_t        checkCount : 7;         // 查验次数                 sizeof(uint16_t)           至多127次
    uint32_t        manufactureDate: 24;    // 生产日期（以天计算）                              max= 16777216天 ≈ 1677万天 自1970年1月1日起计算的天数
    time_t          checkTime;              // 上一次查验时间            sizeof(time_t)          8B
    // total 16B(storage)
    std::vector<uint32_t> backTrace; // 追溯信息，记录每次与本商品相关的交易的交易id，默认最大长度0，即不使用一物一码追溯功能
    // 如果设定最大追溯深度为20，则单件商品占用96B，假设某批次有1000万件商品，则这批商品共占用存储约920MB，如果不使用追溯，则单件商品占用16B，则本批次占用大小约152MB


    std::string     lastTradePhone;         // 最终交易人手机号码，默认不启用


};




// 商品种类信息
class goods {
public:
    goods();
    ~goods();

private:
    // 固定长度信息部分，商品首次时就确定所占空间大小
    goodsid_t               goodsid;                // 唯一标识一种商品，用于索引溯源信息
    uint8_t                 compress : 1;           // 是否启用压缩
    uint8_t                 encrypt : 1;            // 是否启用加密
    uint8_t                 compressLevel : 6;      // 压缩等级
    uint16_t                backTraceDepth;         // 商品追溯深度，默认0表示不启用一物一码追溯功能

    // 下列数据可规划到生产控制表中
    // std::string             name;                   // 商品名称
    // std::string             brand;                  // 商品品牌
    // std::string             producerName;           // 商品厂家名称
    // std::vector<uint8_t>    qualityInspection;      // 商品质量检测报告
    // std::vector<goodsid_t>  ingredient;             // 标识产品组成成分,进行继续溯源时可
    // uint32_t                manufactureDate;        // 商品生产日期(仅保存天数日期)
    // uint32_t                guaranteePeriod;        // 商品保质期(仅保存天数)


    // 控制表在第一次记录商品时写入，表结构不可修改
    // 自定义结构信息控制表，<类型,长度(byte)> 给定长度限制
    // 附加信息，如环境信息等登记时确定的信息，不可修改
    // 控制表名称,0号表为商品控制表
    std::vector<std::string>    tablesName;
    // 如商品控制表，部分商品信息只进行一次录入，此表为必填项,首次录入填写
    std::vector<dataStruct>     fixedInfo;
    // 字段名称
    std::vector<std::string>    fixedFieldName;


    // 交易物流控制表,仓储控制表等
    // 多个控制表
    std::vector<std::string>    varTableName;
    std::vector<dataGroup>      variableInfo;
   
    // 进行商品校验时，使用某种算法快速定位数组中是否存在指定商品id foodObjs[位置]
    // 仅针对当前批次，每批次产品登记时单独注册一个商品类
    // 一物一码 == goodsid . productuniqueid
    // 商品唯一id数组
    std::vector<productobj>     productObjs;  


};

class CInfoLoader {
public:
    CInfoLoader();
    ~CInfoLoader();

    // 在磁盘中读取该种类商品的数据结构信息
    int loadStruct(goodsid_t id) {
        int rc = 0;
        // 读取数据结构信息
    
        if(rc != 0) {
            return -1;
        }
    
        // 读取数据
        if(rc != 0) {
            return -2;
        }
    
        // 返回数据
        if(rc != 0) {
            return -3;
        }
        return rc;
    }

    // 在磁盘中读取该种类商品中某一件的信息
    int getFoodInfo(uint32_t productId) {
        int rc = 0;
        // 读取数据结构信息
        if(rc != 0) {
            return -1;
        }
    
        // 读取数据
        if(rc != 0) {
            return -2;
        }
    
        // 返回数据
        if(rc != 0) {
            return -3;
        }
        return rc;
    }

    // 保存品种信息到磁盘
    int saveStruct(goods& g) {
        int rc = 0;
        // 保存数据结构信息
        if(rc != 0) {
            return -1;
        }
    
        // 保存数据
        if(rc != 0) {
            return -2;
        }
    
        
        return rc;
    }
};

class CInfoCreator {
public:
    CInfoCreator();
    ~CInfoCreator();

    // 创建一个新的商品信息
    int createNewGoods(goods& g) {
        
    }
};



/*
食品：

生产日期
保质期

自定义信息：
存储方式
运输方式
合格信息

ID标记的信息：
食品ID
食品合格信息ID
生产厂家ID
地区ID
运输ID



‌农产品溯源信息包括以下内容‌：

‌基础信息‌：农产品的名称、品种、产地、生产日期、批次号或唯一追溯码等基本信息‌。

‌生产者信息‌：生产企业或农户的名称、地址、联系方式、资质认证情况等‌。
‌种植养殖信息‌：种植基地或养殖环境的地理位置、土壤、水质检测报告；使用的种子、种苗来源及品种信息；种植或养殖的具体地块编号、面积；农事活动记录，如播种、施肥、灌溉、用药等日期和详情‌。

‌投入品管理‌：化肥、农药、兽药、饲料及其供应商信息，使用量、使用时间及频次记录；生物制剂、生长调节剂的使用情况；生长期间的关键环境参数，如温湿度、光照、CO2浓度等‌。

‌农事过程记录‌：全生育期的照片记录、视频监控记录，以及关键生长阶段的状态描述‌。

‌质量检测报告‌：农产品收获前后的质量检测结果，包括农药残留、重金属含量、微生物污染等安全性指标‌。

‌加工与包装信息‌：加工企业、地点、加工日期、加工方法、包装材料及规格等‌。

‌流通与分销‌：产品从产地到销售点的全程物流信息，包括仓储、运输、分销商及零售商信息‌。

‌销售与消费‌：销售日期、销售渠道、终端销售点或电商平台信息，以及消费者反馈渠道‌。

‌认证与标签信息‌：有机认证、绿色食品认证、地理标志产品等特殊标识和认证资料‌。

这些信息通过标识模块（如二维码、RFID标签等）、数据采集模块（传感器、扫描仪、摄像头等设备）、信息查询模块（消费者和监管部门通过扫描或识别标识获取信息）、生产管理模块（记录农事活动和环境监测）、加工储运模块（记录加工和运输信息）等技术手段进行跟踪和记录，确保农产品的全链条透明度和可追溯性‌。

*/