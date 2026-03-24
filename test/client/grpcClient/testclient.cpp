#include <dpfsclient/grpcclient.hpp>
#include <iostream>
#include <csignal>
#include <thread>
#include <dpfsdebug.hpp>
#include <fstream>
#include <basic/dpfsconst.hpp>
using namespace std;

// #define __TEST_SQL__
// #define __TEST_IDXITER__
// #define __TEST_CREATETRACABLEPRO__
// #define __TEST_TRACEABLE_PROSTRUCTURE__
// #define __TEST_TRACEBACK__
// #define __TEST_TRACEBACK_SECOND__
// #define __TEST_MAKETRADEE__
// #define __TEST_MAKETRADEE_SECOND__
// #define __TEST_MAKETRADEE_LOTS__
#define __TEST_VARTRACE__

volatile bool g_exit = false;
void sigfun(int sig) {
    cout << "Signal " << sig << " received, exiting..." << endl;
    g_exit = true;
}

int printTable(CGrpcCli& client, const std::string& schema, const std::string& table, const std::vector<std::string>& idxCol, const std::vector<std::string>& idxVals);

int main(int argc, char* argv[]) {
    // signal(SIGINT, sigfun);
    // signal(SIGKILL, sigfun);
    std::string traceCodePrefix;
    std::vector<std::string> idxVals;
    std::string traceCode;
    std::string traceResult;
    int32_t productionId = 0;
    std::string tradeId;
    bidx spxxbBidx = {0, 0}; 
    int val = 9980;



    std::string ip = "127.0.0.1";
    std::string port = "20500";
    
    if (argc == 3) {
        ip = argv[1];
        port = argv[2];
    }
    
    std::string connStr = ip + ":" + port;
    cout << "connecting to " << connStr << endl;
    
    auto channel = grpc::CreateChannel(connStr, grpc::InsecureChannelCredentials());
    if (channel == nullptr) {
        cerr << "Failed to create gRPC channel" << endl;
        return -1;
    }

    CGrpcCli client(channel);
    string username = "root";
    string password = "root";
    int rc = client.login(username, password);
    if (rc == 0) {
        cout << "Login successful for user: " << username << endl;
    } else {
        cout << "Login failed for user: " << username << ", error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        return 0;
    }

    // test sql execution
    /*


    CREATE TABLE OOO.PPP (A INT NOT NULL PRIMARY KEY, B DOUBLE, C CHAR(20), D DECIMAL(10, 2))
    insert into OOO.PPP values (1, 1.1, 'hello', 1.11), (2, 2.2, 'world', 2.22)
    insert into OOO.PPP values (3, 123.4, 'wuhudasima', 3.33)
    insert into OOO.PPP values (4, 2.29, 'world1', 4.44), (5, 1.15, 'hello2', 5.55), (6, 2.32, 'nihao', 6.66)

    CREATE TABLE OOO.PPP1 (A INT NOT NULL PRIMARY KEY, B DOUBLE, C CHAR(20), D DECIMAL(10, 2))
    insert into OOO.PPP1 values (1, 1.1, 'hello', 1.11), (2, 2.2, 'world', 2.22)
    insert into OOO.PPP1 values (3, 123.4, 'wuhudasima', 3.33)
    insert into OOO.PPP1 values (4, 2.29, 'world1', 4.44), (5, 1.15, 'hello2', 5.55), (6, 2.32, 'nihao', 6.66)
    */
#ifdef __TEST_SQL__
    cout << "Input SQL:" << endl;
    string sql;
    while (getline(cin, sql)) {
        if (sql == "exit") {
            break;
        }
        int rc = client.execSQL(sql);
        if (rc == 0) {
            cout << "SQL executed successfully" << endl;
            cout << "Message: " << client.msg << endl;
        } else {
            cout << "SQL execution failed, error code: " << rc << endl;
            cout << "Error message: " << client.msg << endl;
        }
        cout << "Input SQL:" << endl;
    }
#endif

#ifdef __TEST_IDXITER__
    /* 
        test get table handle
    */
    rc = client.getTableHandle("OOO", "PPP1");
    if (rc != 0) {
        cout << "Get table handle failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
    } else {
        cout << "Get table handle successfully" << endl;
        cout << "Message: " << client.msg << endl;
    }

    /*
        test get table index iterator
    */
    vector<string> idxCol = {"A"};

    // if key is not string, use the string like an int pointer

    idxVals.clear();
    idxVals.resize(1);
    val = 1;
    idxVals[0].resize(sizeof(val));
    memcpy(const_cast<char*>(idxVals[0].data()), &val, sizeof(val));

    IDXHANDLE hidx;

    rc = client.getIdxIter(idxCol, idxVals, hidx);
    if (rc != 0) {
        cout << "Get index iterator failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        return rc;
    } else {
        cout << "Get index iterator successfully" << endl;
        cout << "Message: " << client.msg << endl;
        cout << "Index handle: " << hidx << endl;
    }


    const auto& colInfo = client.getColInfo(hidx);

    rc = client.fetchNextRow(hidx);
    if (rc != 0) {
        cout << "Fetch next row failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        return rc;
    } else {
        cout << "Fetch next row successfully" << endl;
        // cout << "Message: " << client.msg << endl;
    }

    std::string gval;

    for (int i = 0; i < 10; ++i) {
        for (int j = 0; j < colInfo.size(); ++j) {
            rc = client.getDataByIdxIter(hidx, j, gval);
            if (rc != 0) {
                cout << "Get row by index iterator failed, error code: " << rc << endl;
                cout << "Error message: " << client.msg << endl;
                return rc;
            } else {
                cout << "Get row by index iterator success, pos " << j << endl;
                if (colInfo[j].getType() == dpfs_datatype_t::TYPE_CHAR || 
                    colInfo[j].getType() == dpfs_datatype_t::TYPE_VARCHAR ||
                    colInfo[j].getType() == dpfs_datatype_t::TYPE_TIMESTAMP) {
                    cout << "Get row by index iterator success, value: " << gval << endl;
                } else if (colInfo[j].getType() == dpfs_datatype_t::TYPE_DECIMAL) {
                    my_decimal dec;
                    rc = binary2my_decimal(0, (const uchar*)gval.data(), &dec, colInfo[j].getDds().genLen, colInfo[j].getScale());
                    if (rc != 0) {
                        cout << "Convert binary to decimal failed, error code: " << rc << endl;
                        return rc;
                    }
                    // std::string decStr;
                    String decStr;
                    rc = my_decimal2string(0, &dec, &decStr);
                    if (rc != 0) {
                        cout << "Convert decimal to string failed, error code: " << rc << endl;
                        return rc;
                    }
                    cout << "Get row by index iterator success, binary value: " << endl;
                    printMemory(gval.data(), gval.size()); cout << endl;

                    cout << "get decimal value = " << decStr.ptr() << endl;
                } else if (colInfo[j].getType() == dpfs_datatype_t::TYPE_DOUBLE) {
                    double dval;
                    memcpy(&dval, gval.data(), sizeof(dval));
                    cout << "Get row by index iterator success, double value: " << dval << endl;
                } else if (colInfo[j].getType() == dpfs_datatype_t::TYPE_INT) {
                    int ival;
                    memcpy(&ival, gval.data(), sizeof(ival));
                    cout << "Get row by index iterator success, int value: " << ival << endl;
                } else {   
                    cout << "Get row by index iterator success, type is not string, binary value: ";
                    printMemory(gval.data(), gval.size()); cout << endl;
                }
            }
        }

        rc = client.fetchNextRow(hidx);
        if (rc != 0) {
            if (rc == -ENODATA) {
                cout << "No more rows to fetch from server." << endl;
                break;
            }
            cout << "Fetch next row failed, error code: " << rc << endl;
            cout << "Error message: " << client.msg << endl;
            return rc;
        } else {
            cout << "Fetch next row successfully" << endl;
            // cout << "Message: " << client.msg << endl;
        }
    }

#endif

#ifdef __TEST_CREATETRACABLEPRO__

    std::map<std::string, std::string> base_info;
    std::map<std::string, std::string> ingrendian_info;
    base_info["type"] = "apple";
    base_info["validDate"] = "2024-01-01";
    base_info["brand"] = "洛川苹果";
    ingrendian_info["INGRE1"] = "10";
    ingrendian_info["INGRE2"] = "12.5";
    

    rc = client.createTracablePro("OOO", "APPLE", base_info, ingrendian_info, 1000, traceCodePrefix);
    if (rc != 0) {
        cout << "Create traceable production failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        return rc;
    } else {
        cout << "Create traceable production successfully" << endl;
        cout << "Message: " << client.msg << endl;
        cout << "Trace code prefix: " << endl;
        printMemory(traceCodePrefix.data(), traceCodePrefix.size());
        cout << endl;
    }
    fstream outTraceCode;
    outTraceCode.open("trace_code_prefix.bin", ios::out | ios::binary);
    if (!outTraceCode) {
        cout << "Failed to open file for writing trace code prefix" << endl;
        return -1;
    }
    outTraceCode.write(traceCodePrefix.data(), traceCodePrefix.size());
    outTraceCode.close();

#else

    #ifdef __TEST_TRACEBACK__
    fstream inTraceCode("trace_code_prefix.bin", ios::in | ios::binary);
    if (!inTraceCode) {
        cout << "Failed to open file for reading trace code prefix" << endl;
        return -1;
    }
    traceCodePrefix.resize(sizeof(bidx));
    inTraceCode.read(const_cast<char*>(traceCodePrefix.data()), traceCodePrefix.size());
    inTraceCode.close();
    #endif
#endif

#ifdef __TEST_TRACEABLE_PROSTRUCTURE__
    cout << "---------------------------------------------------------------------test traceable production sets---------------------------------------------------------------------" << endl;

    cout << "base table: " << endl;
    rc = printTable(client, "OOO", "APPLE_SPXXB", {"name"}, {"0"});
    if (rc != 0) {
        cout << "Print table SPXXB failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        // return rc;
    }

    cout << "ingredient table: " << endl;
    rc = printTable(client, "OOO", "APPLE_PLKZB", {"name"}, {"INGRE1"});
    if (rc != 0) {
        cout << "Print table PLKZB failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        // return rc;
    }

    idxVals.clear();
    idxVals.resize(1);
    val = 0;
    idxVals[0].resize(sizeof(val));
    memcpy(const_cast<char*>(idxVals[0].data()), &val, sizeof(val));

    cout << "trade control table: " << endl;
    rc = printTable(client, "OOO", "APPLE_SPJYB", {"JYID"}, idxVals);
    if (rc != 0) {
        cout << "Print table SPJYB failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        // return rc;
    }

    cout << "production control table: " << endl;
    rc = printTable(client, "OOO", "APPLE_SPKZB", {"uid"}, idxVals);
    if (rc != 0) {
        cout << "Print table SPKZB failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        // return rc;
    }

#endif


#ifdef __TEST_TRACEBACK__
    cout << "---------------------------------------------------------------------test traceback---------------------------------------------------------------------" << endl;

    idxVals.clear();
    idxVals.resize(1);
    val = 55;
    idxVals[0].resize(sizeof(val));
    memcpy(const_cast<char*>(idxVals[0].data()), &val, sizeof(val));

    spxxbBidx = {0, 0}; 
    memcpy(&spxxbBidx, traceCodePrefix.data(), sizeof(bidx));
    productionId = val;

    traceCode.clear();
    traceResult.clear();
    traceCode.resize(sizeof(spxxbBidx) + sizeof(productionId));
    memcpy(const_cast<char*>(traceCode.data()), &spxxbBidx, sizeof(spxxbBidx));
    memcpy(const_cast<char*>(traceCode.data()) + sizeof(spxxbBidx), &productionId, sizeof(productionId));

    rc = client.traceBack(traceCode, traceResult, true);
    if (rc != 0) {
        cout << "Trace back failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        // return rc;
    } else {
        cout << "Trace back successfully" << endl;
        cout << "Message: " << client.msg << endl;
        cout << "Trace back result: \n\n" << traceResult << endl;
    }

#endif

#ifdef __TEST_MAKETRADEE__

    cout << "---------------------------------------------------------------------test make trade---------------------------------------------------------------------" << endl;

    
    tradeId.clear();
    rc = client.makeTrade("OOO", "APPLE", 0, 10, 50, "芜湖大司马", "芜湖市", "13801369097", "史珍湘", "上海市", "13901369097", "芜湖市至上海市", "交易日期:2026-02-10", "2.5");
    if (rc != 0) {
        cout << "Make trade failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        // return rc;
    } else {
        cout << "Make trade successfully" << endl;
        cout << "Message: " << client.msg << endl;
        cout << "Trade ID: " << tradeId << endl;
    }

#endif

#ifdef __TEST_MAKETRADEE_SECOND__
#ifndef __TEST_TRACEBACK__

#else

    cout << "---------------------------------------------------------------------second test make trade---------------------------------------------------------------------" << endl;

    tradeId.clear();
    rc = client.makeTrade("OOO", "APPLE", 1, 10, 20, "史珍湘", "上海市", "13901369097", "味贞族", "北京市", "13888888888", "上海虹桥冷链运输车牌1234567至北京市大兴区", "交易日期:2026-03-10", "50.2");
    if (rc != 0) {
        cout << "Make trade failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        // return rc;
    } else {
        cout << "Make trade successfully" << endl;
        cout << "Message: " << client.msg << endl;
        cout << "Trade ID: " << tradeId << endl;
    }
#endif
#endif

#ifdef __TEST_TRACEBACK_SECOND__
    cout << "---------------------------------------------------------------------second test traceback---------------------------------------------------------------------" << endl;

    idxVals.clear();
    idxVals.resize(1);
    val = 25;
    idxVals[0].resize(sizeof(val));
    memcpy(const_cast<char*>(idxVals[0].data()), &val, sizeof(val));

    spxxbBidx = {0, 0}; 
    memcpy(&spxxbBidx, traceCodePrefix.data(), sizeof(bidx));
    productionId = val;


    traceCode.clear();
    traceResult.clear();
    traceCode.resize(sizeof(spxxbBidx) + sizeof(productionId));
    memcpy(const_cast<char*>(traceCode.data()), &spxxbBidx, sizeof(spxxbBidx));
    memcpy(const_cast<char*>(traceCode.data()) + sizeof(spxxbBidx), &productionId, sizeof(productionId));

    rc = client.traceBack(traceCode, traceResult, true);
    if (rc != 0) {
        cout << "Trace back failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        // return rc;
    } else {
        cout << "Trace back successfully" << endl;
        cout << "Message: " << client.msg << endl;
        cout << "Trace back result: \n\n" << traceResult << endl;
    }

#endif

#ifdef __TEST_MAKETRADEE_LOTS__
    cout << "---------------------------------------------------------------------second test make trade---------------------------------------------------------------------" << endl;
/*
100 - 300
110 - 290
115 - 285
120 - 280
125 - 275
130 - 270
135 - 265
140 - 260
145 - 255
150 - 250
155 - 245
160 - 240
165 - 235
170 - 230
175 - 225
180 - 220
185 - 215
190 - 210
192 - 208
195 - 205
*/
    tradeId.clear();
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 900; ++i) {
        rc = client.makeTrade("OOO", "APPLE", i,  i, 900, "TESTNAME", "TESTADDRESS", "TESTPHONE", "TESTFNAME", "TESTFADDRESS", "TESTFPHONE", "TEST上海虹桥冷链运输车牌1234567至北京市大兴区", "TEST交易日期:2026-03-10", "50.201");
        if (rc != 0) {
            cout << "Make trade failed, error code: " << rc << endl;
            cout << "Error message: " << client.msg << endl;
            // return rc;
        } 
            // cout << "Make trade successfully" << endl;
            // cout << "Message: " << client.msg << endl;
            // cout << "Trade ID: " << tradeId << endl;
        
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "Elapsed time: " << elapsed.count() << " ms\n";


#endif

#ifdef __TEST_VARTRACE__



    cout << "---------------------------------------------------------------------test var traceback---------------------------------------------------------------------" << endl;

    cout << "Input production id to trace back, input -1 to exit:" << endl;

    while(1) {
        idxVals.clear();
        idxVals.resize(1);
        // cout << "input production id: ";
        // cin >> val;
        string inputTraceCode;
        cout << "input trace code: ";
        cin >> inputTraceCode;

        // std::this_thread::sleep_for(std::chrono::milliseconds(5));

        // inputTraceCode = "00000000000000000b040000000000000f000000";

        if (inputTraceCode == "q") {
            break;
        } else if (inputTraceCode == "m") {
            // make trade
            tradeId.clear();

            std::vector<std::string> tradeParam;
            tradeParam.resize(14);
            cout << "input struct schema : ";
            cin >> tradeParam[0];
            cout << endl;

            cout << "input struct name : ";
            cin >> tradeParam[1];
            cout << endl;

            cout << "input 交易ID : ";
            cin >> tradeParam[2];
            cout << endl;
            
            cout << "input 交易商品起始编号 : ";
            cin >> tradeParam[3];
            cout << endl;
            
            cout << "input 交易商品数量 : ";
            cin >> tradeParam[4];
            cout << endl;

            cout << "input 买方名称 : ";
            cin >> tradeParam[5];
            cout << endl;

            cout << "input 买方地址 : ";
            cin >> tradeParam[6];
            cout << endl;

            cout << "input 买方联系方式 : ";
            cin >> tradeParam[7];
            cout << endl;

            cout << "input 卖方名称 : ";
            cin >> tradeParam[8];
            cout << endl;

            cout << "input 卖方地址 : ";
            cin >> tradeParam[9];
            cout << endl;

            cout << "input 卖方联系方式 : ";
            cin >> tradeParam[10];
            cout << endl;

            cout << "input 物流信息 : ";
            cin >> tradeParam[11];
            cout << endl;

            cout << "input 备注 : ";
            cin >> tradeParam[12];
            cout << endl;

            cout << "input 交易金额 : ";
            cin >> tradeParam[13];
            cout << endl;

            rc = client.makeTrade(
                tradeParam[0], 
                tradeParam[1], 
                atoll(tradeParam[2].c_str()), 
                atoll(tradeParam[3].c_str()), 
                atoll(tradeParam[4].c_str()), 
                tradeParam[5],  
                tradeParam[6], 
                tradeParam[7], 
                tradeParam[8], 
                tradeParam[9], 
                tradeParam[10], 
                tradeParam[11], 
                tradeParam[12], 
                tradeParam[13]);
            if (rc != 0) {
                cout << "Make trade failed, error code: " << rc << endl;
                cout << "Error message: " << client.msg << endl;
                // return rc;
            } else {
                cout << "Make trade successfully" << endl;
                cout << "Message: " << client.msg << endl;
                cout << "Trade ID: " << tradeId << endl;
            }
            continue;
        }

        // |SPXXB BIDX(16B)|PRODUCTION ID(4B)|
        if (inputTraceCode.size() != sizeof(bidx) * 2 + sizeof(int) * 2) {
            cout << "Invalid trace code length, expected " << sizeof(bidx) * 2 + sizeof(int) * 2 << " bytes" << endl;
            continue;
        }

        traceCode = std::move(hex2Binary(inputTraceCode));

        // if (val == -1) {
        //     break;
        // }
        // // val = 55;
        // idxVals[0].resize(sizeof(val));
        // memcpy(const_cast<char*>(idxVals[0].data()), &val, sizeof(val));
        // spxxbBidx = {0, 0}; 
        // memcpy(&spxxbBidx, traceCodePrefix.data(), sizeof(bidx));
        // productionId = val;
        // traceCode.clear();
        // traceResult.clear();
        // traceCode.resize(sizeof(spxxbBidx) + sizeof(productionId));
        // memcpy(const_cast<char*>(traceCode.data()), &spxxbBidx, sizeof(spxxbBidx));
        // memcpy(const_cast<char*>(traceCode.data()) + sizeof(spxxbBidx), &productionId, sizeof(productionId));

        // 计算耗时
        auto start = std::chrono::high_resolution_clock::now();


        rc = client.traceBack(traceCode, traceResult, true);
        if (rc != 0) {
            cout << "Trace back failed, error code: " << rc << endl;
            cout << "Error message: " << client.msg << endl;
            // return rc;
        } else {
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> elapsed = end - start;
            cout << "Trace back successfully" << endl;
            cout << "Message: " << client.msg << endl;
            // cout << "Trace back result: \n\n" << traceResult << endl;
            std::cout << "Elapsed time: " << elapsed.count() << " ms\n";
        }

    }

#endif

    rc = client.logoff();
    if (rc == 0) {
        cout << "Logoff successful for user: " << username << endl;
    } else {
        cout << "Logoff failed for user: " << username << ", error code: " << rc << endl;
        cout << "Error message: \n" << client.msg << endl;
        return rc;
    }

    cout << " test pass " << endl;
    return 0;
}





int printTable(CGrpcCli& client, const std::string& schema, const std::string& table, const std::vector<std::string>& idxCol, const std::vector<std::string>& idxVals) {
    /* 
        test get table handle
    */
    int rc = 0;
    rc = client.getTableHandle(schema, table);
    if (rc != 0) {
        cout << "Get table handle failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
    } else {
        cout << "Get table handle successfully" << endl;
        cout << "Message: " << client.msg << endl;
    }


    IDXHANDLE hidx;

    rc = client.getIdxIter(idxCol, idxVals, hidx);
    if (rc != 0) {
        cout << "Get index iterator failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        return rc;
    } else {
        cout << "Get index iterator successfully" << endl;
        cout << "Message: " << client.msg << endl;
        cout << "Index handle: " << hidx << endl;
    }


    const auto& colInfo = client.getColInfo(hidx);

    rc = client.fetchNextRow(hidx);
    if (rc != 0) {
        cout << "Fetch next row failed, error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        return rc;
    } else {
        cout << "Fetch next row successfully" << endl;
        // cout << "Message: " << client.msg << endl;
    }

    std::string gval;

    for (int i = 0; i < 20000; ++i) {
        for (uint32_t j = 0; j < colInfo.size(); ++j) {
            rc = client.getDataByIdxIter(hidx, j, gval);
            if (rc != 0) {
                cout << "Get row by index iterator failed, error code: " << rc << endl;
                cout << "Error message: " << client.msg << endl;
                return rc;
            } else {
                cout << "Get row by index iterator success, pos " << j << endl;
                if (colInfo[j].getType() == dpfs_datatype_t::TYPE_CHAR || 
                    colInfo[j].getType() == dpfs_datatype_t::TYPE_VARCHAR ||
                    colInfo[j].getType() == dpfs_datatype_t::TYPE_TIMESTAMP) {
                    cout << "Get row by index iterator success, value: " << gval << endl;
                } else if (colInfo[j].getType() == dpfs_datatype_t::TYPE_DECIMAL) {
                    my_decimal dec;
                    rc = binary2my_decimal(0, (const uchar*)gval.data(), &dec, colInfo[j].getDds().genLen, colInfo[j].getScale());
                    if (rc != 0) {
                        cout << "Convert binary to decimal failed, error code: " << rc << endl;
                        return rc;
                    }
                    // std::string decStr;
                    String decStr;
                    rc = my_decimal2string(0, &dec, &decStr);
                    if (rc != 0) {
                        cout << "Convert decimal to string failed, error code: " << rc << endl;
                        return rc;
                    }
                    cout << "Get row by index iterator success, binary value: " << endl;
                    printMemory(gval.data(), gval.size()); cout << endl;

                    cout << "get decimal value = " << decStr.ptr() << endl;
                } else if (colInfo[j].getType() == dpfs_datatype_t::TYPE_DOUBLE) {
                    double dval;
                    memcpy(&dval, gval.data(), sizeof(dval));
                    cout << "Get row by index iterator success, double value: " << dval << endl;
                } else if (colInfo[j].getType() == dpfs_datatype_t::TYPE_INT) {
                    int ival;
                    memcpy(&ival, gval.data(), sizeof(ival));
                    cout << "Get row by index iterator success, int value: " << ival << endl;
                } else {   
                    cout << "Get row by index iterator success, type is not string, binary value: ";
                    printMemory(gval.data(), gval.size()); cout << endl;
                }
            }
        }

        rc = client.fetchNextRow(hidx);
        if (rc != 0) {
            if (rc == -ENODATA) {
                cout << "No more rows to fetch from server." << endl;
                break;
            }
            cout << "Fetch next row failed, error code: " << rc << endl;
            cout << "Error message: " << client.msg << endl;
            return rc;
        } else {
            cout << "Fetch next row successfully" << endl;
            // cout << "Message: " << client.msg << endl;
        }
    }

    return 0;
}
