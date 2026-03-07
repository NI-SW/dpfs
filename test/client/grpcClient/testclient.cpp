#include <dpfsclient/grpcclient.hpp>
#include <iostream>
#include <csignal>
#include <thread>
#include <dpfsdebug.hpp>

using namespace std;

volatile bool g_exit = false;
void sigfun(int sig) {
    cout << "Signal " << sig << " received, exiting..." << endl;
    g_exit = true;
}

int main(int argc, char* argv[]) {
    // signal(SIGINT, sigfun);
    // signal(SIGKILL, sigfun);

    auto channel = grpc::CreateChannel("192.168.34.12:20500", grpc::InsecureChannelCredentials());
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
    vector<string> idxVals;

    idxVals.resize(1);
    int val = 1;
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

    rc = client.logoff();
    if (rc == 0) {
        cout << "Logoff successful for user: " << username << endl;
    } else {
        cout << "Logoff failed for user: " << username << ", error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
        return rc;
    }

    return 0;
}
