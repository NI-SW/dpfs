#include <dpfsclient/grpcclient.hpp>
#include <iostream>
#include <csignal>
#include <thread>
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

    // cout << "正在等待连接..." << endl;
    // while (true) {
    //     auto state = channel->GetState(true); // true 表示尝试连接
    //     if (state == GRPC_CHANNEL_READY) {
    //         cout << "连接已就绪！" << endl;
    //         break;
    //     }
    //     if (state == GRPC_CHANNEL_TRANSIENT_FAILURE) {
    //         cerr << "连接发生瞬时故障，重试中..." << endl;
    //     }
    //     std::this_thread::sleep_for(std::chrono::milliseconds(500));
    //     cout << "state : " << state << endl;
    // }

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
    
    CREATE TABLE OOO.PPP (A INT NOT NULL PRIMARY KEY, B DOUBLE, C CHAR(20))
    insert into OOO.PPP values (1, 1.1, 'hello'), (2, 2.2, 'world')
    
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

    // Simulate some work after login
    // this_thread::sleep_for(chrono::seconds(2));

    rc = client.logoff();
    if (rc == 0) {
        cout << "Logoff successful for user: " << username << endl;
    } else {
        cout << "Logoff failed for user: " << username << ", error code: " << rc << endl;
        cout << "Error message: " << client.msg << endl;
    }

    return 0;
}
