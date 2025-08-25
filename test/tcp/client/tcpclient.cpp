#include <dpfsnet/dpfscli.hpp>
#include <cstring>
#include <iostream>
using namespace std;

int main() {
    int rc = 0;
    CDpfscli* cli = newClient("tcp");
    CDpfscli& client = *cli;

    client.set_log_level(5);
    rc = client.connect("ip:192.168.27.3 port:20500");
    if(!rc) {
        cout << "Client connected successfully!" << endl;
    } else {
        cout << "Failed to connect client, error code: " << rc << endl;
        return rc;
    }

    
    
    client.send("Hello, Server!", 14);
    
    char data[4096];
    client.send(data, 4096);
    
    // while(1) {
    //     string msg;
    //     cin >> msg;
    //     if(msg == "exit") {
    //         break;
    //     }
    //     rc = client.send(msg.c_str(), msg.size());
    //     if(rc != 0) {
    //         cout << "Failed to send data, error code: " << rc << endl;
    //         break;
    //     }
    // }

    cout << "client disconnect" << endl;
    client.disconnect();
    delete cli;
    return 0;
}