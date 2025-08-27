#include <dpfsnet/dpfscli.hpp>
#include <cstring>
#include <iostream>
using namespace std;

int main(int argc, char* argv[]) {
    if(argc < 2) {
        cout << "Usage: app 'ip:<ipaddress> port:<port>'" << endl;
        cout << "Example: app 'ip:192.168.34.12 port:20500'" << endl;
        return -EINVAL;
    }


    int rc = 0;
    CDpfscli* cli = newClient("tcp");
    CDpfscli& client = *cli;

    client.set_log_level(5);
    rc = client.connect(argv[1]);
    if(rc == 0) {
        cout << "Client connected successfully!" << endl;
    } else {
        cout << "Failed to connect client, error code: " << rc << endl;
        goto err;
    }

    
    
    client.send("Hello, Server!", 14);
    
    char data[4096];
    client.send(data, 4096);
    
    while(client.is_connected()) {
        string msg;
        cout << "Enter message to send (or 'exit' to quit): " << endl;
        cin >> msg;
        if(msg == "exit") {
            break;
        }
        rc = client.send(msg.c_str(), msg.size());
        if(rc != 0) {
            cout << "Failed to send data, error code: " << rc << endl;
            break;
        }
    }

    cout << "client disconnect" << endl;
    client.disconnect();
    delete cli;
    return 0;

    err:
    client.disconnect();
    delete cli;
    return rc;

}