#include <dpfsnet/dpfssvr.hpp>
#include <cstring>
#include <iostream>
#include <chrono>
#include <thread>
using namespace std;

void callbackfun(CDpfscli& cli, void* cb_arg) {
    cli.set_log_level(5);
    cout << "New connection established!" << endl;
    char* buffer;
    int size;
    int rc = 0;
    while(1) {
        rc = cli.recv(buffer, &size);
        if(rc != 0) {
            printf("Failed to receive data, error code: %d\n", rc);
            break;
        }

        for(int i = 0; i < size; ++i) {
            printf("%02x ", (unsigned char)buffer[i]);
        }
        printf("\n");

        rc = cli.send("hello client!", 14);
        if(rc != 0) {
            printf("Failed to send data, error code: %d\n", rc);
            break;
        }
        
        cli.buffree(buffer);
    }

    // Handle the new connection here
};

int main() {
    CDpfssvr* svr = newServer("tcp");
    CDpfssvr& server = *svr;

    server.set_log_level(5);
    int rc = server.listen("ip:0.0.0.0 port:20500", callbackfun, nullptr);
    if(!rc) {
        cout << "Server started successfully!" << endl;
    } else {
        cout << "Failed to start server, error code: " << rc << endl;
        return rc;
    }

    while(1) {
        std::this_thread::sleep_for(chrono::seconds(1));
    }

    server.stop();
    delete svr;
    return 0;
}
