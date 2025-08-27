#include <dpfsnet/dpfssvr.hpp>
#include <cstring>
#include <iostream>
#include <chrono>
#include <thread>
#include <csignal>
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
            if(rc == -ENODATA) {
                std::this_thread::sleep_for(chrono::milliseconds(100));
                continue;
            }
            printf("Failed to receive data, error code: %d\n", rc);
            break;
        }
        printf("Received %d bytes: ", size);
        printf("Data: ");
        for(int i = 0; i < size; ++i) {
            printf("%02X ", (unsigned char)buffer[i]);
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

void prtUsage() {
    cout << "Usage: app 'ip:<ipaddress> port:<port>'" << endl;
    cout << "Example: app 'ip:0.0.0.0 port:20500'" << endl;
}

volatile bool g_exit = false;
void sigfun(int sig) {
    cout << "Signal " << sig << " received, exiting..." << endl;
    g_exit = true;
}

int main(int argc, char* argv[]) {
    if(argc < 2) {
        prtUsage();
        return -EINVAL;
    }
    signal(SIGINT, sigfun);
    signal(SIGKILL, sigfun);

    CDpfssvr* svr = newServer("tcp");
    CDpfssvr& server = *svr;
    server.set_log_level(5);

    int rc = server.listen(argv[1], callbackfun, nullptr);
    if(!rc) {
        cout << "Server started successfully!" << endl;
    } else {
        prtUsage();
        goto err;
        cout << "Failed to start server, error code: " << rc << endl;
        return rc;
    }


    while(!g_exit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    server.stop();
    delete svr;
    cout << "Server exited!" << endl;
    return 0;

    err:
    delete svr;
    return rc;
}
