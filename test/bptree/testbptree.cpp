#include <dpfsclient/dpfsclient.hpp>
#include <iostream>
#include <csignal>
using namespace std;

volatile bool g_exit = false;
void sigfun(int sig) {
    cout << "Signal " << sig << " received, exiting..." << endl;
    g_exit = true;
}

int main(int argc, char* argv[]) {
    signal(SIGINT, sigfun);
    signal(SIGKILL, sigfun);

    CDpfsSysCli a("tcp");
    a.log.set_loglevel(logrecord::LOG_DEBUG);
    int rc = 0;
    try {
        a.connect("ip:192.168.34.12 port:20500 user:root passwd:123456");
    } catch (const std::runtime_error& e) {
        cerr << "Error initializing dpfsSystem: " << e.what() << endl;
        return -1;
    }


    
    while(a.is_connected() && !g_exit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }


    return 0;
}
