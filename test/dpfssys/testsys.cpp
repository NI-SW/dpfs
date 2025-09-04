#include <dpfssys/dpfssys.hpp>
#include <iostream>
#include <csignal>
using namespace std;

volatile bool g_exit = false;
void sigfun(int sig) {
    cout << "Signal " << sig << " received, exiting..." << endl;
    g_exit = true;
}

int main() {
    signal(SIGINT, sigfun);
    signal(SIGKILL, sigfun);
    
    dpfsSystem* a;
    int rc = 0;
    try {
        a = new dpfsSystem("./example.json");
        cout << a->conf_file << endl;
        cout << a->dataSvrStr << endl;
        cout << a->controlSvrStr << endl;
        cout << a->replicationSvrStr << endl;
        cout << "size: " << a->engine_list[0]->size() << endl;
        a->log.set_loglevel(logrecord::LOG_DEBUG);
        rc = a->start();
        if(rc != 0) {
            cerr << "Failed to start dpfsSystem, rc=" << rc << endl;
            delete a;
            return rc;
        }
        cout << "dpfsSystem started successfully." << endl;
    } catch (const std::runtime_error& e) {
        cerr << "Error initializing dpfsSystem: " << e.what() << endl;
        return -1;
    }


    while(!g_exit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    delete a;
    return 0;
}