// compile app with spdk feature
#include "test.hpp"
#include <iostream>
#include <thread>
#include <unistd.h>
#include <signal.h>
using namespace std;
pid_t pid = getpid();

static void timmer(int n) {
    if(n <= 0) {
        return;
    }
    int i = 0;
    while(i < n) {
        sleep(1);
        ++i;
    }
    kill(pid, SIGKILL);
}

int main(int argc, char* argv[]) {
    
    cout << "pid : " << pid << endl;
    
    CSpdkControl spdkControl("config1.json");
    spdkControl.start_spdk();

    cout << "start spdk" << endl;
    sleep(5);
    spdkControl.stop_spdk();
    cout << "stop spdk" << endl;
}

