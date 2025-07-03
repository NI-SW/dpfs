#include <storage/spdkcontrol.hpp>
#include <unistd.h>
#include <iostream>
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

void handle_sigint(int sig) {
    printf("Caught signal %d\n", sig);
    // 可以在这里执行清理操作
    if(sig == SIGKILL) {
        exit(0);
    }
}
 


int main(int argc, char **argv) {
    
    signal(SIGINT, handle_sigint);

    CSpdkControl *spdkControl = new CSpdkControl("./test2.json");

    spdkControl->start_spdk();

    thread t1(timmer, 0);
    t1.detach();

    int i = 0;
    while(spdkControl->active()) {
        sleep(1);
        // ++i;
        // if(i > 2) {
        //     break;
        // }
    }

    spdkControl->open_blob_device();
    sleep(3);

    spdkControl->stop_spdk();
    delete spdkControl;

    cout << " stop spdk" << endl;

    return 0;

}
