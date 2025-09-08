#include <iostream>
#include <dpfsnet/dpfscli.hpp>
#include <dlfcn.h>
using namespace std;
int (*add)(int a, int b);
void* (*newNet)();

int main() {
    void* dlhandle = nullptr;
    dlhandle = dlopen("/code/github/dpfs/mytestcode/libmytest.so", RTLD_LAZY | RTLD_GLOBAL);
    if(!dlhandle) {
        cerr << "Cannot open library: " << dlerror() << endl;
        return 1;
    }
    
    newNet = (void* (*)())dlsym(dlhandle, "newNet");
    if (newNet == nullptr) {
        cerr << "Cannot load symbol 'testadd': " << dlerror() << endl;
        return 1;
    }

    // cout << newNet() << endl;
    CDpfscli* cli = (CDpfscli*)newNet();
    cout << cli << endl;
    cout << cli->name() << endl;

    delete cli;



}