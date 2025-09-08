#include <dpfsnet/dpfscli.hpp>

int testadd(int a, int b) {
    return a + b;
}

extern "C" void* newNet() {
    return newClient("tcp");
}

class mytest : CDpfscli {
public:
    mytest() = default;
    virtual ~mytest() = default;

    bool is_connected() const override {
        return true;
    }

    int connect(const char* conn_tring) override {
        return 0;
    }

    int disconnect() override {
        return 0;
    }

    int send(const void* buffer, int size) override {
        return 0;
    }

    int recv(void** buffer, int* retsize) override {
        return 0;
    }

    void buffree(void* buffer) override {
        
    }

    void set_log_path(const char* log_path) override {
        
    }

    void set_log_level(int level) override {
        
    }

    const char* name() const override {
        return "mytest";
    }
};

