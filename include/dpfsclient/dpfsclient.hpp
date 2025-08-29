#include <dpfsnet/dpfscli.hpp>
#include <dpfsconst.hpp>

class CDpfsSysCli {
public:
    CDpfsSysCli() = default;
    ~CDpfsSysCli() = default;

    /*
        @param connStr connection string, format: "ip:port"(if tcp).
        @return 0 if success.
    */
    int connect(const char* connStr);

    /*
        @return 0 if success.
    */
    int disconnect();



};