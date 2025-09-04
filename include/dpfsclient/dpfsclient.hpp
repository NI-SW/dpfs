#include <dpfsnet/dpfscli.hpp>
#include <log/logbinary.h>
#include <string>


class CDpfsSysCli {
public:
    /*
        @param netType: network type, default is "tcp"
        @note this constructor will create a new dpfs client with the given network type
    */
    CDpfsSysCli(const char* netType = "tcp");
    ~CDpfsSysCli();

    /*
        @param connStr connection string, format: "ip:0.0.0.0 port:20500 user:root passwd:123456"(if tcp).
        @return 0 if success.
        @note connect with user and password, and authToken will be retrived if success.
    */
    int connect(const char* connStr);
    /*
        @return 0 if success.
    */
    int disconnect();
    /*
        @return true if connected, false if not connected.
    */
    bool is_connected() const;

    
    logrecord log;
private:
    CDpfscli* m_cli;
    std::string m_netType;
    bool serverEndian = false; // false: little endian, true: big endian
    char authToken[32] = {0};
};