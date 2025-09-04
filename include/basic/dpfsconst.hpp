#pragma once
#include <dpendian.hpp>
#include <cstdint>
#include <cstddef>

constexpr char dpfsVersion[] = {0, 0, 0, 1}; // version 0.0.0.1 version.release.build.bugfix
constexpr uint32_t versionSize = sizeof(dpfsVersion);

enum class dpfsnetType : int {
    TCP = 0,
    MAX,
};

constexpr const char* dpfsnetTypeStr[] = {
    "tcp",
};

/*
    IPC commands between client and server
*/
enum class dpfsipc : uint32_t {
    DPFS_IPC_CONNECT = 0,       // connect to system
    DPFS_IPC_DISCONNECT,        // disconnect from system
    DPFS_IPC_FOODTRACE,         // trace a request
    DPFS_IPC_MAX,
};
constexpr const char* dpfsipcStr[] = {
    "IPC_CONNECT",
    "IPC_DISCONNECT",
    "IPC_FOODTRACE",
};

/*
    dpfs server response codes
*/
enum class dpfsrsp : uint32_t {
    DPFS_RSP_INVALID = 0,           // bad request
    DPFS_RSP_NOTSUPPORT,            // request not supported
    DPFS_RSP_SYSTEMBUSY,            // system is busy
    DPFS_RSP_SYSTEMERROR,           // system error
    DPFS_RSP_CONNECT,               // connect response
    DPFS_RSP_DISCONNECT,            // disconnect response
    DPFS_RSP_FOODTRACE,             // response for a trace request
    DPFS_RSP_MAX,
};
constexpr const char* dpfsrspStr[] = {
    "RSP_INVALID",
    "RSP_NOTSUPPORT",
    "RSP_SYSTEMBUSY",
    "RSP_SYSTEMERROR",
    "RSP_CONNECT",
    "RSP_DISCONNECT",
    "RSP_FOODTRACE",
};

struct ipc_connect {
    char version[versionSize];      // client version
    char user[16];                  // user name
    char password[32];              // password
};

struct ipc_connect_rsp {
    char version[versionSize];      // server version
    uint8_t  retcode = 0;           // return code, 0 if success
    char authToken[32];             // authentication token if success
    bool serverEndian = B_END;      // false: little endian, true: big endian
    // uint16_t msgSize = 0;        // size of return message
    int8_t  message[];              // return message (null terminate string)
};

/*
|IPC|SIZE|PARAM|

    IPC: 4 bytes, command type
    SIZE: 4 bytes, size of PARAM
    DATA: variable length, command parameters or response data

DPFS_IPC_CONNECT
    request DATA: |client version 4B|user 16B|password 32B|

DPFS_RSP_CONNECT
    response DATA: |server version 4B|return code 1B|server endian 1B|authtoken 32B(if success)|message NB|
*/

struct dpfs_cmd {
    dpfsipc cmd;           // command type
    uint32_t size;         // size of parameters
    char data[];           // parameters
};

struct dpfs_rsp {
    dpfsrsp rsp;           // response type
    uint32_t size;         // size of response data
    char data[];           // response data
};

uint32_t bswap32(uint32_t x);

/*
    @note the pointer will be changed, Convert command structure to network byte order or convert back to host byte order
    @param cmd: command structure to be converted
*/
void cmd_edn_cvt(dpfs_cmd* cmd);

void rsp_edn_cvt(dpfs_rsp* rsp);


int parse_string(const char* str, const char* key, char* value, size_t size);



// using msgCallback = dpfs_rsp* (*)(const dpfs_cmd* cmd);
// class CDpfsIPC {
// public:
//     CDpfsIPC() {
//         for(int i = 0; i < (int)dpfsipc::DPFS_IPC_MAX; ++i) {
//             m_exe[i] = nullptr;
//         }
//     }
//     inline const char* name() const { 
//         return "DPFS_IPC"; 
//     }
// protected:
//     msgCallback m_exe[(int)dpfsipc::DPFS_IPC_MAX];
// };

