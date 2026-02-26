/**
 * File: types.hpp
 * Created Time: 2025-04-29
 * Author: NI-SW (947743645@qq.com)
 */
#include <dpendian.hpp>
#include <basic/dpfsconst.hpp>
#include <string>
#include <cstring>
#include <chrono>
#include <iomanip>



uint32_t bswap32(uint32_t x) noexcept {
    uint8_t tmp[4];
    uint8_t* p = (uint8_t*)&x;
    tmp[0] = p[3];
    tmp[1] = p[2];
    tmp[2] = p[1];
    tmp[3] = p[0];
    return *(uint32_t*)tmp;
}

/*
    @note the pointer will be changed, Convert command structure to network byte order or convert back to host byte order
    @param cmd: command structure to be converted
    @return pointer to the converted command structure
*/
void cmd_edn_cvt(dpfs_cmd* cmd) noexcept {
    // need to convert
    cmd->size = bswap32(cmd->size);
    cmd->cmd = (dpfsipc)bswap32((uint32_t)cmd->cmd);
    return;
}

void rsp_edn_cvt(dpfs_rsp* rsp) noexcept {
    // need to convert
    rsp->size = bswap32(rsp->size);
    rsp->rsp = (dpfsrsp)bswap32((uint32_t)rsp->rsp);
    return;
}

bool is_valid_ipc(dpfsipc cmd) noexcept {
    if(cmd >= dpfsipc::DPFS_IPC_MAX) {
        return false;
    }
    return true;
}

bool is_valid_rsp(dpfsrsp rsp) noexcept {
    if(rsp >= dpfsrsp::DPFS_RSP_MAX) {
        return false;
    }
    return true;
}

int parse_string(const char* str, const char* key, char* value, size_t size) {
    if (!value) {
        return -EINVAL;
    }
    std::string fstr = str;

    size_t pos = fstr.find(key);
    if (pos == std::string::npos) {
        return -EINVAL; // Key not found
    }
    pos += strlen(key);
    ++pos;
    size_t end = fstr.find(' ', pos);
    if (end == std::string::npos) {
        end = fstr.length();
    }
    size_t len = end - pos;
    if (len >= size) {
        return -ENAMETOOLONG; // Value too long
    }
    memcpy(value, fstr.c_str() + pos, len);
    value[len] = '\0'; // Null-terminate the string
    return 0; // Success

}

int ull2str(unsigned long long int l, char* buf, size_t len) noexcept {
    
    if (len < 1) {
        return -EINVAL;
    }

    if(l == 0) {
        if(len < 2) {
            return -ENAMETOOLONG;
        }
        buf[0] = '0';
        buf[1] = '\0';
        return 0;
    }

    /*
        max = 18446744073709551614
    */

    char* p = buf + len - 1;

    while(l != 0) {
        --p;
        if (p < buf) {
            return -ENAMETOOLONG;
        }
        *p = '0' + (l % 10);
        l /= 10;
    }


    return 0;

    
}

std::string getCurrentTimestamp() {
    // 获取当前时间点
    auto now = std::chrono::system_clock::now();
    // 转换为time_t以用于localtime
    auto now_c = std::chrono::system_clock::to_time_t(now);
    // 转换为tm结构，这是ctime库需要的格式
    // unsafe 
    // std::tm* ptm = std::localtime(&now_c);

	// thread safe
	std::tm tm;
    #ifdef __linux__
    localtime_r(&now_c, &tm);
    #elif _WIN32
    localtime_s(&tm, &now_c);
    #endif

    // 使用stringstream来格式化输出
    std::stringstream ss;
    ss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return ss.str();
}