/**
 * File: types.hpp
 * Created Time: 2025-04-29
 * Author: NI-SW (947743645@qq.com)
 */
#include <dpendian.hpp>
#include <basic/dpfsconst.hpp>
#include <string>
#include <cstring>

uint32_t bswap32(uint32_t x) {
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
void cmd_edn_cvt(dpfs_cmd* cmd) {
    // need to convert
    cmd->size = bswap32(cmd->size);
    cmd->cmd = (dpfsipc)bswap32((uint32_t)cmd->cmd);
    return;
}

void rsp_edn_cvt(dpfs_rsp* rsp) {
    // need to convert
    rsp->size = bswap32(rsp->size);
    rsp->rsp = (dpfsrsp)bswap32((uint32_t)rsp->rsp);
    return;
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