/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#pragma once
#include <cstring>
static bool B_END = false;
// cpu register max length in bytes
constexpr int maxRegisterLen = 8;

class getEndian {
public:
    getEndian() {
        int a = 1;
        if (*(char*)&a == 0x01) { // 小端存储
            B_END = false;
        } else {               // 大端存储
            B_END = true;
        }

    }
    bool operator()() const {
        return B_END;
    }
};

static getEndian globalEndian;

/*
    @param dest: destination buffer
    @param src: source buffer
    @param scale: number of bytes to copy
    @note copy data from src to dest with endian convertion
*/
inline int cpy2le(char* dest, const char* src, int scale) noexcept {
    if(scale <= 0) {
        return -1;
    }

    if(scale == 1) {
        dest[0] = src[0];
        return 0;
    }

    if(!B_END) {
        // little endian
        memcpy(dest, src, scale);
    } else {
        // big endian
        for(int i = 0; i < scale; ++i) {
            dest[i] = src[scale - 1 - i];
        }
    }
    return 0;

}

/*
    @param dest: destination buffer
    @param src: source buffer
    @param scale: number of bytes to copy
    @note copy data from src to dest with endian convertion
*/
inline int cpyFromle(char* dest, const char* src, int scale) noexcept {
    return cpy2le(dest, src, scale);
}
 
/*
    @note auto type declare version of cpyFromle
    @warning only allow T is basic data type like uint32_t, uint16_t, float, double, etc.
*/
template<typename T, unsigned char Tscale = sizeof(T)>
inline int cpyFromleTp(T& __dest, const T& __src) noexcept {
    if(Tscale <= 0 || Tscale > maxRegisterLen) {
        // -EINVAL
        return -22;
    }

    if(Tscale == 1) {
        reinterpret_cast<char*>(&__dest)[0] = reinterpret_cast<const char*>(&__src)[0];
        return 0;
    }

    char* dest = reinterpret_cast<char*>(&__dest);
    const char* src = reinterpret_cast<const char*>(&__src);

    if(!B_END) {
        // little endian
        if(dest == src) {
            return 0;
        }
        memcpy(dest, src, Tscale);
    } else {
        // big endian
        if(dest == src) {
            // in place convert
            for(int i = 0; i < Tscale / 2; ++i) {
                char tmp = dest[i];
                dest[i] = dest[Tscale - 1 - i];
                dest[Tscale - 1 - i] = tmp;
            }
            return 0;
        } else if((dest < src && dest + Tscale > src) ||
                  (src < dest && src + Tscale > dest)) {

            // overlap memory copy
            char tempBuf[Tscale];
            for(int i = 0; i < Tscale; ++i) {
                tempBuf[i] = src[Tscale - 1 - i];
            }
            memcpy(dest, tempBuf, Tscale);
            return 0;
        }

        
        for(int i = 0; i < Tscale; ++i) {
            dest[i] = src[Tscale - 1 - i];
        }
    }

    return 0;

}

template<typename T, unsigned char Tscale = sizeof(T)>
inline int cpy2leTp(T& __dest, const T& __src) noexcept {
    return cpyFromleTp(__dest, __src);
}

