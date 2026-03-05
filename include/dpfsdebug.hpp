#pragma once
#include <cstdio>
#include <iostream>
using namespace std;

inline void printMemory(const void* ptr, int size) noexcept {

    const unsigned char* p = reinterpret_cast<const unsigned char*>(ptr);
    for(int i = 0; i < size; ++i) {
        printf("%02X ", p[i]);
        if(i % 16 == 15) {
            printf("\n");
        }
    }

}
