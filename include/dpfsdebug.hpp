#include <cstdio>

void printMemory(void* ptr, int size) {

    unsigned char* p = reinterpret_cast<unsigned char*>(ptr);
    for(int i = 0; i < size; ++i) {
        printf("%02X ", p[i]);
        if(i % 16 == 15) {
            printf("\n");
        }
    }

}
