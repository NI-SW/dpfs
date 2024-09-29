#pragma once

static bool BIG_ENDIAN = true;

class getEndian {
public:
    getEndian() {
        int a = 1;
        if (*(char*)&a == 1) {
            BIG_ENDIAN = false;
        } else {
            BIG_ENDIAN = true;
        }

    }
};

static getEndian globalEndian;

