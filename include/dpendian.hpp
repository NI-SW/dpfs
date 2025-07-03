#pragma once

static bool B_END = true;

class getEndian {
public:
    getEndian() {
        int a = 1;
        if (*(char*)&a == 1) { // 小端存储
            B_END = false;
        } else {               // 大端存储
            B_END = true;
        }

    }
};

static getEndian globalEndian;

 