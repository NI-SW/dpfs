/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#pragma once

static bool B_END = true;

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

 