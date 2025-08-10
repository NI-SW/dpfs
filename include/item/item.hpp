/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */

#include <string>
#include <vector>


class CKey {
public:
    CKey();
    CKey(int64_t key) : intKey(key), type(Type::Integer), len(sizeof(int64_t)) {};
    CKey(double key) : floatKey(key), type(Type::Float), len(sizeof(double)) {};
    CKey(const std::string& key) : strKey(key), type(Type::String), len(key.size()) {};

    ~CKey();

    enum class Type {
        None,
        String,
        Integer,
        Float,
    };

    Type type = Type::None;  // 默认类型为None
    union {
        int64_t intKey;         // 整数键
        double floatKey;        // 浮点键
        std::string strKey;     // 字符串键
    };
    size_t len = 0;

};


class CValue {
public:
    CValue();
    ~CValue();
    enum class Type {
        None,
        String,
        Integer,
        Float,
        Binary
    };
    
};

class CItem {
public:
    CItem();
    ~CItem();
    CValue* getValue(int pos);
    CValue* getValueByKey(CKey key);
    std::vector<CValue*> values;

 };


