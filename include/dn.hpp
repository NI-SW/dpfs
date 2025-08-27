/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#include <string>
class CDpfsObject {
public:
    CDpfsObject() { 
        m_name = "dpfsObject";
    }

    inline const char* name() const { 
        
        return m_name.c_str(); 
    }

    inline void setName(std::string& name, bool isRef = false) { 
        if(isRef) {
            m_refName = &name;
            return;
        }
        m_name = name;
    }
private:
    std::string m_name;
    std::string* m_refName = nullptr;
};