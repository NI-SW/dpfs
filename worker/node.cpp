namespace worker {
    enum dataType {
        JSON,
        TEXT,
        PICTURE,
        XML,
        BINARY,
        NONE
    };

    struct block {
        unsigned long long int addr;
        unsigned long long int size;
        dataType type;
        void* data;
        block() {
            addr = 0;
            size = 0;
            type = NONE;
            data = nullptr;
        }
    };

    class node{
        
    };
};



/*

单块数据格式
-------------------------------------------------------------
| 块地址 | 块大小 | 数据类型(JSON|Text|Picture) | BINARY DATA |
-------------------------------------------------------------

整体存储格式



*/



