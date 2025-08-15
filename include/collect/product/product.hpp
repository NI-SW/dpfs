#include <collect/collect.hpp>

/*
* 对于一批产品，每件产品的信息使用固定长度存储，使用柔性数组，对于可变长度的位置（交易链表），存储其块号与偏移量
*/

class CProduct : public CCollection {
public:
    CProduct();
    ~CProduct();
    
};

