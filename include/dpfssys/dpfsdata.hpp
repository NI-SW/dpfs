#include "dpfsdisk.hpp"
#include <vector>
/*
    data service class
    1.process ddl and dml commands
    2.handle client data I/O requests

    data write mode: little endian, if host is big endian, need to convert
*/
class CDatasvc {
public:
    CDatasvc() = default;
    ~CDatasvc() = default;

    // engines managed by this service
    std::vector<CDiskMan> engines; 

    

};