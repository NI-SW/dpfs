#include <iostream>
#include "types.hpp"


using namespace std;
int main() {

    cout << "1 : " << sizeof(time_t)              << endl;
    cout << "2 : " << sizeof(brg_t)               << endl;
    cout << "3 : " << sizeof(goodsid_t)           << endl;
    cout << "4 : " << sizeof(productobj)          << endl;
    cout << "5 : " << sizeof(transportInfo)       << endl;
    cout << "6 : " << sizeof(environmentInfo)     << endl;
    cout << "7 : " << sizeof(goods)               << endl;
    cout << "8 : " << sizeof(std::string)         << endl;
    cout << B_END << endl;
    cout << time(0) / 86400 << endl;

}