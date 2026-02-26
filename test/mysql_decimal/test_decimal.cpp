#include <iostream>
#include <mysql_decimal/my_decimal.h>
#include <dpfsdebug.hpp>
using namespace std;




int main() {
    cout << "Hello, World!" << endl;
    my_decimal dec;

    
    // 110B84CF560979DB
    int rc = 0;
    const char* str = "315222.15898099997";

    const char *end = str + strlen(str);
    rc = str2my_decimal(0, str, &dec, &end);
    cout << "rc = " << rc << endl;
    cout << "Integer part: " << dec.intg << endl;
    cout << "Fractional part: " << dec.frac << endl;
    cout << "end : " << end << endl;
    
    cout << "Precision: " << dec.precision() << endl;

    double result;
    rc = my_decimal2double(0, &dec, &result);
    cout << " double Result: " << result << endl;

    uint8_t buffer[100];
    rc = my_decimal2binary(0, &dec, buffer, dec.precision(), dec.frac);
    cout << "Binary conversion result: " << rc << endl;
    printMemory(buffer, 16);


    my_decimal_add(0, &dec, &dec, &dec);
    cout << "After addition, precision: " << dec.precision() << endl;

    rc = my_decimal2binary(0, &dec, buffer, dec.precision(), dec.frac);
    cout << "Binary conversion result: " << rc << endl;

    binary2my_decimal(0, buffer, &dec, dec.precision(), dec.frac);

    printMemory(buffer, 16);

    return 0;
}