/*
    convert tidb decimal to mysql decimal and vice versa
*/
#include <mysql_decimal/my_decimal.h>
#include <dpfsdebug.hpp>

// decimal convertor between tidb and mysql
namespace deccvt {

/*
    @param tibinary the binary representation of tidb decimal, in format of int128
    @param out the binary representation of mysql decimal, in format of uchar array
    @param out_buffer_len the length of the out buffer, should be enough to hold the mysql decimal binary representation
    @param sign the sign of the decimal, true for negative, false for positive
    @param actureLen the actual length of the mysql decimal binary representation, in bytes, can be nullptr if not needed
    @return 0 for success, non-zero for failure
    @note the out buffer will storage length, scale and the data like |len(1B)|scale(1B)|data(mysql_decimal_Len)|
*/
static int tibinary2mybinary(const char* tibinary, uint8_t* out, int out_buffer_len, bool sign, int* actureLen) {

    if (!out || !tibinary) {
        return -EINVAL; // Invalid output buffer
    }
    // convert tibinary to string first then convert string to mysql decimal binary

    // TODO:: convert without converting to string, directly convert the binary representation of tidb decimal to mysql decimal binary representation
    std::string str;
    str.reserve(MAXDECIMALLEN + 2); // reserve enough space for the string representation of the decimal

    // char buffer[MAXDECIMALLEN + 2]; // buffer to hold the string representation of the decimal, +2 for sign and decimal point
    // size_t strbufLen = 0;
    // CFixLenVec<char, size_t, MAXDECIMALLEN + 2> strBuffer(buffer, strbufLen); // buffer to hold the string representation of the decimal, +2 for sign and decimal point


    uint8_t* dlen = out; // the first byte to store the length of the mysql decimal binary representation
    uint8_t* dscale = out + 1; // the second byte to store the

    // len of natural number
    uint8_t intlen = static_cast<uint8_t>(tibinary[0]);
    uint8_t pointPos = static_cast<uint8_t>(tibinary[1]);
    // len of decimal number
    uint8_t declen = static_cast<uint8_t>(tibinary[2]);
    uint8_t unknow = static_cast<uint8_t>(tibinary[3]);

    if (intlen + declen > MAXDECIMALLEN + 1) {
        return -E2BIG; // Invalid decimal length
    }

    *dlen = intlen + declen;
    *dscale = declen;

    uint32_t* intPart = (uint32_t*)(tibinary + 4);
    
    uint8_t frac_len = intlen % 9 == 0 ? intlen / 9 : intlen / 9 + 1;
    uint8_t decfrac_len = declen % 9 == 0 ? declen / 9 : declen / 9 + 1;

    if (sign) {
        str += '-';
    }

    if (frac_len == 0) {
        str += "0.";
    } else {
        for (int i = 0; i < frac_len; ++i) {
            str += std::to_string(intPart[i]);
        }
        str += '.';
    }

    if (decfrac_len > 0) {
        for (int i = 0; i < decfrac_len; ++i) {
            str += std::to_string(intPart[frac_len + i]);
        }
    } else {
        str += "0";
    }

    // printf("String representation of decimal: %s\n", str.c_str());

    // convert string to mysql decimal binary
    my_decimal dec;
    const char* end = str.c_str() + str.size();

    int rc = str2my_decimal(0, str.c_str(), &dec, &end);
    if (rc != 0) {
        return rc; // Return error code if conversion failed
    }

    rc = my_decimal2binary(0, &dec, out + 2, intlen + declen, declen);
    if (rc != 0) {
        return rc; // Return error code if conversion failed
    }

    if (actureLen) {
        *actureLen = my_decimal_get_binary_size(intlen + declen, declen);
        *actureLen += 2; // add 2 bytes for length and scale
    }

    // printMemory(out, out_buffer_len);

    return 0;
}


}