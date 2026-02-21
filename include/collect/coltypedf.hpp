/*
    col type definition file.
*/

#include <stdint.h>
constexpr uint8_t MAXDECIMALLEN = 65;

enum class dpfs_datatype_t : uint8_t {
    TYPE_NULL = 0,
    TYPE_INT,
    TYPE_BIGINT,
    TYPE_FLOAT,
    TYPE_DECIMAL,
    TYPE_DOUBLE,
    TYPE_CHAR,
    TYPE_VARCHAR,
    TYPE_BINARY,
    TYPE_BLOB,
    TYPE_TIMESTAMP,
    TYPE_BOOL,
    TYPE_DATE,
    MAX_TYPE
};
// decimal use mysql's decimal
// // decimal can not be primary key of collection or index.(for performance consideration)
// class CDecimal {
// public:
//     CDecimal() = default;
//     CDecimal(uint8_t len, uint8_t scale, void* ptr) : m_len(len), m_scale(scale), m_ptr(ptr) {}
//     uint8_t m_scale = 0;
//     uint8_t m_len = 0;
//     void* m_ptr = nullptr;



//     // TODO :: finish the decimal implementation
//     int fromPtr(uint8_t len, uint8_t scale, void* ptr) {
//         m_len = len;
//         m_scale = scale;
//         m_ptr = ptr;
//         return 0;
//     }

//     //TODO
//     double toDouble() const {
        
//         return 0.0;
//     }

//     int toInt() const {
//         return 0;
//     }

// };
