#include <cstddef>

#if __cplusplus >= 201103L
constexpr size_t maxareaBitSize     = 8;
constexpr size_t maxcityBitSize     = 12;
constexpr size_t maxNodeBitSize     = 10;
constexpr size_t maxBlockBitSize    = 10;
constexpr size_t productIdBitSize   = 24; // 商品末尾id大小
#else
const size_t maxareaBitSize     = 8;
const size_t maxcityBitSize     = 12;
const size_t maxNodeBitSize     = 10;
const size_t maxBlockBitSize    = 10;
const size_t productIdBitSize   = 63; // 商品末尾id大小    
#endif