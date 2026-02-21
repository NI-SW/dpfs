#ifndef DECIMAL_STANDALONE_COMPAT_H
#define DECIMAL_STANDALONE_COMPAT_H

#include <cctype>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>

using uchar = unsigned char;
using int8 = std::int8_t;
using uint8 = std::uint8_t;
using int16 = std::int16_t;
using uint16 = std::uint16_t;
using int32 = std::int32_t;
using uint32 = std::uint32_t;
using longlong = long long;
using ulonglong = unsigned long long;
using ulong = unsigned long;

#if defined(__GNUC__) || defined(__clang__)
inline bool likely(bool expr) { return __builtin_expect(expr, true); }
inline bool unlikely(bool expr) { return __builtin_expect(expr, false); }
#else
inline bool likely(bool expr) { return expr; }
inline bool unlikely(bool expr) { return expr; }
#endif

#define my_isspace(cs, c) (std::isspace(static_cast<unsigned char>(c)) != 0)
#define my_isdigit(cs, c) (std::isdigit(static_cast<unsigned char>(c)) != 0)

static inline int8 mi_sint1korr(const uchar *A) { return *A; }
static inline uint8 mi_uint1korr(const uchar *A) { return *A; }

static inline int16 mi_sint2korr(const uchar *A) {
  return (int16)((uint32)(A[1]) + ((uint32)(A[0]) << 8));
}

static inline int32 mi_sint3korr(const uchar *A) {
  return (int32)((A[0] & 128) ? ((255U << 24) | ((uint32)(A[0]) << 16) |
                                 ((uint32)(A[1]) << 8) | ((uint32)A[2]))
                              : (((uint32)(A[0]) << 16) |
                                 ((uint32)(A[1]) << 8) | ((uint32)(A[2]))));
}

static inline int32 mi_sint4korr(const uchar *A) {
  return (int32)((uint32)(A[3]) + ((uint32)(A[2]) << 8) +
                 ((uint32)(A[1]) << 16) + ((uint32)(A[0]) << 24));
}

static inline uint16 mi_uint2korr(const uchar *A) {
  return (uint16)((uint16)A[1]) + ((uint16)A[0] << 8);
}

static inline uint32 mi_uint3korr(const uchar *A) {
  return (uint32)((uint32)A[2] + ((uint32)A[1] << 8) + ((uint32)A[0] << 16));
}

static inline uint32 mi_uint4korr(const uchar *A) {
  return (uint32)((uint32)A[3] + ((uint32)A[2] << 8) + ((uint32)A[1] << 16) +
                  ((uint32)A[0] << 24));
}

static inline longlong mi_sint8korr(const uchar *A) {
  return (longlong)((ulonglong)mi_uint4korr(A + 4) |
                    ((ulonglong)mi_uint4korr(A) << 32));
}

#define mi_int1store(T, A) *((uchar *)(T)) = (uchar)(A)

#define mi_int2store(T, A)                      \
  {                                             \
    const uint def_temp = (uint)(A);            \
    ((uchar *)(T))[1] = (uchar)(def_temp);      \
    ((uchar *)(T))[0] = (uchar)(def_temp >> 8); \
  }
#define mi_int3store(T, A)                       \
  {                                              \
    const ulong def_temp = (ulong)(A);           \
    ((uchar *)(T))[2] = (uchar)(def_temp);       \
    ((uchar *)(T))[1] = (uchar)(def_temp >> 8);  \
    ((uchar *)(T))[0] = (uchar)(def_temp >> 16); \
  }
#define mi_int4store(T, A)                       \
  {                                              \
    const ulong def_temp = (ulong)(A);           \
    ((uchar *)(T))[3] = (uchar)(def_temp);       \
    ((uchar *)(T))[2] = (uchar)(def_temp >> 8);  \
    ((uchar *)(T))[1] = (uchar)(def_temp >> 16); \
    ((uchar *)(T))[0] = (uchar)(def_temp >> 24); \
  }

enum my_gcvt_arg_type { MY_GCVT_ARG_FLOAT, MY_GCVT_ARG_DOUBLE };

static constexpr int DECIMAL_MAX_SCALE = 30;
static constexpr int DECIMAL_NOT_SPECIFIED = DECIMAL_MAX_SCALE + 1;
static constexpr int FLOATING_POINT_BUFFER = 311 + DECIMAL_NOT_SPECIFIED;

inline double my_strtod(const char *str, const char **end, int *error) {
  errno = 0;
  char *local_end = nullptr;
  double value = std::strtod(str, &local_end);
  if (end != nullptr) {
    *end = local_end != nullptr ? local_end : str;
  }
  if (error != nullptr) {
    *error = (local_end == str || errno == ERANGE) ? 1 : 0;
  }
  return value;
}

inline long long my_strtoll10(const char *nptr, const char **endptr,
                              int *error) {
  errno = 0;
  char *local_end = nullptr;
  long long value = std::strtoll(nptr, &local_end, 10);
  if (endptr != nullptr) {
    *endptr = local_end != nullptr ? local_end : nptr;
  }
  if (error != nullptr) {
    *error = (local_end == nptr || errno == ERANGE) ? 1 : 0;
  }
  return value;
}

inline size_t my_gcvt(double x, my_gcvt_arg_type, int width, char *to,
                      bool *error) {
  if (width <= 0 || to == nullptr) {
    if (error != nullptr) {
      *error = true;
    }
    return 0;
  }
  int precision = std::numeric_limits<double>::digits10;
  int rc = std::snprintf(to, static_cast<size_t>(width) + 1, "%.*g", precision,
                         x);
  if (rc < 0) {
    if (error != nullptr) {
      *error = true;
    }
    to[0] = '\0';
    return 0;
  }
  if (rc > width) {
    if (error != nullptr) {
      *error = true;
    }
    rc = width;
    to[width] = '\0';
  } else if (error != nullptr) {
    *error = false;
  }
  return static_cast<size_t>(rc);
}

#endif  // DECIMAL_STANDALONE_COMPAT_H
