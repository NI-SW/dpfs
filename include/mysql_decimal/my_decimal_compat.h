#ifndef DECIMAL_STANDALONE_MY_DECIMAL_COMPAT_H
#define DECIMAL_STANDALONE_MY_DECIMAL_COMPAT_H

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

#include "decimal_compat.h"

using uint8 = std::uint8_t;
using uint32 = std::uint32_t;

struct CHARSET_INFO {
  uint mbminlen{1};
  bool ascii{true};
};

inline bool my_charset_is_ascii_based(const CHARSET_INFO *cs) {
  return cs != nullptr && cs->ascii;
}

inline const CHARSET_INFO my_charset_bin{1, true};
inline const CHARSET_INFO my_charset_latin1{1, true};
inline const CHARSET_INFO my_charset_numeric{1, true};

constexpr std::size_t STRING_BUFFER_USUAL_SIZE = 256;

class String {
 public:
  String() = default;

  String(char *buffer, std::size_t capacity, const CHARSET_INFO *cs)
      : m_ptr(buffer), m_capacity(capacity), m_charset(cs) {}

  bool alloc(std::size_t length) {
    if (length <= m_capacity && m_ptr != nullptr) {
      return false;
    }
    m_storage.resize(length);
    m_ptr = m_storage.data();
    m_capacity = length;
    return false;
  }

  char *ptr() { return m_ptr != nullptr ? m_ptr : m_storage.data(); }
  const char *ptr() const {
    return m_ptr != nullptr ? m_ptr : m_storage.data();
  }

  std::size_t length() const { return m_length; }

  void length(std::size_t length) { m_length = length; }

  void set_charset(const CHARSET_INFO *cs) { m_charset = cs; }

  bool copy(const char *src, std::size_t src_length, const CHARSET_INFO *,
            const CHARSET_INFO *to, uint *errors) {
    if (errors != nullptr) {
      *errors = 0;
    }
    if (alloc(src_length + 1)) {
      return true;
    }
    std::memcpy(ptr(), src, src_length);
    ptr()[src_length] = '\0';
    length(src_length);
    set_charset(to);
    return false;
  }

 private:
  char *m_ptr{nullptr};
  std::size_t m_length{0};
  std::size_t m_capacity{0};
  const CHARSET_INFO *m_charset{nullptr};
  std::string m_storage{};
};

template <std::size_t N>
class StringBuffer : public String {
 public:
  explicit StringBuffer(const CHARSET_INFO *cs) : String(m_buf, N, cs) {}

 private:
  char m_buf[N]{};
};

enum enum_mysql_timestamp_type {
  MYSQL_TIMESTAMP_NONE = -2,
  MYSQL_TIMESTAMP_ERROR = -1,
  MYSQL_TIMESTAMP_DATE = 0,
  MYSQL_TIMESTAMP_DATETIME = 1,
  MYSQL_TIMESTAMP_TIME = 2,
  MYSQL_TIMESTAMP_DATETIME_TZ = 3
};

struct MYSQL_TIME {
  uint year{0};
  uint month{0};
  uint day{0};
  uint hour{0};
  uint minute{0};
  uint second{0};
  ulong second_part{0};
  bool neg{false};
  enum enum_mysql_timestamp_type time_type{MYSQL_TIMESTAMP_NONE};
  int time_zone_displacement{0};
};

struct my_timeval {
  std::int64_t m_tv_sec{0};
  std::int64_t m_tv_usec{0};
};

class Time_val {
 public:
  Time_val() = default;

  Time_val(bool negative, uint hour, uint minute, uint second, uint microsecond)
      : m_negative(negative),
        m_base100(static_cast<longlong>(hour) * 10000 +
                  static_cast<longlong>(minute) * 100 +
                  static_cast<longlong>(second)),
        m_microsecond(microsecond) {}

  bool is_negative() const { return m_negative; }

  longlong to_int_truncated() const { return m_negative ? -m_base100 : m_base100; }

  uint microsecond() const { return m_microsecond; }

 private:
  bool m_negative{false};
  longlong m_base100{0};
  uint m_microsecond{0};
};

inline longlong TIME_to_ulonglong_date(const MYSQL_TIME &t) {
  return static_cast<longlong>(t.year) * 10000 +
         static_cast<longlong>(t.month) * 100 +
         static_cast<longlong>(t.day);
}

inline longlong TIME_to_ulonglong_datetime(const MYSQL_TIME &t) {
  return static_cast<longlong>(t.year) * 10000000000LL +
         static_cast<longlong>(t.month) * 100000000LL +
         static_cast<longlong>(t.day) * 1000000LL +
         static_cast<longlong>(t.hour) * 10000LL +
         static_cast<longlong>(t.minute) * 100LL +
         static_cast<longlong>(t.second);
}

#endif  // DECIMAL_STANDALONE_MY_DECIMAL_COMPAT_H
