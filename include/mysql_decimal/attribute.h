#ifndef DECIMAL_STANDALONE_ATTRIBUTE_H
#define DECIMAL_STANDALONE_ATTRIBUTE_H

#ifndef MY_ATTRIBUTE
#if defined(__GNUC__) || defined(__clang__)
#define MY_ATTRIBUTE(A) __attribute__(A)
#else
#define MY_ATTRIBUTE(A)
#endif
#endif

#endif  // DECIMAL_STANDALONE_ATTRIBUTE_H
