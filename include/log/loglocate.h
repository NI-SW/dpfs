#define log_inf(fmt, ...) log_inf("%s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__);
#define log_notic(fmt, ...) log_notic("%s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__);
#define log_error(fmt, ...) log_error("%s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__);
#define log_fatal(fmt, ...) log_fatal("%s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__);
#define log_debug(fmt, ...) log_debug("%s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__);