#pragma once
#include <string>
#include <string.h>
class logrecord {
	void handle_info();
	int judge_format(std::string::iterator iter);
	void fill_utf8(std::string::iterator iter);
	bool print_screen;
	char* print_info;
	std::string log_path;
	size_t log_length;
	
public:

	enum loglevel {
		LOG_FATAL,
		LOG_ERROR,
		LOG_NOTIC,
		LOG_INFO,
		LOG_DEBUG,
	};
	std::string log_info;
	std::string print_info_format;

	logrecord();
	logrecord(const std::string& s);
	logrecord(const char*& s);

	// out put str to file at log path you set before
	void log_inf(const char* str, ...);
	void log_notic(const char* str, ...);
	void log_error(const char* str, ...);
	void log_fatal(const char* str, ...);
	void log_debug(const char* str, ...);

	// out put binary log into file
	void log_into_file();
	// out put binary log on screen;
	void print_inf();

	/* set binary string to handle
	 * string will be out put like
	 * 31 32 33 34 FD AA BB CC *1124....*
	*/
	void set_string(std::string& s);
	void set_string(unsigned long long int s, size_t length);
	// set log path
	void set_log_path(const char* s);
	void set_log_path(const std::string& s);
	// set log level
	void set_loglevel(loglevel level);

	void reset();
	~logrecord();

private:
	loglevel logl;
};
char* getCmdoutput(size_t& n);

void Cmdoutput(const char* s);