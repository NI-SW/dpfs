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
	std::string log_info;
	std::string print_info_format;

	logrecord();
	logrecord(const std::string& s);
	logrecord(const char*& s);

	void log_inf(const char* str, ...);
	void log_into_file();
	void print_inf();
	void set_string(std::string& s);
	void set_string(unsigned long long int s, size_t length);
	void set_log_path(const char* s);
	void set_log_path(std::string& s);

	void reset();
	~logrecord();
};
char* getCmdoutput(size_t& n);

void Cmdoutput(const char* s);