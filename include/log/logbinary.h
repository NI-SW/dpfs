/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#pragma once
#include <string>
#include <string.h>
#include <vector>
#include <thread>
#include <queue>
#include <threadlock.hpp>
struct log_sequence;
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
		LOG_BINARY,
		LOG_FATAL,
		LOG_ERROR,
		LOG_NOTIC,
		LOG_INFO,
		LOG_DEBUG,
	};
	std::string log_info;
	std::string print_info_format;

	logrecord();
	logrecord(const std::string& s) = delete;
	logrecord(const char*& s) = delete;
	~logrecord();
	// out put str to file at log path you set before
	void log_inf(const char* str, ...);
	void log_notic(const char* str, ...);
	void log_error(const char* str, ...);
	void log_fatal(const char* str, ...);
	void log_debug(const char* str, ...);
	void log_binary(const void* pBuf, size_t len);

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
	// void set_log_path(const char* s);
	void set_log_path(const std::string& s);
	const std::string& get_log_path() const;
	// set log level
	void set_loglevel(loglevel level);

	void set_async_mode(bool async);
	void reset();

private:

	bool async_mode;
	std::thread logGuard;

	std::queue<log_sequence*> log_queue;
	CSpin logQueMutex;

	// std::list<log_sequence*> log_seqs;
	std::queue<log_sequence*> log_seqs;
	CSpin logSequenceMutex;


	std::vector<std::string> log_files;
	bool m_exit;
	loglevel logl;

	static std::vector<const char*> logl_str;
	static volatile size_t logCount;
	static void initlogTimeguard();
};


char* getCmdoutput(size_t& n);

void Cmdoutput(const char* s);