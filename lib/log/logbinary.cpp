/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#include <log/logbinary.h>
#include <threadlock.hpp>
#include <iostream>
#include <cstdarg>
#include <sys/stat.h>
// #include <chrono>
#include <ctime>
#include <thread>
#define IS_ASCII 0
#define IS_UTF8 1
#define IS_GBK 2
#define IS_UNICODE 3
#define IS_OTHER 11

uint8_t cvthex[16] = {0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
		0x38, 0x39, 'a', 'b', 'c', 'd', 'e', 'f'};
		

char nowtmStr[64]{ 0 };
volatile size_t logrecord::logCount = 0;
bool timeGuard = false;
CSpin timeMutex;
std::thread timeguard;
void logrecord::initlogTimeguard() {
	if (timeGuard) {
		return;
	}
	timeGuard = true;

	timeguard = std::thread([](){
		while(logrecord::logCount > 0) {
			time_t nowtm = time(nullptr);
			strftime(nowtmStr, sizeof(nowtmStr), "%Y-%m-%d %H:%M:%S", localtime(&nowtm));
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		timeGuard = false;
	});
	

}

logrecord::logrecord() {
	timeMutex.lock();
	++logCount;
	initlogTimeguard();
	timeMutex.unlock();

	log_path.clear();
	logl = LOG_INFO;
	log_info.reserve(1024);
	print_info_format.reserve(1024);
	print_info = new char[16];
	memset(print_info, '\0', 16);
	print_screen = 0;
}

logrecord::~logrecord() {
	timeMutex.lock();
	--logCount;
	if(logCount == 0) {
		timeguard.join();
	}
	timeMutex.unlock();
	delete print_info;
}
/*
logrecord::logrecord(const std::string& s) {
	log_path.clear();
	logl = LOG_INFO;
	log_info.reserve(1024);
	print_info_format.reserve(1024);
	print_info = new char[16];
	memset(print_info, '\0', 16);
	log_info = s;
	print_screen = 0;
	handle_info();
}

logrecord::logrecord(const char*& s) {
	log_path.clear();
	logl = LOG_INFO;
	log_info.reserve(1024);
	print_info_format.reserve(1024);
	print_info = new char[16];
	memset(print_info, '\0', 16);
	log_info = s;
	print_screen = 0;
	handle_info();
}
*/
void logrecord::set_string(std::string& s) {
	log_info.clear();
	log_info = s;
	handle_info();
}

void logrecord::set_string(unsigned long long int s, size_t length) {
	log_info.clear();
	log_info.assign((const char*)s, length);
	handle_info();
}

void logrecord::set_log_path(const char* s) {
	log_path = s;
}

void logrecord::set_log_path(const std::string& s) {
	log_path.assign(s.c_str(), s.size());
}

const std::string& logrecord::get_log_path() const {
	return log_path;
}

void logrecord::handle_info() {
	std::string::iterator iter = log_info.begin();
	std::string::iterator iter1 = log_info.begin();
	for (; iter != log_info.end(); iter++) {
		// sprintf(print_info, "%02X ", (unsigned char)*iter);
		print_info[0] = cvthex[*iter >> 4];
		print_info[1] = cvthex[*iter & 0x0F];
		print_info[2] = ' ';
		print_info[3] = 0;

		print_info_format.append(print_info);
		if ((iter - log_info.begin()) % 16 == 15) {
			// sprintf(print_info, " *");
			print_info[0] = ' ';
			print_info[1] = '*';
			print_info[2] = 0;
			print_info_format.append(print_info);
			
			for (int i = 0; i < 16; iter1++, i++) {
				switch (judge_format(iter1)) {
					case IS_ASCII: {
						// sprintf(print_info, "%c", (unsigned char)*iter1);
						print_info[0] = *iter1;
						print_info[1] = 0;
						print_info_format.append(print_info);
						break;
					}
					case IS_UTF8: {
						fill_utf8(iter1);
						//i += 2;
						print_info_format.append(print_info);
						// memset(print_info, '\0', 16);
						break;
					}
					case IS_OTHER: {
						// sprintf(print_info, ".");
						print_info[0] = '.';
						print_info[1] = 0;
						print_info_format.append(print_info);
						break;
					}
					default: {
						break;
					}
				}
			}
			// sprintf(print_info, "*\n");
			print_info[0] = '*';
			print_info[1] = '\n';
			print_info[2] = 0;
			print_info_format.append(print_info);
		}
	}
	if (iter == log_info.end() && (iter - log_info.begin()) % 16 != 0) {
		int i = 0;
		while ((iter - log_info.begin()) % 16 != 0) {
			iter--;
			i++;
		}
		i = 16 - i;
		while (i != 0) {
			print_info_format.append("   ");
			i--;
		}
		print_info[0] = ' ';
		print_info[1] = '*';
		print_info[2] = 0;
		print_info_format.append(print_info);
		for (; iter < log_info.end(); iter++) {
			switch (judge_format(iter)) {
				case IS_ASCII: {
					print_info[0] = *iter1;
					print_info[1] = 0;
					print_info_format.append(print_info);
					break;
				}
				case IS_UTF8: {
					fill_utf8(iter);
					print_info_format.append(print_info);
					break;
				}
				case IS_OTHER: {
					print_info[0] = '.';
					print_info[1] = 0;
					print_info_format.append(print_info);
					break;
				}
			}
		}
		print_info[0] = '*';
		print_info[1] = '\n';
		print_info[2] = 0;
		print_info_format.append(print_info);
	}
}

int logrecord::judge_format(std::string::iterator iter)
{
	if ((unsigned char)*iter == 0) {
		return IS_OTHER;
	}
	else if ((unsigned char)*iter > 32 && (unsigned char)*iter < 123) {
		return IS_ASCII;
	}
	else if ((unsigned char)*iter >= 0xE0 && (unsigned char)*iter <= 0xEF) {
		if ((unsigned char)*iter == 0xE0) {
			iter++;
			if ((unsigned char)*iter >= 0xA0 && (unsigned char)*iter <= 0xBF) {
				iter++;
				if ((unsigned char)*iter >= 0x80 && (unsigned char)*iter <= 0xBF) {
					return IS_UTF8;
				}
			}
		}
		else if ((unsigned char)*iter >= 0xE1 && (unsigned char)*iter <= 0xEC) {
			iter++;
			if ((unsigned char)*iter >= 0x80 && (unsigned char)*iter <= 0xBF) {
				iter++;
				if ((unsigned char)*iter >= 0x80 && (unsigned char)*iter <= 0xBF) {
					return IS_UTF8;
				}
			}
		}
		else if ((unsigned char)*iter == 0xED) {
			iter++;
			if ((unsigned char)*iter >= 0x80 && (unsigned char)*iter <= 0x9F) {
				iter++;
				if ((unsigned char)*iter >= 0x80 && (unsigned char)*iter <= 0xBF) {
					return IS_UTF8;
				}
			}
		}
		else if((unsigned char)*iter >= 0xEE && (unsigned char)*iter <= 0xEF) {
			iter++;
			if ((unsigned char)*iter >= 0x80 && (unsigned char)*iter <= 0xBF) {
				iter++;
				if ((unsigned char)*iter >= 0x80 && (unsigned char)*iter <= 0xBF) {
					return IS_UTF8;
				}
			}
		}
	}
	return IS_OTHER;
}

void logrecord::set_loglevel(loglevel level) {
	logl = level;
}

void logrecord::log_inf(const char* str, ...) {
	if(logl < LOG_INFO) {
		return;
	}
	FILE* fp;
	if(!log_path.empty()) {
		fp = fopen(log_path.c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}

	va_list ap;
	va_start(ap, str);
	fprintf(fp, "[%s] [INFO]: ", nowtmStr);
	vfprintf(fp, str, ap);
	va_end(ap);
	fclose(fp);
}
void logrecord::log_notic(const char* str, ...) {
	if(logl < LOG_NOTIC) {
		return;
	}
	FILE* fp;
	if(!log_path.empty()) {
		fp = fopen(log_path.c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}
	va_list ap;
	va_start(ap, str);
	fprintf(fp, "[%s] [NOTIC]: ", nowtmStr);
	vfprintf(fp, str, ap);
	va_end(ap);
	fclose(fp);
}
void logrecord::log_error(const char* str, ...) {
	if(logl < LOG_ERROR) {
		return;
	}
	FILE* fp;
	if(!log_path.empty()) {
		fp = fopen(log_path.c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}
	va_list ap;
	va_start(ap, str);
	fprintf(fp, "[%s] [ERROR]: ", nowtmStr);
	vfprintf(fp, str, ap);
	va_end(ap);
	fclose(fp);
}
void logrecord::log_fatal(const char* str, ...) {
	if(logl < LOG_FATAL) {
		return;
	}
	FILE* fp;
	if(!log_path.empty()) {
		fp = fopen(log_path.c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}
	va_list ap;
	va_start(ap, str);
	fprintf(fp, "[%s] [FATAL]: ", nowtmStr);
	vfprintf(fp, str, ap);
	va_end(ap);
	fclose(fp);
}
void logrecord::log_debug(const char* str, ...) {
	if(logl < LOG_DEBUG) {
		return;
	}
	FILE* fp;
	if(!log_path.empty()) {
		fp = fopen(log_path.c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}
	va_list ap;
	va_start(ap, str);
	fprintf(fp, "[%s] [DEBUG]: ", nowtmStr);
	vfprintf(fp, str, ap);
	va_end(ap);
	fclose(fp);
}

void logrecord::log_into_file() {
	FILE* fp;
	if(!log_path.empty()) {
		fp = fopen(log_path.c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}
	fwrite(print_info_format.c_str(), print_info_format.size(), print_info_format.size(), fp);
	// fprintf(fp, print_info_format.c_str());
	fclose(fp);
}

void logrecord::print_inf() {
	std::cout << print_info_format << std::endl;
}

void logrecord::reset() {
	log_info.clear();
	print_info_format.clear();
	memset(print_info, '\0', 16);
}

void logrecord::fill_utf8(std::string::iterator iter) {
	// memset(print_info, '\0', 16);
	print_info[0] = (unsigned char)(*iter);
	iter++;
	print_info[1] = (unsigned char)(*iter);
	iter++;
	print_info[2] = (unsigned char)(*iter);
	print_info[3] = 0;
}

//return a string which is print by system call with n byte space.
char* getCmdoutput(size_t& n) {
	struct stat statbuf;
	stat("/tmp/templgbin", &statbuf);
	n = statbuf.st_size + 1;
	FILE* fp;
	char* ss = new char[n];

	memset(ss, '\0', n);
	fp = fopen("/tmp/templgbin", "r");
	int rs = fread(ss, 1, n, fp);
	if(rs == 0) {
		delete ss;
		fclose(fp);
		return nullptr;
	}
	fclose(fp);
	return ss;
}


void Cmdoutput(const char* s) {
	std::string op = s;
	op.append(" > /tmp/templgbin");
	int rc = system(op.c_str());
	if (rc != 0) {
		return;
	}
	return;
}