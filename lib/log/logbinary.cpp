/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#include <log/logbinary.h>
#include <threadlock.hpp>
#include <iostream>
#include <cstdarg>
#include <sys/stat.h>
#include <ctime>
#include <thread>
#include <cstring>
#define IS_ASCII 0
#define IS_UTF8 1
#define IS_GBK 2
#define IS_UNICODE 3
#define IS_OTHER 11

uint8_t cvthex[16] = {0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
		0x38, 0x39, 'a', 'b', 'c', 'd', 'e', 'f'};
	
std::vector<const char*> logrecord::logl_str = {
		"BIN  ",
		"FATAL",
		"ERROR",
		"NOTIC",
		"INFO ",
		"DEBUG",
};

char nowtmStr[32]{ 0 };
const size_t nowtmStrSize = 20; // "yyyy-mm-dd HH:MM:SS"
const size_t loglStrLen = 5; // "[DEBUG]" "[INFO ]"
volatile size_t logrecord::logCount = 0;
static bool timeGuard = false;
static CSpin timeMutex;
static std::thread timeguard;

void logrecord::initlogTimeguard() {
	if (timeGuard) {
		return;
	}
	timeGuard = true;
	time_t nowtm = time(nullptr);
	strftime(nowtmStr, sizeof(nowtmStr), "%Y-%m-%d %H:%M:%S", localtime(&nowtm));

	timeguard = std::thread([](){
		while(logrecord::logCount > 0) {
			time_t nowtm = time(nullptr);
			strftime(nowtmStr, sizeof(nowtmStr), "%Y-%m-%d %H:%M:%S", localtime(&nowtm));
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		timeGuard = false;
	});
	

}

struct log_sequence {
	log_sequence() {
		log_str.resize(1024);
	}
	~log_sequence() {
	}
	std::string log_str;
	size_t len;
	logrecord::loglevel logl;
	size_t logfile_pos;
};
 
logrecord::logrecord() {
	timeMutex.lock();
	++logCount;
	initlogTimeguard();
	timeMutex.unlock();

	logl = LOG_INFO;
	log_files.emplace_back("./logbinary.log");
	log_info.reserve(1024);
	print_info_format.reserve(1024);
	print_info = new char[16];
	memset(print_info, '\0', 16);
	print_screen = 0;
	// log_seqs.clear();
	m_exit = false;
	async_mode = false;

}

logrecord::~logrecord() {

	delete print_info;
	m_exit = true;

	set_async_mode(false);

	while(!log_seqs.empty()) {
		log_sequence* seq = log_seqs.front();
		log_seqs.pop();
		delete seq;
	}
	// log_seqs.clear();
	
	timeMutex.lock();
	--logCount;
	if(logCount == 0) {
		timeguard.join();
	}
	timeMutex.unlock();
}

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

void logrecord::set_async_mode(bool async) {
	if(async_mode == async) {
		return;
	}

	async_mode = async;

	if(async_mode == false) {
		if(logGuard.joinable()) {
			logGuard.join();
		}
		return;
	}

	volatile bool complete = false;
	logGuard = std::thread([this, &complete]() {
		std::queue<log_sequence*> logQue;
		size_t logf_pos = -1;
		FILE* fp;

		if(!log_files.empty()) {
			fp = fopen(log_files.back().c_str(), "a");
			logf_pos = log_files.size() - 1;
		} else {
			fp = fopen("./logbinary.log", "a");
		}

		if (!fp) {
			std::cerr << "Failed to open log file: " << log_files.back() << std::endl;
			return;
		}
		complete = true;
		char loghead[64];
		memcpy(loghead, "[YYYY-mm-dd hh:mm:ss] [INFO ]:  ", 33);

		
		while(!log_queue.empty() || async_mode) {
			
			if(log_queue.empty()) {
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
				continue;
			} 

			logQueMutex.lock();
			logQue.swap(log_queue);
			logQueMutex.unlock();

			while(!logQue.empty()) {

				log_sequence* seq = logQue.front();
				logQue.pop();

				if(seq->logl == LOG_BINARY) {
					if(logf_pos != seq->logfile_pos) {
						logf_pos = seq->logfile_pos;
						fclose(fp);
						fp = fopen(log_files[logf_pos].c_str(), "a");
					}
					fwrite(seq->log_str.c_str(), sizeof(char), seq->len, fp);
					fflush(fp);
					logSequenceMutex.lock();
					log_seqs.push(seq);
					logSequenceMutex.unlock();
					continue;
				}

				memcpy(loghead + 1, nowtmStr, 19);
				memcpy(loghead + 23, logl_str[seq->logl], loglStrLen);

				if(logf_pos != seq->logfile_pos) {
					logf_pos = seq->logfile_pos;
					char cghead[256];
					int len = 0;
					len = sprintf(cghead, "[%s] [CHANGELOGPATH]: new log path: %s\n", nowtmStr, log_files[logf_pos].c_str());
					fwrite(cghead, sizeof(char), len, fp);
					fclose(fp);
					fp = fopen(log_files[logf_pos].c_str(), "a");
				}



				fwrite(loghead, sizeof(char), 32, fp);
				fwrite(seq->log_str.c_str(), sizeof(char), seq->len, fp);
				fflush(fp);

				logSequenceMutex.lock();
				log_seqs.push(seq);
				logSequenceMutex.unlock();
			}

		}

	});

	while(!complete) {
		// std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
	// log_inf("async mode enabled, now log path: %s\n", log_files.back().c_str());

}

void logrecord::set_log_path(const std::string& s) {
	log_files.emplace_back(s);
}

const std::string& logrecord::get_log_path() const {
	return log_files.back();
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
		
	va_list ap;
	va_start(ap, str);

	if(async_mode) {
		log_sequence* log_seq;
		size_t len = 0;
		if(log_seqs.size()) {
			logSequenceMutex.lock();
			if(log_seqs.size()) {
				log_seq = log_seqs.front();
				log_seqs.pop();
				logSequenceMutex.unlock();
			} else {
				logSequenceMutex.unlock();
				log_seq = new log_sequence;
			}
		} else {
			log_seq = new log_sequence;
		}
		
		// printf(str, ap);
		len = vsnprintf(const_cast<char*>(log_seq->log_str.c_str()), log_seq->log_str.size(), str, ap);
		va_end(ap);
		// printf("str1 is : %s len : %lu\n", log_seq->log_str.c_str(), len);
		if(len >= log_seq->log_str.size()) {
			log_seq->log_str.resize(len + 1);
			va_start(ap, str);
			len = vsnprintf(const_cast<char*>(log_seq->log_str.c_str()), log_seq->log_str.size(), str, ap);
			va_end(ap);
		}
		

		log_seq->logl = LOG_INFO;
		log_seq->len = len;
		log_seq->logfile_pos = log_files.size() - 1; // current log file position

		// printf("str is : %s len: %lu\n", log_seq->log_str.c_str(), len);


		logQueMutex.lock();
		log_queue.push(log_seq);
		logQueMutex.unlock();
		
		
		return;
	} 

	FILE* fp;
	if(!log_files.empty()) {
		fp = fopen(log_files.back().c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}
	fprintf(fp, "[%s] [INFO ]: ", nowtmStr);
	vfprintf(fp, str, ap);
	va_end(ap);
	fclose(fp);
	
	return;
}

void logrecord::log_notic(const char* str, ...) {
	if(logl < LOG_NOTIC) {
		return;
	}
		
	va_list ap;
	va_start(ap, str);

	if(async_mode) {
		log_sequence* log_seq;
		size_t len = 0;
		if(log_seqs.size()) {
			logSequenceMutex.lock();
			if(log_seqs.size()) {
				log_seq = log_seqs.front();
				log_seqs.pop();
				logSequenceMutex.unlock();
			} else {
				logSequenceMutex.unlock();
				log_seq = new log_sequence;
			}
		} else {
			log_seq = new log_sequence;
		}

		len = vsnprintf(const_cast<char*>(log_seq->log_str.c_str()), log_seq->log_str.size(), str, ap);
		va_end(ap);
		if(len >= log_seq->log_str.size()) {
			log_seq->log_str.resize(len + 1);
			va_start(ap, str);
			len = vsnprintf(const_cast<char*>(log_seq->log_str.c_str()), log_seq->log_str.size(), str, ap);
			va_end(ap);
		}

		log_seq->logl = LOG_NOTIC;
		log_seq->len = len;
		log_seq->logfile_pos = log_files.size() - 1;

		logQueMutex.lock();
		log_queue.push(log_seq);
		logQueMutex.unlock();
		
		
		return;
	}

	FILE* fp;
	if(!log_files.empty()) {
		fp = fopen(log_files.back().c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}
	fprintf(fp, "[%s] [NOTIC]: ", nowtmStr);
	vfprintf(fp, str, ap);
	va_end(ap);
	fclose(fp);
}

// error and fatal should not be asynchronous.
void logrecord::log_error(const char* str, ...) {
	if(logl < LOG_ERROR) {
		return;
	}
		
	va_list ap;
	va_start(ap, str);
	if(async_mode) {
		log_sequence* log_seq;
		size_t len = 0;
		if(log_seqs.size()) {
			logSequenceMutex.lock();
			if(log_seqs.size()) {
				log_seq = log_seqs.front();
				log_seqs.pop();
				logSequenceMutex.unlock();
			} else {
				logSequenceMutex.unlock();
				log_seq = new log_sequence;
			}
		} else {
			log_seq = new log_sequence;
		}

		len = vsnprintf(const_cast<char*>(log_seq->log_str.c_str()), log_seq->log_str.size(), str, ap);
		va_end(ap);
		if(len >= log_seq->log_str.size()) {
			log_seq->log_str.resize(len + 1);
			va_start(ap, str);
			len = vsnprintf(const_cast<char*>(log_seq->log_str.c_str()), log_seq->log_str.size(), str, ap);
			va_end(ap);
		}

		log_seq->logl = LOG_ERROR;
		log_seq->len = len;
		log_seq->logfile_pos = log_files.size() - 1; // current log file position

		// printf("str is : %s len: %lu\n", log_seq->log_str.c_str(), len);


		logQueMutex.lock();
		log_queue.push(log_seq);
		logQueMutex.unlock();
		
		
		return;
	} 

	FILE* fp;
	if(!log_files.empty()) {
		fp = fopen(log_files.back().c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}
	fprintf(fp, "[%s] [ERROR]: ", nowtmStr);
	vfprintf(fp, str, ap);
	va_end(ap);
	fclose(fp);
}

void logrecord::log_fatal(const char* str, ...) {
	if(logl < LOG_FATAL) {
		return;
	}

	va_list ap;
	va_start(ap, str);

	if(async_mode) {
		log_sequence* log_seq;
		size_t len = 0;
		if(log_seqs.size()) {
			logSequenceMutex.lock();
			if(log_seqs.size()) {
				log_seq = log_seqs.front();
				log_seqs.pop();
				logSequenceMutex.unlock();
			} else {
				logSequenceMutex.unlock();
				log_seq = new log_sequence;
			}
		} else {
			log_seq = new log_sequence;
		}
		

		len = vsnprintf(const_cast<char*>(log_seq->log_str.c_str()), log_seq->log_str.size(), str, ap);
		va_end(ap);
		if(len >= log_seq->log_str.size()) {
			log_seq->log_str.resize(len + 1);
			va_start(ap, str);
			len = vsnprintf(const_cast<char*>(log_seq->log_str.c_str()), log_seq->log_str.size(), str, ap);
			va_end(ap);
		}

		log_seq->logl = LOG_FATAL;
		log_seq->len = len;
		log_seq->logfile_pos = log_files.size() - 1; // current log file position

		// printf("str is : %s len: %lu\n", log_seq->log_str.c_str(), len);


		logQueMutex.lock();
		log_queue.push(log_seq);
		logQueMutex.unlock();
		
		
		return;
	} 

	FILE* fp;
	if(!log_files.empty()) {
		fp = fopen(log_files.back().c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}
	fprintf(fp, "[%s] [FATAL]: ", nowtmStr);
	vfprintf(fp, str, ap);
	va_end(ap);
	fclose(fp);
}

void logrecord::log_debug(const char* str, ...) {
	if(logl < LOG_DEBUG) {
		return;
	}

	va_list ap;
	va_start(ap, str);

	if(async_mode) {
		log_sequence* log_seq;
		size_t len = 0;
		if(log_seqs.size()) {
			logSequenceMutex.lock();
			if(log_seqs.size()) {
				log_seq = log_seqs.front();
				log_seqs.pop();
				logSequenceMutex.unlock();
			} else {
				logSequenceMutex.unlock();
				log_seq = new log_sequence;
			}
		} else {
			log_seq = new log_sequence;
		}
		

		len = vsnprintf(const_cast<char*>(log_seq->log_str.c_str()), log_seq->log_str.size(), str, ap);
		va_end(ap);
		if(len >= log_seq->log_str.size()) {
			log_seq->log_str.resize(len + 1);
			va_start(ap, str);
			len = vsnprintf(const_cast<char*>(log_seq->log_str.c_str()), log_seq->log_str.size(), str, ap);
			va_end(ap);
		}

		log_seq->logl = LOG_DEBUG;
		log_seq->len = len;
		log_seq->logfile_pos = log_files.size() - 1; // current log file position

		// printf("str is : %s len: %lu\n", log_seq->log_str.c_str(), len);


		logQueMutex.lock();
		log_queue.push(log_seq);
		logQueMutex.unlock();
		
		
		return;
	} 

	FILE* fp;
	if(!log_files.empty()) {
		fp = fopen(log_files.back().c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}
	fprintf(fp, "[%s] [DEBUG]: ", nowtmStr);
	vfprintf(fp, str, ap);
	va_end(ap);
	fclose(fp);
}

void logrecord::log_binary(const void* pBuf, size_t len) {
	if(logl < LOG_BINARY) {
		return;
	}

	if(async_mode) {
		log_sequence* log_seq;
		if(log_seqs.size()) {
			logSequenceMutex.lock();
			if(log_seqs.size()) {
				log_seq = log_seqs.front();
				log_seqs.pop();
				logSequenceMutex.unlock();
			} else {
				logSequenceMutex.unlock();
				log_seq = new log_sequence;
			}
		} else {
			log_seq = new log_sequence;
		}

		log_seq->logl = LOG_BINARY;
		log_seq->len = len;
		log_seq->logfile_pos = log_files.size() - 1;
		if(log_seq->log_str.size() < len) {
			log_seq->log_str.resize(len * 2);
		}
		memcpy(const_cast<char*>(log_seq->log_str.c_str()),pBuf, len);

		logQueMutex.lock();
		log_queue.push(log_seq);
		logQueMutex.unlock();
		
		return;
	}

	FILE* fp;
	if(!log_files.empty()) {
		fp = fopen(log_files.back().c_str(), "a");
	} else {
		fp = fopen("./logbinary.log", "a");
	}
	fwrite(pBuf, sizeof(char), len, fp);
	fclose(fp);
}

void logrecord::log_into_file() {
	FILE* fp;
	if(!log_files.empty()) {
		fp = fopen(log_files.back().c_str(), "a");
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
std::string getCmdoutput(size_t& n) {
	struct stat statbuf;
	stat("/tmp/templgbin", &statbuf);
	n = statbuf.st_size + 1;
	FILE* fp;
	// char* ss = new char[n];
	std::string ret;
	ret.resize(n);

	// memset(ss, '\0', n);
	fp = fopen("/tmp/templgbin", "r");
	int rs = fread(&ret[0], 1, n, fp);
	if(rs == 0) {
		// delete ss;
		fclose(fp);
		return "";
	}
	fclose(fp);
	return ret;
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
