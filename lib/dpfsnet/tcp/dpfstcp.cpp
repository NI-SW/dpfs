/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */

#include <dpfsnet/tcp/dpfstcp.hpp>
#include <string>
#include <cstring>
#include <threadlock.hpp>
#include <thread>


#ifdef _WIN64
#include <winsock2.h>
#include <ws2tcpip.h>
#ifndef SHUT_RDWR
#define SHUT_RDWR SD_BOTH
#endif
CSpin g_wsainitlock;
volatile bool WSA_initialized = false;
static WORD wVer = MAKEWORD(2, 2);
static WSAData wd;
#pragma comment(lib, "Ws2_32.lib")
static int closefd(int fd) {
    return closesocket(fd);
}
void initWinsock() {
    if (WSA_initialized) {
        return;
    }
    CSpinGuard lock(g_wsainitlock);
    if (WSA_initialized) {
        return;
    }
    WSA_initialized = true;
    WSAStartup(wVer, &wd);
}
void destroyWinsock() {
    if (!WSA_initialized) {
        return;
    }
    CSpinGuard lock(g_wsainitlock);
    if (!WSA_initialized) {
        return;
    }
    WSACleanup();
}

#else
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
static int closefd(int fd) {
    return close(fd);
}
void initWinsock() { }
void destroyWinsock() { }
#endif

static constexpr uint8_t aliveStr[0] {};
static constexpr uint8_t keepAliveDuration = 15;

static inline bool is_disconnect(const char* buffer, int size) {

    if(size != sizeof(aliveStr)) {
        return false;
    } else if(buffer == nullptr) {
        return false;
    } else if(memcmp(buffer, aliveStr, sizeof(aliveStr)) == 0) {
        return true;
    }
    return false;
}


static size_t dpfstcp_count = 0;
static CSpin g_dtlock;
static std::thread dpfstcp_guard;

void dpfstcp_guard_thread() {
    initWinsock();
    while (dpfstcp_count) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    destroyWinsock();
}



CDpfsTcp::CDpfsTcp() {
    // Constructor implementation
    g_dtlock.lock();
    if (dpfstcp_count == 0) {
        dpfstcp_guard = std::thread(dpfstcp_guard_thread);
    }
    ++dpfstcp_count;
    g_dtlock.unlock();
    log.set_log_path("./dpfstcp.log");

    m_exit = false;
    sockfd = -1;
    sendGuard = std::thread([this]() {

        std::unique_lock<std::mutex> lk(this->sendMutex);

        while (!m_exit) {

            // if not empty, continue.
            sendCv.wait_for(lk, std::chrono::milliseconds(1000), [this] { return (!sendQueue.empty() || m_exit); });
            if(sendQueue.empty() || !m_connected) {
                continue; // No messages to send, wait for more
            }

            sendLock.lock();
            sendthdQueue.swap(sendQueue); // Swap the local queue with the send queue
            sendLock.unlock();

            while(!sendthdQueue.empty() && m_connected && !m_exit) {
                dpfsmsg* msg = sendthdQueue.front();
                // sendthdQueue.pop();

                //  4B   nB
                // |len|data|
                size_t totalSent = 0;
                uint32_t msgSize = msg->size + sizeof(msg->size);

                msg->size = htonl(msg->size); // Convert size to network byte order
                int retry = 0;
                log.log_debug("Sending message of size: %u\n", msgSize);
                while (totalSent < msgSize) {
                    int sent = ::send(sockfd, msg->data - sizeof(dpfsmsg) + totalSent, msgSize - totalSent, 0);
                    if (sent < 0) {
                        // retry 2 times in 2 seconds
                        if(retry++ < 2 && (!m_exit && m_connected)) {
                            log.log_error("%s:%d Failed to send message, error: %d, retrying...\n", __FILE__, __LINE__, errno);
                            std::this_thread::sleep_for(std::chrono::seconds(1));
                            continue; // Retry sending
                        }
                        goto connerror;
                    }
                    log.log_debug("Sent %d bytes, total sent: %zu/%u\n", sent, totalSent, msgSize);
                    totalSent += sent;
                }
                sendthdQueue.pop();
                log.log_debug("total sent: %zu/%u\n", totalSent, msgSize);

                delete msg; // Free the message after sending
                msg = nullptr;
                continue;

                connerror:
                // if error disconnect
                log.log_error("%s:%d Failed to send message, error: %d\n", __FILE__, __LINE__, errno);
                // clearCache();
                clearEnv();
            }
            
        }
    });

    recvGuard = std::thread([this]() {

        uint32_t totalReceived = 0;
        int retry = 0;
        dpfsmsg* msg = nullptr;
        uint32_t msgSize = 0;
        // bool disconnMsg = false;
        char buf[4096] { 0 };

        while (!m_exit) {
            // Receiving logic here
            if(!m_connected) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }
            lastActiveTime = time(nullptr);

            // recv length first
            int received = ::recv(sockfd, (char*)&msgSize, sizeof(uint32_t), 0);
            if(received < 0) {
                log.log_debug("%s:%d Failed to receive message size, error: %d, rc:%d, he:%d\n", __FILE__, __LINE__, errno, received, h_errno);
                if(errno == ENOTCONN) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    continue;
                }
                goto connerror;
            } else if (received == 0) {
                log.log_debug("Connection closed by peer\n");
                disconnect();
                continue;
            } else {
                log.log_debug("Received %d bytes for message size\n", received);
            }

            if(msgSize == 0) {
                log.log_debug("%s:%d receive heart beat msg.\n", __FILE__, __LINE__);
                continue;
            }

            // Convert size from network byte order
            msgSize = ntohl(msgSize);
            totalReceived = 0;
            retry = 0;

            msg = (dpfsmsg*)malloc(sizeof(dpfsmsg) + msgSize);
            if(!msg) {
                log.log_error("%s:%d Failed to allocate memory for message, size: %u\n", __FILE__, __LINE__, msgSize);
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
            
            // recv according to length
            while(totalReceived < msgSize) {
                received = ::recv(sockfd, buf, msgSize - totalReceived, 0);
                if (received < 0) {
                    if(retry++ < 2) {
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        continue; // Retry sending
                    }
                    log.log_error("%s:%d Failed to receive message data, error: %d\n", __FILE__, __LINE__, errno);
                    goto connerror;
                } else if (received == 0) {
                    log.log_debug("Connection closed by peer\n");
                    goto connerror;
                }
                memcpy(msg->data + totalReceived, buf, received);
                totalReceived += received;
                log.log_debug("Received %d bytes, total received: %u/%u\n", received, totalReceived, msgSize);
            }

            msg->size = totalReceived;
            log.log_debug("Received message of size: %u\n", msg->size);
            log.log_debug("Message data: %s\n", std::string(msg->data, msg->size).c_str());
            
            // if(disconnMsg) {
            //     if(is_disconnect(msg->data, totalReceived)) {
            //         log.log_debug("Disconnect message received\n");
            //         free(msg);
            //         m_connected = false;
            //         // wait all data sent
            //         syncSend();
            //         shutdown(sockfd, SHUT_RDWR);
            //         closefd(sockfd);
            //         sockfd = -1; // Reset sockfd on failure
            //         if (targetAddr) {
            //             freeaddrinfo(targetAddr);
            //             targetAddr = nullptr;
            //         }
            //         disconnMsg = false;
            //         continue;
            //     }
            //     disconnMsg = false;
            // }



            recvthdLock.lock();
            recvthdQueue.push(msg);
            recvthdLock.unlock();
            msg = nullptr;
            continue;

            
            connerror:
            // if error disconnect
            if(msg) {
                free(msg);
                msg = nullptr;
            }
            // clearCache();
            clearEnv();
            continue;

        }



    });

    keepAliveGuard = std::thread([this]() {
        uint8_t i = 0;
        while(!m_exit) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            if(!m_connected || m_reject) {
                continue;
            }
            

            // if time out over 4 times keepAliveDuration, disconnect
            if(time(nullptr) - lastActiveTime > keepAliveDuration * 4) {
                log.log_debug("Connection timeout, disconnecting...\n");
                disconnect();
                continue;
            }

            ++i;
            if(i >= keepAliveDuration * 2) {
                this->send(aliveStr, sizeof(aliveStr));
                i = 0;
            }
        }
    });

}

CDpfsTcp::~CDpfsTcp() {
    disconnect();
    m_exit = true;


    if (sendGuard.joinable()) {
        sendGuard.join();
    }

    if (recvGuard.joinable()) {
        recvGuard.join();
    }

    if(keepAliveGuard.joinable()) {
        keepAliveGuard.join();
    }

    if (targetAddr) {
        freeaddrinfo(targetAddr);
        targetAddr = nullptr;
    }
    clearCache();
    // if (localAddr) {
    //     freeaddrinfo(localAddr);
    //     localAddr = nullptr;
    // }
        
        
    // Destructor implementation
    g_dtlock.lock();
    if (--dpfstcp_count == 0) {
        if (dpfstcp_guard.joinable()) {
            dpfstcp_guard.join();
        }
    }
    g_dtlock.unlock();
}

bool CDpfsTcp::is_connected() const {
    return (m_connected && (sockfd != -1));
}

int CDpfsTcp::connect(const char* connString) {
    CSpinGuard lock(m_lock);
    if(sockfd != -1) {
        return -EISCONN; // Already connected
    }

    int rc = 0;
    size_t i = 0;
    char addr[256]{0};
    char port[16]{0};
    struct addrinfo hints;


    std::string connectionString = connString;
    size_t ipPos = connectionString.find("ip:");
    size_t portPos = connectionString.find("port:");

    if (ipPos == std::string::npos || portPos == std::string::npos) {
        return -EADDRNOTAVAIL; // Invalid connection string
    }


    for(ipPos += 3; ipPos < connectionString.length() && connectionString[ipPos] != ' '; ++ipPos) {
        if (connectionString[ipPos] == '\\' || connectionString[ipPos] == '/') {
            return -EADDRNOTAVAIL; // Invalid connection string
        }
        addr[i++] = connectionString[ipPos];
        if(i >= sizeof(addr) - 1) {
            return -EADDRNOTAVAIL; // Address too long
        }
    }

    addr[i] = '\0'; // Null-terminate the address string            
    i = 0;
    for(portPos += 5; portPos < connectionString.length() && connectionString[portPos] != ' '; ++portPos) {
        if (connectionString[portPos] < 0x30 || connectionString[portPos] > 0x39) {
            return -EADDRNOTAVAIL; // Invalid connection string
        }
        port[i++] = connectionString[portPos];
        if(i >= sizeof(port) - 1) {
            return -EADDRNOTAVAIL; // Port too long
        }
    }
    port[i] = '\0'; // Null-terminate the port string

    
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0;
    hints.ai_protocol = IPPROTO_TCP;


    sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if(sockfd < 0) {
        return -EIO; // Socket creation failed
    }

    // set up ip:port for targetAddr
    log.log_inf("Connecting to %s:%s\n", addr, port);

    rc = getaddrinfo(addr, port, &hints, &targetAddr);
    if (rc) {
        goto connerror; // Address resolution failed
    }

    rc = ::connect(sockfd, targetAddr->ai_addr, targetAddr->ai_addrlen);
    if (rc) {
        goto connerror; // Connection failed
    }

    lastActiveTime = time(nullptr);

    m_connected = true;
    return 0;
connerror:
    m_connected = false;
    if(sockfd != -1) {
        closefd(sockfd);
        sockfd = -1; // Reset sockfd on failure
    }
    if (targetAddr) {
        freeaddrinfo(targetAddr);
        targetAddr = nullptr;
    }
    return rc; // Connection error
}

// wait all send pending data sent, then disconnect
int CDpfsTcp::disconnect() {

    if(!m_connected || sockfd == -1) {
        return 0; // Not connected
    }
    m_reject = true;

    m_lock.lock();
    // wait all data sent
    syncSend();
    m_connected = false;
    shutdown(sockfd, SHUT_RDWR);

    closefd(sockfd);
    sockfd = -1; // Reset sockfd on failure

    if (targetAddr) {
        freeaddrinfo(targetAddr);
        targetAddr = nullptr;
    }
    m_lock.unlock();

    // clearCache();
    clearEnv();
    m_reject = false;
    return 0;
}

int CDpfsTcp::send(const void* buffer, int size) {
    if(m_reject) {
        return -ENOTCONN;
    }

    if (buffer == nullptr) {
        return -EINVAL;
    }
    
    dpfsmsg* msg = (dpfsmsg*)malloc(sizeof(dpfsmsg) + size);
    if(!msg) {
        return -ENOMEM;
    }

    msg->size = size;
    memcpy(msg->data, buffer, size);

    sendLock.lock();
    sendQueue.push(msg);
    sendLock.unlock();

    // Notify the send thread to process the queue
    sendCv.notify_one();

    
    
    return 0;
}

int CDpfsTcp::recv(void** buffer, int* retsize) {
    // if (retsize == nullptr) {
    //     return -EINVAL;
    // }
    if(buffer == nullptr) {
        return -EINVAL;
    }
    
    if(recvQueue.empty()) {
        while(recvthdQueue.empty() && !m_exit && m_connected) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if(!m_connected && recvthdQueue.empty()) {
            return -ECONNRESET;
        }
        recvthdLock.lock();
        recvQueue.swap(recvthdQueue);
        recvthdLock.unlock();
    }

    dpfsmsg* msg = nullptr;

    recvLock.lock();
    if(recvQueue.empty()) {
        recvLock.unlock();
        return -ENODATA; // No data to retrieve
    }
    msg = recvQueue.front();
    recvQueue.pop();
    recvLock.unlock();

    if(!msg) {
        return -ENOBUFS;
    }

    if(retsize) {
        *retsize = msg->size;
    }
    *buffer = msg->data;
    
    return 0;
}

// void* CDpfsTcp::bufalloc(size_t size) {
//     if(size == 0) {
//         return nullptr;
//     }
//     return (char*)malloc(size + sizeof(dpfsmsg)) + sizeof(dpfsmsg);
// }

void CDpfsTcp::buffree(void* buf) {
    free((char*)buf - sizeof(dpfsmsg));
}

void CDpfsTcp::set_log_path(const char* log_path) {
    if(!log_path) {
        return;
    }
    log.set_log_path(log_path);
}

void CDpfsTcp::set_log_level(int level) {
    log.set_loglevel((logrecord::loglevel)level);
}

void CDpfsTcp::syncSend() const {
    while(!m_exit && !sendQueue.empty() && m_connected) {
        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(100));
    while(!m_exit && !sendthdQueue.empty() && m_connected) {
        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void CDpfsTcp::clearCache() {
    CSpinGuard lock(m_lock);
    
    sendLock.lock();
    while(!sendQueue.empty()) {
        dpfsmsg* msg = sendQueue.front();
        sendQueue.pop();
        if(msg) {
            free(msg);
        }
    }
    sendLock.unlock();

    sendthdLock.lock();
    while(!sendthdQueue.empty()) {
        dpfsmsg* msg = sendthdQueue.front();
        sendthdQueue.pop();
        if(msg) {
            free(msg);
        }
    }
    sendthdLock.unlock();

    recvLock.lock();
    while(!recvQueue.empty()) {
        dpfsmsg* msg = recvQueue.front();
        recvQueue.pop();
        if(msg) {
            free(msg);
        }
    }
    recvLock.unlock();

    recvthdLock.lock();
    while(!recvthdQueue.empty()) {
        dpfsmsg* msg = recvthdQueue.front();
        recvthdQueue.pop();
        if(msg) {
            free(msg);
        }
    }
    recvthdLock.unlock();
}

void CDpfsTcp::clearEnv() {
    CSpinGuard lock(m_lock);
    
    if(sockfd != -1) {
        closefd(sockfd);
        sockfd = -1;
    }
    m_connected = false;
    m_reject = false;
    if (targetAddr) {
        freeaddrinfo(targetAddr);
        targetAddr = nullptr;
    }

}

void* newTcpClient() {
    return new CDpfsTcp();
}
