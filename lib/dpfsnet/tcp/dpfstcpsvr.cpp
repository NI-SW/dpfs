#include <dpfsnet/tcp/dpfstcpsvr.hpp>
#include <string>
#include <cstring>
#include <threadlock.hpp>
#include <thread>
#include <map>
#ifdef _WIN64
#include <winsock2.h>
#include <ws2tcpip.h>
static int closefd(int fd) {
    return closesocket(fd);
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
#endif

static size_t dpfstcpsvr_count = 0;
static CSpin g_tcpsvrlock;
static std::thread dpfstcpsvr_guard;

void dpfstcpsvr_guard_thread() {

    initWinsock();
    while (dpfstcpsvr_count) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    destroyWinsock();
}


CDpfstcpsvr::CDpfstcpsvr() {
    // Constructor implementation
    g_tcpsvrlock.lock();
    if (dpfstcpsvr_count == 0) {
        dpfstcpsvr_guard = std::thread(dpfstcpsvr_guard_thread);
    }
    ++dpfstcpsvr_count;
    g_tcpsvrlock.unlock();
    log.set_log_path("./dpfstcpsvr.log");

}

CDpfstcpsvr::~CDpfstcpsvr() {
    // Destructor implementation
    stop();
    g_tcpsvrlock.lock();
    if (--dpfstcpsvr_count == 0) {
        if(dpfstcpsvr_guard.joinable()) {
            dpfstcpsvr_guard.join();
        }
    }
    g_tcpsvrlock.unlock();
}

int CDpfstcpsvr::listen(const char* server_string, listenCallback cb, void* cb_arg) {
    CMutexGuard lock(m_lock);
    m_exit = false;
    if (sockfd != -1) {
        return -EISCONN; // Already listening
    }
    int rc = 0;
    struct addrinfo hints;
    std::string connectionString = server_string;
    size_t ipPos = connectionString.find("ip:");
    size_t portPos = connectionString.find("port:");
    char addr[256]{0};
    char port[16]{0};
    size_t i = 0;

   

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;



    if (ipPos == std::string::npos || portPos == std::string::npos) {
        log.log_error("Invalid connection string: %s\n", server_string);
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


    sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if(sockfd < 0) {
        log.log_error("Socket creation failed\n");
        return -EIO; // Socket creation failed
    }

    // set up ip:port for targetAddr
    rc = getaddrinfo(addr, port, &hints, &localAddr);
    if (rc) {
        goto connerror; // Address resolution failed
    }

    rc = bind(sockfd, localAddr->ai_addr, localAddr->ai_addrlen);
    if (rc) {
        goto connerror; // Binding failed
    }

    rc = ::listen(sockfd, listenQueue);
    if (rc) {
        goto connerror; // Listen failed
    }

    destroyGuard = std::thread([this](){
        std::queue<dpfsconn*> tmpQue;

        while (!m_exit || !destroyQueue.empty() || !clients.empty()) {
            if (!destroyQueue.empty()) {
                destroyLock.lock();
                tmpQue.swap(destroyQueue);
                destroyLock.unlock();
            }

            while(!tmpQue.empty()) {
                dpfsconn* dcon = tmpQue.front();
                tmpQue.pop();

                // destroy the client connection
                if(dcon->thd.joinable()) {
                    dcon->thd.join();
                }
                delete dcon;

            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    });

    // start the listening thread
    listenGuard = std::thread([this, cb, cb_arg]() {
        while (!m_exit) {
            struct sockaddr_in clientAddr;
            socklen_t clientAddrLen = sizeof(clientAddr);
            int clifd = accept(sockfd, (struct sockaddr*)&clientAddr, &clientAddrLen);
            if (clifd < 0) {
                continue; // Accept failed, continue to next iteration
            }

            // Create a new CDpfsTcp instance for the accepted connection
            dpfsconn* dcon = new dpfsconn;
            dcon->cli.sockfd = clifd;
            
            // add to active list
            clientLock.lock();
            clients.emplace_front(dcon);
            dcon->it = clients.begin();
            clientLock.unlock();
            
            // Start a new thread for the client connection
            dcon->thd = std::thread([this, clifd, dcon, cb, cb_arg](){
                dcon->cli.m_connected = true;
                if (cb) {
                    cb(dcon->cli, cb_arg);
                }
                // job is done, put this connection to destroy queue
                // Remove the client from the list
                clientLock.lock();
                clients.erase(dcon->it); 
                clientLock.unlock();

                destroyLock.lock();
                destroyQueue.push(dcon);
                destroyLock.unlock();
                
            });
        }
    });
    
    return 0;

connerror:
    if(sockfd != -1) {
        closefd(sockfd);
        sockfd = -1; // Reset sockfd on failure
    }
    if (localAddr) {
        freeaddrinfo(localAddr);
        localAddr = nullptr;
    }
    return rc; // Connection error
}

int CDpfstcpsvr::stop() {
    CMutexGuard lock(m_lock);
    int rc = 0;
    // stop listen
    rc = shutdown(sockfd, SHUT_RDWR);
    if(rc) {
        log.log_error("Shutdown socket failed, rc=%d\n", rc);
        return -EIO;
    }
    m_exit = true;
    if(listenGuard.joinable()) {
        listenGuard.join(); // Wait for the listen thread to finish
    }
    
    // force all client disconnect
    clientLock.lock();
    for(auto& cli : clients) {
        cli->cli.disconnect();
    }
    clientLock.unlock();
    
    // while(!clients.empty());
    // // clean up destroy queue
    // destroyLock.lock();
    // while (!destroyQueue.empty()) {
    //     dpfsconn* dcon = destroyQueue.front();
    //     destroyQueue.pop();
    //     dcon->cli.disconnect();
    //     if (dcon->thd.joinable()) {
    //         dcon->thd.join(); // Wait for the client thread to finish
    //     }
    //     delete dcon;
    // }
    // destroyLock.unlock();

    // stop destroy thread
    if (destroyGuard.joinable()) {
        destroyGuard.join(); // Wait for the destroy thread to finish
    }

    if (sockfd != -1) {
        closefd(sockfd);
        sockfd = -1;
    }
    if (localAddr) {
        freeaddrinfo(localAddr);
        localAddr = nullptr;
    }

    return 0;
}

void CDpfstcpsvr::set_log_path(const char* log_path) {
    if(!log_path) {
        return;
    }
    log.set_log_path(log_path);
}

void CDpfstcpsvr::set_log_level(int level) {
    log.set_loglevel((logrecord::loglevel)level);
}

void* newTcpServer() {
    return new CDpfstcpsvr();
}