#include <iostream>
#include <string>
#include <fstream>
#include <queue>
#include <pthread.h>
#include <thread>
#include <unistd.h>
#include <cstring>
using namespace std;

class CSpinlock {
public:
    CSpinlock() {
        pthread_spin_init(&m_lock, PTHREAD_PROCESS_PRIVATE);
    }

    ~CSpinlock() {
        pthread_spin_destroy(&m_lock);
    }

    void lock() {
        pthread_spin_lock(&m_lock);
    }

    void unlock() {
        pthread_spin_unlock(&m_lock);
    }

private:
    pthread_spinlock_t m_lock;
};
CSpinlock lock;
CSpinlock tlock;
queue<string*> messageque;
queue<thread*> threadque;

bool exit_flag = false;

int cmdline(const string& str, int timeout, int unique_id) {

    string fname = "./cmdout." + to_string(unique_id);

    // nohup timeout  3 telnet 182.92.20.18 443 > qwer 2>&1 &
    string cmd = "nohup timeout " + to_string(timeout) + " " + str + " > "+ fname + " 2>&1 &";
    //cout << "Running command: " << cmd << endl;
    if(system(cmd.c_str()) != 0) {
        // lock.lock();
        // messageque.push(new string("Failed to run cmd. id = " + to_string(unique_id)));
        // lock.unlock();
    }

    cout << "Command executed, waiting for output... id: " << unique_id << endl;
    
    fstream file;
    sleep(timeout);
    

    file.open(fname, ios::in);

    while(!file.is_open()) {
        // cout << "File not found, retrying..." << endl;
        sleep(1);
        file.open(fname, ios::in);
    }



    string* output = new string("port : " + to_string(unique_id) + "\n");
    char buffer[1024];
    while(file.peek() != EOF) {
        file.read(buffer, 1024);
        *output += buffer;
        memset(buffer, 0, 1024);
    }


    file.close();

    
    lock.lock();
    messageque.push(output);
    lock.unlock();
    
    system(("rm -f " + fname).c_str());


    return 0;
}



int main() {
    // string ip = "182.92.20.18";
    // string ip = "192.168.34.12";
    string ip = "108.160.170.41";
    thread t1([]() {
        while (!exit_flag || !messageque.empty()) {
            if (!messageque.empty()) {
                string* message = messageque.front();
                lock.lock();
                messageque.pop();
                lock.unlock();
                
                if(message->find("Connect") != string::npos) {
                    cout << "Message: " << *message << endl;

                    fstream file;
                    file.open("cmdout", ios::out | ios::app);
                    while(!file.is_open()) {
                        // cerr << "Failed to open cmdout file." << endl;
                        sleep(1);
                        file.open("cmdout", ios::out | ios::app);
                    }

                    file.write(message->c_str(), message->size());
                    file.flush();
                    file.close();
                    
                }
                delete message;
                continue;
            }
            this_thread::sleep_for(chrono::milliseconds(100));
        }
        // sleep(10);
    });

    thread t2([](){
        while (!exit_flag || !threadque.empty()) {
            if (!threadque.empty()) {
                thread* th = threadque.front();
                tlock.lock();
                threadque.pop();
                tlock.unlock();
                
                if(th->joinable()) {
                    th->join();
                }
                delete th;
                continue;
            }
            this_thread::sleep_for(chrono::milliseconds(100));
        }
    });

    for(int i = 1; i < 65536; ++i) {
        string cmd = "telnet " + ip + " " + to_string(i);
        tlock.lock();
        threadque.push(new thread([cmd, i](){
            cmdline(cmd, 1, i);
        }));
        tlock.unlock();

        if(i % 100 == 0) {
            usleep(1000 * 100);
        }

    }
    
    
    while(!threadque.empty()) {
        this_thread::sleep_for(chrono::milliseconds(100));
    }
    exit_flag = true;
    t1.join();
    t2.join();
    return 0;
}