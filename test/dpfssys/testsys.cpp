#include <dpfssys/dpfssys.hpp>
#include <iostream>
#include <csignal>
#include <string>
#include <dpfssys/dpfsdata.hpp>
#include <parser/dpfsparser.hpp>
#include <dpfssys/user.hpp>

using namespace std;

volatile bool g_exit = false;
void sigfun(int sig) {
    cout << "Signal " << sig << " received, exiting..." << endl;
    g_exit = true;
}
void Analy_Input(int argc, char** argv);
void printValue(const KEY_T& key, const CCollection& clt, CItem* itm);
bool init_system = false;
string config_file = "./example.json";

int main(int argc, char** argv) {
    signal(SIGINT, sigfun);
    signal(SIGKILL, sigfun);

    Analy_Input(argc, argv);


    dpfsSystem* a;
    int rc = 0;
    try {
        a = new dpfsSystem(config_file.c_str());
        cout << a->conf_file << endl;
        cout << a->dataSvrStr << endl;
        cout << a->controlSvrStr << endl;
        cout << a->replicationSvrStr << endl;
        cout << "size: " << a->engine_list[0]->size() << endl;
        a->log.set_loglevel(logrecord::LOG_DEBUG);
        rc = a->start();
        if(rc != 0) {
            cerr << "Failed to start dpfsSystem, rc=" << rc << endl;
            delete a;
            return rc;
        }
        cout << "dpfsSystem started successfully." << endl;
    } catch (const std::runtime_error& e) {
        cerr << "Error initializing dpfsSystem: " << e.what() << endl;
        return -1;
    }

    if(init_system) {
        rc = a->initDataSvc();
        if(rc != 0) {
            cerr << "Failed to initialize data system, rc=" << rc << endl;
            a->stop();
            delete a;
            return rc;
        }
        cout << "data System initialized." << endl;
    } else {
        rc = a->dataService->load();
        if(rc != 0) {
            cerr << "Failed to load data system, rc=" << rc << endl;
            a->stop();
            delete a;
            return rc;
        }
        cout << "data System loaded." << endl;
    }

    char buffer[64];
    KEY_T key(buffer, sizeof(buffer), a->dataService->m_sysSchema->systemboot.m_cmpTyps);


    // system boot item
    // system version:
    cout << "get version: " << endl;
    key.len = sizeof("VERSION");
    memcpy(key.data, "VERSION", key.len);
    // for one row

    auto& sysBoot = a->dataService->m_sysSchema->systemboot;
    cacheLocker bootLocker(sysBoot.m_cltInfoCache, sysBoot.m_page);
    rc = bootLocker.read_lock();
    if (rc != 0) {
        cerr << "Failed to lock system boot cache, rc=" << rc << endl;
    } else {
        cout << "System boot cache locked successfully." << endl;
    }
    CCollection::collectionStruct bootcs(sysBoot.m_cltInfoCache->getPtr(), sysBoot.m_cltInfoCache->getLen());
    CItem itm(bootcs.m_cols);
    bootLocker.read_unlock();

    printValue(key, a->dataService->m_sysSchema->systemboot, &itm);

    cout << " get code set " << endl;
    key.len = sizeof("CODESET");
    memcpy(key.data, "CODESET", key.len);
    printValue(key, a->dataService->m_sysSchema->systemboot, &itm);


    // TODO:: FINISH PARSER FOR COMMAND LINE

    CUser usr;
    usr.userid = 0;
    usr.username = "SYSTEM";
    usr.dbprivilege = dbPrivilege::DBPRIVILEGE_FATAL;

    CParser parser(usr, *a->dataService);

    //TODO :: FINISH INSERT AND SELECT
    
    while(1) {
        cout << "input sql : " << endl;
        string sql;
        getline(cin, sql);
        if(sql == "exit" || sql == "quit") {
            break;
        }

        parser(sql);
        CPlanHandle handle(a->dataService->m_page, a->dataService->m_diskMan);
        rc = parser.buildPlan(sql, handle);
        if (rc != 0) {
            cout << "message " << parser.message << endl;
            cerr << "Failed to build execution plan, rc=" << rc << endl;
        } else {
            cout << "Execution plan built successfully." << endl;
        }

        /*
            create table qwer.asdf(a int not null primary key, b char(20), c double)
            insert into qwer.asdf(a,b,c) values(-1, 'hello', -3.15) , (156, '123654', 315222.15898099997)

            Create table test1.test(a int not null primary key, b int)
        */
    }




    // a->stop();
    delete a;
    return 0;
}




void Args_Error()
{
    cout << "\tThe args begin with:\n" <<
        "\t--init       [init the system]\n" << 
        "\t-f           [configuration file path]\n" << endl;

        // "\t--cluster    [remote queue manager name]\n" <<
        // "\t--local      [local IPaddress]\n" <<
        // "\t--connect    [remote IPaddress(port)] use format \\(port\\) to fill in port\n" <<
        // "\t--user       [remote user name]\n" <<
        // "\t--password   [remote user password]\n" <<
        // "\t--channel    [remote channel name]\n" << 
        // "\t--path       [message save path]\n" << endl;
}

void Analy_Input(int argc, char** argv) {
    vector<string> Input;
    if (argc <= 1) {
        cout << "missing args,exit" << endl;
        Args_Error();
        exit(99);
    }

    // if (argc % 2 == 0) {
    //     cout << "missing args,exit" << endl;
    //     Args_Error();
    //     exit(99);
    // }

    for (int i = 0; i < argc; i++)
    {
        Input.emplace_back(argv[i]);
    }

    for (int i = 1; i < argc;)
    {
        //cout << i << endl;
        //cout << Input[i] << endl;
        // if (Input[i][0] == '-') {
        //     cout << "args error,exit" << endl;
        //     Args_Error();
        //     exit(99);
        // } 
        
        
        if (Input[i] == "--init") {
            init_system = true;
            ++i;
        } else if (Input[i] == "-f") {
            config_file = Input[i + 1];
            i += 2;
        } else {
            cout << "missing args,exit" << endl;
            Args_Error();
            exit(99);
        }
    }
}


void printValue(const KEY_T& key, const CCollection& clt, CItem* itm) {
    int rc = 0;
    // for one row
    rc = clt.getRow(key, itm);
    if(rc != 0) {
        cerr << "Failed to get system boot item." << endl;
    } else {
        for(auto it = itm->begin(); it != itm->end(); ++it) {
            for(uint32_t i = 0; i < itm->cols.size(); ++i) {
                CValue val = it[i];
                cout << "System Boot Item Value for column " << itm->cols[i].getName() << endl;
                printMemory(val.data, val.len);
                cout << endl;
            }
        }
    }
}