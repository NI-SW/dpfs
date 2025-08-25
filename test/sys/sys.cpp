#include <dpfssys/dpfssys.hpp>
#include <iostream>
using namespace std;


int main() {
    dpfsSystem* a;
    try {
        a = new dpfsSystem("./example.json");
        cout << a->conf_file << endl;
        cout << (short)a->dataport << endl;
        cout << (short)a->controlport << endl;
        cout << (short)a->replicationport << endl;
        cout << "size: " << a->engine_list[0]->size() << endl;
    } catch (const std::runtime_error& e) {
        cerr << "Error initializing dpfsSystem: " << e.what() << endl;
        return -1;
    }

    

    delete a;
    return 0;
}