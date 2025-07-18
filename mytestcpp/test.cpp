#include <iostream>
#include <memory>
using namespace std;

int main() {
    shared_ptr<int> p = make_shared<int>(10);
    cout << "Value: " << *p << endl;
    cout << "Use count: " << p.use_count() << endl;
    cout << "Unique: " << (p.unique() ? "Yes" : "No") << endl;
    cout << "Pointer: " << p.get() << endl;
    cout << "Address: " << p << endl;
    cout << "Hello, World!" << endl;
    cout << "ptr p :" << &p << endl;
    printf("ptr by C: %p\n", p);


    shared_ptr<int> q = make_shared<int>(20);
    q.swap(p);

    cout << "After swap:" << endl;
    cout << "Value of p: " << *p << endl;
    cout << "Value of q: " << *q << endl;
    return 0;
}