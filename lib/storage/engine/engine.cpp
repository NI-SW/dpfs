#include <storage/engine.hpp>
#include <unordered_map>


// defination in nvmf.cpp (-ldpfs_nvmf)
void* newNvmf();

std::unordered_map<std::string, void* (*)()> engineTyps {{"nvmf", newNvmf}};


dpfsEngine* newEngine(std::string& engine_type) {

    if(engineTyps.find(engine_type) != engineTyps.end()) {
        return reinterpret_cast<dpfsEngine*>(engineTyps[engine_type]());
    } else {
        return nullptr; // or throw an exception
    }
    return nullptr;
    // if(engine_type == "nvmf") {
    //     return static_cast<dpfsEngine*>(newNvmf());
    // } else {
    //     return nullptr; // or throw an exception
    // }
}
