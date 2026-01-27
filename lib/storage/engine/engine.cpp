#include <storage/engine.hpp>
#include <unordered_map>


// defination in nvmf.cpp (-ldpfs_nvmf)
void* newNvmf();

std::unordered_map<std::string, void* (*)()> engineTypes {{"nvmf", newNvmf}};


dpfsEngine* newEngine(const std::string& engine_type) {

    if(engineTypes.find(engine_type) != engineTypes.end()) {
        return reinterpret_cast<dpfsEngine*>(engineTypes[engine_type]());
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
