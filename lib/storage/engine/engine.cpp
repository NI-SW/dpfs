#include <storage/engine.hpp>

// defination in nvmf.cpp (-ldpfs_nvmf)
void* newNvmf();

dpfsEngine* newEngine(std::string engine_type) {
    if(engine_type == "nvmf") {
        return reinterpret_cast<dpfsEngine*>(newNvmf());
    } else {
        return nullptr; // or throw an exception
    }
}
