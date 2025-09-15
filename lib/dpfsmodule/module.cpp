#include <iostream>
#include <dlfcn.h>
#include <unordered_map>
#include <dpfsmodule/module.hpp>
#include <vector>
#include <cstddef>
#include <threadlock.hpp>
/*

dpfsModules:
    engine:
        {{nvmf, newNvmf}}
    net_client:
        {{tcp, newClient}}
    net_server:
        {{tcp, newServer}}
*/

extern std::unordered_map<std::string, void* (*)()> engineTypes;
extern std::unordered_map<std::string, void* (*)()> clientTypes;
extern std::unordered_map<std::string, void* (*)()> serverTypes;

struct ModuleInfo {
    ModuleInfo() = delete;
    std::string name;
    void* handle;
    void* (*Func)();
    std::unordered_map<std::string, void* (*)()>* moduleMap;

    ModuleInfo(const std::string& modName, void* hnd, void* (*func)(), std::unordered_map<std::string, void* (*)()>* modMap)
        : name(modName), handle(hnd), Func(func), moduleMap(modMap) {}

    ModuleInfo(ModuleInfo&& tgt) noexcept {
        name = std::move(tgt.name);
        handle = tgt.handle;
        Func = tgt.Func;
        moduleMap = tgt.moduleMap;

        tgt.name.clear();
        tgt.handle = nullptr;
        tgt.Func = nullptr;
        tgt.moduleMap = nullptr;
    }

    ~ModuleInfo() {
        if(handle) {
            dlclose(handle);
            handle = nullptr;
        }

        if(moduleMap) {
            if(moduleMap->find(name) != moduleMap->end()) {
                moduleMap->erase(name);
            }
        }
        Func = nullptr;
    }
};

static std::mutex moduleMutex;
static std::unordered_map<std::string, ModuleInfo> loadedModules;
static std::unordered_map<std::string, std::unordered_map<std::string, void* (*)()>*> dpfsModules;

enum class ModuleType : uint32_t {
    ENGINE,
    NET_CLIENT,
    NET_SERVER
};

const std::vector<std::string> moduleTypeStr = {"engine", "net_client", "net_server"};

CModuleMan::CModuleMan() {
    dpfsModules.clear();
    dpfsModules[moduleTypeStr[(uint32_t)ModuleType::ENGINE]] = &engineTypes;
    dpfsModules[moduleTypeStr[(uint32_t)ModuleType::NET_CLIENT]] = &clientTypes;
    dpfsModules[moduleTypeStr[(uint32_t)ModuleType::NET_SERVER]] = &serverTypes;
    loadedModules.clear();
}

CModuleMan::~CModuleMan() {
    loadedModules.clear();
    dpfsModules.clear();
}


int CModuleMan::load_module(const char* modType, const char* modName, const char* funcName, const char* path) {
    if(modType == nullptr || path == nullptr || funcName == nullptr) {
        std::cerr << "Invalid parameters to load_module" << std::endl;
        return -EINVAL;
    }
    std::string typeStr(modType);

    CMutexGuard guard(moduleMutex);

    std::unordered_map<std::string, std::unordered_map<std::string, void* (*)()>*>::iterator type = dpfsModules.find(typeStr);
    if(type == dpfsModules.end()) {
        std::cerr << "Unsupported module type: " << modType << std::endl;
        return -EINVAL;
    }

    void* handle = dlopen(path, RTLD_NOW | RTLD_GLOBAL);
    if(!handle) {
        std::cerr << "Cannot open library: " << dlerror() << std::endl;
        return -EINVAL;
    }

    // Clear any existing error
    dlerror();

    // std::string symbolName = funcName;
    void* (*func)() = (void* (*)())dlsym(handle, funcName);
    const char* dlsym_error = dlerror();
    if (dlsym_error) {
        std::cerr << "Cannot load symbol '" << funcName << "': " << dlsym_error << std::endl;
        dlclose(handle);
        return -EINVAL;
    }

    // loadedModules[modName] = ModuleInfo(modName, handle, func, type->second);
    loadedModules.insert({modName, ModuleInfo(modName, handle, func, type->second)});
    (*type->second)[modName] = func;

    return 0;
}

int CModuleMan::unload_module(const char* modType, const char* modName) {
    if(modType == nullptr || modName == nullptr) {
        std::cerr << "Invalid parameters to unload_module" << std::endl;
        return -EINVAL;
    }
    std::string typeStr(modType);

    CMutexGuard guard(moduleMutex);

    std::unordered_map<std::string, ModuleInfo>::iterator mod = loadedModules.find(modName);
    if(mod == loadedModules.end()) {
        std::cerr << "Module not loaded: " << modName << std::endl;
        return -EINVAL;
    }

    loadedModules.erase(mod);
    return 0;
}


