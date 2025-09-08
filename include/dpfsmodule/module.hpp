#pragma once

class CModuleMan {
public:
    CModuleMan();
    virtual ~CModuleMan();

    /*
        @param modType: Type of the module, e.g., "net_client", "net_server", "engine".
        @param modName: Name of the module to insert, e.g.
        @param funcName: Name of the function to create an instance, e.g., "
        @param path: Path to the shared library.
        @return 0 on success, else on failure.
    */
    virtual int load_module(const char* modType, const char* modName, const char* funcName, const char* path);

    /*
        @param modType: Type of the module, e.g., "net_client", "net_server", "engine".
        @param modName: Name of the module to unload, e.g.
        @return 0 on success, else on failure.
    */
    virtual int unload_module(const char* modType, const char* modName);

};
