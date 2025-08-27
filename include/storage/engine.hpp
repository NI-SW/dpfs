/*  DPFS-License-Identifier: Apache-2.0 license
 *  Copyright (C) 2025 LBR.
 *  All rights reserved.
 */
#pragma once
#include <string>

constexpr size_t dpfs_lba_size = 4096; 

class dpfsEngine {
public:
 	dpfsEngine() = default;
 	virtual ~dpfsEngine() = default;
    
    /*
        @param devdesc_str: device description string, format is user defined
        @return 0 on success, else on failure
    */
 	virtual int attach_device(const std::string& devdesc_str) = 0;
    
    /*
        @param devdesc_str: device description string, format is user defined
        @return 0 on success, else on failure
    */
 	virtual int detach_device(const std::string& devdesc_str) = 0;
    
    // @note: try to detach all devices, and free all resources
    virtual void cleanup() = 0;

    // @param log_path: log path to write log, if not set, will use default path "./logbinary.log"
    virtual void set_logdir(const std::string& log_path) = 0;

    // @param async: set asynchronous mode, if true, will use asynchronous I/O, else will use synchronous I/O
    virtual void set_async_mode(bool async) = 0;

    // @return true if asynchronous mode is set, else false
    virtual bool async() const = 0;
    
    /*
        @param lbaPos: logical block address position
        @param pBuf: pointer to the buffer to read data into
        @param lbc: logic block count, number of blocks to read
        @return number of submitted requests on success, else on failure
    */
    virtual int read(size_t lbaPos, void* pBuf, size_t lbc) = 0;
    
    /*
        @param lbaPos: logical block address position
        @param pBuf: pointer to the buffer to read data into
        @param lbc: logic block count, number of blocks to write
        @return number of submitted requests on success, else on failure
    */
    virtual int write(size_t lbaPos, void* pBuf, size_t lbc) = 0;
    
    // flush all data to the device, ensure all data is written to the device
    virtual int flush() = 0;
    
    /*
       @note sync n async submit, if not specified, sync all asynchronous operations, before use this func you must set_async_mode(true)
       @param n: number of asynchronous operations to sync, if 0, sync all
    */
    virtual int sync(size_t n = 0) = 0;
 
	/*
        @param trid_str: old device trid string
        @param new_trid_str: new device trid string to replace
        @return 0 on success, else on failure
        @attention the size of new device must be equal or lager than old device, redundant space in new device will not be used.
        the new device will be initialized, data will be copied from old device to new device, 
        after replacement, the old device will be removed from the nvmfhost list.
        @note replace a device with new one
	*/
	virtual int replace_device(const std::string& trid_str, const std::string& new_trid_str) = delete;

    /*
        @param tgt: target engine to copy from
        @return 0 on success, else on failure
        @note copy the target device to this engine, the target engine must be of the same type as this engine. will create new device handlers to same device
    */
    virtual int copy(const dpfsEngine& tgt) = 0;

    /*
        @return total number of blocks in all devices attached to this engine
        @note this function is used to get the total number of blocks in all devices attached to this engine, 
        it will return the sum of all devices block count, if no device is attached, return 0.
    */
    virtual size_t size() const = 0;

    /*
        @param size: size of memory to allocate
        @return pointer to the allocated memory, nullptr on failure
    */
    virtual void* zmalloc(size_t size) const = 0;

    /*
        @param ptr: pointer to the memory to free
    */
    virtual void zfree(void* ptr) const = 0;

    /*
        @return name of the engine
    */
    virtual const char* name() const = 0;

};

dpfsEngine* newEngine(std::string engine_type);