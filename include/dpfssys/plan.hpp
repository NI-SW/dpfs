
#pragma once
#include <collect/collect.hpp>
#include <tiparser/tiparser_enum.hpp>


struct CPlanObjects {

    CPlanObjects() = default;
    ~CPlanObjects() = default;

    CPlanObjects(const CPlanObjects& p) {
        m_flags.flagByte = p.m_flags.flagByte;
        // indexName = p.indexName;
        indexPos = p.indexPos;
        collectionBidx = p.collectionBidx;
        keyColSequences = p.keyColSequences;
        colSequences = p.colSequences;
    }

    CPlanObjects(CPlanObjects&& p) {
        collectionBidx = p.collectionBidx;
        keyColSequences = std::move(p.keyColSequences);
        colSequences = std::move(p.colSequences);
        m_flags.flagByte = p.m_flags.flagByte;
        // indexName = std::move(p.indexName);
        indexPos = p.indexPos;
    }

    CPlanObjects& operator=(const CPlanObjects& p) {
        if (this == &p) {
            return *this;
        }
        collectionBidx = p.collectionBidx;
        keyColSequences = p.keyColSequences;
        colSequences = p.colSequences;
        m_flags.flagByte = p.m_flags.flagByte;
        // indexName = p.indexName;
        indexPos = p.indexPos;
        return *this;
    }

    CPlanObjects& operator=(CPlanObjects&& p) {
        if (this == &p) {
            return *this;
        }
        collectionBidx = p.collectionBidx;
        keyColSequences = std::move(p.keyColSequences);
        colSequences = std::move(p.colSequences);
        m_flags.flagByte = p.m_flags.flagByte;
        // indexName = std::move(p.indexName);
        indexPos = p.indexPos;
        return *this;
    }


    // the bidx of collection that to be operated.
    bidx collectionBidx {0, 0};
    std::vector<int> keyColSequences;
    std::vector<int> colSequences;
    union flags {
        struct {
            bool usepk : 1;
            bool useIndex : 1;
            bool reserved : 6;
        };
        uint8_t flagByte = 0;
    } m_flags;
    // -1 means not use index, otherwise is the index position in the collection struct
    int indexPos = -1;
    // std::string indexName;


};

class CPlan {
public:
    CPlan() = default;
    ~CPlan() = default;
    CPlan(const CPlan& p) {
        originalSQL = p.originalSQL;
        planObjects = p.planObjects;
    }
    CPlan(CPlan&& p) {
        originalSQL = std::move(p.originalSQL);
        planObjects = std::move(p.planObjects);
    }

    CPlan& operator=(const CPlan& p) {
        if (this == &p) {
            return *this;
        }
        originalSQL = p.originalSQL;
        planObjects = p.planObjects;
        return *this;
    }

    CPlan& operator=(CPlan&& p) {
        if (this == &p) {
            return *this;
        }
        originalSQL = std::move(p.originalSQL);
        planObjects = std::move(p.planObjects);
        return *this;
    }

    std::string originalSQL;
    std::vector<CPlanObjects> planObjects;
};



class CPlanHandle {
public:
    CPlanHandle(CPage& pge, CDiskMan& dm) : 
    m_page(pge), 
    m_diskMan(dm),
    tmpTable(m_diskMan, m_page) {

    }
    CPlanHandle(const CPlanHandle& h) = delete;

    CPlanHandle(CPlanHandle&& h) :
    m_page(h.m_page),
    m_diskMan(h.m_diskMan),
    tmpTable(m_diskMan, m_page) {
        plan = std::move(h.plan);
        tmpTable = std::move(h.tmpTable);
        type = h.type;
    }

    ~CPlanHandle() {
        
    }


    CPlanHandle& operator=(const CPlanHandle& h) = delete;

    CPlanHandle& operator=(CPlanHandle&& h) {
        if (this == &h) {
            return *this;
        }
        plan = std::move(h.plan);
        tmpTable = std::move(h.tmpTable);
        type = h.type;
        return *this;
    }

    CPage& m_page;
    CDiskMan& m_diskMan;
    CPlan plan;
    planType type = planType::PLAN_NULL;
    // temp table for select
    CCollection tmpTable; 
};