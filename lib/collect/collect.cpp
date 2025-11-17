#include <collect/collect.hpp>

uint64_t nodeId = 0;


/*
    @param pos: position of the item in the item list
    @return CValue pointer on success, else nullptr
*/
CValue CItem::getValue(size_t pos) const noexcept {
    CValue val;
    if(pos < 0 || pos >= cols.size()) {
        return val;
    }

    size_t offSet = getDataOffset(pos);

    if(cols[pos]->type == dpfs_datatype_t::TYPE_VARCHAR) {
        // first 4 bytes is actual length
        val.len = *(uint32_t*)((char*)rowPtr + offSet);
        val.data = (char*)rowPtr + offSet + sizeof(uint32_t);
    } else {
        val.len = cols[pos]->len;
        val.data = (char*)rowPtr + offSet;
    }

    return val;
}

// int CItem::resetOffset(size_t begPos) noexcept {
//     for(size_t i = begPos + 1; i < cols.size(); ++i) {
//         cols[i]->offSet = cols[i - 1]->offSet + cols[i - 1]->len;
//         // isVariableType(cols[i - 1]->type) ? cols[i]->offSet += 4 : 0;
//     }
//     return 0;
// }

size_t CItem::getDataOffset(size_t pos) const noexcept {
    size_t offSet = 0;
    for(size_t i = 0; i < pos; ++i) {
        if(isVariableType(cols[i]->type)) {
            offSet += sizeof(uint32_t) + (*(uint32_t*)(rowPtr + offSet));
        } else {
            offSet += cols[i]->len;
        }
    }
    return offSet;
}

int CItem::dataCopy(size_t pos, const CValue* value) noexcept {
    size_t offSet = getDataOffset(pos);

    if(isVariableType(cols[pos]->type)) {
        // first 4 bytes is actual length
        uint32_t actualLen = value->len;
        if(actualLen > cols[pos]->len) {
            return -E2BIG;
        }
        // use col offset of row to find the pointer and copy data
        *(uint32_t*)((char*)rowPtr + offSet) = actualLen;
        memcpy(((char*)rowPtr + offSet + sizeof(uint32_t)), value->data, actualLen);
    } else {

        memcpy(((char*)rowPtr + offSet), value->data, value->len);
        if(value->len < cols[pos]->len) {
            memset(((char*)rowPtr + offSet + value->len), 0, cols[pos]->len - value->len);
        }
    }

    return value->len;
}

/*
    @param col: column of the item, maybe column name
    @return CValue pointer on success, else nullptr
    @note this function will search the item list for the column, and possible low performance
*/
CValue CItem::getValueByKey(CColumn* col) const noexcept {
    CValue val;

    for(size_t i = 0; i < cols.size(); ++i) {
        if (*cols[i] == *col) {
            size_t offSet = getDataOffset(i);
            if(cols[i]->type == dpfs_datatype_t::TYPE_VARCHAR) {
                // first 4 bytes is actual length
                val.len = *(uint32_t*)((char*)rowPtr + offSet);
                val.data = (char*)rowPtr + offSet + 4;
            } else {
                val.len = cols[i]->len;
                val.data = (char*)rowPtr + offSet;
            }
            break;
            // return (CValue*)((char*)data + offSet + (sizeof(CValue) * i));
        }
    }
    return val;
}

/*
    @param pos: position of the item in the item list
    @param value: CValue pointer to update
    @return bytes updated on success, else on failure
*/
int CItem::updateValue(size_t pos, CValue* value) noexcept {
    if(pos < 0 || pos >= cols.size()) {
        return -EINVAL;
    }
    if(cols[pos]->getLen() < value->len) {
        return -E2BIG;
    }
    return dataCopy(pos, value);
}

/*
    @param col: col of the item, maybe column name
    @param value: CValue pointer to update
    @return CValue pointer on success, else nullptr
*/
int CItem::updateValueByKey(CColumn* col, CValue* value) noexcept {
    for(size_t i = 0; i < cols.size(); ++i) {
        if (*cols[i] == *col) {
            return dataCopy(i, value);
        }
    }
    // values not found
    return -ENXIO;
}

/*
    @param writeBack write to storage immediate if true
    @return 0 on success, else on failure
    @note commit the item changes to the storage.
    need to do : 
    write commit log, and then write data, finally update the index
*/
int CItem::commit(bool writeBack) { 
    // TODO

    return 0;
};

/*
    @param cs: column info
    @return CItem pointer on success, else nullptr
    @note this function will create a new CItem with the given column info
*/
CItem* CItem::newItem(const std::vector<CColumn*>& cs) noexcept {
    size_t len = 0;
    for(size_t i = 0; i < cs.size(); ++i) {
        len += cs[i]->getLen();
        isVariableType(cs[i]->type) ? len += 4 : 0;
    }

    // row data in data[]
    CItem* item = (CItem*)malloc(sizeof(CItem) + len);
    if(!item) {
        return nullptr;
    }
    new (item) CItem(cs);
    item->rowLen = len;
    item->maxRowNumber = 1;
    item->rowNumber = 0;
    item->rowPtr = item->data;
    return item;
}

/*
    @param cs: column info
    @return CItem pointer on success, else nullptr
    @note this function will create a new CItem with the given column info
*/
CItem* CItem::newItems(const std::vector<CColumn*>& cs, size_t maxRowNumber) noexcept {
    size_t len = 0;
    for(size_t i = 0; i < cs.size(); ++i) {
        len += cs[i]->getLen();
        isVariableType(cs[i]->type) ? len += 4 : 0;
    }

    // row data in data[]
    CItem* item = (CItem*)malloc(sizeof(CItem) + len * maxRowNumber);
    if(!item) {
        return nullptr;
    }
    new (item) CItem(cs);
    item->rowLen = len;
    item->maxRowNumber = maxRowNumber;
    item->rowNumber = 0;
    item->rowPtr = item->data;
    return item;
}

void CItem::delItem(CItem*& item) noexcept {
    if(item) {
        free(item);
    }
    item = nullptr;
}

// private:

/*
    @param cs column info
    @param value of row data
*/
CItem::CItem(const std::vector<CColumn*>& cs) : cols(cs), beginIter(this), endIter(this) {
    locked = false;
    error = false;
    beginIter.m_pos = 0;
    beginIter.m_ptr = data;
    endIter.m_pos = 0;
    endIter.m_ptr = nullptr;
}

int CItem::nextRow() noexcept {
    if(rowNumber + 1 >= maxRowNumber) {
        return -ERANGE;
    }
    ++rowNumber;
    endIter.m_pos = rowNumber + 1;

    for(auto& pos : cols) {
        if(isVariableType(pos->type)) {
            // first 4 bytes is actual length
            uint32_t vallEN = *(uint32_t*)((char*)rowPtr);
            rowPtr += sizeof(uint32_t) + vallEN;
        } else {
            rowPtr += pos->getLen();
        }
    }

    return 0;
}

int CItem::resetScan() noexcept {
    rowNumber = 0;
    rowPtr = data;
    return 0;
}

CItem::~CItem() {

}


int CCollection::addItem(const CItem& item) {

    // save item to storage and update index
    // search where the item should be in storage, use b plus tree to storage the data
    
    // bpt.insert(key, value);

    if(m_btreeIndex) {
        // use b plus tree to index the item
    }

    // sequential storage
    
    

    return 0;
}