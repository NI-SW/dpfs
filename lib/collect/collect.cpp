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

    if(cols[pos]->type == dpfs_datatype_t::TYPE_VARCHAR) {
        // first 4 bytes is actual length
        val.len = *(uint32_t*)((char*)data + cols[pos]->offSet);
        val.data = (char*)data + cols[pos]->offSet + 4;
    } else {
        val.len = cols[pos]->len;
        val.data = (char*)data + cols[pos]->offSet;
    }

    return val;
}

int CItem::resetOffset(size_t begPos) noexcept {
    for(size_t i = begPos + 1; i < cols.size(); ++i) {
        cols[i]->offSet = cols[i - 1]->offSet + cols[i - 1]->len;
        isVariableType(cols[i]->type) ? cols[i]->offSet += 4 : 0;
    }
    return 0;
}

int CItem::dataCopy(size_t pos, const CValue* value) noexcept {
    if(cols[pos]->type == dpfs_datatype_t::TYPE_VARCHAR) {
        // first 4 bytes is actual length
        uint32_t actualLen = value->len;
        if(actualLen > cols[pos]->len - 4) {
            return -E2BIG;
        }
        // use col offset of row to find the pointer and copy data
        *(uint32_t*)((char*)data + cols[pos]->offSet) = actualLen;
        memcpy(((char*)data + cols[pos]->offSet + 4), value->data, actualLen);
        cols[pos]->len = actualLen + 4;
        // reset the offset
        resetOffset(pos);
    } else {
        memcpy(((char*)data + cols[pos]->offSet), value->data, value->len);
        if(value->len < cols[pos]->len) {
            memset(((char*)data + cols[pos]->offSet + value->len), 0, cols[pos]->len - value->len);
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
            if(cols[i]->type == dpfs_datatype_t::TYPE_VARCHAR) {
                // first 4 bytes is actual length
                val.len = *(uint32_t*)((char*)data + cols[i]->offSet);
                val.data = (char*)data + cols[i]->offSet + 4;
            } else {
                val.len = cols[i]->len;
                val.data = (char*)data + cols[i]->offSet;
            }
            break;
            // return (CValue*)((char*)data + cols[i]->offSet + (sizeof(CValue) * i));
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
CItem::CItem(const std::vector<CColumn*>& cs) : cols(cs) {
    locked = false;
    error = false;
}

CItem::CItem(const CItem& other) : cols(other.cols) {
    locked = false;
    error = false;
}

CItem::~CItem() {

}