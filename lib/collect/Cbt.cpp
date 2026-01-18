#include <collect/Cbt.hpp>
#include <algorithm>
extern uint64_t nodeId;

CbtItem::CbtItem(int64_t offset, int64_t size) {
    m_size = size;
    m_offset = offset;
}

bool CbtItem::doOverlap(const CbtItem& other) const noexcept {
    return (m_offset <= (other.GetOffset() + other.GetSize()) && (m_offset + m_size) >= other.GetOffset());
}

void CbtItem::mergeWith(const CbtItem& other) noexcept {
    int64_t newOffset = std::min(m_offset, other.GetOffset());
    int64_t newSize = std::max(m_offset + m_size, other.GetOffset() + other.GetSize()) - newOffset;
    m_offset = newOffset;
    m_size = newSize;
}

void Cbt::AddBidx(uint64_t gid, int64_t count) {
    m_gidOffset.emplace(m_gidOffset.rbegin()->first + count, gid);
}

void Cbt::Init() {
    Load();
}

void Cbt::Put(int64_t offset, int64_t size)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_cbtVec.emplace_back(offset, size);
    m_change = true;
    m_cbtCount++;
    Save();
}

void Cbt::OnlyPut(int64_t offset, int64_t size)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_cbtVec.emplace_back(offset, size);
    m_change = true;
    m_cbtCount++;
}

int64_t Cbt::OnlyGet(int64_t size) {
    std::lock_guard<std::mutex> lock(m_mutex);
    for(auto it = m_cbtVec.begin(); it != m_cbtVec.end(); ++it) {
        if((*it).GetSize() >= size) {
            int64_t offset = (*it).GetOffset();
            if((*it).GetSize() == size) {
                m_cbtVec.erase(it);
                m_cbtCount--;
            }
            else {
                (*it) = CbtItem(offset + size, (*it).GetSize() - size);
            }
            m_change= true;
            return offset;
        }
    }
    return -1;
}


int64_t Cbt::Get(int64_t size) {
    std::lock_guard<std::mutex> lock(m_mutex);
    for(auto it = m_cbtVec.begin(); it != m_cbtVec.end(); ++it) {
        if((*it).GetSize() >= size) {
            int64_t offset = (*it).GetOffset();
            if((*it).GetSize() == size) {
                m_cbtVec.erase(it);
                m_cbtCount--;
            }
            else {
                (*it) = CbtItem(offset + size, (*it).GetSize() - size);
            }
            m_change= true;
            Save();
            return offset;
        }
    }
    return -1;
}

void Cbt::merge() {
    if(m_cbtVec.size() <= 1) {
        return;
    }
    
    std::sort(m_cbtVec.begin(), m_cbtVec.end(),
              [](const CbtItem& a, const CbtItem& b) {
                  return a.GetOffset() < b.GetOffset();
              });

    auto it = m_cbtVec.begin();
    while (it != std::prev(m_cbtVec.end())) {
        if (it->doOverlap(*std::next(it))) {
            it->mergeWith(*std::next(it));
            m_cbtVec.erase(std::next(it));
        } else {
            ++it;
        }
    }
    m_cbtCount = m_cbtVec.size();
}

/*
    testbid.gid = nodeId;
    testbid.bid = 0;
    记录cbt的位置

    每次save都将cbt写到新的磁盘块上

*/

int64_t Cbt::bidxToOffset(bidx bid)/*可优化*/
{
    int64_t offset = -1;
    for(auto& it:m_gidOffset)
    {
        if(it.second == bid.gid)
        {
            offset = it.first + bid.bid;
            break;
        }
    }
    return offset;
}

bidx Cbt::offsetToBid(int64_t offset)/*可优化*/
{
    bidx bid;
    bid.gid = nodeId;
    bid.bid = 1;
    for(auto& it:m_gidOffset)
    {
        if(it.first > static_cast<uint64_t>(offset))
        {
            break;
        }
        bid.bid = offset - it.first;
        bid.gid = it.second;
    }
    return bid;
}
bool Cbt::Save() {//写整个cbt到磁盘
    std::vector<CbtItem> cbtVec;
    
    /*释放上上次的cbt块*/
    for(auto& it:m_header.LastCbttbid)
    {
        OnlyPut(bidxToOffset(it),1);
    }
    
    int pageCount = m_cbtVec.size() / ((4096 - sizeof(bidx)) / sizeof(CbtItem)) + 1;

    m_header.LastCbttbid = m_header.Cbtbid;
    m_header.Cbtbid.clear();
    /*获取本次cbt块*/
    for(int i=0;i<pageCount;i++)
    {
        int offset = OnlyGet(1);

        if (offset == -1) {
            return false;
        }

        bidx bid = offsetToBid(offset);
        m_header.Cbtbid.push_back(bid);
    }
    
    /*获取本次要写的整个cbt*/
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        merge();
        cbtVec = m_cbtVec;
    }
    //write

    // 针对一个CBT可以只使用一个磁盘组号
    bidx testbid;
    // 磁盘组号
    testbid.gid = nodeId;
    // 磁盘块号
    testbid.bid = 0;

    m_header.Cbtbid.push_back(testbid);
    int cbtCount = cbtVec.size();    
    int cbtOffset = 0;
    vector<int> finish_indicator;
    for(int i = 0; i < pageCount; i++) 
    {
        void* zptr = m_page->alloczptr(1 /* 块数量 1个块4096B*/);
        memcpy(zptr, &m_header.Cbtbid[i+1], sizeof(bidx));
        int offset = sizeof(bidx);
        while(offset + sizeof(CbtItem) <= 4096 && cbtOffset < cbtCount) 
        {
            memcpy(static_cast<uint8_t*>(zptr) + offset, &cbtVec[cbtOffset], sizeof(CbtItem));
            cbtOffset++;
        }
        finish_indicator.push_back(0);
        int rc = m_page->put(m_header.Cbtbid[i], zptr, &finish_indicator[i], 1 /* 写入的块数量 */, true /* 立即写回磁盘 */);
        if(rc) {
            // 写入失败，处理错误
            return false;
        }
    }

    //写cbt头信息
    // 准备写入的数据
    // 申请dma内存， 写入完成后dma内存会被CPage接管， 不需要释放
    void* zptr = m_page->alloczptr(1 /* 块数量 1个块4096B*/);
    m_header.item = cbtCount;
    memcpy(zptr, &m_header.Cbtbid[0], sizeof(bidx));
    memcpy(static_cast<uint8_t*>(zptr) + sizeof(bidx), &m_header.item, sizeof(int64_t));

    finish_indicator.push_back(0);
    // write to disk
    int rc = m_page->put(testbid, zptr, &finish_indicator[finish_indicator.size() - 1], 1 /* 写入的块数量 */, true /* 立即写回磁盘 */);
    if(rc) {
        // 写入失败，处理错误
        return false;
    }
    while(finish_indicator.size() != 0) {
        for(auto it = finish_indicator.begin(); it != finish_indicator.end(); ++it) {
            if(*it == 1)
            {
                finish_indicator.erase(it);
                break;
            }
            else if(*it == -1) {
                // 写入错误，处理错误
                return false;
            }
        }
    }

    return true;
}

bool Cbt::Load() {
    //read
    bidx testbid;
    // 磁盘组号
    testbid.gid = nodeId;
    // 磁盘块号
    testbid.bid = 0;

    // 保存读取出来数据的结构，可以考虑写死在cbt里？
    // cacheStruct* m_pagePtr[cbtMaxBlocks];
    auto ptr = new cacheStruct*[1];

    // read from disk
    int rc = m_page->get(ptr[0], testbid, 1);
    if(rc) {
        // 读取失败，处理错误
        return false;
    }

    // 等待数据读取完成
    while(ptr[0]->getStatus() != cacheStruct::VALID) {
        if(ptr[0]->getStatus() == cacheStruct::ERROR) {
            // 读取错误，处理错误
            
        } else if(ptr[0]->getStatus() == cacheStruct::INVALID) {
            // 读取无效，处理错误
            return false;
        }
        // 等待
         std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ptr[0]->read_lock();
    // 读取数据
    void* cachePtr = ptr[0]->getPtr();
    m_header.Cbtbid.clear();
    m_header.Cbtbid.emplace_back();
    m_header.Cbtbid[0] = *reinterpret_cast<const bidx*>(cachePtr);
    memcpy(&m_header.item, static_cast<uint8_t*>(cachePtr) + sizeof(bidx), sizeof(int64_t));

    ptr[0]->read_unlock();

    // 释放资源， 不调用release，DMA内存不会释放，
    ptr[0]->release();

    delete ptr;

    int nowCbtPages = 0;
    int cbtOffset = 0;
    CbtItem item(0,0);
    while(m_header.Cbtbid[nowCbtPages].gid != testbid.gid && m_header.Cbtbid[nowCbtPages].gid != testbid.bid && cbtOffset < m_header.item)
    {
        auto ptr = new cacheStruct*[1];

        // read from disk
        int rc = m_page->get(ptr[0], m_header.Cbtbid[nowCbtPages], 1);
        if(rc) {
            // 读取失败，处理错误
            return false;
        }

        // 等待数据读取完成
        while(ptr[0]->getStatus() != cacheStruct::VALID) {
            if(ptr[0]->getStatus() == cacheStruct::ERROR) {
                // 读取错误，处理错误
                
            } else if(ptr[0]->getStatus() == cacheStruct::INVALID) {
                // 读取无效，处理错误
                return false;
            }
            // 等待
            // std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        ptr[0]->read_lock();
        // 读取数据
        void* cachePtr = ptr[0]->getPtr();
        nowCbtPages++;
        m_header.Cbtbid.emplace_back();
        m_header.Cbtbid[nowCbtPages] = *reinterpret_cast<const bidx*>(cachePtr);
        int offset = sizeof(bidx);
        while(offset + sizeof(CbtItem) <= 4096 && cbtOffset < m_header.item) 
        {
            memcpy(&item, static_cast<uint8_t*>(cachePtr) + offset, sizeof(CbtItem));
            m_cbtVec.push_back(item);
            cbtOffset++;
        }

        ptr[0]->read_unlock();

        // 释放资源， 不调用release，DMA内存不会释放，
        ptr[0]->release();

        delete ptr;
    }

    return true;
}