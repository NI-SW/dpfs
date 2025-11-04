#include <collect/Cbt.hpp>
extern uint64_t nodeId;

CbtItem::CbtItem(int64_t offset, int64_t size) {

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

void Cbt::Init() {
    
}

void Cbt::Put(int64_t offset, int64_t size)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_cbtList.emplace_back(offset, size);
    m_change = true;
    m_cbtCount++;
}

int64_t Cbt::Get(int64_t size) {
    std::lock_guard<std::mutex> lock(m_mutex);
    for(auto it = m_cbtList.begin(); it != m_cbtList.end(); ++it) {
        if((*it).GetSize() >= size) {
            int64_t offset = (*it).GetOffset();
            if((*it).GetSize() == size) {
                m_cbtList.erase(it);
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

void Cbt::merge() {
    if(m_cbtList.size() <= 1) {
        return;
    }
    
    m_cbtList.sort([](const CbtItem& a, const CbtItem& b) {return a.GetOffset() < b.GetOffset();});
    auto it = m_cbtList.begin();
    while (it != std::prev(m_cbtList.end())) {
        if (it->doOverlap(*std::next(it))) {
            it->mergeWith(*std::next(it));
            m_cbtList.erase(std::next(it));
        } else {
            ++it;
        }
    }
    m_cbtCount = m_cbtList.size();
}


bool Cbt::Save() {
    std::list<CbtItem> cbtList;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        merge();
        cbtList = m_cbtList;
    }
    //write

    // 针对一个CBT可以只使用一个磁盘组号
    bidx testbid;
    // 磁盘组号
    testbid.gid = nodeId;
    // 磁盘块号
    testbid.bid = 0;

    // 准备写入的数据
    // 申请dma内存， 写入完成后dma内存会被CPage接管， 不需要释放
    void* zptr = m_page->alloczptr(1 /* 块数量 1个块4096B*/);

    int finish_indicator = 0;
    // write to disk
    int rc = m_page->put(testbid, zptr, &finish_indicator, 1 /* 写入的块数量 */, true /* 立即写回磁盘 */);
    if(rc) {
        // 写入失败，处理错误
        return false;
    }

    while(finish_indicator != 1) {
        // 等待写入完成
        // std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if(finish_indicator == -1) {
            // 写入错误，处理错误
            return false;
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
        // std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ptr[0]->read_lock();
    // 读取数据

    ptr[0]->read_unlock();

    // 释放资源， 不调用release，DMA内存不会释放，
    ptr[0]->release();

    delete ptr;

    return true;
}