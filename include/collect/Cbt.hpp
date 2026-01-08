#include <collect/page.hpp>
#include <cstring>
#include <thread>
#include <iostream>
#include <map>
using namespace std;
struct CbtHeader {
    std::vector<bidx> Cbtbid;
	std::vector<bidx> LastCbttbid;
	int64_t item = 0;
};

class CbtItem {
public:
	CbtItem(int64_t offset, int64_t size);
	/*
		@return number of items
	*/
	int64_t GetSize() const noexcept { return m_size; }
	/*
		@return offset of current item
	*/
	int64_t GetOffset() const noexcept { return m_offset; }
	/*
		@note check if two item is overlapped.
		@return true if overlap exists
	*/
	bool doOverlap(const CbtItem& other) const noexcept;

	/*
		Merge two overlapping items
	*/
	void mergeWith(const CbtItem& other) noexcept;
private:
	int64_t m_size;
	int64_t m_offset;
};


class Cbt {
public:
	Cbt(){};
	void Init();
	void Put(int64_t offset, int64_t size);
	void OnlyPut(int64_t offset, int64_t size);
	bool Save();
	bool Load();
	int64_t Get(int64_t size);
	int64_t OnlyGet(int64_t size);
	void AddBidx(uint64_t gid, int64_t count);

	int64_t bidxToOffset(bidx bid);
	bidx offsetToBid(int64_t offset);
private:
	void merge();
	bool m_change; 
	CbtHeader m_header;
	int64_t m_cbtCount;
	std::vector<CbtItem> m_cbtVec;
	std::mutex m_mutex;
	CPage* m_page;
	int mode = 0;/*0:每次变动都写到磁盘*/
	std::map<uint64_t, uint64_t> m_gidOffset;//每个gid的起始偏移量
};


