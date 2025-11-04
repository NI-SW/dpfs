#include <collect/page.hpp>
#include <cstring>
#include <thread>


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
	Cbt();
	void Init();
	void Put(int64_t offset, int64_t size);
	bool Save();
	bool Load();
	int64_t Get(int64_t size);
private:
	void merge();
	bool m_change; 
	int64_t m_cbtCount;
	std::list<CbtItem> m_cbtList;
	std::mutex m_mutex;
	CPage* m_page;

};


