#include <cstddef>
#include <stdexcept>

template <typename VALUE_T = int, typename SIZETYPE = size_t, int maxSize = 20>
class CFixLenVec {
public:

    CFixLenVec(VALUE_T* begin, SIZETYPE& sz) : vecSize(sz), values(begin) {
        // extra one reserve for split action
    }
    ~CFixLenVec() = default;

    /*
        @param begin: begin pointer (inclusive)
        @param end: end pointer (exclusive)
        @return 0 on success, else on failure
        @note assign value from begin to end to this vector
    */
    int assign(const VALUE_T* begin, const VALUE_T* end) noexcept {
        if (end < begin) {
            return -EINVAL;
        }

        size_t newSize = static_cast<size_t>(end - begin);
        if (newSize > maxSize) {
            return -ERANGE;
        }
        std::memcpy(values, begin, newSize * sizeof(VALUE_T));
        vecSize = static_cast<uint16_t>(newSize);

        return 0;
    }

    /*
        @param val: val to insert
        @return 0: success
                -ERANGE on exceed max size
        @note insert val to the end of the vector
    */
    int push_back(const VALUE_T& val) noexcept {
        if (vecSize >= maxSize) {
            return -ERANGE;
        }
        values[vecSize] = val;
        ++vecSize;
        return 0;
    }

    template<typename... _Valty>
    int emplace_back(_Valty&&... _Val) {
        if(vecSize >= maxSize) {
            return -ERANGE;
		}
		values[vecSize] = VALUE_T(std::forward<_Valty>(_Val)...);
        ++vecSize;
        return 0;
    }

    int concate_back(const CFixLenVec<VALUE_T, SIZETYPE, maxSize>& fromVec) noexcept {
        if (vecSize + fromVec.vecSize > maxSize) {
            return -ERANGE;
        }
        std::memcpy(&values[vecSize], fromVec.values, fromVec.vecSize * sizeof(VALUE_T));
        vecSize += fromVec.vecSize;
        return 0;
    }

    int push_front(const VALUE_T& val) noexcept {
        if (vecSize >= maxSize) {
            return -ERANGE;
        }
        std::memmove(&values[1], &values[0], vecSize * sizeof(VALUE_T));
        values[0] = val;
        ++vecSize;
        return 0;
    }

    int concate_front(const CFixLenVec<VALUE_T, SIZETYPE, maxSize>& fromVec) noexcept {
        if (vecSize + fromVec.vecSize > maxSize) {
            return -ERANGE;
        }
        std::memmove(&values[fromVec.vecSize], &values[0], vecSize * sizeof(VALUE_T));
        std::memcpy(&values[0], fromVec.values, fromVec.vecSize * sizeof(VALUE_T));
        vecSize += fromVec.vecSize;
        return 0;
    }


    /*
		@param pos position to insert
        @param val value to insert
        @return 0 on success, -ERANGE on exceed max size
        @note insert val to the vector at pos

    */
    int insert(int pos, const VALUE_T& val) noexcept {
        if (vecSize + 1 >= maxSize) {
			return -ERANGE;
        }

		memmove(&values[pos + 1], &values[pos], (vecSize - pos) * sizeof(VALUE_T));
        values[pos] = val;
        ++vecSize;
		return 0;
    }

    int erase(const VALUE_T& val) noexcept {
        int pos = search(val);
        if (pos < 0) {
            return pos;
        }
        std::memmove(&values[pos], &values[pos + 1], (vecSize - pos - 1) * sizeof(VALUE_T));
        --vecSize;
		return 0;
    }

    int erase(VALUE_T* it) {
        size_t pos = static_cast<size_t>(it - values);
        if (pos >= vecSize) {
            return -ERANGE;
        }
        std::memmove(&values[pos], &values[pos + 1], (vecSize - pos - 1) * sizeof(VALUE_T));
        --vecSize;
        return 0;
    }

    /*
        @param begin: begin position (inclusive)
        @param end: end position (exclusive)
        @return 0 on success, else on failure
        @note erase values from begin to end
    */
    int erase(const uint8_t& begin, const uint8_t& end) noexcept {
        if (end < begin) {
            return -EINVAL;
        }
        if (begin >= vecSize) {
            return -ENOENT;
        }
        std::memmove(&values[begin], &values[end], (vecSize - end) * sizeof(VALUE_T));
        vecSize -= static_cast<uint16_t>(end - begin);
        return 0;
    }

    int pop_back() noexcept {
        if (vecSize == 0) {
            return -ENOENT;
        }
        --vecSize;
        return 0;
    }

    int pop_front() noexcept {
        if (vecSize == 0) {
            return -ENOENT;
        }
        std::memmove(&values[0], &values[1], (vecSize - 1) * sizeof(VALUE_T));
        --vecSize;
        return 0;
    }


    /*
        @param val: value to search
        @return position of the value on success, -ENOENT if not found
        @note search value in the vector
    */
    int search(const VALUE_T& val) const noexcept {
        for(int i = 0 ;i < vecSize; ++i) {
            if (values[i] == val) {
                return i;
            }
		}
        return -ENOENT;
    }


    /*
        @param pos: position to get value
        @param outval: output reference value pointer
        @return 0 on success, else on failure
    */
    int at(uint32_t pos, VALUE_T*& outval) const noexcept {
        if (pos >= vecSize) {
            return -ERANGE;
        }
        outval = &values[pos];
        return 0;
    }

    VALUE_T& operator[](uint32_t pos) const {
        if (pos >= vecSize) {
            throw std::out_of_range("Index out of range");
        }
        return values[pos];
    }

    VALUE_T* begin() const noexcept {
        return values;
    }

    VALUE_T* end() const noexcept {
		return &values[vecSize];
    }

    void clear() noexcept {
        vecSize = 0;
	}

    uint32_t size() const noexcept {
        return vecSize;
    }

private:
    SIZETYPE& vecSize;
    // first value pointer
    VALUE_T* values;
};


class CExampleValue {
public:
    CExampleValue() : data(0), len(0) {}
    CExampleValue(const CExampleValue& exp) : data(exp.data), len(exp.len) {}
    CExampleValue(void* val, size_t len) : data(reinterpret_cast<char*>(val)), len(len) {}
	~CExampleValue() = default;
    bool operator==(const CExampleValue& other) const {
		return memcmp(data, other.data, len) == 0;
    }
    bool operator<(const CExampleValue& other) const {
        return memcmp(data, other.data, len) < 0;
    }
    char* data = nullptr;
    size_t len = 0;
};




/*
    this class for the element which is allocated on heap(variable length),
    if data is sorted, use binary search to improve search performance
*/
template <typename VALUE_T = CExampleValue, typename SIZETYPE = size_t>
class CVarLenVec {
public:
    class iterator {
    public:
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = VALUE_T;
        using pointer = uint8_t*;
        using reference = VALUE_T&;

        iterator(const iterator& it) : m_ptr(it.m_ptr), valueLen(it.valueLen) {}

        iterator& operator++() {
            m_ptr += valueLen;
            return *this;
        }

        iterator operator++(int) {
            iterator temp = *this;
            m_ptr += valueLen;
            return temp;
		}

        iterator& operator--() {
            m_ptr -= valueLen;
            return *this;
        }

        iterator operator--(int) {
            iterator temp = *this;
            m_ptr -= valueLen;
            return temp;
		}

        iterator& operator+=(difference_type n) {
            m_ptr += n * valueLen;
            return *this;
        }

        iterator& operator-=(difference_type n) {
            m_ptr -= n * valueLen;
            return *this;
		}   

        iterator operator+(difference_type n) const {
            return iterator(m_ptr + n * valueLen, valueLen);
        }

        iterator operator-(difference_type n) const {
            return iterator(m_ptr - n * valueLen, valueLen);
        }

        difference_type operator-(const iterator& other) const {
            return (m_ptr - other.m_ptr) / valueLen;
        }

        difference_type operator+(const iterator& other) const {
            return (m_ptr - other.m_ptr) / valueLen;
        }

        bool operator!=(const iterator& other) const {
            return m_ptr != other.m_ptr;
        }

        bool operator<(const iterator& other) const {
            return m_ptr < other.m_ptr;
		}

        bool operator<=(const iterator& other) const {
            return m_ptr <= other.m_ptr;
        }

        VALUE_T* operator->() {
			value.data = reinterpret_cast<decltype(value.data)>(m_ptr);
            value.len = valueLen;
            return &value;
		}

        VALUE_T operator*() const {
            return {m_ptr, valueLen};
        }

        iterator& operator=(const iterator& other) {
            if (this != &other) {
                m_ptr = other.m_ptr;
                valueLen = other.valueLen;
            }
            return *this;
        }

        bool operator==(const iterator& other) {
			return m_ptr == other.m_ptr;
        }

    private:
        friend class CVarLenVec<VALUE_T, SIZETYPE>;
        iterator(uint8_t* ptr, size_t valLen) : m_ptr(ptr), valueLen(valLen) {}
        uint8_t* m_ptr = nullptr;
        size_t valueLen = 0;
        VALUE_T value;
    };



    CVarLenVec(uint8_t* begin, SIZETYPE& sz, size_t valueLength, size_t maxsize) : 
    vecSize(sz), 
    values(reinterpret_cast<uint8_t*>(begin)), 
    valueLen(valueLength), 
    maxSize(maxsize) {
        // extra one reserve for split action
    }
    virtual ~CVarLenVec() = default;
    CVarLenVec() = default;

    /*
        @param begin: begin pointer (inclusive)
        @param end: end pointer (exclusive)
        @return 0 on success, else on failure
        @note assign value from begin to end to this vector
    */
    int assign(const iterator& begin, const iterator& end) noexcept {
        if (end < begin) {
            return -EINVAL;
        }

        size_t newSize = static_cast<size_t>(end - begin);
        if (newSize > maxSize) {
            return -ERANGE;
        }
        std::memcpy(values, begin.m_ptr, newSize * valueLen);
        vecSize = static_cast<SIZETYPE>(newSize);

        return 0;
    }

    /*
        @param val: val to insert
        @return 0: success
                -ERANGE on exceed max size
        @note insert val to the end of the vector
    */
    int push_back(const VALUE_T& val) noexcept {
        if (vecSize >= maxSize) {
            return -ERANGE;
        }
        std::memcpy(&values[vecSize * valueLen], val.data, val.len);
        ++vecSize;
        return 0;
    }

    template<typename... _Valty>
    int emplace_back(_Valty&&... _Val) {
        if (vecSize >= maxSize) {
            return -ERANGE;
        }
		VALUE_T val(std::forward<_Valty>(_Val)...);

        std::memcpy(&values[vecSize * valueLen], val.data, val.len);
        ++vecSize;
        return 0;
    }

    int concate_back(const CVarLenVec<VALUE_T, SIZETYPE>& fromVec) noexcept {
        if (vecSize + fromVec.vecSize > maxSize) {
            return -ERANGE;
        }
        std::memcpy(&values[vecSize * valueLen], fromVec.values, fromVec.vecSize * valueLen);
        vecSize += fromVec.vecSize;
        return 0;
    }

    int push_front(const VALUE_T& val) noexcept {
        if (vecSize >= maxSize) {
            return -ERANGE;
        }
        std::memmove(&values[1 * valueLen], &values[0 * valueLen], vecSize * valueLen);
		std::memcpy(values, val.data, val.len);
        // values[0] = val;
        ++vecSize;
        return 0;
    }

    int concate_front(const CVarLenVec<VALUE_T, SIZETYPE>& fromVec) noexcept {
        if (vecSize + fromVec.vecSize > maxSize) {
            return -ERANGE;
        }
        std::memmove(&values[fromVec.vecSize * valueLen], &values[0], vecSize * valueLen);
        std::memcpy(&values[0 * valueLen], fromVec.values, fromVec.vecSize * valueLen);
        vecSize += fromVec.vecSize;
        return 0;
    }


    /*
        @param pos position to insert
        @param val value to insert
        @return 0 on success, -ERANGE on exceed max size
        @note insert val to the vector at pos, mark as virtual if need override insert method
    */
    int insert(int pos, const VALUE_T& val) noexcept {
        if (vecSize + 1 >= maxSize) {
            return -ERANGE;
        }

        std::memmove(&values[(pos + 1) * valueLen], &values[pos * valueLen], (vecSize - pos) * valueLen);
        std::memcpy(&values[pos * valueLen], val.data, val.len);
        ++vecSize;
        return 0;
    }

    virtual int erase(const VALUE_T& val) noexcept {
        int pos = search(val);
        if (pos < 0) {
            return pos;
        }
        std::memmove(&values[pos * valueLen], &values[(pos + 1) * valueLen], (vecSize - pos - 1) * valueLen);
        --vecSize;
        return 0;
    }
    /*
		@param it iterator to erase
        @return 0 on success, -ERANGE on exceed max size
		@note erase value at it position, the it should belong to this vector, after erase, it will point to the next value
    */
    int erase(const iterator& it) {
        size_t pos = static_cast<size_t>(it - begin());
        if (pos >= vecSize) {
            return -ERANGE;
        }
        std::memmove(&values[pos * valueLen], &values[(pos + 1) * valueLen], (vecSize - pos - 1) * valueLen);
        --vecSize;

        return 0;
    }

    /*
        @param begin: begin position (inclusive)
        @param end: end position (exclusive)
        @return 0 on success, else on failure
        @note erase values from begin to end
    */
    int erase(const int& begin, const int& end) noexcept {
        if (end < begin) {
            return -EINVAL;
        }
        if (begin >= vecSize) {
            return -ENOENT;
        }
        std::memmove(&values[begin * valueLen], &values[end * valueLen], (vecSize - end) * valueLen);
        vecSize -= static_cast<uint16_t>(end - begin);
        return 0;
    }

    int pop_back() noexcept {
        if (vecSize == 0) {
            return -ENOENT;
        }
        --vecSize;
        return 0;
    }

    int pop_front() noexcept {
        if (vecSize == 0) {
            return -ENOENT;
        }
        std::memmove(&values[0 * valueLen], &values[1 * valueLen], (vecSize - 1) * valueLen);
        --vecSize;
        return 0;
    }


    /*
        @param val: value to search
        @return position of the value on success, -ENOENT if not found
        @note search value in the vector
    */
    virtual int search(const VALUE_T& val) const noexcept {
        for (int i = 0; i < vecSize; ++i) {
            if (memcmp(values + i * valueLen, val.data, val.len) == 0) {
                return i;
            }
        }
        return -ENOENT;
    }


    /*
        @param pos: position to get value
        @param outval: output iterator of the value
        @return 0 on success, else on failure
        @note get the reference of value at pos
    */
    int at(uint64_t pos, VALUE_T& outval) const noexcept {
        if (pos >= vecSize) {
            return -ERANGE;
        }
		outval.data = (decltype(outval.data))&values[pos * valueLen];
        outval.len = valueLen;
        return 0;
    }

    VALUE_T operator[](uint64_t pos) const {
        if (pos >= vecSize) {
            throw std::out_of_range("Index out of range");
        }
        return VALUE_T(&values[pos * valueLen], valueLen);
    }

    iterator begin() const noexcept {
        return iterator(values, valueLen);
    }

    iterator end() const noexcept {
        return iterator(&values[vecSize * valueLen], valueLen);
    }

    void clear() noexcept {
        vecSize = 0;
    }

    uint32_t size() const noexcept {
        return vecSize;
    }



protected:
    SIZETYPE& vecSize;
    // first value pointer
    uint8_t* values = nullptr;
    size_t valueLen = 0;
    const SIZETYPE maxSize;
};

/*
    if data is sorted, use binary search to improve search performance
*/
// template <typename VALUE_T = int, typename SIZETYPE = size_t>
// class CVarLenVec {
// public:

//     CVarLenVec(VALUE_T* begin, SIZETYPE& sz, size_t valueLength, size_t maxsize) : vecSize(sz), values(begin), valueLen(valueLength), maxSize(maxsize){
//         // extra one reserve for split action
//     }
// 	virtual ~CVarLenVec() = default;
//     CVarLenVec() = default;

//     /*
//         @param begin: begin pointer (inclusive)
//         @param end: end pointer (exclusive)
//         @return 0 on success, else on failure
//         @note assign value from begin to end to this vector
//     */
//     int assign(const VALUE_T* begin, const VALUE_T* end) noexcept {
//         if (end < begin) {
//             return -EINVAL;
//         }

//         size_t newSize = static_cast<size_t>(end - begin);
//         if (newSize > maxSize) {
//             return -ERANGE;
//         }
//         std::memcpy(values, begin, newSize * valueLen);
//         vecSize = static_cast<SIZETYPE>(newSize);

//         return 0;
//     }

//     /*
//         @param val: val to insert
//         @return 0: success
//                 -ERANGE on exceed max size
//         @note insert val to the end of the vector
//     */
//     int push_back(const VALUE_T& val) noexcept {
//         if (vecSize >= maxSize) {
//             return -ERANGE;
//         }
//         values[vecSize] = val;
//         ++vecSize;
//         return 0;
//     }

//     template<typename... _Valty>
//     int emplace_back(_Valty&&... _Val) {
//         if (vecSize >= maxSize) {
//             return -ERANGE;
//         }
//         values[vecSize] = VALUE_T(std::forward<_Valty>(_Val)...);
//         ++vecSize;
//         return 0;
//     }

//     int concate_back(const CVarLenVec<VALUE_T, SIZETYPE>& fromVec) noexcept {
//         if (vecSize + fromVec.vecSize > maxSize) {
//             return -ERANGE;
//         }
//         std::memcpy(&values[vecSize], fromVec.values, fromVec.vecSize * valueLen);
//         vecSize += fromVec.vecSize;
//         return 0;
//     }

//     int push_front(const VALUE_T& val) noexcept {
//         if (vecSize >= maxSize) {
//             return -ERANGE;
//         }
//         std::memmove(&values[1], &values[0], vecSize * valueLen);
//         values[0] = val;
//         ++vecSize;
//         return 0;
//     }

//     int concate_front(const CVarLenVec<VALUE_T, SIZETYPE>& fromVec) noexcept {
//         if (vecSize + fromVec.vecSize > maxSize) {
//             return -ERANGE;
//         }
//         std::memmove(&values[fromVec.vecSize], &values[0], vecSize * valueLen);
//         std::memcpy(&values[0], fromVec.values, fromVec.vecSize * valueLen);
//         vecSize += fromVec.vecSize;
//         return 0;
//     }


//     /*
//         @param pos position to insert
//         @param val value to insert
//         @return 0 on success, -ERANGE on exceed max size
//         @note insert val to the vector at pos, mark as virtual if need override insert method
//     */
//     int insert(int pos, const VALUE_T& val) noexcept {
//         if (vecSize + 1 >= maxSize) {
//             return -ERANGE;
//         }

//         memmove(&values[pos + 1], &values[pos], (vecSize - pos) * valueLen);
//         values[pos] = val;
//         ++vecSize;
//         return 0;
//     }

//     virtual int erase(const VALUE_T& val) noexcept {
//         int pos = search(val);
//         if (pos < 0) {
//             return pos;
//         }
//         std::memmove(&values[pos], &values[pos + 1], (vecSize - pos - 1) * valueLen);
//         --vecSize;
//         return 0;
//     }

//     int erase(VALUE_T* it) {
//         size_t pos = static_cast<size_t>(it - values);
//         if (pos >= vecSize) {
//             return -ERANGE;
//         }
//         std::memmove(&values[pos], &values[pos + 1], (vecSize - pos - 1) * valueLen);
//         --vecSize;
//         return 0;
//     }

//     /*
//         @param begin: begin position (inclusive)
//         @param end: end position (exclusive)
//         @return 0 on success, else on failure
//         @note erase values from begin to end
//     */
//     int erase(const int& begin, const int& end) noexcept {
//         if (end < begin) {
//             return -EINVAL;
//         }
//         if (begin >= vecSize) {
//             return -ENOENT;
//         }
//         std::memmove(&values[begin], &values[end], (vecSize - end) * valueLen);
//         vecSize -= static_cast<uint16_t>(end - begin);
//         return 0;
//     }

//     int pop_back() noexcept {
//         if (vecSize == 0) {
//             return -ENOENT;
//         }
//         --vecSize;
//         return 0;
//     }

//     int pop_front() noexcept {
//         if (vecSize == 0) {
//             return -ENOENT;
//         }
//         std::memmove(&values[0], &values[1], (vecSize - 1) * valueLen);
//         --vecSize;
//         return 0;
//     }


//     /*
//         @param val: value to search
//         @return position of the value on success, -ENOENT if not found
//         @note search value in the vector
//     */
//     virtual int search(const VALUE_T& val) const noexcept {
//         for (int i = 0; i < vecSize; ++i) {
//             if (values[i] == val) {
//                 return i;
//             }
//         }
//         return -ENOENT;
//     }


//     /*
//         @param pos: position to get value
//         @param outval: output reference value pointer
//         @return 0 on success, else on failure
//     */
//     int at(uint64_t pos, VALUE_T*& outval) const noexcept {
//         if (pos >= vecSize) {
//             return -ERANGE;
//         }
//         outval = &values[pos];
//         return 0;
//     }

//     VALUE_T& operator[](uint64_t pos) const {
//         if (pos >= vecSize) {
//             throw std::out_of_range("Index out of range");
//         }
//         return values[pos];
//     }

//     VALUE_T* begin() const noexcept {
//         return values;
//     }

//     VALUE_T* end() const noexcept {
//         return &values[vecSize];
//     }

//     void clear() noexcept {
//         vecSize = 0;
//     }

//     uint32_t size() const noexcept {
//         return vecSize;
//     }

// protected:
//     SIZETYPE& vecSize;
//     size_t valueLen = 0;
//     // first value pointer
//     VALUE_T* values = nullptr;
//     const size_t maxSize;
// };




