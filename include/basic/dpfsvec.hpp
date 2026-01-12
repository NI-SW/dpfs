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