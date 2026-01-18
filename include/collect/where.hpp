#include <collect/bp.hpp>
#include <collect/collect.hpp>
enum dpfsCmpType {
    DPFS_WHERE_EQ = 0,
    DPFS_WHERE_NEQ = 1,
    DPFS_WHERE_LT = 2,
    DPFS_WHERE_LTE = 3,
    DPFS_WHERE_GT = 4,
    DPFS_WHERE_GTE = 5,
};

/*
    eq -> left, right, colName or value

*/

class CWhere {
public:

    CWhere(const CCollection& clt) : m_clt(clt) {

    }

    ~CWhere() {

    }

    class condition {
    public:
        condition(const std::string& col, dpfsCmpType ct, const std::string& val) : columnName(col), cmpType(ct), value(val) {

        }
        
        condition(const condition& other) : columnName(other.columnName), value(other.value), cmpType(other.cmpType) {

        }
        condition(condition&& other) {
            columnName.swap(other.columnName);
            value.swap(other.value);

        }

        condition& operator=(const condition& other) {
            if (this != &other) {
                columnName = other.columnName;
                value = other.value;
                cmpType = other.cmpType;
            }
            return *this;
        }


    private:
        std::string columnName;
        std::string value;
        dpfsCmpType cmpType;

    };

private:
    const CCollection& m_clt;
    std::vector<condition> conditions;
};