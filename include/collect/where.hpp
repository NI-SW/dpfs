#include <collect/bp.hpp>
#include <collect/collect.hpp>
enum dpfsCmpType {
    DPFS_WHERE_EQ = 0,
    DPFS_WHERE_NE = 1,
    DPFS_WHERE_LT = 2,
    DPFS_WHERE_GT = 3,
    DPFS_WHERE_LE = 4,
    DPFS_WHERE_GE = 5,
};
enum cmpUnitType { NODE_OP, NODE_IDENTIFIER, NODE_NUMBER, NODE_STRING };

struct CmpUnit {
    CmpUnit() : type(NODE_OP) {}
    CmpUnit(cmpUnitType t, std::string& data) : type(t) {
        strVal = std::move(data);
    };
    CmpUnit(cmpUnitType t, int64_t data) : type(t) {
        intVal = data;
    };
    CmpUnit(cmpUnitType t, double data) : type(t) {
        doubleVal = data;
    };
    CmpUnit(CmpUnit&& other) {
        type = other.type;
        strVal.swap(other.strVal);
        intVal = other.intVal;
        doubleVal = other.doubleVal;
    }
    CmpUnit& operator=(CmpUnit&& other) {
        if(this != &other) {
            type = other.type;
            strVal.swap(other.strVal);
            intVal = other.intVal;
            doubleVal = other.doubleVal;
        }
        return *this;
    }

    cmpUnitType type = NODE_OP;
    std::string strVal;
    int64_t intVal = 0;
    double doubleVal = 0;

};

/*
    eq -> left, right, colName or value
*/
struct predictNode {
    predictNode(dpfsCmpType ct, CmpUnit& left, CmpUnit& right) : cmpType(ct) {
        left = std::move(left);
        right = std::move(right);
    }
    dpfsCmpType cmpType;
    CmpUnit left;
    CmpUnit right;

    union {
        std::string* strVal;
        int64_t intVal;
        double doubleVal;
        
    } leftData, rightData;

};

struct conditionNode {
public:
    enum conditionType {
        NODE_TYPE_CONDITION,
        NODE_TYPE_AND,
        NODE_TYPE_OR,
        NODE_TYPE_NOT,
    };

    conditionType type;
    // for condition node
    std::string columnName;
    dpfsCmpType cmpType;
    std::string value;

    // for operator node
    conditionNode* left = nullptr;
    conditionNode* right = nullptr;


};

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
