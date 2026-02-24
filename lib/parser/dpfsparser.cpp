#include <parser/dpfsparser.hpp>
#include <tiparser/tidb_parser.h>
#include <tiparser/tiparser_enum.hpp>
#include <dpfssys/plan.hpp>
#include <tiparser/decimal_convertor.hpp>

#ifdef PARSER_DEBUG
using namespace std;

#endif



dpfs_datatype_t colTpCvt(planColTypes tp, bool isBinary) {
    switch (tp) {
        case planColTypes::TypeTiny:
        case planColTypes::TypeShort:
        case planColTypes::TypeLong:
        case planColTypes::TypeInt24:
            return dpfs_datatype_t::TYPE_INT;
        case planColTypes::TypeLonglong:
            return dpfs_datatype_t::TYPE_BIGINT;
        case planColTypes::TypeFloat:
            return dpfs_datatype_t::TYPE_FLOAT;
        case planColTypes::TypeDouble:
            return dpfs_datatype_t::TYPE_DOUBLE;
        case planColTypes::TypeVarchar:
        case planColTypes::TypeVarString:
            if (isBinary) {
                return dpfs_datatype_t::TYPE_BINARY;
            } else {
                return dpfs_datatype_t::TYPE_VARCHAR;
            }
        case planColTypes::TypeString:
            if (isBinary) {
                return dpfs_datatype_t::TYPE_BINARY;
            } else {
                return dpfs_datatype_t::TYPE_CHAR;
            }
        case planColTypes::TypeNewDecimal:
            return dpfs_datatype_t::TYPE_DECIMAL;
        case planColTypes::TypeTimestamp:
        case planColTypes::TypeDatetime:
            return dpfs_datatype_t::TYPE_TIMESTAMP;
        case planColTypes::TypeDate:    
            return dpfs_datatype_t::TYPE_DATE;
        case planColTypes::TypeBit:
            return dpfs_datatype_t::TYPE_BOOL;
        default:
            return dpfs_datatype_t::TYPE_NULL; // Default to NULL for unsupported types
    }
}

constexpr char tiparserPrefix[] = "github.com/pingcap/tidb/pkg/parser/ast.";
constexpr size_t prefixLen = sizeof(tiparserPrefix) - 1; // Exclude null terminator

CParser::CParser(const CUser& user, CDatasvc& dsvc) : usr(user), dataSvc(dsvc) {

}

CParser::~CParser() {
    if (m_pParseResult) {
        TidbFreeParseResult(m_pParseResult);
        m_pParseResult = nullptr;
    }
}
/*
    @param sql: SQL string to parse
    @return 0 on success, else on failure
    @note this function will parse the given SQL string
*/
int CParser::operator()(const std::string& sql) {

    if (m_pParseResult) {
        // free previous parse result if any
        // Assume a function TidbFreeParseResult exists to free the result
        TidbFreeParseResult(m_pParseResult);
        m_pParseResult = nullptr;
    }

    m_pParseResult = TidbParseSQL(const_cast<char*>(sql.c_str()));
    if (m_pParseResult->ok) {
        return 0; // Successfully parsed
    } else {
        message = "Failed to parse SQL: " + sql;
        // Free the parse result if parsing failed
        TidbFreeParseResult(m_pParseResult);
        m_pParseResult = nullptr;
        return -1; // Failed to parse
    }

}


/*
    check user privilege for the given schema and object
    @param schemaName: name of the schema
    @param objName: name of the object
    @return 0 on success, else on failure
*/
int CParser::checkPrivilege() {
    if (!m_pParseResult) {
        return -EINVAL; // No parse result to check
    }

    // TODO

    return 0;
}

int CParser::buildPlan(const std::string& osql, CPlanHandle& out) {
    if (!m_pParseResult) {
        return -EINVAL; // No parse result to build plan from
    }
    int rc = 0;
    TidbParseResult*& ast = m_pParseResult;


    // field name : CreateTable, SelectStmtOpts
    ast->stmt_count;

    for (int32_t i = 0; i < ast->stmt_count; ++i) {
        const TidbAstNode* stmt = ast->statements[i];
        // Process each statement node
        rc = buildPlanForStmt(osql, stmt, out);
        if (rc != 0) {
            return rc;
        }
    }

    return 0;

}

struct colOption {
    union {
        CColumn::constraint_flags flag;
        uint8_t constraints;
    } flg;
};

static int parsColOptions(const TidbAstValue* optionNode, colOption& colOpt) {
    // TODO
    colOpt.flg.constraints = 0;

    // for each item
    for (int i = 0; i < optionNode->array.len; ++i) {
        const TidbAstValue& opt = optionNode->array.items[i];
        if (opt.node == nullptr) {
            continue; // Skip non-node options
        }
        // for item, get constraints
        planColOptionTp Tp = static_cast<planColOptionTp>(opt.node->fields[static_cast<int>(planColOptions::Tp)].value.i64); // ColumnOptionType

        if (Tp == planColOptionTp::ColumnOptionNotNull) {
            colOpt.flg.constraints |= CColumn::constraint_flags::NOT_NULL;
        } else if (Tp == planColOptionTp::ColumnOptionPrimaryKey) {
            colOpt.flg.constraints |= CColumn::constraint_flags::PRIMARY_KEY;
        } else if (Tp == planColOptionTp::ColumnOptionUniqKey) {
            colOpt.flg.constraints |= CColumn::constraint_flags::UNIQUE;
        } else if (Tp == planColOptionTp::ColumnOptionAutoIncrement) {
            colOpt.flg.constraints |= CColumn::constraint_flags::AUTO_INC;
        }

    }

    return 0;
}

/*
    @param arrView: string view of the array definition, should be in the format of "[ str1 str2 str3 ... ]"
    @param out: vector to store the parsed array values
    @return 0 on success, else on failure
    @note this function will parse the array definition and extract the values into a vector
*/
static int parsArray(const std::string_view& arrView, std::vector<std::string_view>& out) {
    // TODO
    size_t startPos = 0;
    size_t sz = 0;

    startPos = arrView.find('[');
    if (startPos == std::string_view::npos) {
        return -EINVAL; // Invalid format, missing '['
    }
    ++startPos; // Move past the '['

    const char* data = nullptr;

    for (; startPos < arrView.size(); ++startPos) {
        if (arrView[startPos] == ' ' || arrView[startPos] == ']' || arrView[startPos] == '[') {
            if (sz > 0) {
                out.emplace_back(&arrView[startPos - sz], sz); // Extract the value
                sz = 0; // Reset size for the next value
            }
            // ++startPos; // Skip delimiters
        } else {
            ++sz; // Increase size of current item
        }
    }


    return 0;

}

/*
    @param typeView: string view of the column type definition, should be in the format of "{ str1 str2 str3 str4 str5 ... }"
    @param values: vector to store the parsed column parameters
    @return 0 on success, else on failure
    @note this function will parse the column type definition and extract the first 6 parameters into a vector
*/
static int parsParamStr(const string_view& typeView, std::vector<std::string>& values) {

    values.clear();
    values.reserve(16);

    /*
        pars forman { str1, str2, [array], str3 }
    */
    
    size_t pos = typeView.find('{') + 1;
    if (pos == string_view::npos) {
        return -EINVAL; // Invalid format, missing '{'
    }

    size_t endPos = typeView.rfind('}');

    if (endPos == string_view::npos || endPos <= pos) {
        return -EINVAL; // Invalid format, missing '}' or misplaced
    }

    for (; pos < endPos; ++pos) {
        std::string parm;
        parm.reserve(16);
        while (pos < endPos && typeView[pos] != ' ' && typeView[pos] != '}' && typeView[pos] != '[' && typeView[pos] != ']') {
            parm += typeView[pos];
            ++pos;
        }
        // found array definition, parse array and append to parm
        if (typeView[pos] == '[') {
            // parse array
            size_t end = typeView.find(']', pos);
            if (end == string_view::npos) {
                return -EINVAL; // Invalid format, missing ']'
            }
            std::string_view arrv(&typeView[pos], end - pos + 1);

            std::vector<std::string_view> arrItems;
            int rc = parsArray(arrv, arrItems);
            if (rc != 0) {
                return rc; // Return error code if array parsing failed
            }

            // for each arritem, convert to binary and append to parm
            for (const auto& item : arrItems) {
                char c = 0;
                c = static_cast<char>(std::atoi(item.data()));
                parm += c; // Add space before each array item
            }
            
            // for (const auto& item : arrItems) {
            //     parm += ' '; // Add space before each array item
            //     parm += item; // Append the array item to the parameter string
            // }
            
            pos = end; // Move position to the end of the array definition
        }
        
        values.emplace_back(std::move(parm));
    }


    return 0;
}


/*
    @param colDef: AST node of the column definition
    @param colParams: vector to store the parsed column parameters, should have at least 6 elements
    @return 0 on success, else on failure
    @note this function will parse the column definition and extract the first 6 parameters into colParams
*/
static int parsColumn(const TidbAstNode* colDef, CCollection& coll) {

    /*
        fields[0] -> Name
        fields[1] -> Type
        fields[2] -> Options
    */
    int rc = 0;
    std::string name(
        colDef->fields[0].value.node->fields[2].value.object.fields[0].value.str.data, 
        colDef->fields[0].value.node->fields[2].value.object.fields[0].value.str.len);
    std::string typeStr(colDef->fields[1].value.typed.inner->str.data, colDef->fields[1].value.typed.inner->str.len);
    std::string options; // colDef->fields[2]

    auto& optionNode = colDef->fields[2].value;

    /*
pos:    0    1         2   3    4      5      6  7  8 
        3    0        -1  -1    N      N      [] [] false
        type unknown  len scale binary binary [] [] unknown 
    */
    std::string_view typeView(typeStr); 


    std::vector<std::string> colParams;

    rc = parsParamStr(typeView, colParams);
    if (rc != 0) {
        return rc; // Return error code if parameter parsing failed
    }

    // colParams.clear();
    // colParams.reserve(6);

    // for (int i = 0, j = 0; i < 6; ++i) {
    //     const char* start = typeView.data() + j;
    //     size_t sz = 0;

    //     for (; typeView[j] != ' ' && typeView[j] != '\0' && typeView[j] != '}'; ++j) {
    //         ++sz;
    //     }
    //     colParams.emplace_back(start, sz);
    
    //     if (typeView[j] == ' ') {
    //         ++j; // skip space
    //     }
    // }

    int typeCode = std::stoi(colParams[0]);
    int unknown = std::stoul(colParams[1]);
    int len = std::stoul(colParams[2]); if (len == -1) len = 0;
    int scale = std::stoul(colParams[3]); if (scale == -1) scale = 0;

    bool isBinary = colParams[4] == "binary";
    dpfs_datatype_t colTp = dpfs_datatype_t::TYPE_NULL;
    colOption colOpt;
    colOpt.flg.constraints = 0;
    if (optionNode.kind == TIDB_AST_ARRAY) {
        // find which is primary key, and other constraints, and set the column constraint accordingly, currently just set binary flag
        rc = parsColOptions(&optionNode, colOpt);
        if (rc != 0) {
            goto errReturn; // Return error code if column options parsing failed
        }
    }

    colTp = colTpCvt(static_cast<planColTypes>(typeCode), isBinary);

    

    

#ifdef PARSER_DEBUG
    cout << "Column Name: " << name << endl;
    cout << "Column Type: " << typeStr << endl;
    cout << "Parsed Column Parameters: " << endl;
    for (size_t i = 0; i < colParams.size(); ++i) {
        cout << "  Param " << i + 1 << ": \"" << colParams[i] << "\"" << endl;
    }

    cout << "after converting type code to type name: " << static_cast<int>(typeCode) << endl;
    cout << "len: " << len << ", scale: " << scale << ", isBinary: " << isBinary << endl;
    cout << "column constraints: " << (colOpt.flg.constraints & CColumn::constraint_flags::NOT_NULL ? "NOT NULL " : "")
         << (colOpt.flg.constraints & CColumn::constraint_flags::PRIMARY_KEY ? "PRIMARY KEY " : "")
         << (colOpt.flg.constraints & CColumn::constraint_flags::UNIQUE ? "UNIQUE " : "")
         << (colOpt.flg.constraints & CColumn::constraint_flags::AUTO_INC ? "AUTO_INCREMENT " : "") << endl;
#endif
    try {
        coll.addCol(name, colTp, len, scale, colOpt.flg.constraints);
    } catch (const std::invalid_argument& e) {
        rc = -ENAMETOOLONG;
    } catch (...) {
        throw; // Rethrow the exception after logging
    }
    
    return 0;

    errReturn:

    return rc;
}

static int parsConstraint(const TidbAstNode* constraintDef, CCollection& coll) {
    // TODO
    return 0;
}

static int parsIndex(const TidbAstNode* indexDef, CCollection& coll) {
    // TODO

    return 0;
}


static int pushValue(std::vector<std::vector<std::pair<dpfs_datatype_t, CValue>>>& valVec, const std::vector<std::string>& colParams, const std::vector<std::string>& valParams, bool negative) {
    // get value's col type
    int typeCode = std::stoi(colParams[0]);
    int unknown = std::stoul(colParams[1]);
    int len = std::stoul(colParams[2]); if (len == -1) len = 0;
    int scale = std::stoul(colParams[3]); if (scale == -1) scale = 0;
    bool isBinary = colParams[4] == "binary";

    dpfs_datatype_t colTp = dpfs_datatype_t::TYPE_NULL;
    colTp = colTpCvt(static_cast<planColTypes>(typeCode), isBinary);

    switch (colTp) {
        case dpfs_datatype_t::TYPE_INT: {
            CValue v(sizeof(int32_t));
            int32_t val = std::stoi(valParams[1]);
            if (negative) {
                val = -val;
            }
            v.setData(&val, sizeof(int32_t));
            valVec.back().emplace_back(std::make_pair(colTp, std::move(v)));
            break;
        }
        case dpfs_datatype_t::TYPE_BIGINT: {
            CValue v(sizeof(int64_t));
            int64_t val = std::stoll(valParams[1]);
            if (negative) {
                val = -val;
            }
            v.setData(&val, sizeof(int64_t));
            valVec.back().emplace_back(std::make_pair(colTp, std::move(v)));
            break;
        }
        case dpfs_datatype_t::TYPE_FLOAT: {
            CValue v(sizeof(float));
            uint32_t val = std::stoul(valParams[1]);
            if (negative) {
                val = -val;
            }
            v.setData(&val, sizeof(float)); // directly use the binary representation of the float value
            valVec.back().emplace_back(std::make_pair(colTp, std::move(v)));
            break;
        }
        case dpfs_datatype_t::TYPE_DOUBLE: {
            // {4 4667987610839285760 [] <nil>}
            CValue v(sizeof(double));
            uint64_t val = std::stoull(valParams[1]);
            if (negative) {
                val = -val;
            }
            v.setData(&val, sizeof(double)); // directly use the binary representation of the double value
            valVec.back().emplace_back(std::make_pair(colTp, std::move(v)));
            break;
        }
        case dpfs_datatype_t::TYPE_VARCHAR:
        case dpfs_datatype_t::TYPE_CHAR:
        case dpfs_datatype_t::TYPE_BINARY: {
            // {5 0 [50 48 50 53 45 49 49 45 50 53 32 48 56 58 48 53 58 48 49] <nil>}
            CValue v(valParams[2].size());
            v.setData(valParams[2].data(), valParams[2].size());
            valVec.back().emplace_back(std::make_pair(colTp, std::move(v)));
            break;
        }
        case dpfs_datatype_t::TYPE_DECIMAL: {
                uint64_t ptr = 0;
                // 0x prefix is added in the string representation of the pointer, need to skip it when converting to uint64_t
                std::string_view decPtrView(valParams[4].data() + 2, valParams[4].size() - 2);
                ptr = std::stoull(decPtrView.data(), nullptr, 16);

                char binaryDecimal[64];
                int binLen = 0;
                int rc = deccvt::tibinary2mybinary(reinterpret_cast<const char*>(ptr), (uint8_t*)binaryDecimal, 64, negative, &binLen);
                if (rc != 0) {
                    return rc; // Return error code if decimal conversion failed
                }
                CValue v(binLen); 
                v.setData(binaryDecimal, binLen);
                valVec.back().emplace_back(std::make_pair(dpfs_datatype_t::TYPE_DECIMAL, std::move(v)));
            }
            break;
        case dpfs_datatype_t::TYPE_TIMESTAMP:
        case dpfs_datatype_t::TYPE_DATE: {
            // TODO : parse timestamp and date value
            CValue v(valParams[2].size());
            v.setData(valParams[2].data(), valParams[2].size());
            valVec.back().emplace_back(std::make_pair(dpfs_datatype_t::TYPE_TIMESTAMP, std::move(v)));
            break;
        }
        case dpfs_datatype_t::TYPE_BOOL: {
            CValue bv(sizeof(bool));
            valParams[1] == "1" ? bv.setData("\x01", 1) : bv.setData("\x00", 1);
            // bv.setData(valParams[1] == "1" || valParams[1] == "true");
            valVec.back().emplace_back(std::make_pair(dpfs_datatype_t::TYPE_BOOL, std::move(bv)));
            break;  
        }
        default:
            return -EINVAL; // Unsupported data type
    }
    

    return 0;
}


int CParser::buildPlanForStmt(const std::string& osql, const TidbAstNode* stmt, CPlanHandle& out) {
    // Implementation for building plan for a single statement
    if (!stmt) {
        return -EINVAL; // Invalid statement node
    }

    if (stmt->type_name.len < prefixLen) {
        return -EINVAL; // Not a valid Tidb AST node type
    }

    int rc = 0;
    std::string_view typeName(stmt->type_name.data + prefixLen, stmt->type_name.len - prefixLen);


    // TODO : build different execution plan for different statement types, currently just print the type name
    // if is ddl, commit immediately, if is dml, build the plan and return, the execution will be done in executor module
    if (typeName == "CreateTableStmt") {
        // Handle CreateTable statement
        std::cout << "Building plan for CreateTable statement" << std::endl;
        
        rc = buildCreatePlan(stmt, out);
    } else if (typeName == "SelectStmt") {
        // Handle Select statement
        std::cout << "Building plan for Select statement" << std::endl;
        rc = buildSelectPlan(osql, stmt, out);
    } else if (typeName == "DropTableStmt") {
        // Handle other statement types
        std::cout << "Building plan for DropTable statement" << std::endl;
        rc = buildDropPlan(stmt, out);
    } else if (typeName == "InsertStmt") {
        // Handle Insert statement
        std::cout << "Building plan for Insert statement" << std::endl;
        rc = buildInsertPlan(osql, stmt, out);
    } else if (typeName == "DeleteStmt") {
        // Handle Delete statement
        std::cout << "Building plan for Delete statement" << std::endl;
        rc = buildDeletePlan(osql, stmt, out);
    } else {
        return -EINVAL; // Unsupported statement type
    }

    if (rc != 0) {
        return rc; // Return error code if plan building failed
    }

    return 0;
}


int CParser::buildCreatePlan(const TidbAstNode* stmt, CPlanHandle& out) {
    
    if (!stmt) {
        return -EINVAL; // Invalid statement node
    }
    int rc = 0;
    bool ifNotExist = false;
    std::string schema;
    std::string tabName;


    // if not exist
    ifNotExist = stmt->fields[(int)createPlanMap::IfNotExists].value.b;

    // find schema and table name
    auto& tabNode = stmt->fields[(int)createPlanMap::Table].value.node;
    schema = std::string(tabNode->fields[0].value.object.fields[0].value.str.data, 
                         tabNode->fields[0].value.object.fields[0].value.str.len);

    if (schema == "") {
        // schema = "NULLID"; // default schema
        schema = usr.username;
    }

    tabName = std::string(tabNode->fields[1].value.object.fields[0].value.str.data,
                         tabNode->fields[1].value.object.fields[0].value.str.len);

    // table init struct
    CCollectionInitStruct cis;
    cis.name = tabName;

    // table in memory
    CCollection coll(dataSvc.m_diskMan, dataSvc.m_page);
    rc = coll.initialize(cis);
    if (rc != 0) {
        return rc; // Return error code if collection initialization failed
    }


    // coll.addCol("pk", dpfs_datatype_t::TYPE_BIGINT, sizeof(int64_t), 0, static_cast<uint8_t>(CColumn::constraint_flags::PRIMARY_KEY));
    

    // find table columns
    const TidbAstArray& colArray = stmt->fields[(int)createPlanMap::Cols].value.array;
    for(int i = 0; i < colArray.len; ++i) {
        // std::vector<std::string> colParams;
        // colParams.clear();
        const TidbAstNode* colDef = colArray.items[i].node;
        // will add column to collection in parsColumn function.
        rc = parsColumn(colDef, coll);
        if (rc != 0) {
            return rc; // Return error code if column parsing failed
        }
    }

    // find table constraints
    const TidbAstArray& constraintArray = stmt->fields[(int)createPlanMap::Constraints].value.array;
    for (int i = 0; i < constraintArray.len; ++i) {
        const TidbAstNode* constraintDef = constraintArray.items[i].node;

        rc = parsConstraint(constraintDef, coll);
        if (rc != 0) {
            return rc; // Return error code if constraint parsing failed
        }
    }
    
    // find table index
    const TidbAstArray& indexArray = stmt->fields[(int)createPlanMap::SplitIndex].value.array;
    for (int i = 0; i < indexArray.len; ++i) {
        const TidbAstNode* indexDef = indexArray.items[i].node;
        // init index info 
        rc = parsIndex(indexDef, coll);
        if (rc != 0) {
            return rc; 
        }
    }

    #ifdef PARSER_DEBUG
    cout << "Create Table: " << schema << "." << tabName << endl;
    cout << "If Not Exists: " << (ifNotExist ? "Yes" : "No") << endl;
    cout << "struct : " << coll.printStruct() << endl;
    #endif
    
    rc = this->dataSvc.createTable(usr, schema, coll, out);
    if (rc != 0) {
        this->dataSvc.m_log.log_error("Failed to create table in data service, rc=%d\n", rc);
        return rc; // Return error code if table creation failed
    }
    this->dataSvc.m_log.log_inf("Successfully created table in data service, schema=%s, table=%s\n", schema.c_str(), tabName.c_str());
    return 0;
}

static int buildTableTree(const TidbAstNode* fromNode, CTableTree& out) {
    // TODO

    // table refs
    // fromNode->fields[0];
    enum tabrefMap {
        Left = 0,
        Right,
        Tp,
        On,
        Using,
        NaturalJoin,
        StraightJoin,
        ExplicitParens
    };


    fromNode->fields[0].value.node->fields[tabrefMap::Left];

    auto* nameNode = fromNode->fields[0].value.node->fields[tabrefMap::Left].value.node->fields[0/* source */].value.node;
    out.schemaName = nameNode->fields[0].value.object.fields[0].value.str.data;
    out.tableName = nameNode->fields[1].value.object.fields[0].value.str.data;

    #ifdef PARSER_DEBUG
    cout << "Table Tree - Schema: " << out.schemaName << ", Table: " << out.tableName << endl;
    #endif

    return 0;
}

/*
    @param whereNode: AST node of the WHERE clause
    @param out: vector to store the sequence of conditions extracted from the condition tree
    @return 0 on success, else on failure
    @note convert a condition tree to sequence of conditions.
*/
static int buildConditionSequence(const TidbAstNode* whereNode, CWhereSeq& out) {
    // TODO



    return 0;
}

int CParser::buildSelectPlan(const std::string& osql, const TidbAstNode* stmt, CPlanHandle& out) {
    // DOING
    int rc = 0;

    

    // parseName only support one table for now
    CTableTree tableTree;
    rc = buildTableTree(stmt->fields[static_cast<int>(selectPlanMap::From)].value.node, tableTree);
    if (rc != 0) {
        return rc; // Return error code if table tree building failed
    }

    

    // 构建可用的查询序列，查询时针对每行进行过滤 dpfsdata.cpp:82
    CWhereSeq whereSeq;
    rc = buildConditionSequence(stmt->fields[static_cast<int>(selectPlanMap::Where)].value.node, whereSeq);
    if (rc != 0) {
        return rc; // Return error code if condition sequence building failed
    }

    rc = dataSvc.resortWhereSequence(tableTree.schemaName, tableTree.tableName, whereSeq, out);
    if (rc != 0) {
        return rc; // Return error code if where sequence resorting failed
    }

    rc = dataSvc.makeTmpCollection(whereSeq.conditionSeq[0], out);
    if (rc != 0) {
        return rc; // Return error code if temporary collection creation failed
    }


    return -ENOTSUP; // Not supported yet
    return 0;
}

int CParser::buildDropPlan(const TidbAstNode* stmt, CPlanHandle& out) {
    //TODO
    return -ENOTSUP; // Not supported yet
    return 0;
}

int CParser::buildInsertPlan(const std::string& osql, const TidbAstNode* stmt, CPlanHandle& out) {

    
    int rc = 0;
    // parseName only support one table for now
    std::string schema(
        stmt->fields[static_cast<int>(insertPlanMap::Table)].value.node->fields[0].value.node->fields[0].value.node->fields[0].value.node->fields[0].value.object.fields[0].value.str.data, 
        stmt->fields[static_cast<int>(insertPlanMap::Table)].value.node->fields[0].value.node->fields[0].value.node->fields[0].value.node->fields[0].value.object.fields[0].value.str.len
    );
    std::string tabName(
        stmt->fields[static_cast<int>(insertPlanMap::Table)].value.node->fields[0].value.node->fields[0].value.node->fields[0].value.node->fields[1].value.object.fields[static_cast<int>(nameCase::Original)].value.str.data, 
        stmt->fields[static_cast<int>(insertPlanMap::Table)].value.node->fields[0].value.node->fields[0].value.node->fields[0].value.node->fields[1].value.object.fields[static_cast<int>(nameCase::Original)].value.str.len
    );


    #ifdef PARSER_DEBUG

    cout << "Insert Table: " << schema << "." << tabName << endl;
    #endif

    /*
        Array Length: 4
    Array Item 1:
|       Node Type: github.com/pingcap/tidb/pkg/parser/ast.ColumnName
|       Position: 0
|       Field Count: 3
|       Field 1 Name: Schema
|   |       Object Type: github.com/pingcap/tidb/pkg/parser/ast.CIStr
|   |       Field Count: 2
|   |       object Field 1 Name: O
|   |        kind : 5
|   |        str value :
|   |       object Field 2 Name: L
|   |        kind : 5
|   |        str value :
|   |       ,
|       Field 2 Name: Table
|   |       Object Type: github.com/pingcap/tidb/pkg/parser/ast.CIStr
|   |       Field Count: 2
|   |       object Field 1 Name: O
|   |        kind : 5
|   |        str value :
|   |       object Field 2 Name: L
|   |        kind : 5
|   |        str value :
|   |       ,
|       Field 3 Name: Name
|   |       Object Type: github.com/pingcap/tidb/pkg/parser/ast.CIStr
|   |       Field Count: 2
|   |       object Field 1 Name: O
|   |        kind : 5
|   |        str value : A
|   |       object Field 2 Name: L
|   |        kind : 5
|   |        str value : a
|   |       ,

    */
    
    std::vector<std::string> planCols;
    planCols.reserve(stmt->fields[static_cast<int>(insertPlanMap::Columns)].value.array.len);

    // parseColumns, if the column list is empty, means insert into all columns, otherwise insert into specified columns
    if (stmt->fields[static_cast<int>(insertPlanMap::Columns)].value.array.len) {
        // from an array
        for (int i = 0; i < stmt->fields[static_cast<int>(insertPlanMap::Columns)].value.array.len; ++i) {
            const TidbAstValue& colVal = stmt->fields[static_cast<int>(insertPlanMap::Columns)].value.array.items[i].node->fields[static_cast<int>(colNamefieldType::Name)].value.object.fields[static_cast<int>(nameCase::Original)].value; // get column name from ColumnName node
            // std::string colName(colVal.value.str.data, colVal.value.str.len);
            planCols.emplace_back(colVal.str.data, colVal.str.len);
            #ifdef PARSER_DEBUG
            cout << "Insert Column: " << planCols.back() << endl;
            #endif
        }
    }

    // int planColSize = 0;
    // int planColSequence[MAX_COL_NUM];
    // CFixLenVec<int, int, MAX_COL_NUM> planColVec(planColSequence, planColSize);
    


    // parseValues();
    rc = dataSvc.createInsertPlan(osql, schema, tabName, planCols, out);
    if (rc != 0) {
        this->dataSvc.m_log.log_error("Failed to create insert plan in data service, rc=%d", rc);
        return rc; // Return error code if insert plan creation failed
    }

    // TODO
    // parse values and insert the data
    // maybe more than one row to insert, so it's an array of array
    std::vector<std::vector<std::pair<dpfs_datatype_t, CValue>>> valVec;
    
    // Lists

    auto& lists = stmt->fields[static_cast<int>(insertPlanMap::Lists)].value.array;
    valVec.reserve(lists.len);


    // for each row tp insert
    for (int i = 0; i < lists.len; ++i) {
        valVec.emplace_back();

        std::vector<std::string> colParams;
        std::vector<std::string> valParams;
        bool negative = false;
        bool hasUnarrNode = false;
        

        const TidbAstNode* colValNode = nullptr;
        // for each column
        for (int j = 0; j < lists.items[i].array.len; ++j) {
            negative = false;
            if (lists.items[i].array.items[j].node)
                colValNode = lists.items[i].array.items[j].node;

            /*
                op
                V
                    {
                        exprnode
                    }

                exprNode
            
            */

            if (memcmp(colValNode->fields[0].name.data, "Op", 2) == 0) {
                // it's an operator node, currently only support unary minus for negative number
                if (colValNode->fields[0].value.i64 == 12) { // UnaryMinus
                    negative = true;
                }
                colValNode = colValNode->fields[1].value.node; // move to the actual value node
            }

            auto& typeExpr = colValNode->fields[0].value.object.fields[0].value.typed.inner->str;
            auto& datum = colValNode->fields[1].value.typed.inner->str;

            string_view typeExprView(typeExpr.data, typeExpr.len);
            rc = parsParamStr(typeExprView, colParams);
            if (rc != 0) {
                return rc; // Return error code if parameter parsing failed
            }

            string_view datumView(datum.data, datum.len);
            rc = parsParamStr(datumView, valParams);
            if (rc != 0) {
                return rc; // Return error code if parameter parsing failed
            }

            /*


                0 -> type (how to parse?)      
                    8 => pointer
                    5 => string
                    1 => int/bigint
                    4 => double/float
                1 -> int value
                2 -> string value


                // need convert to mysql_decimal
                3 -> pointer value (for binary data or large data)
            */

            #ifdef PARSER_DEBUG
            cout << "Column Parameters for value " << j + 1 << ": " << endl;
            for (size_t k = 0; k < colParams.size(); ++k) {
                cout << "  Param " << k + 1 << ": \"" << colParams[k] << "\"" << endl;
            }
            cout << "Value Parameters for value " << j + 1 << ": " << endl;
            for (size_t k = 0; k < valParams.size(); ++k) {
                if (negative && k == 1) { // if it's a negative number, add '-' before the value
                    cout << "  Param " << k + 1 << ": \"-" << valParams[k] << "\"" << endl;
                } else {
                    cout << "  Param " << k + 1 << ": \"" << valParams[k] << "\"" << endl;
                }
            }
            #endif

            rc = pushValue(valVec, colParams, valParams, negative);
            if (rc != 0) {
                return rc; // Return error code if pushing value failed
            }

            // if (colTp == dpfs_datatype_t::TYPE_DECIMAL) {
            //     uint64_t ptr = 0;
            //     std::string_view decPtrView(valParams[4].data() + 2, valParams[4].size() - 2);
            //     ptr = std::stoull(decPtrView.data(), nullptr, 16);
            //     cout << "Decimal value pointer: " << hex << ptr << dec << endl;

            //     char binaryDecimal[32];
            //     int binLen = 0;
            //     rc = deccvt::tibinary2mybinary(reinterpret_cast<const char*>(ptr), (uint8_t*)binaryDecimal, 32, negative, &binLen);
            //     if (rc != 0) {
            //         return rc; // Return error code if decimal conversion failed
            //     }
            //     CValue v(binLen); 
            //     v.setData(binaryDecimal, binLen);
            //     valVec.back().emplace_back(std::move(v));
            // }
        }



    }

    #ifdef PARSER_DEBUG
    cout << "Parsed Values: " << endl;
    for (size_t i = 0; i < valVec.size(); ++i) {
        cout << "Row " << i + 1 << ": " << endl;
        for (size_t j = 0; j < valVec[i].size(); ++j) {
            printMemory(valVec[i][j].second.data, valVec[i][j].second.len);
            cout << endl;
        }
    }
    #endif

    // TODO :: insert the values

    rc = dataSvc.planInsert(out.plan, &valVec);
    if (rc != 0) {
        this->dataSvc.m_log.log_error("Failed to plan insert in data service, rc=%d", rc);
        return rc; // Return error code if insert planning failed
    }

    #ifdef PARSER_DEBUG

    // test insert result by loading the collection and print the struct
    const bidx& cbid = out.plan.planObjects[0].collectionBidx; // get collection bidx

    CCollection coll(dataSvc.m_diskMan, dataSvc.m_page);

    rc = coll.loadFrom(cbid);
    if (rc != 0) {
        dataSvc.m_log.log_error("Failed to load collection for executing insert plan, rc=%d\n", rc);
        return rc;
    }

    std::string rest = coll.printStruct();
    cout << "Collection struct after insert: " << rest << endl;

    // coll.m_btreeIndex->printTree();

    #endif

    return 0;

}

int CParser::buildDeletePlan(const std::string& osql, const TidbAstNode* stmt, CPlanHandle& ph) {
    //TODO

    return -ENOTSUP; // Not supported yet
    return 0;
}