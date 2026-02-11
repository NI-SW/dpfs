#include <parser/dpfsparser.hpp>
#include <tiparser/tidb_parser.h>


#ifdef PARSER_DEBUG
using namespace std;

#endif


enum class createPlanMap : int {
    IfNotExists = 0,
    TemporaryKeyword,
    OnCommitDelete,
    Table,
    ReferTable,
    Cols,
    Constraints,
    SplitIndex,
    Options,
    Partition,
    OnDuplicate,
    Select
};

// translate from 
// github.com/pingcap/tidb/pkg/parser@v0.0.0-20260205034954-d4f0daa8570e/mysql/type.go

enum class planColTypes : uint8_t {
	TypeUnspecified = 0,
	TypeTiny        = 1, // TINYINT
	TypeShort       = 2, // SMALLINT
	TypeLong        = 3, // INT
	TypeFloat       = 4,
	TypeDouble      = 5,
	TypeNull        = 6,
	TypeTimestamp   = 7,
	TypeLonglong    = 8, // BIGINT
	TypeInt24       = 9, // MEDIUMINT
	TypeDate        = 10,
	/* TypeDuration original name was TypeTime, renamed to TypeDuration to resolve the conflict with Go type Time.*/
	TypeDuration = 11,
	TypeDatetime = 12,
	TypeYear     = 13,
	TypeNewDate  = 14,
	TypeVarchar  = 15,
	TypeBit      = 16,

	TypeJSON       = 0xf5,
	TypeNewDecimal = 0xf6,
	TypeEnum       = 0xf7,
	TypeSet        = 0xf8,
	TypeTinyBlob   = 0xf9,
	TypeMediumBlob = 0xfa,
	TypeLongBlob   = 0xfb,
	TypeBlob       = 0xfc,
	TypeVarString  = 0xfd,
	TypeString     = 0xfe, /* TypeString is char type */
	TypeGeometry   = 0xff,
	TypeTiDBVectorFloat32 = 0xe1
};

/*
// ColumnOption is used for parsing column constraint info from SQL.
GO TYPE
type ColumnOption struct {
	node

	Tp ColumnOptionType
	// Expr is used for ColumnOptionDefaultValue/ColumnOptionOnUpdateColumnOptionGenerated.
	// For ColumnOptionDefaultValue or ColumnOptionOnUpdate, it's the target value.
	// For ColumnOptionGenerated, it's the target expression.
	Expr ExprNode
	// Stored is only for ColumnOptionGenerated, default is false.
	Stored bool
	// Refer is used for foreign key.
	Refer       *ReferenceDef
	StrValue    string
	AutoRandOpt AutoRandomOption
	// Enforced is only for Check, default is true.
	Enforced bool
	// Name is only used for Check Constraint name.
	ConstraintName      string
	PrimaryKeyTp        PrimaryKeyType
	SecondaryEngineAttr string
}
*/

enum class planColOptions : uint8_t {
    Tp = 0,
    Expr,
    Stored,
    Refer,
    StrValue,
    AutoRandOpt,
    Enforced,
    ConstraintName,
    PrimaryKeyTp,
    SecondaryEngineAttr
};

// ColumnOption types.
enum class planColOptionTp {
	ColumnOptionNoOption = 0,
	ColumnOptionPrimaryKey,
	ColumnOptionNotNull,
	ColumnOptionAutoIncrement,
	ColumnOptionDefaultValue,
	ColumnOptionUniqKey,
	ColumnOptionNull,
	ColumnOptionOnUpdate, // For Timestamp and Datetime only.
	ColumnOptionFulltext,
	ColumnOptionComment,
	ColumnOptionGenerated,
	ColumnOptionReference,
	ColumnOptionCollate,
	ColumnOptionCheck,
	ColumnOptionColumnFormat,
	ColumnOptionStorage,
	ColumnOptionAutoRandom,
	ColumnOptionSecondaryEngineAttribute
};

enum class planColFlag : uint32_t {
// Flag information.
	NotNullFlag         = 1 << 0,  /* Field can't be NULL */
	PriKeyFlag          = 1 << 1,  /* Field is part of a primary key */
	UniqueKeyFlag       = 1 << 2,  /* Field is part of a unique key */
	MultipleKeyFlag     = 1 << 3,  /* Field is part of a key */
	BlobFlag            = 1 << 4,  /* Field is a blob */
	UnsignedFlag        = 1 << 5,  /* Field is unsigned */
	ZerofillFlag        = 1 << 6,  /* Field is zerofill */
	BinaryFlag          = 1 << 7,  /* Field is binary   */
	EnumFlag            = 1 << 8,  /* Field is an enum */
	AutoIncrementFlag   = 1 << 9,  /* Field is an auto increment field */
	TimestampFlag       = 1 << 10, /* Field is a timestamp */
	SetFlag             = 1 << 11, /* Field is a set */
	NoDefaultValueFlag  = 1 << 12, /* Field doesn't have a default value */
	OnUpdateNowFlag     = 1 << 13, /* Field is set to NOW on UPDATE */
	PartKeyFlag         = 1 << 14, /* Intern: Part of some keys */
	NumFlag             = 1 << 15, /* Field is a num (for clients) */

	GroupFlag              = 1 << 15, /* Internal: Group field */
	UniqueFlag             = 1 << 16, /* Internal: Used by sql_yacc */
	BinCmpFlag             = 1 << 17, /* Internal: Used by sql_yacc */
	ParseToJSONFlag        = 1 << 18, /* Internal: Used when we want to parse string to JSON in CAST */
	IsBooleanFlag          = 1 << 19, /* Internal: Used for telling boolean literal from integer */
	PreventNullInsertFlag  = 1 << 20, /* Prevent this Field from inserting NULL values */
	EnumSetAsIntFlag       = 1 << 21, /* Internal: Used for inferring enum eval type. */
	DropColumnIndexFlag    = 1 << 22, /* Internal: Used for indicate the column is being dropped with index */
	GeneratedColumnFlag    = 1 << 23, /* Internal: TiFlash will check this flag and add a placeholder for this column */
	UnderScoreCharsetFlag  = 1 << 24, /* Internal: Indicate whether charset is specified by underscore like _latin1'abc' */
};



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
    return m_pParseResult ? 0 : -1;
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

int CParser::buildPlan() {
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
        rc = buildPlanForStmt(stmt);
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
    std::string_view typeView(typeStr.c_str() + typeStr.find('{') + 1); 


    std::vector<std::string> colParams;
    colParams.clear();
    colParams.reserve(6);

    for (int i = 0, j = 0; i < 6; ++i) {
        const char* start = typeView.data() + j;
        size_t sz = 0;

        for (; typeView[j] != ' ' && typeView[j] != '\0' && typeView[j] != '}'; ++j) {
            ++sz;
        }
        colParams.emplace_back(start, sz);
    
        if (typeView[j] == ' ') {
            ++j; // skip space
        }
    }

    int typeCode = std::stoi(colParams[0]);
    int unknown = std::stoul(colParams[1]);
    int len = std::stoul(colParams[2]); if (len == -1) len = 0;
    int scale = std::stoul(colParams[3]); if (scale == -1) scale = 0;

    bool isBinary = colParams[4] == "binary";
    dpfs_datatype_t colTp = dpfs_datatype_t::TYPE_NULL;
    colOption colOpt;
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

int CParser::buildPlanForStmt(const TidbAstNode* stmt) {
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
        rc = buildCreatePlan(stmt);
    } else if (typeName == "SelectStmt") {
        // Handle Select statement
        std::cout << "Building plan for Select statement" << std::endl;
        rc = buildSelectPlan(stmt);
    } else if (typeName == "DropTableStmt") {
        // Handle other statement types
        std::cout << "Building plan for DropTable statement" << std::endl;
        rc = buildDropPlan(stmt);
    } else if (typeName == "InsertStmt") {
        // Handle Insert statement
        std::cout << "Building plan for Insert statement" << std::endl;
        rc = buildInsertPlan(stmt);
    } else if (typeName == "DeleteStmt") {
        // Handle Delete statement
        std::cout << "Building plan for Delete statement" << std::endl;
        rc = buildDeletePlan(stmt);
    } else {
        return -EINVAL; // Unsupported statement type
    }

    if (rc != 0) {
        return rc; // Return error code if plan building failed
    }

    return 0;
}


int CParser::buildCreatePlan(const TidbAstNode* stmt) {
    
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
    
    rc = this->dataSvc.createTable(usr, schema, coll);
    if (rc != 0) {
        this->dataSvc.m_log.log_error("Failed to create table in data service, rc=%d", rc);
        return rc; // Return error code if table creation failed
    }
    this->dataSvc.m_log.log_inf("Successfully created table in data service, schema=%s, table=%s", schema.c_str(), tabName.c_str());
    return 0;
}



int CParser::buildSelectPlan(const TidbAstNode* stmt) {
    //TODO
    return -ENOTSUP; // Not supported yet
    return 0;
}

int CParser::buildDropPlan(const TidbAstNode* stmt) {
    //TODO
    return -ENOTSUP; // Not supported yet
    return 0;
}

int CParser::buildInsertPlan(const TidbAstNode* stmt) {
    //TODO
    return -ENOTSUP; // Not supported yet
    return 0;
}

int CParser::buildDeletePlan(const TidbAstNode* stmt) {
    //TODO

    return -ENOTSUP; // Not supported yet
    return 0;
}