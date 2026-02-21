#pragma once

#include <cstdint>

enum class nameCase {
    Original = 0,
    Little
};

enum class planType {
	PLAN_NULL = 0,
	CreateTable,
	CreateIndex,
	Select,
	Insert,
	Delete,
	DropTable,
	DropIndex,
	Update,
	AlterTable,
};

enum class colNamefieldType : uint8_t {
    Schema = 0,
    Table,
    Name
};

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

enum class insertPlanMap : int {
    IsReplace = 0,
    IgnoreErr,
    Table,
    Columns,
    Lists,
    Setlist,
    Priority,
    OnDuplicate,
    Select,
    TableHints,
    PartitionNames
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


enum class parsedValueTp {
	INT = 1,
	STRING = 5,
	POINTER = 8
};


