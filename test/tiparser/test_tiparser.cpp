
#include <tiparser/tidb_parser.h>
#include <iostream>
#include <cstring>


using namespace std;

void printNode(const TidbAstNode* ast, std::string indent = "|");
void printArray(const TidbAstArray& arr, std::string indent = "|");

std::string SQL = "insert into qwer.asdf(a,b,c,d,e,f,g) values(true, -1.23e4, -2, -1.2, 'qwer', '2025-11-25 08:05:01', -1.23e50)"; //"INSERT INTO TEST1(a,B,C,D,E,F) VALUES(-11.8, -11.8, 'qwer', 1, -1, -5000000000);";

/*              |    ?                     |    | 3 |
0xc0001563c0:   0x0a    0x03    0x03    0x00    0x03    0x00    0x00    0x00
0xc0001563c8:   0x00    0x65    0xcd    0x1d    0x40    0xa7    0xdc    0x1d
                |500000000                 |    |    501                     |
*/

/*
    0xc00003e3f0 => 12.5  => 1100 0000 0000 0000 0000 0011 1110 0011 1111 0000
    0xc00003e3f0 => -12.5
    0xc000156300 => 13.5
    0xc0001c2300 => 14.5
    0xc00003e420 => 15.5
    0xc000156300 => 16.5
*/

// std::string SQL = "CREATE TABLE QWeR.TEST112 (\
// A INT NOT NULL, \
// C_CHAR CHAR(4) NOT NULL UNIQUE, \
// B VARCHAR(255) NOT NULL, \
// C DOUBLE DEFAULT 12.5, \
// D DECIMAL(9,5), \
// E BOOL NOT NULL, \
// F BINARY(10), \
// G VARBINARY(255), \
// I FLOAT NOT NULL UNIQUE, \
// K TIMESTAMP, \
// L DATE, \
// M TIME, \
// UNIQUE (D,E), \
// PRIMARY KEY(A,E)\
// );";

void printInner(const TidbAstValue* inner, std::string indent = "|") {
    cout << indent << "data : " << inner->str.data << endl;
}

void printObject(const TidbAstObject& obj, std::string indent = "|") {
    cout << indent << "Object Type: " << obj.type_name.data << endl;
    cout << indent << "Field Count: " << obj.field_count << endl;

    // for each field
    for (int i = 0; i < obj.field_count; ++i) {
        cout << indent << "object Field " << i + 1 << " Name: " << obj.fields[i].name.data << endl;
        if (obj.fields[i].value.node != nullptr) {
            printNode(obj.fields[i].value.node, "|   " + indent);
        }
        cout << indent << " kind : " << obj.fields[i].value.kind << endl;
        if (obj.fields[i].value.kind == TIDB_AST_STRING) {
            cout << indent << " str value : " << obj.fields[i].value.str.data << endl;
        } else if (obj.fields[i].value.kind == TIDB_AST_BOOL) {
            cout << indent << " bool value : " << obj.fields[i].value.b << endl;
        } else if (obj.fields[i].value.kind == TIDB_AST_I64) {
            cout << indent << " i64 value : " << obj.fields[i].value.i64 << endl;
        } else if (obj.fields[i].value.kind == TIDB_AST_U64) {
            cout << indent << " u64 value : " << obj.fields[i].value.u64 << endl;
        } else if (obj.fields[i].value.kind == TIDB_AST_F64) {
            cout << indent << " f64 value : " << obj.fields[i].value.f64 << endl;
        } else if (obj.fields[i].value.kind == TIDB_AST_TYPED) {
            cout << indent << " typed value : " << obj.fields[i].value.typed.inner->str.data << endl;
        }
        
    }

    cout << indent << "," << endl;
}

void printNode(const TidbAstNode* ast, std::string indent) {
    cout << indent << "Node Type: " << ast->type_name.data << endl;
    cout << indent << "Position: " << ast->pos << endl;
    cout << indent << "Field Count: " << ast->field_count << endl;

    // for each field
    for (int i = 0; i < ast->field_count; ++i) {
        cout << indent << "Field " << i + 1 << " Name: " << ast->fields[i].name.data << endl;
        
        if (ast->fields[i].value.node != nullptr) {
            printNode(ast->fields[i].value.node, "|   " + indent);
        } else if (ast->fields[i].value.object.field_count) {
            printObject(ast->fields[i].value.object, "|   " + indent);
        } else if (ast->fields[i].value.typed.inner) {
            printInner(ast->fields[i].value.typed.inner, "|   " + indent);
        } else if (ast->fields[i].value.array.len) {
            printArray(ast->fields[i].value.array, "|   " + indent);
        } else {
            cout << indent << "Value Kind: " << ast->fields[i].value.kind << endl;
            switch (ast->fields[i].value.kind) {
                case TIDB_AST_NULL:
                    cout << indent << "Value: null" << endl;
                    break;
                case TIDB_AST_BOOL:
                    cout << indent << "Value: " << (ast->fields[i].value.b ? "true" : "false") << endl;
                    break;
                case TIDB_AST_I64:
                    cout << indent << "Value: " << ast->fields[i].value.i64 << endl;
                    break;
                case TIDB_AST_U64:
                    cout << indent << "Value: " << ast->fields[i].value.u64 << endl;
                    break;
                case TIDB_AST_F64:
                    cout << indent << "Value: " << ast->fields[i].value.f64 << endl;
                    break;
                case TIDB_AST_STRING:
                    cout << indent << "Value: \"" << ast->fields[i].value.str.data << "\"" << endl;
                    break;
                case TIDB_AST_BYTES:
                    cout << indent << "Value: bytes(len=" << ast->fields[i].value.bytes.len << ")" << endl;
                    break;
                default:
                    cout << indent << "Value: <unknown kind=" << ast->fields[i].value.kind << ">" << endl;
            }
        }
    }

    cout << indent << "," << endl;
}

void printArray(const TidbAstArray& arr, std::string indent) {
    cout << indent << "Array Length: " << arr.len << endl;
    for (int i = 0; i < arr.len; ++i) {
        cout << indent << "Array Item " << i + 1 << ":" << endl;
        if (arr.items[i].node != nullptr) {
            printNode(arr.items[i].node, "|   " + indent);
        } else if (arr.items[i].object.field_count) {
            printObject(arr.items[i].object, "|   " + indent);
        } else if (arr.items[i].array.len) {
            printArray(arr.items[i].array, "|   " + indent);
        }
    }
    cout << indent << "," << endl;
}

int main() {


    TidbParseResult result;
    // std::string sql = "SELECT * FROM users WHERE age >= 30;";
    std::string sql = SQL;

    TidbParseResult* rst = TidbParseSQL((char*)sql.c_str());

    if (rst->ok) {
        std::cout << "SQL parsed successfully!" << std::endl;
        std::cout << "Number of statements: " << rst->stmt_count << std::endl;
    } else {
        std::cout << "SQL parsing failed: " << rst->err.data << std::endl;
    }

    const TidbAstNode** ast = rst->statements;
    for(auto i = 0; i < rst->stmt_count; ++i) {
        std::cout << "Statement " << i + 1 << " AST Type: " << ast[i]->type_name.data << std::endl;
    
        for(auto j = 0; j < ast[i]->field_count; ++j) {
            std::cout << "  Field " << j + 1 << ": " << ast[i]->fields[j].name.data << std::endl;
            if (memcmp(ast[i]->fields[j].name.data, "Lists", ast[i]->fields[j].name.len) == 0) {
                cout << " Found, printing its AST node:" << endl;
                if (ast[i]->fields[j].value.node != nullptr) {
                    printNode(ast[i]->fields[j].value.node, "    ");
                } else if (ast[i]->fields[j].value.object.field_count) {
                    printObject(ast[i]->fields[j].value.object, "    ");
                } else if (ast[i]->fields[j].value.array.len) {
                    printArray(ast[i]->fields[j].value.array, "    ");
                }
                // return 0;
            }
        }
    }
    

    return 0;

}
