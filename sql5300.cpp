// AUTHOR:      Bobby Brown rbrown3 & Denis Rajic drajic
// PROGRAM:     sql5300.cpp for 5300-BUTTERFLY
// DATE:        Created 01/11/2023
// PURPOSE:     
// INPUT:       
// PROCESS:     
// OUTPUT:      
//
#include <SQLParser.h>
#include "db_cxx.h"

using namespace hsql;
using namespace std;

const char *DATABASE = "database.db";
const unsigned int BLOCK_SZ = 4096;


string execute(const SQLStatement*);

string unparseSelect(const SelectStatement*);

string unparseCreate(const CreateStatement*) ;

string unparseExpression(Expr*);

string unparseTableRef(TableRef*);

string unparseJoin(JoinDefinition*);


int main(int argc, char *argv[])
{

    // Get DB directory from command line
    string dbDirectory = argv[1];

    // Initialize DB
	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(dbDirectory.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    Db db(&env, 0);
	db.set_message_stream(env.get_message_stream());
	db.set_error_stream(env.get_error_stream());
	db.set_re_len(BLOCK_SZ); // Set record length to 4K
	db.open(NULL, DATABASE, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there

    // Prompt user for sql statement
    while (true) {
        cout << "SQL> ";

        string input;
        getline(cin, input);

        if (input == "quit") {
            return 0;
        }

        // Parse sql statement
        SQLParserResult* const parseTree = SQLParser::parseSQLString(input);
        size_t ptSize = parseTree->size();


        if (parseTree->isValid()) {
            for (size_t i = 0; i < ptSize; i++) {
                const SQLStatement* sqlStatment = parseTree->getStatement(i);
                cout << execute(sqlStatment) << endl;
            }
        } else {
            cout << "Invalid SQL: " << input << endl;
        }
    }

    return 0;
}

string execute(const SQLStatement* sqlStatement) {
    string canonicalStatement = "";

    switch(sqlStatement->type()) {
        case kStmtCreate:
            return unparseCreate(dynamic_cast<const CreateStatement*>(sqlStatement));
            break;
        case kStmtSelect:
            return unparseSelect(dynamic_cast<const SelectStatement*>(sqlStatement));
            break;
        default:
            return "";
    }

}

string unparseCreate(const CreateStatement* sqlStatement) {
    return "";
}

string unparseSelect(const SelectStatement* sqlStatement) {
    // Unparse SELECT statement
    string statement = "SELECT ";
    size_t selectListSize = sqlStatement->selectList->size();
    for (size_t i = 0; i < selectListSize; i++) {
        statement += unparseExpression(sqlStatement->selectList->at(i));
        if (i < selectListSize - 1) {
            statement += ",";
        }
        statement += " ";
    }

    // Unparse FROM expression
    statement += "FROM ";
    statement += unparseTableRef(sqlStatement->fromTable);

    // Unparse WHERE expression
    if (sqlStatement->whereClause != NULL) {
        statement += " WHERE ";
        statement += unparseExpression(sqlStatement->whereClause);
    }

    return statement;
}

string unparseExpression(Expr* expr) {
    switch (expr->type) {
        case kExprLiteralFloat:
            return to_string(expr->fval);
        case kExprLiteralString:
            return expr->name;
        case kExprLiteralInt:
            return to_string(expr->ival);
        case kExprStar:
            return "*";
        case kExprColumnRef:{
            string tableName = expr->table == NULL ? "" : expr->table;
            string columnName = expr->name == NULL ? "" : expr->name;
            return tableName == "" ? columnName : tableName + "." + columnName;
        }
        default:
            return "";
    }
}

string unparseTableRef(TableRef* tableRef) {
    string tableExpression = "";
    switch(tableRef->type) {
        case kTableName:
            return tableRef->name;
        case kTableJoin:
            return unparseJoin(tableRef->join);
        case kTableCrossProduct: {
            size_t numTables = tableRef->list->size();
            for (size_t i = 0; i < numTables; i++) {
                tableExpression += unparseTableRef(tableRef->list->at(i));
                tableExpression += " ";
            }
            return tableExpression;
        }
        default:
            return "";
    }
}

string unparseJoin(JoinDefinition* joinDefinition) {
    // left table
    string joinStatement = unparseTableRef(joinDefinition->left);

    // add join type
    switch(joinDefinition->type) {
        case kJoinLeft:
            joinStatement += " LEFT JOIN ";
            break;
        default:
            return "";
    }

    // right table
    joinStatement += unparseTableRef(joinDefinition->right);

    // expression that tables are joined on
    joinStatement += " ON ";
    joinStatement += unparseExpression(joinDefinition->condition);
    return joinStatement;
}