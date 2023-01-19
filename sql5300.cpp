// AUTHOR:      Bobby Brown rbrown3 & Denis Rajic drajic
// PROGRAM:     sql5300.cpp for 5300-BUTTERFLY
// DATE:        Created 01/11/2023
// PURPOSE:     This is a program that runs from the command line and prompts
//              the user for SQL statements and then executes them one at a
//              time.
// INPUT:       The user enters SQL statements via the command line.
// OUTPUT:      Displays the parsed SQL statements from the parse tree to the
//              screen.
//

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <vector>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace hsql;
using namespace std;

const char *DATABASE = "database.db";
const unsigned int BLOCK_SZ = 4096;

// Configures the database environment.
// @param The string of the environment variable
void configureDB(string);

// Executes the SQL statement from the given parse tree.
string execute(const SQLStatement*);

// Unparses the SELECT statement from the given parse tree to a string.
// @param A pointer to a SELECT statement
// @return The string representation of the statement
string unparseSelect(const SelectStatement*);

// Unparses the CREATE statement from the given parse tree to a string.
// @param A pointer to a CREATE statement
// @return The string representation of the statement
string unparseCreate(const CreateStatement*) ;

// Unparses an Expr type into a string.
// @param A pointer to an Expr statement
// @return The string representation of the expression
string unparseExpression(Expr*);

// Unparses a TableRef type into a string.
// @param A pointer to a TableRef statement
// @return The string representation of the TableRef
string unparseTableRef(TableRef*);

// Unparses a JoinDefinition type into a string.
// @param A pointer to a JoinDefinition statement
// @return The string representation of the JoinDefinition
string unparseJoin(JoinDefinition*);

// Unparses a ColumnDefinition type into a string.
// @param A pointer to a ColumnDefinition statement
// @return The string representation of the ColumnDefinition
string unparseColumn(ColumnDefinition*);


int main(int argc, char *argv[])
{
    // Get DB directory from command line
    string dbDirectory = argv[1];

    // Call configureDB to initialize and create database
    configureDB(dbDirectory);

    // Prompt user for sql statement
    while (true) {      // May want to switch while loop.. run while input != quit?
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

void configureDB(string dir)
{
    // Initialize environment
    DbEnv env(0U);
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    env.open(dir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    // Establish database
    Db db(&env, 0);
    db.set_message_stream(env.get_message_stream());
    db.set_error_stream(env.get_error_stream());
    db.set_re_len(BLOCK_SZ); // Set record length to 4K
    // Erases existing database
    db.open(NULL, DATABASE, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644);
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

string unparseCreate(const CreateStatement* sqlStatement) 
{
    if (sqlStatement->type != CreateStatement::kTable)
    {
        return "nothing";
    }

    string statement = "CREATE TABLE ";

    // Add " (" to statement
    statement.append(sqlStatement->tableName).append(" (");

    // Get size, based on number of columns
    size_t columnSize = sqlStatement->columns->size();

    // Loop through statement for number of columns
    for (size_t i = 0; i < columnSize; i++)
    {

        // Get column definition
        ColumnDefinition *col = sqlStatement->columns->at(i);

        // Convert column definition to string and append to statement
        statement.append(unparseColumn(col));

        if (i < columnSize - 1)
        {
            statement.append(", ");
        }
    }

    statement.append(")");

    return statement;
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

string unparseColumn(ColumnDefinition* col)
{
    string column(col->name);

    switch (col->type)
    {
        case ColumnDefinition::DOUBLE:
            column.append(" DOUBLE");
            break;
        case ColumnDefinition::INT:
            column.append(" INT");
            break;
        case ColumnDefinition::TEXT:
            column.append(" TEXT");
            break;
        default:
            column.append(" ...");
            break;
    }

    return column;
}