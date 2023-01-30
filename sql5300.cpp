/**
 * @file        sql5300.cpp for 5300-BUTTERFLY
 *              This is a program that runs from the command line and prompts
 *              the user for SQL statements and then executes them one at a
 *              time.
 * @authors     Bobby Brown rbrown3 & Denis Rajic drajic
 * @date        Created 01/11/2023
 * @version     Milestone 1 & Milestone 2
 * 
 */ 

#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <string>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
#include "heap_storage.h"

using namespace hsql;
using namespace std;

const char *DATABASE = "database.db";
const unsigned int BLOCK_SZ = 4096;

// Global database environment variable for storage engine
DbEnv *_DB_ENV;

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

// Unparses an operator Expr* type into a string.
// @param A pointer to a Expr statement
// @return The string representation of the expression
string unparseOperatorExpr(Expr*);


int main(int argc, char *argv[])
{
    // Get DB directory from command line
    char* dbDirectory = argv[1];

    // Call configureDB to initialize and create database
    // Initialize environment
    DbEnv env(0U);
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    try {
        env.open(dbDirectory, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException &exc) {
        cerr << "(sql5300: " << exc.what() << ")";
        exit(1);
    }
    _DB_ENV = &env;

    // Display message to user
    cout << "(sql5300: running with database environment at " << dbDirectory << ")" << endl;

    // Prompt user for sql statement
    while (true) {      // May want to switch while loop.. run while input != quit?
        cout << "SQL> ";

        string input;
        getline(cin, input);

        if (input == "quit") {
            return EXIT_SUCCESS;
        }

        if (input == "test") {
            cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
            return EXIT_SUCCESS;
        
        }

        // Parse sql statement
        SQLParserResult* const parseTree = SQLParser::parseSQLString(input); // Probably need to delete parseTree?
        size_t ptSize = parseTree->size();

        // Check if parseTree is valid
        if (parseTree->isValid()) 
        {
            // Traverse parseTree, and execute each statement
            for (size_t i = 0; i < ptSize; i++) {
                const SQLStatement* sqlStatment = parseTree->getStatement(i);
                cout << execute(sqlStatment) << endl;
            }
        } else {
            cout << "Invalid SQL: " << input << endl;
        }
    }

    return EXIT_SUCCESS;
}

string execute(const SQLStatement* sqlStatement)
{
    // Call function based on passed in statement
    switch(sqlStatement->type()) {
        case kStmtCreate:
            return unparseCreate(dynamic_cast<const CreateStatement*>(sqlStatement));
        case kStmtSelect:
            return unparseSelect(dynamic_cast<const SelectStatement*>(sqlStatement));
        default:
            return "";
    }
}

string unparseCreate(const CreateStatement* sqlStatement) 
{
    // Check if passed in statement is not a kTable create statement
    if (sqlStatement->type != CreateStatement::kTable)
    {
        return "";
    }

    // String to define start of create statement
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

        // Add comma if there are still statements to unparse
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
    // String to define start of select statement
    string statement = "SELECT ";

    // Get size, based on the number of items being selected
    size_t selectListSize = sqlStatement->selectList->size();

    // Loop through statement for the size of the statement
    for (size_t i = 0; i < selectListSize; i++)
    {
        // Get select list item definition
        statement += unparseExpression(sqlStatement->selectList->at(i));
        
        // Add comma if there are still statements to unparse
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

string unparseExpression(Expr* expr)
{
    // Call function based on passed in Expr
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
        case kExprFunctionRef:
            return string(expr->name) + "?" + expr->expr->name;
        case kExprOperator:
            return unparseOperatorExpr(expr);
        default:
            return "";
    }
}

string unparseOperatorExpr(Expr* expr)
{
    // Check if passed in Expr is NULL
    if (expr == NULL) {
        return "null";
    }

    string operatorExpr = ""; // String to hold operator statement

    // Append expression statement to string
    operatorExpr += unparseExpression(expr->expr);

    // Append string based on expression operator type
    switch (expr->opType) {
        case Expr::AND:
            operatorExpr += "AND";
            break;
        case Expr::OR:
            operatorExpr += "OR";
            break;
        case Expr::SIMPLE_OP:
            operatorExpr += " ";
            operatorExpr += expr->opChar;
            break;
        default:
            operatorExpr += "";
    }

    // Check if expr2 is not null to continue unparsing expressions
    if (expr->expr2 != NULL) {
        operatorExpr += " " + unparseExpression(expr->expr2);
    }

    return operatorExpr;
}

string unparseTableRef(TableRef* tableRef) {
    string tableExpression = "";
    switch(tableRef->type) {
        case kTableName:
            tableExpression += tableRef->name;
            if (tableRef->alias != NULL) {
                tableExpression += string(" AS ") + tableRef->alias;
            }
            return tableExpression;
        case kTableJoin:
            return unparseJoin(tableRef->join);
        case kTableCrossProduct: {
            size_t numTables = tableRef->list->size();
            for (size_t i = 0; i < numTables; i++) {
                tableExpression += unparseTableRef(tableRef->list->at(i));
                if (i < numTables - 1) {
                   tableExpression += ",";
                }
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
        case kJoinCross:
        case kJoinInner:
            joinStatement += " JOIN ";
            break;
        default:
            return "";
    }

    // right table
    joinStatement += unparseTableRef(joinDefinition->right);

    // expression that tables are joined on
    if (joinDefinition->condition != NULL) {
        joinStatement += " ON ";
        joinStatement += unparseExpression(joinDefinition->condition);
    }
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