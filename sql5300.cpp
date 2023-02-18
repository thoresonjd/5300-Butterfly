/**
 * @file sql5300.cpp - main entry for the relation manager's SQL shell
 * @authors Kevin Lundeen, Justin Thoreson
 * @see "Seattle University, CPSC5300, Winter 2023"
 */

#include <cstdlib>
#include <iostream>
#include <string>
#include "db_cxx.h"
#include "ParseTreeToString.h"
#include "SQLExec.h"
#include "tests.h"

using namespace std;
using namespace hsql;

DbEnv* _DB_ENV; // Global DB environment
const u_int32_t ENV_FLAGS = DB_CREATE | DB_INIT_MPOOL;
const std::string TEST = "test", QUIT = "quit";

/**
 * Establishes a database environment
 * @param envDir The database environment directory
 */
void initDbEnv(string);

/**
 * Runs the SQL shell loop and listens for queries
 */
void runSQLShell();

/**
 * Processes a single SQL query
 * @param sql A SQL query (or queries) to process
 */
void handleSQL(string);

/**
 * Processes SQL statements within a parsed query
 * @param parsedSQL A pointer to a parsed SQL query
 */
void handleStatements(SQLParserResult*);

/**
 * Main entry point of the sql5300 program
 * @args dbenvpath  the path to the BerkeleyDB database environment
 */
int main(int argc, char** argv) {
    if (argc != 2) {
        cerr << "USAGE: " << argv[0] << " [db_environment]\n";
        return EXIT_FAILURE;
    }
    initDbEnv(argv[1]);
    runSQLShell();
    return EXIT_SUCCESS;
}

void initDbEnv(string envHome) {
    cout << "(sql5300: running with database environment at " << envHome << ")" << endl;
    _DB_ENV = new DbEnv(0U);
    _DB_ENV->set_message_stream(&cout);
    _DB_ENV->set_error_stream(&cerr);
    try {
        _DB_ENV->open(envHome.c_str(), ENV_FLAGS, 0);
    } catch (DbException& e) {
        cerr << "(sql5300: " << e.what() << ")" << endl;
        exit(EXIT_FAILURE);
    }
    initialize_schema_tables();
}

void runSQLShell() {
    std::string sql = "";
    while (sql != QUIT) {
        std::cout << "SQL> ";
        std::getline(std::cin, sql);
        handleSQL(sql);
    }
}

void handleSQL(std::string sql) {
    if (sql == QUIT || !sql.length()) return;
    SQLParserResult* const parsedSQL = SQLParser::parseSQLString(sql);
    if (parsedSQL->isValid())
        handleStatements(parsedSQL);
    else if (sql == TEST) {
        cout << "test_heap_storage: " << (test_heap_storage() ? "Passed" : "Failed") << endl;
        cout << "test_sql_exec: " << (test_sql_exec() ? "Passed" : "Failed") << endl;
    } else
        cerr << "invalid SQL: " << sql << endl << parsedSQL->errorMsg() << endl;
    delete parsedSQL;
}

void handleStatements(hsql::SQLParserResult* parsedSQL) {
    size_t nStatements = parsedSQL->size();
    for (size_t i = 0; i < nStatements; ++i) {
        const SQLStatement* statement = parsedSQL->getStatement(i);
        try {
            cout << ParseTreeToString::statement(statement) << endl;
            QueryResult* result = SQLExec::execute(statement);
            cout << *result << endl;
            delete result;
        } catch (SQLExecError& e) {
            cerr << "Error: " << e.what() << endl;
        }
    }
}