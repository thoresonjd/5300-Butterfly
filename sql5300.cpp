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

using namespace std;
using namespace hsql;

const char *DATABASE = "database.db";
const unsigned int BLOCK_SZ = 4096;


string execute(SQLStatement sqlStatement) {
    return NULL;
}

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
        SQLParserResult* parseTree = SQLParser::parseSQLString(input);


        if (parseTree->isValid()) {
            // TODO: We need to go through the parseTree here and call execute() on the SQL statements
            cout << "Valid" << endl;
        } else {
            cout << "Invalid SQL: " << input << endl;
        }
    }

    return 0;
}