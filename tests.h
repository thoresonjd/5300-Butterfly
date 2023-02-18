/**
 * 
 */

#pragma once
#include <iostream>
#include <cstring>
#include "db_cxx.h"
#include "SlottedPage.h"
#include "HeapTable.h"
#include "SQLExec.h"
#include "ParseTreeToString.h"

/**
 * Print out given failure message and return false.
 * @param message reason for failure
 * @return false
 */
bool assertion_failure(std::string message, double x = -1, double y = -1) {
    std::cout << "FAILED TEST: " << message;
    if (x >= 0)
        std::cout << " " << x;
    if (y >= 0)
        std::cout << " " << y;
    std::cout << std::endl;
    return false;
}


/*
 * ****************************
 * Slotted page tests
 * ****************************
 */

/**
 * Testing function for SlottedPage.
 * @return true if testing succeeded, false otherwise
 */
bool test_slotted_page() {
    // construct one
    char blank_space[DbBlock::BLOCK_SZ];
    Dbt block_dbt(blank_space, sizeof(blank_space));
    SlottedPage slot(block_dbt, 1, true);

    // add a record
    char rec1[] = "hello";
    Dbt rec1_dbt(rec1, sizeof(rec1));
    RecordID id = slot.add(&rec1_dbt);
    if (id != 1)
        return assertion_failure("add id 1");

    // get it back
    Dbt *get_dbt = slot.get(id);
    std::string expected(rec1, sizeof(rec1));
    std::string actual((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back " + actual);

    // add another record and fetch it back
    char rec2[] = "goodbye";
    Dbt rec2_dbt(rec2, sizeof(rec2));
    id = slot.add(&rec2_dbt);
    if (id != 2)
        return assertion_failure("add id 2");

    // get it back
    get_dbt = slot.get(id);
    expected = std::string(rec2, sizeof(rec2));
    actual = std::string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back " + actual);

    // test put with expansion (and slide and ids)
    char rec1_rev[] = "something much bigger";
    rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after expanding put
    get_dbt = slot.get(2);
    expected = std::string(rec2, sizeof(rec2));
    actual = std::string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back after expanding put of 1 " + actual);
    get_dbt = slot.get(1);
    expected = std::string(rec1_rev, sizeof(rec1_rev));
    actual = std::string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back after expanding put of 1 " + actual);

    // test put with contraction (and slide and ids)
    rec1_dbt = Dbt(rec1, sizeof(rec1));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after contracting put
    get_dbt = slot.get(2);
    expected = std::string(rec2, sizeof(rec2));
    actual = std::string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back after contracting put of 1 " + actual);
    get_dbt = slot.get(1);
    expected = std::string(rec1, sizeof(rec1));
    actual = std::string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back after contracting put of 1 " + actual);

    // test del (and ids)
    RecordIDs *id_list = slot.ids();
    if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2)
        return assertion_failure("ids() with 2 records");
    delete id_list;
    slot.del(1);
    id_list = slot.ids();
    if (id_list->size() != 1 || id_list->at(0) != 2)
        return assertion_failure("ids() with 1 record remaining");
    delete id_list;
    get_dbt = slot.get(1);
    if (get_dbt != nullptr)
        return assertion_failure("get of deleted record was not null");

    // try adding something too big
    rec2_dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 10); // too big, but only because we have a record in there
    try {
        slot.add(&rec2_dbt);
        return assertion_failure("failed to throw when add too big");
    } catch (const DbBlockNoRoomError &exc) {
        // test succeeded - this is the expected path
    } catch (...) {
        // Note that this won't catch segfault signals -- but in that case we also know the test failed
        return assertion_failure("wrong type thrown when add too big");
    }

    // more volume
    std::string gettysburg = "Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal.";
    int32_t n = -1;
    uint16_t text_length = gettysburg.size();
    uint total_size = sizeof(n) + sizeof(text_length) + text_length;
    char* data = new char[total_size];
    *(int32_t *) data = n;
    *(uint16_t *) (data + sizeof(n)) = text_length;
    std::memcpy(data + sizeof(n) + sizeof(text_length), gettysburg.c_str(), text_length);
    Dbt dbt(data, total_size);
    std::vector<SlottedPage> page_list;
    BlockID block_id = 1;
    Dbt slot_dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
    slot = SlottedPage(slot_dbt, block_id++, true);
    for (int i = 0; i < 10000; i++) {
        try {
            slot.add(&dbt);
        } catch (DbBlockNoRoomError &exc) {
            page_list.push_back(slot);
            slot_dbt = Dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
            slot = SlottedPage(slot_dbt, block_id++, true);
            slot.add(&dbt);
        }
    }
    page_list.push_back(slot);
    for (const auto &slot : page_list) {
        RecordIDs *ids = slot.ids();
        for (RecordID id : *ids) {
            Dbt *record = slot.get(id);
            if (record->get_size() != total_size)
                return assertion_failure("more volume wrong size", block_id - 1, id);
            void *stored = record->get_data();
            if (std::memcmp(stored, data, total_size) != 0)
                return assertion_failure("more volume wrong data", block_id - 1, id);
            delete record;
        }
        delete ids;
        delete[] (char *) slot.block.get_data();  // this is why we need to be a friend--just convenient
    }
    delete[] data;
    return true;
}


/*
 * ****************************
 * Heap storage tests
 * ****************************
 */

/**
 * Test helper. Sets the row's a and b values.
 * @param row to set
 * @param a column value
 * @param b column value
 */
void test_set_row(ValueDict& row, int a, std::string b) {
    row["a"] = Value(a);
    row["b"] = Value(b);
    row["c"] = Value(a % 2 == 0);  // true for even, false for odd
}

/**
 * Test helper. Compares row to expected values for columns a and b.
 * @param table    relation where row is
 * @param handle   row to check
 * @param a        expected column value
 * @param b        expected column value
 * @return         true if actual == expected for both columns, false otherwise
 */
bool test_compare(DbRelation& table, Handle handle, int a, std::string b) {
    ValueDict* result = table.project(handle);
    Value value = (*result)["a"];
    if (value.n != a) {
        delete result;
        return false;
    }
    value = (*result)["b"];
    if (value.s != b) {
		delete result;
        return false;
	}
    value = (*result)["c"];
	delete result;
    if (value.n != (a % 2 == 0))
        return false;
    return true;
}

/**
 * Testing function for heap storage engine.
 * @return true if the tests all succeeded
 */
bool test_heap_storage() {
    if (!test_slotted_page())
        return assertion_failure("slotted page tests failed");
    std::cout << std::endl << "slotted page tests ok" << std::endl;

    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    column_names.push_back("c");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::BOOLEAN);
    column_attributes.push_back(ca);

    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exists ok" << std::endl;

    ValueDict row;
    std::string b = "Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal.";
    test_set_row(row, -1, b);
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    if (!test_compare(table, (*handles)[0], -1, b))
        return false;
    std::cout << "select/project ok " << handles->size() << std::endl;
    delete handles;

    Handle last_handle;
    for (int i = 0; i < 1000; i++) {
        test_set_row(row, i, b);
        last_handle = table.insert(&row);
    }
    handles = table.select();
    if (handles->size() != 1001)
        return false;
    int i = -1;
    for (auto const &handle: *handles) {
        if (!test_compare(table, handle, i++, b))
            return false;
    }
    std::cout << "many inserts/select/projects ok" << std::endl;
    delete handles;

    table.del(last_handle);
    handles = table.select();
    if (handles->size() != 1000)
        return false;
    i = -1;
    for (auto const &handle: *handles) {
        if (!test_compare(table, handle, i++, b))
            return false;
    }
    std::cout << "del ok" << std::endl;
    table.drop();
    delete handles;
    return true;
}

/*
 * ****************************
 * SQLExec tests
 * ****************************
 */

QueryResult* parse(std::string sql) {
    hsql::SQLParserResult* const parsedSQL = hsql::SQLParser::parseSQLString(sql);
    if (!parsedSQL->isValid()) {
        assertion_failure("invlid SQL: " + sql);
        return nullptr;
    }
    const hsql::SQLStatement* statement = parsedSQL->getStatement(0);
    std::cout << ParseTreeToString::statement(statement) << std::endl;
    QueryResult* result = SQLExec::execute(statement);
    delete parsedSQL;
    return result;
}

bool test_show_tables(std::size_t nExpectedTables) {
    std::cout << "\n=====================\n";
    std::string sql = "show tables";
    QueryResult* result = parse(sql);
    if (!result)
        return false;
    std::cout << *result << std::endl;
    ValueDicts* rows = result->get_rows();
    if (rows->size() != nExpectedTables)
        return false;
    delete result;
    std::cout << "show tables ok\n";
    return true;
}

bool test_show_columns_from_schema_tables() {
    std::cout << "\n=====================\n";
    std::string sql = "show columns from _tables";
    QueryResult* result = parse(sql);
    if (!result)
        return false;
    std::cout << *result << std::endl;
    ValueDicts* rows = result->get_rows();
    if (rows->size() != 1)
        return false;
    delete result;
    sql = "show columns from _columns";
    result = parse(sql);
    if (!result)
        return false;
    std::cout << *result << std::endl;
    rows = result->get_rows();
    if (rows->size() != 3)
        return false;
    delete result;
    sql = "show columns from _indices";
    result = parse(sql);
    if (!result)
        return false;
    std::cout << *result << std::endl;
    rows = result->get_rows();
    if (rows->size() != 6)
        return false;
    delete result;
    std::cout << "show columns from schema tables ok\n";
    return true;
}

bool test_create_table() {
    std::cout << "\n=====================\n";
    std::string sql = "create table egg (yolk text, white int, shell int)";
    QueryResult* result = parse(sql);
    if (!result)
        return false;
    std::cout << *result << std::endl;
    std::string message = result->get_message();
    if (message != "created table egg")
        return false;
    delete result;
    std::cout << "create table ok\n";
    return true;
}

bool test_create_index() {
    std::cout << "\n=====================\n";
    std::string sql = "create index chicken on egg (yolk, shell)";
    QueryResult* result = parse(sql);
    if (!result)
        return false;
    std::cout << *result << std::endl;
    std::string message = result->get_message();
    if (message != "created index chicken")
        return false;
    delete result;
    std::cout << "create index ok\n";
    return true;
}

bool test_show_index(std::size_t nExpectedIndices) {
    std::cout << "\n=====================\n";
    std::string sql = "show index from egg";
    QueryResult* result = parse(sql);
    if (!result)
        return false;
    std::cout << *result << std::endl;
    ValueDicts* rows = result->get_rows();
    if (rows->size() != nExpectedIndices)
        return false;
    delete result;
    std::cout << "show index ok\n";
    return true;
}

bool test_drop_index() {
    std::cout << "\n=====================\n";
    std::string sql = "drop index chicken from egg";
    QueryResult* result = parse(sql);
    if (!result)
        return false;
    std::cout << *result << std::endl;
    std::string message = result->get_message();
    if (message != "dropped index chicken")
        return false;
    delete result;
    std::cout << "drop index ok\n";
    return true;
}

bool test_drop_table() {
    std::cout << "\n=====================\n";
    std::string sql = "drop table egg";
    QueryResult* result = parse(sql);
    if (!result)
        return false;
    std::cout << *result << std::endl;
    std::string message = result->get_message();
    if (message != "dropped table egg")
        return false;
    delete result;
    std::cout << "drop table ok\n";
    return true;
}

bool test_sql_exec() {
    if (!test_show_columns_from_schema_tables())
        return false;

    // test create table
    if (!test_show_tables(0))
        return false;
    if (!test_create_table())
        return false;
    if (!test_show_tables(1))
        return false;
    
    // test create index
    if (!test_show_index(0))
        return false;
    if (!test_create_index())
        return false;
    if (!test_show_index(2))
        return false;
    
    // test drop index
    if (!test_drop_index())
        return false;
    if (!test_show_index(0))
        return false;
    
    // test drop table
    if (!test_drop_table())
        return false;
    if (!test_show_tables(0))
        return false;
    
    // test indices dropped on drop table
    if (!test_create_table())
        return false;
    if (!test_create_index())
        return false;
    if (!test_show_index(2))
        return false;
    if (!test_drop_table())
        return false;
    if (!test_show_tables(0))
        return false;
    if (!test_show_index(0))
        return false;

    return true;
}