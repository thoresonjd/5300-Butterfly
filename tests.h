/**
 * 
 */

#pragma once
#include "HeapTable.h"
#include "SQLExec.h"

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

bool test_sql_exec() {
    return false;
}