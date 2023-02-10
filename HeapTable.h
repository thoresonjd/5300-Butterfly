/**
 * @file HeapTable.h - Implementation of storage_engine with a heap file structure.
 * HeapTable: DbRelation
 *
 * @authors Kevin Lundeen, Justin Thoreson
 * @see "Seattle University, CPSC5300, Winter 2023"
 */

#pragma once

#include <string>
#include "storage_engine.h"
#include "SlottedPage.h"
#include "HeapFile.h"

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */
class HeapTable : public DbRelation {
public:
    /**
     * Constructor
     * @param table_name
     * @param column_names
     * @param column_attributes
     */
    HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes);

    virtual ~HeapTable() {}

    HeapTable(const HeapTable& other) = delete;

    HeapTable(HeapTable&& temp) = delete;

    HeapTable& operator=(const HeapTable& other) = delete;

    HeapTable& operator=(HeapTable&& temp) = delete;

    /**
     * Creates the HeapTable relation
     */
    virtual void create();

    /** 
     * Creates the HeapTable relation if it doesn't already exist
     */ 
    virtual void create_if_not_exists();

    /**
     * Drops the HeapTable relation
     */
    virtual void drop();

    /**
     * Opens the HeapTable relation
     */
    virtual void open();

    /**
     * Closes the HeapTable relation
     */
    virtual void close();

    /**
     * Inserts a data tuple into the table
     * @param row The data tuple to insert
     * @return A handle locating the block ID and record ID of the inserted tuple
     */
    virtual Handle insert(const ValueDict* row);

    /**
     * Updates a record to a database
     * @param handle The location (block ID, record ID) of the record
     * @param new_values The new fields to replace the existing fields with
     */
    virtual void update(const Handle handle, const ValueDict* new_values);

    /**
     * Deletes a row from the table using the given handle for the row
     * @param handle The handle for the row being deleted
     */
    virtual void del(const Handle handle);

    /**
     * Select all data tuples (rows) from the table
     */
    virtual Handles* select();

    /**
     * Selects data tuples (rows) from the table matching given predicates
     * @param where The where-clause predicates
     * @return Handles locating the block IDs and record IDs of the matching rows
     */
    virtual Handles* select(const ValueDict* where);

    /**
     * Return a sequence of all values for handle (SELECT *).
     * @param handle Location of row to get values from
     * @returns Dictionary of values from row (keyed by all column names)
     */
    virtual ValueDict* project(Handle handle);

    /**
     * Return a sequence of values for handle given by column_names
     * @param handle Location of row to get values from
     * @param column_names List of column names to project
     * @returns Dictionary of values from row (keyed by column_names)
     */
    virtual ValueDict* project(Handle handle, const ColumnNames* column_names);

    using DbRelation::project;

protected:
    HeapFile file;

    /**
     * Checks if a row is valid to the table
     * @param row The data tuple to validate
     * @return The fully validated data tuple
     */
    virtual ValueDict* validate(const ValueDict* row) const;

    /**
     * Writes a data tuple to the database file
     * @param row The data tuple to add
     * @return A handle locating the block ID and record ID of the written tuple
     */
    virtual Handle append(const ValueDict* row);

    /**
     * Return the bits to go into the file. Caller responsible for freeing the
     * returned Dbt and its enclosed ret->get_data().
     */
    virtual Dbt* marshal(const ValueDict* row) const;
    
    /**
     * Converts data bytes into concrete types
     */
    virtual ValueDict* unmarshal(Dbt* data) const;

    /**
     * See if the row at the given handle satisfies the given where clause
     * @param handle  row to check
     * @param where   conditions to check
     * @return        true if conditions met, false otherwise
     */
    virtual bool selected(Handle handle, const ValueDict* where);
};

/**
 * Test helper. Sets the row's a and b values.
 * @param row to set
 * @param a column value
 * @param b column value
 */
void test_set_row(ValueDict& row, int a, std::string b);

/**
 * Test helper. Compares row to expected values for columns a and b.
 * @param table    relation where row is
 * @param handle   row to check
 * @param a        expected column value
 * @param b        expected column value
 * @return         true if actual == expected for both columns, false otherwise
 */
bool test_compare(DbRelation& table, Handle handle, int a, std::string b);

/**
 * Testing function for heap storage engine.
 * @return true if the tests all succeeded
 */
bool test_heap_storage();
