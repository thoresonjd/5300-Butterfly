/**
 * @file schema_tables.h - schema table classes:
 * 		Columns
 * 		Tables
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#pragma once

#include "heap_storage.h"

/**
 * Initialize access to the schema tables.
 * Must be called before anything else is done with any of the schema
 * data structures.
 */
void initialize_schema_tables();


class Columns; // forward declare

/**
 * @class Tables - The singleton table that stores the metadata for all other tables.
 * For now, we are not indexing anything, so a query requires sequential scan
 * of the table.
 */
class Tables : public HeapTable {
public:
    /**
     * Name of the tables table ("_tables")
     */
    static const Identifier TABLE_NAME;

    // ctor/dtor
    Tables();

    virtual ~Tables() {}

    // HeapTable overrides
    virtual void create();

    virtual Handle insert(const ValueDict *row);

    virtual void del(Handle handle);

    /**
     * Get the columns and their attributes for a given table.
     * @param table_name         table to get column info for
     * @param column_names       returned by reference: list of column names
     *                           for table_name
     * @param column_attributes  returned by reference: list of corresponding
     *                           attributes for column_names
     */
    virtual void get_columns(Identifier table_name, ColumnNames &column_names, ColumnAttributes &column_attributes);

    /**
     * Get the correctly instantiated DbRelation for a given table.
     * @param table_name  table to get
     * @returns           instantiated DbRelation of the correct type
     */
    virtual DbRelation &get_table(Identifier table_name);

protected:
    // hard-coded columns for _tables table
    static ColumnNames &COLUMN_NAMES();

    static ColumnAttributes &COLUMN_ATTRIBUTES();

    // keep a reference to the columns table (for get_columns method)
    static Columns *columns_table;

private:
    // keep a cache of all the tables we've instantiated so far
    static std::map<Identifier, DbRelation *> table_cache;
};


/**
 * @class Columns - The singleton table that stores the column metadata for all tables.
 */
class Columns : public HeapTable {
public:
    /**
     * Name of the columns table ("_columns")
     */
    static const Identifier TABLE_NAME;

    // ctor/dtor
    Columns();

    virtual ~Columns() {}

    // HeapTable overrides
    virtual void create();

    virtual Handle insert(const ValueDict *row);

protected:
    // hard-coded columns for the _columns table
    static ColumnNames &COLUMN_NAMES();

    static ColumnAttributes &COLUMN_ATTRIBUTES();
};

