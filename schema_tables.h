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

    virtual Handle insert(const ValueDict* row);

    virtual void del(Handle handle);

    /**
     * Get the columns and their attributes for a given table.
     * @param table_name         table to get column info for
     * @param column_names       returned by reference: list of column names
     *                           for table_name
     * @param column_attributes  returned by reference: list of corresponding
     *                           attributes for column_names
     */
    static void get_columns(Identifier table_name, ColumnNames& column_names, ColumnAttributes& column_attributes);

    /**
     * Get the correctly instantiated DbRelation for a given table.
     * @param table_name  table to get
     * @returns           instantiated DbRelation of the correct type
     */
    static DbRelation& get_table(Identifier table_name);

protected:
    // hard-coded columns for _tables table
    static ColumnNames& COLUMN_NAMES();

    static ColumnAttributes& COLUMN_ATTRIBUTES();

    // keep a reference to the columns table (for get_columns method)
    static Columns* columns_table;

private:
    // keep a cache of all the tables we've instantiated so far
    static std::map<Identifier, DbRelation*> table_cache;
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

    virtual Handle insert(const ValueDict* row);

protected:
    // hard-coded columns for the _columns table
    static ColumnNames& COLUMN_NAMES();

    static ColumnAttributes& COLUMN_ATTRIBUTES();
};


using IndexNames = ColumnNames;

class Indices : public HeapTable {
public:
    /**
     * Name of the indices table ("_indices")
     */
    static const Identifier TABLE_NAME;

    // ctor/dtor
    Indices();

    virtual ~Indices() {}

    /**
     * Get the search key for the given index.
     * @param table_name      what table the requested index is on
     * @param index_name      name of index (unique by table)
     * @param column_names    returned by reference: list of column names
     *                        in search key in order
     * @param is_hash         returned by reference: set to False if the
     *                        requested index is a btree index
     * @param is_unique       search key for this index is a key for the relation
     */
    virtual void get_columns(Identifier table_name, Identifier index_name, ColumnNames& column_names, bool& is_hash,
                             bool& is_unique);

    /**
     * Get the instantiated DbIndex for the given index.
     * @param table_name  what table the requested index is on
     * @param index_name  name of index (unique by table)
     * @returns           DbIndex for requested index
     */
    virtual DbIndex& get_index(Identifier table_name, Identifier index_name);

    /**
     * Get the list of indices on a given table.
     * @param table_name  which table to lookup the indices on
     * @returns           list of index names for table_name
     */
    virtual IndexNames get_index_names(Identifier table_name);

    // overrides
    virtual Handle insert(const ValueDict* row);

    virtual void del(Handle handle);

protected:
    static ColumnNames& COLUMN_NAMES();

    static ColumnAttributes& COLUMN_ATTRIBUTES();

private:
    static std::map<std::pair<Identifier, Identifier>, DbIndex*> index_cache;
};