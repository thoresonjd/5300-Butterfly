/**
 * @file schema_tables.cpp - implementation of schema table classes
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "schema_tables.h"
#include "ParseTreeToString.h"


void initialize_schema_tables() {
    Tables tables;
    tables.create_if_not_exists();
    tables.close();
    Columns columns;
    columns.create_if_not_exists();
    columns.close();
}

// Not terribly useful since the parser weeds most of these out
bool is_acceptable_identifier(Identifier identifier) {
    if (ParseTreeToString::is_reserved_word(identifier))
        return true;
    try {
        std::stoi(identifier);
        return false;
    } catch (std::exception &e) {
        // can't be converted to an integer, so good
    }
    for (auto const &c: identifier)
        if (!isalnum(c) && c != '$' && c != '_')
            return false;
    return true;
}

bool is_acceptable_data_type(std::string dt) {
    return dt == "INT" || dt == "TEXT";  // for now
}


/*
 * ***************************
 * Tables class implementation
 * ***************************
 */
const Identifier Tables::TABLE_NAME = "_tables";
Columns *Tables::columns_table = nullptr;
std::map<Identifier, DbRelation *> Tables::table_cache;

// get the column name for _tables column
ColumnNames &Tables::COLUMN_NAMES() {
    static ColumnNames cn;
    if (cn.empty())
        cn.push_back("table_name");
    return cn;
}

// get the column attribute for _tables column
ColumnAttributes &Tables::COLUMN_ATTRIBUTES() {
    static ColumnAttributes cas;
    if (cas.empty()) {
        ColumnAttribute ca(ColumnAttribute::TEXT);
        cas.push_back(ca);
    }
    return cas;
}

// ctor - we have a fixed table structure of just one column: table_name
Tables::Tables() : HeapTable(TABLE_NAME, COLUMN_NAMES(), COLUMN_ATTRIBUTES()) {
    Tables::table_cache[TABLE_NAME] = this;
    if (Tables::columns_table == nullptr)
        columns_table = new Columns();
    Tables::table_cache[columns_table->TABLE_NAME] = columns_table;
}

// Create the file and also, manually add schema tables.
void Tables::create() {
    HeapTable::create();
    ValueDict row;
    row["table_name"] = Value("_tables");
    insert(&row);
    row["table_name"] = Value("_columns");
    insert(&row);
}

// Manually check that table_name is unique.
Handle Tables::insert(const ValueDict *row) {
    // Try SELECT * FROM _tables WHERE table_name = row["table_name"] and it should return nothing
    Handles *handles = select(row);
    bool unique = handles->empty();
    delete handles;
    if (!unique)
        throw DbRelationError(row->at("table_name").s + " already exists");
    return HeapTable::insert(row);
}

// Remove a row, but first remove from table cache if there
// NOTE: once the row is deleted, any reference to the table (from get_table() below) is gone! So drop the table first.
void Tables::del(Handle handle) {
    // remove from cache, if there
    ValueDict *row = project(handle);
    Identifier table_name = row->at("table_name").s;
    if (Tables::table_cache.find(table_name) != Tables::table_cache.end()) {
        DbRelation *table = Tables::table_cache.at(table_name);
        Tables::table_cache.erase(table_name);
        delete table;
    }

    HeapTable::del(handle);
}

// Return a list of column names and column attributes for given table.
void Tables::get_columns(Identifier table_name, ColumnNames &column_names, ColumnAttributes &column_attributes) {
    // SELECT * FROM _columns WHERE table_name = <table_name>
    ValueDict where;
    where["table_name"] = table_name;
    Handles *handles = Tables::columns_table->select(&where);

    ColumnAttribute column_attribute;
    for (auto const &handle: *handles) {
        ValueDict *row = Tables::columns_table->project(
                handle);  // get the row's values: {'column_name': <name>, 'data_type': <type>}

        Identifier column_name = (*row)["column_name"].s;
        column_names.push_back(column_name);

        column_attribute.set_data_type((*row)["data_type"].s == "INT" ? ColumnAttribute::INT : ColumnAttribute::TEXT);
        column_attributes.push_back(column_attribute);

        delete row;
    }
    delete handles;
}

// Return a table for given table_name.
DbRelation &Tables::get_table(Identifier table_name) {
    // if they are asking about a table we've once constructed, then just return that one
    if (Tables::table_cache.find(table_name) != Tables::table_cache.end())
        return *Tables::table_cache[table_name];

    // otherwise assume it is a HeapTable (for now)
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    get_columns(table_name, column_names, column_attributes);
    DbRelation *table = new HeapTable(table_name, column_names, column_attributes);
    Tables::table_cache[table_name] = table;
    return *table;
}


/*
 * ****************************
 * Columns class implementation
 * ****************************
 */
const Identifier Columns::TABLE_NAME = "_columns";

// get the column name for _tables column
ColumnNames &Columns::COLUMN_NAMES() {
    static ColumnNames cn;
    if (cn.empty()) {
        cn.push_back("table_name");
        cn.push_back("column_name");
        cn.push_back("data_type");
    }
    return cn;
}

// get the column attribute for _tables column
ColumnAttributes &Columns::COLUMN_ATTRIBUTES() {
    static ColumnAttributes cas;
    if (cas.empty()) {
        ColumnAttribute ca(ColumnAttribute::TEXT);
        cas.push_back(ca);
        cas.push_back(ca);
        cas.push_back(ca);
    }
    return cas;
}

// ctor - we have a fixed table structure of just one column: table_name
Columns::Columns() : HeapTable(TABLE_NAME, COLUMN_NAMES(), COLUMN_ATTRIBUTES()) {
}

// Create the file and also, manually add schema columns.
void Columns::create() {
    HeapTable::create();
    ValueDict row;
    row["data_type"] = Value("TEXT");  // all these are TEXT fields
    row["table_name"] = Value("_tables");
    row["column_name"] = Value("table_name");
    insert(&row);
    row["table_name"] = Value("_columns");
    row["column_name"] = Value("table_name");
    insert(&row);
    row["column_name"] = Value("column_name");
    insert(&row);
    row["column_name"] = Value("data_type");
    insert(&row);
}

// Manually check that (table_name, column_name) is unique.
Handle Columns::insert(const ValueDict *row) {
    // Check that datatype is acceptable
    if (!is_acceptable_identifier(row->at("table_name").s))
        throw DbRelationError("unacceptable table name '" + row->at("table_name").s + "'");
    if (!is_acceptable_identifier(row->at("column_name").s))
        throw DbRelationError("unacceptable column name '" + row->at("column_name").s + "'");
    if (!is_acceptable_data_type(row->at("data_type").s))
        throw DbRelationError("unacceptable data type '" + row->at("data_type").s + "'");

    // Try SELECT * FROM _columns WHERE table_name = row["table_name"] AND column_name = column_name["column_name"]
    // and it should return nothing
    ValueDict where;
    where["table_name"] = row->at("table_name");
    where["column_name"] = row->at("column_name");
    Handles *handles = select(&where);
    bool unique = handles->empty();
    delete handles;
    if (!unique)
        throw DbRelationError("duplicate column " + row->at("table_name").s + "." + row->at("column_name").s);

    return HeapTable::insert(row);
}

