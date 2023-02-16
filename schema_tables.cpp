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
    Indices indices;
    indices.create_if_not_exists();
    indices.close();
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
    return dt == "INT" || dt == "TEXT" || dt == "BOOLEAN";  // for now
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
ColumnNames& Tables::COLUMN_NAMES() {
    static ColumnNames cn;
    if (cn.empty())
        cn.push_back("table_name");
    return cn;
}

// get the column attribute for _tables column
ColumnAttributes& Tables::COLUMN_ATTRIBUTES() {
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
    row["table_name"] = Value("_indices");
    insert(&row);
}

// Manually check that table_name is unique.
Handle Tables::insert(const ValueDict* row) {
    // Try SELECT * FROM _tables WHERE table_name = row["table_name"] and it should return nothing
    Handles* handles = select(row);
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
    ValueDict* row = project(handle);
    Identifier table_name = row->at("table_name").s;
    delete row;
    if (Tables::table_cache.find(table_name) != Tables::table_cache.end()) {
        DbRelation* table = Tables::table_cache.at(table_name);
        Tables::table_cache.erase(table_name);
        delete table;
    }
    HeapTable::del(handle);
}

// Return a list of column names and column attributes for given table.
void Tables::get_columns(Identifier table_name, ColumnNames& column_names, ColumnAttributes& column_attributes) {
    // SELECT * FROM _columns WHERE table_name = <table_name>
    ValueDict where;
    where["table_name"] = table_name;
    Handles* handles = Tables::columns_table->select(&where);

    ColumnAttribute column_attribute;
    for (Handle& handle: *handles) {
        ValueDict *row = Tables::columns_table->project(
                handle);  // get the row's values: {'column_name': <name>, 'data_type': <type>}

        Identifier column_name = (*row)["column_name"].s;
        column_names.push_back(column_name);

        ColumnAttribute::DataType data_type;
        if ((*row)["data_type"].s == "INT")
            data_type = ColumnAttribute::INT;
        else if ((*row)["data_type"].s == "TEXT")
            data_type = ColumnAttribute::TEXT;
        else if ((*row)["data_type"].s == "BOOLEAN")
            data_type = ColumnAttribute::BOOLEAN;
        else
            throw DbRelationError("Unknown data type");
        column_attribute.set_data_type(data_type);

        column_attributes.push_back(column_attribute);

        delete row;
    }
    delete handles;
}

// Return a table for given table_name.
DbRelation& Tables::get_table(Identifier table_name) {
    // if they are asking about a table we've once constructed, then just return that one
    if (Tables::table_cache.find(table_name) != Tables::table_cache.end())
        return *Tables::table_cache[table_name];

    // otherwise assume it is a HeapTable (for now)
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    get_columns(table_name, column_names, column_attributes);
    DbRelation* table = new HeapTable(table_name, column_names, column_attributes);
    Tables::table_cache[table_name] = table;
    return *table;
}


/*
 * ****************************
 * Columns class implementation
 * ****************************
 */
const Identifier Columns::TABLE_NAME = "_columns";

// get the column name for _columns column
ColumnNames& Columns::COLUMN_NAMES() {
    static ColumnNames cn;
    if (cn.empty()) {
        cn.push_back("table_name");
        cn.push_back("column_name");
        cn.push_back("data_type");
    }
    return cn;
}

// get the column attribute for _columns column
ColumnAttributes& Columns::COLUMN_ATTRIBUTES() {
    static ColumnAttributes cas;
    if (cas.empty()) {
        ColumnAttribute ca(ColumnAttribute::TEXT);
        cas.push_back(ca);
        cas.push_back(ca);
        cas.push_back(ca);
    }
    return cas;
}

// ctor - we have a fixed table structure
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
    row["table_name"] = Value("_indices");
    row["column_name"] = Value("table_name");
    insert(&row);
    row["column_name"] = Value("index_name");
    insert(&row);
    row["column_name"] = Value("column_name");
    insert(&row);
    row["column_name"] = Value("index_type");
    insert(&row);
    row["column_name"] = Value("seq_in_index");
    row["data_type"] = Value("INT");
    insert(&row);
    row["column_name"] = Value("is_unique");
    row["data_type"] = Value("BOOLEAN");
    insert(&row);
}

// Manually check that (table_name, column_name) is unique.
Handle Columns::insert(const ValueDict* row) {
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


/*
 * ****************************
 * Indices class implementation
 * ****************************
 */
const Identifier Indices::TABLE_NAME = "_indices";
std::map<std::pair<Identifier, Identifier>, DbIndex *> Indices::index_cache;

// get the column name for _indices column
ColumnNames &Indices::COLUMN_NAMES() {
    static ColumnNames cn;
    if (cn.empty()) {
        cn.push_back("table_name");
        cn.push_back("index_name");
        cn.push_back("column_name");
        cn.push_back("seq_in_index");
        cn.push_back("index_type");
        cn.push_back("is_unique");
    }
    return cn;
}

// get the column attribute for _indices column
ColumnAttributes &Indices::COLUMN_ATTRIBUTES() {
    static ColumnAttributes cas;
    if (cas.empty()) {
        ColumnAttribute ca(ColumnAttribute::TEXT);
        cas.push_back(ca);  // table_name
        cas.push_back(ca);  // index_name
        cas.push_back(ca);  // column_name
        ca.set_data_type(ColumnAttribute::INT);
        cas.push_back(ca);  // seq_in_index
        ca.set_data_type(ColumnAttribute::TEXT);
        cas.push_back(ca);  // index_type
        ca.set_data_type(ColumnAttribute::BOOLEAN);
        cas.push_back(ca);  // is_unique
    }
    return cas;
}

// ctor - we have a fixed table structure
Indices::Indices() : HeapTable(TABLE_NAME, COLUMN_NAMES(), COLUMN_ATTRIBUTES()) {}

// Manually check constraints -- unique on (table, index, column)
Handle Indices::insert(const ValueDict *row) {
    // Check that datatype is acceptable
    if (!is_acceptable_identifier(row->at("index_name").s))
        throw DbRelationError("unacceptable index name '" + row->at("index_name").s + "'");

    // Try SELECT * FROM _indices WHERE table_name = row["table_name"] AND index_name = row["index_name"]
    //     AND column_name = column_name["column_name"]
    // and it should return nothing
    ValueDict where;
    where["table_name"] = row->at("table_name");
    where["index_name"] = row->at("index_name");
    if (row->at("seq_in_index").n > 1)
        where["column_name"] = row->at("column_name");  // check for duplicate columns on the same index
    Handles *handles = select(&where);
    bool unique = handles->empty();
    delete handles;
    if (!unique)
        throw DbRelationError("duplicate index " + row->at("table_name").s + " " + row->at("index_name").s);
    return HeapTable::insert(row);
}

// Remove a row, but first remove from index cache if there
// NOTE: once the row is deleted, any reference to the index (from get_index() below) is gone! So drop the index
void Indices::del(Handle handle) {
    // remove from cache, if there
    ValueDict* row = project(handle);
    Identifier table_name = row->at("table_name").s;
    Identifier index_name = row->at("index_name").s;
    delete row;
    std::pair<Identifier, Identifier> cache_key(table_name, index_name);
    if (Indices::index_cache.find(cache_key) != Indices::index_cache.end()) {
        DbIndex* index = Indices::index_cache.at(cache_key);
        Indices::index_cache.erase(cache_key);
        delete index;
    }
    HeapTable::del(handle);
}

// Return a list of column names and column attributes for given table.
void Indices::get_columns(Identifier table_name, Identifier index_name, ColumnNames &column_names, bool &is_hash,
                          bool &is_unique) {
    // SELECT * FROM _indices WHERE table_name = <table_name> AND index_name = <index_name>
    ValueDict where;
    where["table_name"] = table_name;
    where["index_name"] = index_name;
    Handles *handles = select(&where);

    Identifier colnames[DbIndex::MAX_COMPOSITE];
    uint size = 0;
    for (auto const &handle: *handles) {
        ValueDict *row = project(handle);

        Identifier column_name = (*row)["column_name"].s;
        uint which = (uint) (*row)["seq_in_index"].n;
        colnames[which - 1] = column_name;  // seq_in_index is 1-based
        if (which > size)
            size = which;
        is_unique = (*row)["is_unique"].n != 0;
        is_hash = (*row)["index_type"].s == "HASH";
        delete row;
    }
    for (uint i = 0; i < size; i++)
        column_names.push_back(colnames[i]);
    delete handles;
}

// FIXME - use this for now until we have BTreeIndex and HashIndex
class DummyIndex : public DbIndex {
public:
    DummyIndex(DbRelation &rel, Identifier idx, ColumnNames key, bool unq) : DbIndex(rel, idx, key, unq) {}

    void create() {}

    void drop() {}

    void open() {}

    void close() {}

    Handles *lookup(ValueDict *key_values) const { return nullptr; }

    void insert(Handle handle) {}

    void del(Handle handle) {}
};


// Return a table for given table_name.
DbIndex &Indices::get_index(Identifier table_name, Identifier index_name) {
    // if they are asking about an index we've once constructed, then just return that one
    std::pair<Identifier, Identifier> cache_key(table_name, index_name);
    if (Indices::index_cache.find(cache_key) != Indices::index_cache.end())
        return *Indices::index_cache[cache_key];

    // otherwise assume it is a DummyIndex (for now)
    ColumnNames column_names;
    bool is_hash, is_unique;
    get_columns(table_name, index_name, column_names, is_hash, is_unique);
    DbRelation &table = Tables::get_table(table_name);
    DbIndex *index;
    if (is_hash) {
        index = new DummyIndex(table, index_name, column_names, is_unique);  // FIXME - change to HashIndex
    } else {
        index = new DummyIndex(table, index_name, column_names, is_unique);  // FIXME - change to BTreeIndex
    }
    Indices::index_cache[cache_key] = index;
    return *index;
}

IndexNames Indices::get_index_names(Identifier table_name) {
    IndexNames ret;
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["seq_in_index"] = Value(1);  // only get the row for the first column if composite index
    Handles *handles = select(&where);
    for (auto const &handle: *handles) {
        ValueDict *row = project(handle);
        ret.push_back((*row)["index_name"].s);
        delete row;
    }
    delete handles;
    return ret;
}
