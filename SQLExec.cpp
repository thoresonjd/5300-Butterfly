/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @authors Kevin Lundeen, Justin Thoreson
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

// make query result be printable
ostream& operator<<(ostream& out, const QueryResult& qres) {
    if (qres.column_names != nullptr) {
        for (Identifier& column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (ValueDict* row: *qres.rows) {
            for (Identifier& column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    if (this->column_names)
        delete this->column_names;
    if (this->column_attributes)
        delete this->column_attributes;
    if (this->rows) {
        for (ValueDict* row: *this->rows)
            delete row;
        delete this->rows;
    }
}

QueryResult* SQLExec::execute(const SQLStatement* statement) {
    if (!SQLExec::tables)
        SQLExec::tables = new Tables();
    if (!SQLExec::indices)
        SQLExec::indices = new Indices();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement*) statement);
            case kStmtDrop:
                return drop((const DropStatement*) statement);
            case kStmtShow:
                return show((const ShowStatement*) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError("DbRelationError: " + string(e.what()));
    }
}

void SQLExec::column_definition(const ColumnDefinition* col, Identifier& column_name, ColumnAttribute& column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::DataType::INT:
            column_attribute = ColumnAttribute::DataType::INT;
            break;
        case ColumnDefinition::DataType::TEXT:
            column_attribute = ColumnAttribute::DataType::TEXT;
            break;
        default:
            throw SQLExecError("not implemented");
    }
}

QueryResult* SQLExec::create(const CreateStatement* statement) {
    switch(statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

QueryResult* SQLExec::create_table(const CreateStatement* statement) {
    // update _tables schema
    ValueDict row = {{"table_name", Value(statement->tableName)}};
    Handle tableHandle = SQLExec::tables->insert(&row);
    try {
        // update _columns schema
        Handles columnHandles;
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (ColumnDefinition* column : *statement->columns) {
                Identifier cn;
                ColumnAttribute ca;
                column_definition(column, cn, ca);
                std::string type = ca.get_data_type() == ColumnAttribute::DataType::TEXT ? "TEXT" : "INT";
                ValueDict row = {
                    {"table_name", Value(statement->tableName)},
                    {"column_name", Value(cn)},
                    {"data_type", Value(type)}
                };
                columnHandles.push_back(columns.insert(&row));
            }

            // create table
            DbRelation& table = SQLExec::tables->get_table(statement->tableName);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();
        } catch (...) {
            // attempt to undo the insertions into _columns
            try {
                for (Handle& columnHandle : columnHandles)
                    columns.del(columnHandle);
            } catch (...) {}
            throw;
        }
    } catch (...) {
        // attempt to undo the insertion into _tables
        try {
            SQLExec::tables->del(tableHandle);
        } catch (...) {}
        throw;
    }

    return new QueryResult("created table " + string(statement->tableName));
}

QueryResult* SQLExec::create_index(const CreateStatement* statement) {
    DbRelation& table = SQLExec::tables->get_table(statement->tableName);

    // check that all the index columns exist in the table
    const ColumnNames& cn = table.get_column_names();
    for (char* column_name : *statement->indexColumns)
        if (find(cn.begin(), cn.end(), string(column_name)) == cn.end())
            throw SQLExecError("no such column " + string(column_name) + " in table " + statement->tableName);

    // insert a row for each column in index key into _indices
    ValueDict row = {
        {"table_name", Value(statement->tableName)},
        {"index_name", Value(statement->indexName)},
        {"column_name", Value("")},
        {"seq_in_index", Value()},
        {"index_type", Value(statement->indexType)},
        {"is_unique", Value(string(statement->indexType) == "BTREE")}
    };
    for (char* column_name : *statement->indexColumns) {
        row["column_name"] = Value(column_name);
        row["seq_in_index"].n += 1;
        SQLExec::indices->insert(&row);
    }

    // call get_index to get a reference to the new index and then invoke the create method on it
    DbIndex& index = SQLExec::indices->get_index(string(statement->tableName), string(statement->indexName));
    index.create();

    return new QueryResult("created index " + string(statement->indexName));
}

QueryResult* SQLExec::drop(const DropStatement* statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

QueryResult* SQLExec::drop_table(const DropStatement* statement) {
    if (statement->type != DropStatement::kTable)
        throw SQLExecError("unrecognized DROP type");
    
    // get table name
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME || table_name == Indices::TABLE_NAME)
        throw SQLExecError("Cannot drop a schema table!");
    ValueDict where = {{"table_name", Value(table_name)}};

    // before dropping the table, drop each index on the table
    Handles* selected = SQLExec::indices->select(&where);
    for (Handle& row : *selected)
        SQLExec::indices->del(row);
    delete selected;

    // remove columns    
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles* rows = columns.select(&where);
    for (Handle& row : *rows)
        columns.del(row);
    delete rows;

    // remove table
    DbRelation& table = SQLExec::tables->get_table(table_name);
    table.drop();
    rows = SQLExec::tables->select(&where);
    SQLExec::tables->del(*rows->begin());
    delete rows;

    return new QueryResult("dropped table " + table_name);    
}

QueryResult* SQLExec::drop_index(const DropStatement* statement) {
    // call get_index to get a reference to the index and then invoke the drop method on it
    DbIndex& index = SQLExec::indices->get_index(string(statement->name), string(statement->indexName));
    index.drop();

    // remove all the rows from _indices for this index
    ValueDict where = {
        {"table_name", Value(statement->name)},
        {"index_name", Value(statement->indexName)}
    };
    Handles* selected = SQLExec::indices->select(&where);
    for (Handle& row : *selected)
        SQLExec::indices->del(row);
    delete selected;

    return new QueryResult("dropped index " + string(statement->indexName));
}

QueryResult* SQLExec::show(const ShowStatement* statement) {
    switch(statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

QueryResult* SQLExec::show_tables() {
    // get column names and attributes
    ColumnNames* cn = new ColumnNames();
    ColumnAttributes* ca = new ColumnAttributes();
    SQLExec::tables->get_columns(Tables::TABLE_NAME, *cn, *ca);

    // get table names
    Handles* tables = SQLExec::tables->select();
    ValueDicts* rows = new ValueDicts();
    for (Handle& table : *tables) {
        ValueDict* row = SQLExec::tables->project(table, cn);
        Identifier table_name = (*row)["table_name"].s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete tables;
    return new QueryResult(cn, ca, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

QueryResult* SQLExec::show_columns(const ShowStatement* statement) {
    ColumnNames* cn = new ColumnNames({"table_name", "column_name", "data_type"});
    ColumnAttributes* ca = new ColumnAttributes({ColumnAttribute(ColumnAttribute::DataType::TEXT)});
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    ValueDict where = {{"table_name", Value(statement->tableName)}};
    Handles* selected = columns.select(&where);
    ValueDicts* rows = new ValueDicts();
    for (Handle& row : *selected)
        rows->push_back(columns.project(row, cn));
    delete selected;
    return new QueryResult(cn, ca, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

QueryResult* SQLExec::show_index(const ShowStatement* statement){
    ColumnNames* cn = new ColumnNames({
        "table_name", "index_name", "column_name",
        "seq_in_index", "index_type", "is_unique"
    });
    ColumnAttributes* ca = new ColumnAttributes({
        ColumnAttribute(ColumnAttribute::DataType::TEXT),
        ColumnAttribute(ColumnAttribute::DataType::TEXT),
        ColumnAttribute(ColumnAttribute::DataType::TEXT),
        ColumnAttribute(ColumnAttribute::DataType::INT),
        ColumnAttribute(ColumnAttribute::DataType::TEXT),
        ColumnAttribute(ColumnAttribute::DataType::BOOLEAN),
    });
    ValueDict where = {{"table_name", Value(statement->tableName)}};
    Handles* selected = SQLExec::indices->select(&where);
    ValueDicts* rows = new ValueDicts();
    for (Handle& row : *selected)
        rows->push_back(SQLExec::indices->project(row, cn));
    delete selected;
    return new QueryResult(cn, ca, rows, "successfully returned " + to_string(rows->size()) + " rows");
}
