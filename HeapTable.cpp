/**
 * @file HeapTable.cpp
 * @author K Lundeen
 * @see Seattle University, CPSC5300
 */
#include <cstring>
#include "HeapTable.h"

using u16 = u_int16_t;

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes), file(table_name) {
}

void HeapTable::create() {
    this->file.create();
}

void HeapTable::create_if_not_exists() {
    try {
        this->open();
    } catch (DbException &e) {
        this->create();
    }
}

void HeapTable::drop() {
    this->file.drop();
}

void HeapTable::open() {
    this->file.open();
}

void HeapTable::close() {
    this->file.close();
}

Handle HeapTable::insert(const ValueDict* row) {
    this->open();
    ValueDict *full_row = this->validate(row);
    Handle handle = this->append(full_row);
    delete full_row;
    return handle;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw DbRelationError("Not implemented");
}

void HeapTable::del(const Handle handle) {
    this->open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = this->file.get(block_id);
    block->del(record_id);
    this->file.put(block);
    delete block;
}

Handles* HeapTable::select() {
    return this->select(nullptr);
}

Handles* HeapTable::select(const ValueDict* where) {
    this->open();
    Handles* handles = new Handles();
    BlockIDs* block_ids = this->file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = this->file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids) {
            Handle handle(block_id, record_id);
            if (this->selected(handle, where))
                handles->push_back(handle);
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

ValueDict* HeapTable::project(Handle handle) {
    return this->project(handle, &this->column_names);
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = this->file.get(block_id);
    Dbt* data = block->get(record_id);
    ValueDict* row = this->unmarshal(data);
    delete data;
    delete block;
    if (column_names->empty())
        return row;
    ValueDict* result = new ValueDict();
    for (auto const& column_name: this->column_names) {
        if (row->find(column_name) == row->end())
            throw DbRelationError("table does not have column named '" + column_name + "'");
        (*result)[column_name] = (*row)[column_name];
    }
    delete row;
    return result;
}

ValueDict* HeapTable::validate(const ValueDict* row) const {
    ValueDict* full_row = new ValueDict();
    for (auto const& column_name: this->column_names) {
        Value value;
        ValueDict::const_iterator column = row->find(column_name);
        if (column == row->end())
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        else
            value = column->second;
        (*full_row)[column_name] = value;
    }
    return full_row;
}

Handle HeapTable::append(const ValueDict* row) {
    Dbt* data = marshal(row);
    SlottedPage* block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
        record_id = block->add(data);
    } catch (DbBlockNoRoomError &e) {
        // need a new block
        delete block;
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    delete block;
    delete[] (char*)data->get_data();
    delete data;
    return Handle(this->file.get_last_block_id(), record_id);
}

Dbt* HeapTable::marshal(const ValueDict* row) const {
    char* bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            if (offset + 4 > DbBlock::BLOCK_SZ - 4)
                throw DbRelationError("row too big to marshal");
            *(int32_t*)(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u_long size = value.s.length();
            if (size > UINT16_MAX)
                throw DbRelationError("text field too long to marshal");
            if (offset + 2 + size > DbBlock::BLOCK_SZ)
                throw DbRelationError("row too big to marshal");
            *(u16*)(bytes + offset) = size;
            offset += sizeof(u16);
            std::memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char* right_size_bytes = new char[offset];
    std::memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt* data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data) const {
    ValueDict* row = new ValueDict();
    Value value;
    char* bytes = (char*)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        value.data_type = ca.get_data_type();
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t*)(bytes + offset);
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16*)(bytes + offset);
            offset += sizeof(u16);
            char buffer[DbBlock::BLOCK_SZ];
            std::memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.s = std::string(buffer);  // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
        (*row)[column_name] = value;
    }
    return row;
}

bool HeapTable::selected(Handle handle, const ValueDict* where) {
    if (where == nullptr)
        return true;
    ValueDict* row = this->project(handle, where);
    return *row == *where;
}

/**
 * Test helper. Sets the row's a and b values.
 * @param row to set
 * @param a column value
 * @param b column value
 */
void test_set_row(ValueDict &row, int a, std::string b) {
    row["a"] = Value(a);
    row["b"] = Value(b);
}

/**
 * Test helper. Compares row to expected values for columns a and b.
 * @param table    relation where row is
 * @param handle   row to check
 * @param a        expected column value
 * @param b        expected column value
 * @return         true if actual == expected for both columns, false otherwise
 */
bool test_compare(DbRelation &table, Handle handle, int a, std::string b) {
    ValueDict *result = table.project(handle);
    Value value = (*result)["a"];
    if (value.n != a) {
        delete result;
        return false;
    }
    value = (*result)["b"];
    delete result;
    return !(value.s != b);
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
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
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
