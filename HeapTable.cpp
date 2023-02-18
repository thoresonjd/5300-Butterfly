/**
 * @file HeapTable.cpp
 * @authors Kevin Lundeen, Justin Thoreson
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
    } catch (DbException& e) {
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
    ValueDict* full_row = this->validate(row);
    Handle handle = this->append(full_row);
    delete full_row;
    return handle;
}

void HeapTable::update(const Handle handle, const ValueDict* new_values) {
    throw DbRelationError("Not implemented");
}

void HeapTable::del(const Handle handle) {
    this->open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = this->file.get(block_id);
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
    for (BlockID& block_id: *block_ids) {
        SlottedPage* block = this->file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (RecordID& record_id: *record_ids) {
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
    for (auto const& column_name: *column_names) {
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
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            if (offset + 1 > DbBlock::BLOCK_SZ - 1)
                throw DbRelationError("row too big to marshal");
            *(uint8_t*)(bytes + offset) = (uint8_t) value.n;
            offset += sizeof(uint8_t);
        } else {
            throw DbRelationError("Only know how to marshal INT, TEXT, and BOOLEAN");
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
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            value.n = *(uint8_t *) (bytes + offset);
            offset += sizeof(uint8_t);
        } else {
            throw DbRelationError("Only know how to unmarshal INT, TEXT, and BOOLEAN");
        }
        (*row)[column_name] = value;
    }
    return row;
}

bool HeapTable::selected(Handle handle, const ValueDict* where) {
    if (where == nullptr)
        return true;
    ValueDict* row = this->project(handle, where);
    bool is_selected = *row == *where;
    delete row;
    return is_selected;
}
