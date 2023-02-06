/**
 * @file storage_engine.cpp - implementation of some of the abstract classes' methods
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "storage_engine.h"

bool Value::operator==(const Value &other) const {
    if (this->data_type != other.data_type)
        return false;
    if (this->data_type == ColumnAttribute::INT)
        return this->n == other.n;
    return this->s == other.s;
}

bool Value::operator!=(const Value &other) const {
    return !(*this == other);
}

// Just pulls out the column names from a ValueDict and passes that to the usual form of project().
ValueDict *DbRelation::project(Handle handle, const ValueDict *where) {
    ColumnNames t;
    for (auto const &column: *where)
        t.push_back(column.first);
    return this->project(handle, &t);
}

