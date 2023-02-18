/**
 * @file SlottedPage.cpp
 * @authors Kevin Lundeen, Justin Thoreson
 * @see Seattle University, CPSC5300
 */

#include <cstring>
#include "SlottedPage.h"

using u16 = u_int16_t;

/**
 * SlottedPage constructor
 * @param block
 * @param block_id
 * @param is_new
 */
SlottedPage::SlottedPage(Dbt& block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        this->put_header();
    } else {
        this->get_header(this->num_records, this->end_free);
    }
}

RecordID SlottedPage::add(const Dbt* data) {
    if (!this->has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16)data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1U;
    this->put_header();
    this->put_header(id, size, loc);
    std::memcpy(this->address(loc), data->get_data(), size);
    return id;
}

Dbt* SlottedPage::get(RecordID record_id) const {
    u16 size, loc;
    this->get_header(size, loc, record_id);
    if (!loc) return nullptr; // Tombstone
    return new Dbt(this->address(loc), size);
}

void SlottedPage::put(RecordID record_id, const Dbt& data) {
    u16 size, loc;
    this->get_header(size, loc, record_id);
    u16 new_size = (u16)data.get_size();
    if (new_size > size) {
        u16 extra = new_size - size;
        if (!this->has_room(extra))
            throw DbBlockNoRoomError("not enough room for enlarged record");
        this->slide(loc, loc - extra);
        std::memcpy(this->address(loc - extra), data.get_data(), new_size);
    } else {
        std::memcpy(this->address(loc), data.get_data(), new_size);
        this->slide(loc + new_size, loc + size);
    }
    this->get_header(size, loc, record_id);
    this->put_header(record_id, new_size, loc);
}

void SlottedPage::del(RecordID record_id) {
    u16 size, loc;
    this->get_header(size, loc, record_id);
    this->put_header(record_id);
    this->slide(loc, loc + size);
}

RecordIDs* SlottedPage::ids(void) const {
    RecordIDs* record_ids = new RecordIDs();
    u16 size, loc;
    for (RecordID record_id = 1; record_id <= this->num_records; record_id++) {
        this->get_header(size, loc, record_id);
        if (loc) 
            record_ids->push_back(record_id);
    }
    return record_ids;
}

void SlottedPage::get_header(u16& size, u16& loc, RecordID id) const {
    size = this->get_n((u16) 4 * id);
    loc = this->get_n((u16) (4 * id + 2));
}

void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    this->put_n((u16) 4 * id, size);
    this->put_n((u16) (4 * id + 2), loc);
}

bool SlottedPage::has_room(u16 size) const {
    return 4 * (this->num_records + 1) + size <= this->end_free;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    int shift = end - start;
    if (shift == 0)
        return;

    // slide data
    void* to = this->address((u16) (this->end_free + 1 + shift));
    void* from = this->address((u16) (this->end_free + 1));
    int bytes = start - (this->end_free + 1U);
    std::memmove(to, from, bytes);

    // fix up headers to the right
    RecordIDs *record_ids = ids();
    for (RecordID& record_id : *record_ids) {
        u16 size, loc;
        this->get_header(size, loc, record_id);
        if (loc <= start) {
            loc += shift;
            this->put_header(record_id, size, loc);
        }
    }
    delete record_ids;
    this->end_free += shift;
    put_header();
}

u16 SlottedPage::get_n(u16 offset) const {
    return *(u16 *) this->address(offset);
}

void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16 *) this->address(offset) = n;
}

void* SlottedPage::address(u16 offset) const {
    return (void *) ((char *) this->block.get_data() + offset);
}
