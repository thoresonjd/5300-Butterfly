/**
 * @file SlottedPage.cpp
 * @author K Lundeen
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
    for (auto const &record_id : *record_ids) {
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

bool assertion_failure(std::string message, double x, double y) {
    std::cout << "FAILED TEST: " << message;
    if (x >= 0)
        std::cout << " " << x;
    if (y >= 0)
        std::cout << " " << y;
    std::cout << std::endl;
    return false;
}

bool test_slotted_page() {
    // construct one
    char blank_space[DbBlock::BLOCK_SZ];
    Dbt block_dbt(blank_space, sizeof(blank_space));
    SlottedPage slot(block_dbt, 1, true);

    // add a record
    char rec1[] = "hello";
    Dbt rec1_dbt(rec1, sizeof(rec1));
    RecordID id = slot.add(&rec1_dbt);
    if (id != 1)
        return assertion_failure("add id 1");

    // get it back
    Dbt *get_dbt = slot.get(id);
    std::string expected(rec1, sizeof(rec1));
    std::string actual((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back " + actual);

    // add another record and fetch it back
    char rec2[] = "goodbye";
    Dbt rec2_dbt(rec2, sizeof(rec2));
    id = slot.add(&rec2_dbt);
    if (id != 2)
        return assertion_failure("add id 2");

    // get it back
    get_dbt = slot.get(id);
    expected = std::string(rec2, sizeof(rec2));
    actual = std::string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back " + actual);

    // test put with expansion (and slide and ids)
    char rec1_rev[] = "something much bigger";
    rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after expanding put
    get_dbt = slot.get(2);
    expected = std::string(rec2, sizeof(rec2));
    actual = std::string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back after expanding put of 1 " + actual);
    get_dbt = slot.get(1);
    expected = std::string(rec1_rev, sizeof(rec1_rev));
    actual = std::string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back after expanding put of 1 " + actual);

    // test put with contraction (and slide and ids)
    rec1_dbt = Dbt(rec1, sizeof(rec1));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after contracting put
    get_dbt = slot.get(2);
    expected = std::string(rec2, sizeof(rec2));
    actual = std::string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 2 back after contracting put of 1 " + actual);
    get_dbt = slot.get(1);
    expected = std::string(rec1, sizeof(rec1));
    actual = std::string((char *) get_dbt->get_data(), get_dbt->get_size());
    delete get_dbt;
    if (expected != actual)
        return assertion_failure("get 1 back after contracting put of 1 " + actual);

    // test del (and ids)
    RecordIDs *id_list = slot.ids();
    if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2)
        return assertion_failure("ids() with 2 records");
    delete id_list;
    slot.del(1);
    id_list = slot.ids();
    if (id_list->size() != 1 || id_list->at(0) != 2)
        return assertion_failure("ids() with 1 record remaining");
    delete id_list;
    get_dbt = slot.get(1);
    if (get_dbt != nullptr)
        return assertion_failure("get of deleted record was not null");

    // try adding something too big
    rec2_dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 10); // too big, but only because we have a record in there
    try {
        slot.add(&rec2_dbt);
        return assertion_failure("failed to throw when add too big");
    } catch (const DbBlockNoRoomError &exc) {
        // test succeeded - this is the expected path
    } catch (...) {
        // Note that this won't catch segfault signals -- but in that case we also know the test failed
        return assertion_failure("wrong type thrown when add too big");
    }

    // more volume
    std::string gettysburg = "Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal.";
    int32_t n = -1;
    uint16_t text_length = gettysburg.size();
    uint total_size = sizeof(n) + sizeof(text_length) + text_length;
    char* data = new char[total_size];
    *(int32_t *) data = n;
    *(uint16_t *) (data + sizeof(n)) = text_length;
    std::memcpy(data + sizeof(n) + sizeof(text_length), gettysburg.c_str(), text_length);
    Dbt dbt(data, total_size);
    std::vector<SlottedPage> page_list;
    BlockID block_id = 1;
    Dbt slot_dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
    slot = SlottedPage(slot_dbt, block_id++, true);
    for (int i = 0; i < 10000; i++) {
        try {
            slot.add(&dbt);
        } catch (DbBlockNoRoomError &exc) {
            page_list.push_back(slot);
            slot_dbt = Dbt(new char[DbBlock::BLOCK_SZ], DbBlock::BLOCK_SZ);
            slot = SlottedPage(slot_dbt, block_id++, true);
            slot.add(&dbt);
        }
    }
    page_list.push_back(slot);
    for (const auto &slot : page_list) {
        RecordIDs *ids = slot.ids();
        for (RecordID id : *ids) {
            Dbt *record = slot.get(id);
            if (record->get_size() != total_size)
                return assertion_failure("more volume wrong size", block_id - 1, id);
            void *stored = record->get_data();
            if (std::memcmp(stored, data, total_size) != 0)
                return assertion_failure("more volume wrong data", block_id - 1, id);
            delete record;
        }
        delete ids;
        delete[] (char *) slot.block.get_data();  // this is why we need to be a friend--just convenient
    }
    delete[] data;
    return true;
}