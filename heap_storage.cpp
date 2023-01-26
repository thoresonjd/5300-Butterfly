/**
 * @file        heap_storage.cpp for 5300-BUTTERFLY
 *              This is the heap storage engine for a database management 
 *              system.
 * @authors     Bobby Brown rbrown3 & Denis Rajic drajic
 * @date        Created 1/21/2023
 * @version     Milestone 2
 * 
*/

#include <cstring>
#include "heap_storage.h"

typedef u_int16_t u16;

/**
 * Test heap storage function. Returns true if all tests pass.
 * 
 * Code provided from requirements.
*/
bool test_heap_storage() 
{
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
    // drop makes the object unusable because of BerkeleyDB restriction
    table1.drop();  
    std::cout << "drop ok" << std::endl;
    
    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
    	return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
		return false;
    table.drop();

    return true;
}

// ---- SlottedPage methods ---- // 
// Constructor
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

// Get
Dbt* SlottedPage::get(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);

    if (loc == 0) {
        return NULL;
    }

    // TODO: I think this is right but not sure
    Dbt* dbt = new Dbt(this->get_block() + loc, size);
}

// Delete
void SlottedPage::del(RecordID record_id) {
    u16 size, loc;
    this->get_header(size, loc, record_id);
    this->put_header(record_id, 0, 0);
    this->slide(loc, loc + size);
}

// Put
void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16) data.get_size();

    if (new_size > size) {
        u16 extra = new_size - size;
        if (!this->has_room(extra)) {
            throw DbBlockNoRoomError("not enough room in block");
        }
        this->slide(loc + new_size, loc + size);
        memcpy(this->address(loc - extra), data.get_data(), loc + new_size);
    } else {
        memcpy(this->address(loc - new_size), data.get_data(), loc + new_size);
        this->slide(loc + new_size, loc + size);
    }

    get_header(size, loc, record_id);
    this->put_header(record_id, new_size, loc);
}

// Get ids
RecordIDs* SlottedPage::ids(void) {
    RecordIDs* recordIds = new RecordIDs();
    for (int i = 1; i <= this->num_records; i++) {
        u16 size, loc;
        get_header(size, loc, i);
        if (loc != 0) {
            recordIds->push_back(i);
        }
    }
    return recordIds;
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// Get header
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) {
    size = this->get_n(4 * id);
    loc = this->get_n(4 * id + 2);
}

// Put header
// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

// Has room
bool SlottedPage::has_room(u16 size) {
    u16 available = this->end_free - (this->num_records + 1) * 4;
    return size <= available;
}

void SlottedPage::slide(u16 start, u16 end) 
{
    // TODO
    int delta = end - start;

    if (delta == 0)
    {
        return;
    }

    u16 moveLoc = this->end_free + 1;
    u16 moveSize =  start - moveLoc;
    u16 newLoc = moveLoc + delta;

    // Current record location
    Dbt tempData(this->address(moveLoc), moveSize);
    memcpy(this->address(newLoc), this->address(moveLoc), moveSize);

    // Update headers
    u16 size, location;
    for (auto const& record_id: *ids())
    {
         get_header(size, location, record_id);

         if (location != 0 && location <= start)
         {
            location += delta;
            put_header(record_id, size, location);
         }
    }

    // Update end_free
    this->end_free += delta;

    this->put_header();
}



// ---- HeapFile methods ---- // 


// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

void HeapFile::create(void) {
    // TODO
    return;
}

void HeapFile::open(void) {
    // TODO
    return;
}

void HeapFile::close(void) {
    // TODO
    return;
}

SlottedPage* HeapFile::get(BlockID block_id) {
    // TODO
    return NULL;
}

void HeapFile::put(DbBlock *block) {
    // TODO
    return;
}

BlockIDs* HeapFile::block_ids() {
    // TODO
    return NULL;
}



// ---- HeapTable methods ---- // 

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

void HeapTable::create() {
    // TODO
    return;
}

void HeapTable::create_if_not_exists() {
    // TODO
    return;
}

void HeapTable::open() {
    // TODO
    return;
}

void HeapTable::close() {
    // TODO
    return;
}

void HeapTable::drop() {
    // TODO
    return;
}

Handle HeapTable::insert(const ValueDict *row) {
    // TODO
    // Only handle two data types for now, INTEGER (or INT) and TEXT
}