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
    // Setup for table column names and attributes
    ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);

    // Test create and drop of table
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    // drop makes the object unusable because of BerkeleyDB restriction
    table1.drop();  
    std::cout << "drop ok" << std::endl;
    
    // Test create table if table does not exist
    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    // Insert rows into table
    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    
    // Select and project rows in the table
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);

    // Store values from the table
    Value valueA = (*result)["a"], valueB = (*result)["b"];
    std::cout << "project ok" << std::endl;

    // Drop table and cleanup memory
    table.drop();
    delete handles;
    delete result;

    // Test stored values from projection
    if (valueA.n != 12)
        return false;
    
    if (valueB.s != "Hello!")
        return false;

    return true;
}

// ---- SlottedPage methods ---- // 
// Constructor
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new)
{
    // Check is_new flag
    if (is_new) {
        // Set number of records and end_free
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        
        // Create block header
        put_header();
    } else {
        // Get the header of the existing slotted page
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data)
{
    // Check if the slotted page has room
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    
    u16 id = ++this->num_records;   // Current id based on number of reccords
    u16 size = (u16) data->get_size();  // Current size
    this->end_free -= size;         // Setting the free end
    u16 loc = this->end_free + 1;   // Setting the location
    
    put_header();               // Update block header
    put_header(id, size, loc);  // Update

    // Memory copy of data
    memcpy(this->address(loc), data->get_data(), size);
    
    return id;
}

// Get
Dbt* SlottedPage::get(RecordID record_id)
{
    // size and loc based on passed in record_id
    u16 size = get_n(4 * record_id);
    u16 loc = get_n(4 * record_id + 2);

    // get_header with all three variables
    get_header(size, loc, record_id);

    // If loc is zero, return NULL
    if (loc == 0) {
        return NULL;
    }

    // Return new Dbt based on loc and size
    return new Dbt(this->address(loc), size);
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
    for (u16 i = 1; i <= this->num_records; i++) {
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

void HeapFile::create(void)
{
    // Open database
    this->db_open(DB_CREATE | DB_TRUNCATE);

    // Create new block and add to database
    SlottedPage* block = this->get_new();
    this->put(block);

    // Delete memory for empty block
    delete block;
}

void HeapFile::open(void) {
    this->db_open();
}

void HeapFile::drop(void)
{
    // Close the current HeapFile
    this->close();

    // character array to hold home path
    const char **homePath = new const char*[1024];

    // Get homepath for current database environment
    _DB_ENV->get_home(homePath);

    // Build string for path to database file
    std::string filePath = std::string(*homePath) + "/" + this->dbfilename;

    // Remove filePath and store as integer
    int dropResult = std::remove(filePath.c_str());

    // Free memory for homePath char array
    delete[] homePath;

    // Check result of remove operation and display error if file not removed
    if (dropResult != 0)
        throw std::logic_error("Unabel to remove DB file");
}

void HeapFile::close(void)
{
    this->db.close(0);
    this->closed = true;
}

SlottedPage* HeapFile::get(BlockID block_id)
{
    // Dbt key for given block_id
    Dbt key(&block_id, sizeof(block_id));
    Dbt block;  // Empty block

    // Get data from database, and store in empty block
    this->db.get(NULL, &key, &block, 0);

    // Return new slotted page from the block
    return new SlottedPage(block, block_id);

}

void HeapFile::put(DbBlock *block)
{
    // Get the block_id for the given block
    BlockID block_id = block->get_block_id();
    
    // Dbt key for given block_id
    Dbt key(&block_id, sizeof(block_id));

    // Put the key and block in the database
    this->db.put(NULL, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids()
{
    // Create new BlockIDs structure (vector)
    BlockIDs* block_ids = new BlockIDs();

    // Traverse through block_ids and return the vector
    for (BlockID block_id = 1; block_id <= this->last; block_id++)
    {
        block_ids->push_back(block_id);
    }

    return block_ids;
}

void HeapFile::db_open(uint flags) {
    if (!this->closed) {
        return;
    }
    this->db.set_re_len(DbBlock::BLOCK_SZ);
    this->dbfilename = this->name + ".db";
    this->db.set_message_stream(_DB_ENV->get_message_stream());
    this->db.set_error_stream(_DB_ENV->get_error_stream());
    this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0);
    this->closed = false;
}


// ---- HeapTable methods ---- // 

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes), file(table_name) {
        
}

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

ValueDict* HeapTable::unmarshal(Dbt *data) {
    ValueDict* row = new ValueDict();
    char* bytes = (char*) data->get_data();
    int offset = 0;

    for (u_int32_t i = 0; i < column_names.size(); i++) {
        ColumnAttribute columnAttribute = this->column_attributes.at(i);
        if (columnAttribute.get_data_type() == ColumnAttribute::DataType::INT) {
            Value value = Value(*(u_int32_t*) (bytes + offset));
            row->insert({column_names.at(i), value});
            offset += sizeof(u_int32_t);
        } else if (columnAttribute.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16*)(bytes + offset);
            offset+= sizeof(u16);
            Value value(std::string(bytes + offset, size));
            row->insert({column_names.at(i), value});
            offset += size;
        } else {
            throw DbRelationError("Cannot unmarshal " + columnAttribute.get_data_type());
        }
    }

    return row;
}


void HeapTable::create() {
    this->file.create();
}

void HeapTable::create_if_not_exists() {
    try {
        this->file.open();
    } catch (DbException &e) {
        this->file.create();
    }
}

void HeapTable::open() {
    this->file.open();
}

void HeapTable::close() {
    this->file.close();
}

void HeapTable::drop() {
    this->file.drop();
}

Handle HeapTable::insert(const ValueDict *row) {
    this->open();
    ValueDict* validatedRow = this->validate(row);
    return this->append(validatedRow);
}

Handle HeapTable::append(const ValueDict *row) {
    // Marshal the data
    Dbt* data = this->marshal(row);
    // Get last block
    SlottedPage* block = this->file.get(this->file.get_last_block_id());

    RecordID recordId;
    // Add data to block, if full create new blcok
    try {
        recordId = block->add(data);
    } catch (DbRelationError& e) {
        block = this->file.get_new();
        recordId = block->add(data);
    }

    // Add new block to file, return block and record ids
    this->file.put(block);
    return Handle(this->file.get_last_block_id(), recordId);

}

ValueDict* HeapTable::validate(const ValueDict *row) {
    ValueDict* fullRow = new ValueDict();
    for (Identifier &columnName : this->column_names) {
        ValueDict::const_iterator column = row->find(columnName);
        if (column == row->end()) {
            throw DbRelationError("no column name");
        } else {
            fullRow->insert({columnName, column->second});
        }
    }
    return fullRow;
}

void HeapTable::del(const Handle handle) {
    return;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    return;
}


Handles* HeapTable::select() {
    return this->select(nullptr);
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

ValueDict* HeapTable::project(Handle handle) {
    return this->project(handle, nullptr);
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
    BlockID blockId = handle.first;
    RecordID recordId = handle.second;
    SlottedPage* block = this->file.get(blockId);
    Dbt* data = block->get(recordId);
    ValueDict* row = this->unmarshal(data);

    if (column_names == NULL) {
        return row;
    }

    ValueDict* newRow = new ValueDict();
    for (Identifier columnName : *column_names) {
        newRow->insert({columnName, row->at(columnName)});
    }
    return row;
}