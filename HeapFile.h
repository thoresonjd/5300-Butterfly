/**
 * @file HeapFile.h - Implementation of storage_engine with a heap file structure.
 * HeapFile: DbFile
 *
 * @authors Kevin Lundeen, Justin Thoreson
 * @see "Seattle University, CPSC5300, Winter 2023"
 */

#pragma once

#include "db_cxx.h"
#include "SlottedPage.h"


/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one
 * of our database blocks for each Berkeley DB record in the RecNo file.
 * In this way we are using Berkeley DB for buffer management and file
 * management. Uses SlottedPage for storing records within blocks.
 */
class HeapFile : public DbFile {
public:
    /**
     * Constructor
     * @param name
     */
    HeapFile(std::string name);

    virtual ~HeapFile() {}

    HeapFile(const HeapFile& other) = delete;

    HeapFile(HeapFile&& temp) = delete;

    HeapFile& operator=(const HeapFile& other) = delete;

    HeapFile& operator=(HeapFile&& temp) = delete;

    /**
     * Create physical database file
     */
    virtual void create(void);

    /**
     * Remove physical database file
     */
    virtual void drop(void);

    /**
     * Open the database file
     */
    virtual void open(void);

    /**
     * Close the database file
     */
    virtual void close(void);

    /**
     * Allocate a new block for the database file.
     * Returns the new empty DbBlock that is managing the records in this block and its block id.
     */
    virtual SlottedPage* get_new(void);

    /**
     * Retrieves a block from the database file
     * @param block_id The id of the block to retrieve
     * @return A slotted page, the data of the block requested
     */
    virtual SlottedPage* get(BlockID block_id);

    /**
     * Writes a block to the database file
     * @param block The block to write to the database file
     */
    virtual void put(DbBlock* block);

    /**
     *  Retrieves all block IDs of blocks within the database file
     */
    virtual BlockIDs* block_ids() const;

    /**
     * Retrieves the last block ID within the file
     */
    virtual u_int32_t get_last_block_id() { return last; }

protected:
    std::string dbfilename;
    u_int32_t last;
    bool closed;
    Db db;

    /**
     * Open the Berkeley DB database file
     * @param flags Flags to provide the Berkeley DB database file
     */ 
    virtual void db_open(uint flags = 0);

    virtual uint32_t get_block_count();
};
