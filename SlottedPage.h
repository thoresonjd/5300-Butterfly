/**
 * @file SlottedPage.h - Implementation of storage_engine with a heap file structure.
 * SlottedPage: DbBlock
 *
 * @authors Kevin Lundeen, Justin Thoreson
 * @see "Seattle University, CPSC5300, Winter 2023"
 */

#pragma once

#include <string>
#include "storage_engine.h"


/**
 * @class SlottedPage - heap file implementation of DbBlock.
 *
 * Manage a database block that contains several records.
 * Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.
 *
 * Record id are handed out sequentially starting with 1 as records are added with add().
 * Each record has a header which is a fixed offset from the beginning of the block:
 *     Bytes 0x00 - Ox01: number of records
 *     Bytes 0x02 - 0x03: offset to end of free space
 *     Bytes 0x04 - 0x05: size of record 1
 *     Bytes 0x06 - 0x07: offset to record 1
 *     etc.
 */
class SlottedPage : public DbBlock {
public:
    SlottedPage(Dbt& block, BlockID block_id, bool is_new = false);

     // Big 5 - use the defaults
    virtual ~SlottedPage() {}

    /**
     * Adds a new record to a slotted page
     * @param data The data of the record
     * @return The record ID of the record
     */
    virtual RecordID add(const Dbt* data);

    /**
     * Retrieves a record from a slotted page
     * @param record_id The ID of the record to retrieve
     * @return The data of the record (location and size)
     */
    virtual Dbt* get(RecordID record_id) const;

    /**
     * Puts a new record in the place of an existing record in a slotted page
     * @param record_id The ID of the record to replace
     * @param data The data to replace the existing record with
     */
    virtual void put(RecordID record_id, const Dbt& data);

    /**
     * Removes a record from a slotted page
     * @param record_id The ID of the record to remove 
     */
    virtual void del(RecordID record_id);

    /**
     * Retrieves the IDs of all records in a slotted page
     */
    virtual RecordIDs* ids(void) const;

    u_int16_t num_records;
    u_int16_t end_free;
protected:

    /**
     * Retrieves the header (size and location) of the record within a slotted page
     * @param size The size of the record
     * @param loc The location (offset) of the record
     * @param id The ID of the record
     */
    void get_header(u_int16_t& size, u_int16_t& loc, RecordID id = 0) const;

    /**
     * Store the size and offset for given ID. For ID = 0, store the block header.
     * @param id The ID of a record
     * @param size The size of the record
     * @param loc The location (offset) of the record
     */
    void put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0);

    /**
     * Checks the slotted page if there is enough free memory to add a new record
     * @param size The size of the new record
     * @return True if the record can fit into the slotted page, false otherwise
     */
    bool has_room(u_int16_t size) const;

    /**
     * If start < end, then remove data from offset start up to but not including
     * offset end by sliding data that is to the left of start to the right. If
     * start > end, then make room for extra data from end to start by sliding data
     * that is to the left of start to the left. Also fix up any record headers
     * whose data has slid. Assumes there is enough room if it is a left shift 
     * (end < start).
     * @param start The begining of section of memory in a slotted page
     * @param end The end of section of memory in a slotted page
     */
    virtual void slide(u_int16_t start, u_int16_t end);

    /** 
     * Get 2-byte integer at given offset in block.
     */ 
    u_int16_t get_n(u_int16_t offset) const;

    /** 
     * Put a 2-byte integer at given offset in block.
     */
    void put_n(u_int16_t offset, u_int16_t n);

    /** 
     * Make a void* pointer for a given offset into the data block.
     */
    void* address(u_int16_t offset) const;

    /**
     * Test function in tests.h must be friend for convenience
     */
    friend bool test_slotted_page();
};
