# 5300-Butterfly
DB Relation Manager project for CPSC5300 at Seattle U, Winter 2023, Project Butterfly

The objective of this project is to build a relation manager in C++.
It utilizes the [Hyrise SQL Parser for C++](https://github.com/hyrise/sql-parser) and the [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html) as a file manager.

A relation manager is part of a full DBMS and is the core of the system, and this project will be able to handle some simple queries.

## **Sprint Verano**

### Team:
Bobby Brown and Denis Rajic

### **Milestone 1: Skeleton**

Objective: To assembly an SQL shell program in C++ that runs from the command line and prompts the user for SQL statements, and executes them one at a time.

The CREATE TABLE and SELECT SQL statements are supported in this Milestone.

### **Milestone 2: Rudimentary Storage Engine**

Objective: To implement a rudimentary storage engine in the relation manager program, using a heap file organization as the storage engine. This heap file organization utilzes a slotted page as the block architecture, with the `RecNo` file type from Berkeley DB.

### Compliation

Run `$ make` on the command line to compile.

### Usage

To run the program, enter `$ ./sql5300 [PATH]` where `[PATH]` is the directory where the database environment resides (most likely it is `~/cpsc5300/data`).

### Testing

To test the program, enter `SQL> test` at the prompt. This will run the `test_heap_storage` function in the [heap_storage.cpp](https://github.com/klundeen/5300-Butterfly/blob/main/heap_storage.cpp) file.

If there are any issues, first try clearing out files from your database environment.

### Handoff
https://seattleu.instructuremedia.com/embed/45579d45-20d7-405c-8cd3-4851fb004d18