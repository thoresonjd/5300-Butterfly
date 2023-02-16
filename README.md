# 5300-Butterfly
DB Relation Manager project for CPSC5300 at Seattle U, Winter 2023, Project Butterfly

The objective of this project is to build a relation manager in C++.
It utilizes the [Hyrise SQL Parser for C++](https://github.com/hyrise/sql-parser) and the [Berkeley DB](https://www.oracle.com/database/technologies/related/berkeleydb.html) as a file manager.

A relation manager is part of a full DBMS and is the core of the system, and this project will be able to handle some simple queries.

## **Sprint Verano**

### **Team**:
Bobby Brown and Denis Rajic

### **Milestone 1: Skeleton**

Objective: To assembly an SQL shell program in C++ that runs from the command line and prompts the user for SQL statements, and executes them one at a time.

The CREATE TABLE and SELECT SQL statements are supported in this Milestone.

### **Milestone 2: Rudimentary Storage Engine**

Objective: To implement a rudimentary storage engine in the relation manager program, using a heap file organization as the storage engine. This heap file organization utilzes a slotted page as the block architecture, with the `RecNo` file type from Berkeley DB.

### **Compliation**

Run `$ make` on the command line to compile.

### **Usage**

To run the program, enter `$ ./sql5300 [PATH]` where `[PATH]` is the directory where the database environment resides (most likely it is `~/cpsc5300/data`).

### **Testing**

To test the program, enter `SQL> test` at the prompt. This will run the `test_heap_storage` function in the [`heap_storage.cpp`](https://github.com/klundeen/5300-Butterfly/blob/main/heap_storage.cpp) file.

If there are any issues, first try clearing out files from your database environment.

### **Handoff**

[Handoff video](https://seattleu.instructuremedia.com/embed/45579d45-20d7-405c-8cd3-4851fb004d18)

## **Sprint Otoño**

### **Team**

Justin Thoreson & Grandi Radhakrishna Rakesh

### **Milestone 3: Schema Storage**

Rudimentary implementation of the following SQL queries (shown in [BNF](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form) notation):

CREATE TABLE
```sql
<table_definition> ::= CREATE TABLE <table_name> ( <column_definitions> )
<column_definitions> ::= <column_definition> | <column_definition>, <column_definitions>
<column_definition> ::= <column_name> <datatype>
```
    
DROP TABLE
```sql
<drop_table_statement> ::= DROP TABLE <table_name>
```

SHOW TABLES
```sql
SELECT table_name FROM _tables WHERE table_name NOT IN ("_tables", "_columns");
```

SHOW COLUMNS
```sql
<show_columns_statement> ::= SHOW COLUMNS FROM <table_name>
```

### **Milestone 4: Indexing Setup**

Setting up SQL index commands prior to actual index implementation. The following index commands (modeled after [MySQL](https://dev.mysql.com/doc/refman/5.7/en/create-index.html)) are supported:
```sql
CREATE INDEX index_name ON table_name [USING {BTREE | HASH}] (col1, col2, ...)
SHOW INDEX FROM table_name
DROP INDEX index_name ON table_name
```

### **Compilation**

To compile, execute the [`Makefile`](./Makefile) via:
```
$ make
```

### **Usage**

To execute, run: 
```
$ ./sql5300 [ENV_DIR]
``` 
where `ENV_DIR` is the directory where the database environment resides.

SQL statements can be provided to the SQL shell when running. To terminate the SQL shell, run: 
```sql
SQL> quit
```

### **Testing**

To test the functionality of the rudimentary storage engine, run:
```sql
SQL> test
```

### **Error & Memory Leak Checking**

If any issues arise, first try clearing out all the files within the database environment directory.

Checking for memory leaks can be done with [Valgrind](https://valgrind.org/). Valgrind error suppressions have been configured in the [`valgrind.supp`](./valgrind.supp) file. A target within the Makefile has been configured with relevant flags to execute Valgrind via running the command: 
```
$ make check
```