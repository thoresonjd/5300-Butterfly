# Makefile
# Kevin Lundeen, Justin Thoreson
# Seattle University, CPSC5300, Winter 2023

CCFLAGS = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c
VGFLAGS = --suppressions=valgrind.supp --leak-check=full # --show-leak-kinds=all --track-fds=yes
COURSE = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR = $(COURSE)/lib

# Rule for linking to create executable
OBJS = sql5300.o SlottedPage.o HeapFile.o HeapTable.o ParseTreeToString.o SQLExec.o schema_tables.o storage_engine.o
sql5300 : $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $^ -ldb_cxx -lsqlparser

# Header file dependencies
HEAP_STORAGE_H = heap_storage.h SlottedPage.h HeapFile.h HeapTable.h storage_engine.h
SCHEMA_TABLES_H = schema_tables.h $(HEAP_STORAGE_H)
SQLEXEC_H = SQLExec.h $(SCHEMA_TABLES_H)
ParseTreeToString.o : ParseTreeToString.h
SQLExec.o : $(SQLEXEC_H)
SlottedPage.o : SlottedPage.h
HeapFile.o : HeapFile.h SlottedPage.h
HeapTable.o : $(HEAP_STORAGE_H)
schema_tables.o : $(SCHEMA_TABLES_) ParseTreeToString.h
sql5300.o : $(SQLEXEC_H) ParseTreeToString.h
storage_engine.o : storage_engine.h

# General rule for compilation
%.o : %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o $@ $<

# Compile sql5300 and check for errors
check : sql5300
	valgrind $(VGFLAGS) ./$< ~/cpsc5300/data

# Rule for removing all non-source files
clean : 
	rm -f *.o sql5300
