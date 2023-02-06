# Makefile
# Justin Thoreson
# Seattle University, CPSC5300, Winter 2023

CCFLAGS = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c
VGFLAGS = --suppressions=valgrind.supp --leak-check=full --show-leak-kinds=all --track-fds=yes
COURSE = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR = $(COURSE)/lib
OBJS = sql5300.o heap_storage.o

# Rule for linking to create executable
sql5300 : $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $^ -ldb_cxx -lsqlparser

# Header file dependencies
sql5300.o : heap_storage.h storage_engine.h
heap_storage.o : heap_storage.h storage_engine.h

# General rule for compilation
%.o : %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o $@ $<

# Compile sql5300 and check for errors
check : sql5300
	valgrind $(VGFLAGS) ./$< ~/cpsc5300/data

# Rule for removing all non-source files
clean : 
	rm -f *.o sql5300
