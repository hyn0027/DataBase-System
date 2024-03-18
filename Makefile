# Target to build the main executable
all: main

# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++20 -Wall

# Source files
SRCDIR = src/src

# Include directories
INCLUDES = -I src/include -I src/include/antlr4 -I src/include/antlr4/atn -I src/include/antlr4/dfa -I src/include/antlr4/internal -I src/include/antlr4/misc -I src/include/antlr4/support -I src/include/antlr4/tree

# List of source files
LIBS = $(wildcard $(SRCDIR)/*/*.cpp src/include/antlr4/*.cpp src/include/antlr4/*/*.cpp)

# Output executable
main: $(SRCS)
	mkdir -p bin
	mkdir -p data
	mkdir -p data/global
	mkdir -p data/base
	$(CXX) $(CXXFLAGS) $(INCLUDES) src/main.cpp $(LIBS) -o bin/main

# Clean target to remove generated files
clean:
	rm -rf bin/*
