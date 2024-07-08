# Output binary
PROGRAM_TARGET = database
TEST_TARGET = test
EXPERIMENT_TARGET = experiment

# Compiler to use
CXX = g++

# Compiler flags
CXXFLAGS = -g -Wall -std=c++11

# Source files for test and experiment
GENERAL_SOURCES = bufferpool.cpp database.cpp hashTable.cpp memtable.cpp SST.cpp SSTManager.cpp 
PROGRAM_SOURCES = $(GENERAL_SOURCES) user_interface.cpp
TEST_SOURCES = $(GENERAL_SOURCES) test.cpp
EXPERIMENT_SOURCES = $(GENERAL_SOURCES) experiments.cpp

# Object files for test and experiment
PROGRAM_OBJECTS = $(PROGRAM_SOURCES:.cpp=.o)
TEST_OBJECTS = $(TEST_SOURCES:.cpp=.o)
EXPERIMENT_OBJECTS = $(EXPERIMENT_SOURCES:.cpp=.o)

all: $(PROGRAM_TARGET) $(TEST_TARGET) $(EXPERIMENT_TARGET)

# Rule to compile
$(PROGRAM_TARGET): $(PROGRAM_OBJECTS)
	$(CXX) $(CXXFLAGS) $(PROGRAM_OBJECTS) -o $(PROGRAM_TARGET)

$(TEST_TARGET): $(TEST_OBJECTS)
	$(CXX) $(CXXFLAGS) $(TEST_OBJECTS) -o $(TEST_TARGET)

$(EXPERIMENT_TARGET): $(EXPERIMENT_OBJECTS)
	$(CXX) $(CXXFLAGS) $(EXPERIMENT_OBJECTS) -o $(EXPERIMENT_TARGET)

# Rule to compile
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean up object files
clean:
	rm -f $(PROGRAM_OBJECTS) $(PROGRAM_TARGET) $(TEST_OBJECTS) $(TEST_TARGET) $(EXPERIMENT_OBJECTS) $(EXPERIMENT_TARGET)
	rm -f -r ./SSTs/*
	rm -f *.txt *.png
run:
	./$(PROGRAM_TARGET)