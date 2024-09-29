# Compiler
CXX = g++

# Compiler flags
CXXFLAGS = -Wall -g

# Source files
SRCS = $(wildcard *.cpp)

# Object files
OBJS = $(SRCS:.cpp=.o)

# Executable name
TARGET = app

# Include path
CXXFLAGS += -I../include

# Default target
all: $(TARGET)

# Link object files to create the executable
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

# Compile source files into object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean up build files
clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean