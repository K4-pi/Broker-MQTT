CXX := g++
CXXFLAGS := -Wall -Wextra -Wpedantic -std=c++20 -Iinclude
SRC_DIR := src
BUILD_DIR := build
TARGET := $(BUILD_DIR)/broker-mqtt

SRC_SOURCES := $(wildcard $(SRC_DIR)/*.cpp)
ROOT_SOURCES := main.cpp
OBJECTS := $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SRC_SOURCES)) \
	$(patsubst %.cpp,$(BUILD_DIR)/%.o,$(ROOT_SOURCES))

.PHONY: all run clean

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) $^ -o $@

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR)/%.o: %.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -rf $(BUILD_DIR)
