CXX := g++ -std=c++20
CXXFLAGS := -Wall -Wextra -Wpedantic -Iinclude \
			-DBOOST_BIND_GLOBAL_PLACEHOLDERS -D_GLIBCXX_DEBUG \
			-Wno-volatile -Wno-catch-value -g
LDLIBS := -lboost_thread -luring -pthread

# Add -DDEBUG to CXXFLAGS for debug prints/logs

SOURCES := main.cpp $(wildcard src/*.cpp)
TARGET := build/broker-mqtt

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SOURCES)
	mkdir -p build
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDLIBS)

clean:
	rm -rf build
