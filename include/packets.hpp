#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

constexpr std::size_t PACKET_BUFFER_SIZE = 1024;

enum PACKET_TYPE {
    NONE        = 0,
    CONNECT     = 1,
    CONNACK     = 2,
    PUBLISH     = 3,
    PUBACK      = 4,
    PUBREC      = 5,
    PUBREL      = 6,
    PUBCOMP     = 7,
    SUBSCRIBE   = 8,
    SUBACK      = 9,
    UNSUBSCRIBE = 10,
    UNSUBACK    = 11,
    PINGREQ     = 12,
    PINGRESP    = 13,
    DISCONNECT  = 14,
    AUTH        = 15
};

enum MESSAGE_STATUS {
    OK,
    FAILURE
};

struct MessageAccumulator {
    PACKET_TYPE type;
    uint8_t offset;
    size_t size;
    std::vector<uint8_t> data;
};

struct ConnectionPacket {
    int fd;
    bool initialized = false;
    std::array<uint8_t, PACKET_BUFFER_SIZE> buffer{};
    MessageAccumulator message;
    std::chrono::time_point<std::chrono::steady_clock> keep_alive;
};

MESSAGE_STATUS handle_message_data(int client_fd, MessageAccumulator *message);
bool check_keep_alive(ConnectionPacket *packet);
