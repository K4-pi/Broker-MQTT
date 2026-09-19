#pragma once

#include <array>
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
    FINISHED,
    INVALID_VALUE,
    INVALID_TYPE,
    SECOND_CONNECT
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
};

MESSAGE_STATUS handle_message_data(int client_fd, MessageAccumulator *message);
