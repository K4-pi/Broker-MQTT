#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

constexpr std::size_t PACKET_BUFFER_SIZE = 8;

enum PACKET_TYPE {
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

struct MessageAccumulator {
    PACKET_TYPE type;
    uint8_t offset;
    size_t size;
    std::vector<uint8_t> data;
};

struct ConnectionPacket {
    int fd;
    std::array<uint8_t, PACKET_BUFFER_SIZE> buffer{};
    MessageAccumulator message;
};
