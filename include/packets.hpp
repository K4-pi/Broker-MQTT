#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

constexpr std::size_t packetBufferSize = 1024;

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

// First field of every io_uring user-data object, so a completion can tell a recv from a send
enum class RING_OP : std::uint8_t {
    RECV,
    SEND
};

struct io_uring;

struct message_accumulator_t {
    PACKET_TYPE type;
    uint8_t flags; // Lower 4 bits of fixed header byte 1
    uint8_t offset;
    size_t size;
    std::vector<uint8_t> data;
};

struct connection_packet_t {
    RING_OP ringOp = RING_OP::RECV;
    int fd;
    bool initialized = false;
    std::array<uint8_t, packetBufferSize> buffer{};
    message_accumulator_t message;
    std::chrono::time_point<std::chrono::steady_clock> keepAlive;
};

class Topic;

namespace broker
{
    MESSAGE_STATUS QueueSend(io_uring *ring, int fd, std::vector<std::uint8_t> data);
}

MESSAGE_STATUS HandleMessageData(int clientFd, message_accumulator_t *message, Topic *topicsRoot, io_uring *ring);
bool CheckKeepAlive(connection_packet_t *packet);
