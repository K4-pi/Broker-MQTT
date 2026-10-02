#pragma once

#include "packets.hpp"
#include "sys/epoll.h"

#include <inttypes.h>
#include <mutex>
#include <map>

namespace broker
{
    constexpr uint8_t  mqttMessageHeaderSize = 6;   // MQTT max header size = 6
    constexpr uint16_t epollTimeoutMs        = 250;
    constexpr uint16_t ringEntries           = 256;
    constexpr uint16_t maxEvents             = 16;

    /**
     * Store epoll data
     */
    struct epoll_registry_t
    {
        int epollFd;
        struct epoll_event event;
        struct epoll_event events[maxEvents];
    };

    /**
     * Store connections with mutex for it
     */
    struct connection_registry_t
    {
        std::map<int, connection_packet_t*> entries; // Key = fd, Value = packet
        std::mutex mutex;
    };

    /**
     * @brief Start the broker event loop.
     *
     * Waits for epoll events, accepts new connections, schedules read
     * submissions, and processes io_uring completions.
     */
    void Start(char *address, int port);

    /**
     * @brief Print startup/info message for the broker process.
     */
    void PrintInfo();
}
