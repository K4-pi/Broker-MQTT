#include "broker.hpp"
#include "packets.hpp"
#include "error.hpp"
#include "threadpool/pool.hpp"

#include <algorithm>
#include <array>
#include <asm-generic/socket.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <sys/types.h>
#include <system_error>
#include <thread>

#include "sys/epoll.h"
#include "sys/socket.h"
#include "netinet/in.h"
#include "linux/io_uring.h"
#include "liburing.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#ifdef DEBUG
#include "stdio.h"
#endif

namespace broker
{
    void Setup(
        char *address,
        int port,
        int *listenSock,
        sockaddr_in *serverAddr,
        struct io_uring *ringBuffer,
        struct epoll_registry_t *epollRegistry
    );
    void Start(char *address, int port);
    void PrintInfo();
    void FdHandlerSubmit(
        int clientFd,
        struct io_uring *ringBuffer,
        struct connection_registry_t *connections
    );
    void ProcessMessage(
        connection_packet_t *packet,
        io_uring_cqe *cqe,
        struct io_uring *ringBuffer,
        struct epoll_registry_t *epollRegistry,
        struct connection_registry_t *connections
    );
    void ProcessPackets(
        struct io_uring *ringBuffer,
        struct epoll_registry_t *epollRegistry,
        struct connection_registry_t *connections
    );
    ssize_t DecodeMessageLength(connection_packet_t *packet);
    void ManagePacketLiveTime(
        epoll_registry_t *epollRegistry,
        connection_registry_t *connections
    );
    static void RequestMessage(
        connection_packet_t *packet,
        size_t messageSize,
        struct io_uring *ringBuffer
    );
    static void DisconnectClient(
        int clientFd,
        struct epoll_registry_t *epollRegistry,
        struct connection_registry_t *connections
    );

    /**
     * @brief Submit async recv request for the next message chunk.
     *
     * @param packet Per-connection packet state to associate with completion.
     * @param messageSize Requested number of bytes to read (capped to buffer size).
     * @param ringBuffer IO uring buffer used to write/read data between client and broker.
     */
    static void RequestMessage(
        connection_packet_t *packet,
        size_t messageSize,
        struct io_uring *ringBuffer
    )
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(ringBuffer);
        if (sqe)
        {
            io_uring_prep_recv(
                sqe,
                packet->fd,
                packet->buffer.data(),
                std::min<std::size_t>(messageSize, packet->buffer.size()), // Min so we don't exceed array size
                0
            );
            io_uring_sqe_set_data(sqe, packet);
            io_uring_submit(ringBuffer);
        }
    }

    /**
     * @brief Disconnects client from a server, also removes them from connection registery.
     *
     * @param clientFd client connection file descriptor.
     * @param epollRegistry epoll status.
     * @param connections currently connected clients.
     */
    static void DisconnectClient(
        int clientFd,
        struct epoll_registry_t *epollRegistry,
        struct connection_registry_t *connections
    )
    {
        epoll_ctl(epollRegistry->epollFd, EPOLL_CTL_DEL, clientFd, nullptr);
        close(clientFd);
        {
            std::lock_guard<std::mutex> lock(connections->mutex);
            connections->entries.erase(clientFd);
        }

        #ifdef DEBUG
        std::cout << "client with fd [" << clientFd << "] disconnected" << std::endl;
        #endif
    }

    /**
     * @brief Configure listening socket, epoll, and io_uring resources.
     *
     * @param address IPv4 bind address in text form.
     * @param port TCP bind port.
     * @param listenSock server listen socket file descriptor.
     * @param serverAddr pointer to sockaddr_in data where address will be saved.
     * @param ringBuffer IO uring buffer used to write/read data between client and broker.
     * @param epollRegistry epoll status.
     */
    void Setup(
        char *address,
        int port,
        int *listenSock,
        sockaddr_in *serverAddr,
        struct io_uring *ringBuffer,
        struct epoll_registry_t *epollRegistry
    )
    {
        try
        {
            memset(serverAddr, 0, sizeof(*serverAddr));

            serverAddr->sin_family = AF_INET;
            serverAddr->sin_port = htons(port);
            ThrowIfError(inet_pton(serverAddr->sin_family, address, &serverAddr->sin_addr), "inet_pton");

            *listenSock = socket(AF_INET, SOCK_STREAM, 0);
            ThrowIfError(*listenSock, "listen_sock");

            const int enable = 1;
            ThrowIfError(setsockopt(*listenSock, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)), "setsockopt: listenSock");
            ThrowIfError(bind(*listenSock, (sockaddr *)serverAddr, sizeof(*serverAddr)), "bind: listenSock");
            ThrowIfError(listen(*listenSock, 8), "listen");

            epollRegistry->epollFd = epoll_create1(0);
            ThrowIfError(epollRegistry->epollFd, "epoll_create1");

            int ringInitRc = io_uring_queue_init(ringEntries, ringBuffer, 0);
            if (ringInitRc < 0)
                throw std::system_error(-ringInitRc, std::generic_category(), "io_uring_queue_init");

            epollRegistry->event.events = EPOLLIN;
            epollRegistry->event.data.fd = *listenSock;
            ThrowIfError(epoll_ctl(epollRegistry->epollFd, EPOLL_CTL_ADD, *listenSock, &epollRegistry->event), "epoll_ctl: listen_sock");
        } // try
        catch (const std::system_error &e)
        {
            std::cout << e.what() << "\n";
            exit(EXIT_FAILURE);
        }
    }

    /**
     * @brief Run the main broker loop with I/O handling.
     *
     * @param address IPv4 bind address in text form.
     * @param port TCP bind port.
     */
    void Start(char *address, int port)
    {
        int listenSock;
        struct sockaddr_in serverAddr;
        struct io_uring ringBuffer;
        struct epoll_registry_t epollRegistry;
        Setup(address, port, &listenSock, &serverAddr, &ringBuffer, &epollRegistry);

        static boost::threadpool::pool workers(std::thread::hardware_concurrency());
        struct connection_registry_t connections;

        while (true)
        {
            ManagePacketLiveTime(&epollRegistry, &connections);

            int nFds = epoll_wait(epollRegistry.epollFd, epollRegistry.events, maxEvents, epollTimeoutMs);
            try
            {
                ThrowIfError(nFds, "epoll_wait");
            }
            catch (const std::system_error &e)
            {
                std::cerr << e.what() << std::endl;
                exit(EXIT_FAILURE);
            }

            for (int n = 0; n < nFds; ++n)
            {
                int fd = epollRegistry.events[n].data.fd;
                if (fd == listenSock)
                {
                    int connectionSock;
                    try
                    {
                        socklen_t serverAddrLen = sizeof(serverAddr);
                        connectionSock = accept4(listenSock, (struct sockaddr *) &serverAddr, &serverAddrLen, SOCK_NONBLOCK);
                        ThrowIfError(connectionSock, "accept4");

                        epollRegistry.event.events = EPOLLIN | EPOLLET;
                        epollRegistry.event.data.fd = connectionSock;
                        ThrowIfError(epoll_ctl(epollRegistry.epollFd, EPOLL_CTL_ADD, connectionSock, &epollRegistry.event), "epoll_ctl: connection_sock");
                    }
                    catch (const std::system_error &e)
                    {
                        std::cerr << e.what() << std::endl;

                        // Close socket if accept4 succeded but epoll failed
                        if (connectionSock != -1) close(connectionSock);
                        continue;
                    }
                }
                else
                {
                    {
                        std::lock_guard<std::mutex> lock(connections.mutex);

                        // if packet is not already there, we create new one
                        if (!connections.entries.contains(fd))
                        {
                            connection_packet_t *packet = new connection_packet_t();
                            packet->keepAlive = std::chrono::steady_clock::now();

                            if (packet) connections.entries.emplace(fd, packet);
                        }
                    }

                    workers.schedule([fd, &ringBuffer, &connections]() {
                        FdHandlerSubmit(fd, &ringBuffer, &connections);
                    });
                }
            } // for
            ProcessPackets(&ringBuffer, &epollRegistry, &connections);
        } // while (true)
    }

    /**
     * @brief Print info.
     */
    void PrintInfo()
    {
        std::cout << "Broker-MQTT, started!" << std::endl;
    }

    /**
     * @brief Fetch connection state for @p clientFd and submit initial read.
     *
     * Scheduled by worker threads after an epoll readability event is observed.
     *
     * @param clientFd Connected client socket file descriptor.
     * @param ringBuffer IO uring buffer used to write/read data between client and broker.
     * @param connections currently connected clients.
     */
    void FdHandlerSubmit(
        int clientFd,
        struct io_uring *ringBuffer,
        struct connection_registry_t *connections)
    {
        connection_packet_t* packet;
        try
        {
            std::lock_guard<std::mutex> lock(connections->mutex);
            packet = connections->entries.at(clientFd);
        }
        catch (const std::out_of_range&) { return; }

        packet->fd = clientFd;
        packet->message.offset = 0;
        packet->message.size = 0;
        RequestMessage(packet, mqttMessageHeaderSize, ringBuffer);
    }

    /**
     * @brief Take actions based on ongoing message allocation.
     *
     * @param packet pointer to packet with processed message.
     * @param cqe IO Completion Queue Entry
     * @param ringBuffer IO uring buffer used to write/read data between client and broker.
     * @param epollRegistry epoll status.
     * @param connections currently connected clients.
     */
    void ProcessMessage(
        connection_packet_t *packet,
        io_uring_cqe *cqe,
        struct io_uring *ringBuffer,
        struct epoll_registry_t *epollRegistry,
        struct connection_registry_t *connections
    )
    {
        auto bytesToCopy = std::min<std::size_t>(packet->buffer.size(), static_cast<std::size_t>(cqe->res));
        auto it = packet->buffer.begin();
        packet->message.data.insert(packet->message.data.end(), it, it + bytesToCopy);

        size_t remaining = (packet->message.size > 0) ? (packet->message.size - packet->message.data.size()) : 0;

        #ifdef DEBUG
        std::cout << "remaining size = " << remaining << "\n";
        for (uint8_t b : packet->message.data) printf("%.02X ", b);
        std::cout << "\nMsg: ";
        if (packet->message.size > 0)
            for (uint8_t b : packet->message.data) std::cout << b;
        std::cout << std::endl;
        #endif

        if (remaining > 0) RequestMessage(packet, remaining, ringBuffer);
        else
        {
            MESSAGE_STATUS status = HandleMessageData(packet->fd, &packet->message);
            switch (status) {
                case OK: // Just clear packet for next possible message
                    packet->message.data.clear();
                    packet->message.size = 0;
                    packet->message.offset = 0;
                    packet->initialized = false;
                    break;

                case FAILURE: // Disconnect on failure
                    DisconnectClient(packet->fd, epollRegistry, connections);
                    delete packet;
                    break;
            };
        }
        if (packet)
        {
            struct io_uring_sqe *sqe = io_uring_get_sqe(ringBuffer);
            if (sqe)
            {
                io_uring_sqe_set_data(sqe, packet);
                io_uring_submit(ringBuffer);
            }
        }
    }

    /**
     * @brief Drain io_uring completion queue and advance packet parsing state.
     *
     * Handles both first header chunk and subsequent payload chunks per client.
     *
     * @param ringBuffer IO uring buffer used to write/read data between client and broker.
     * @param epollRegistry epoll status.
     * @param connections currently connected clients.
     */
    void ProcessPackets(
        struct io_uring *ringBuffer,
        struct epoll_registry_t *epollRegistry,
        struct connection_registry_t *connections
    )
    {
        struct io_uring_cqe *cqe;
        while (io_uring_peek_cqe(ringBuffer, &cqe) == 0)
        {
            connection_packet_t *packet = (connection_packet_t *)io_uring_cqe_get_data(cqe);
            if (cqe->res > 0 && packet) // cqe->res, read bytes
            {
                if (!packet->initialized) // Uninitialized message, read Header
                {
                    packet->keepAlive = std::chrono::steady_clock::now();
                    packet->message.type = static_cast<PACKET_TYPE>(packet->buffer.at(0) >> 4);

                    ssize_t messageSize = DecodeMessageLength(packet);
                    if (messageSize == -1)
                    {
                        delete packet;
                        continue;
                    }

                    packet->message.size = static_cast<size_t>(messageSize);

                    // Request the next part of a message
                    if (packet->message.size > 0) RequestMessage(packet, packet->message.size, ringBuffer);
                    else ProcessMessage(packet, cqe, ringBuffer, epollRegistry, connections);
                }
                else // Read message
                {
                    ProcessMessage(packet, cqe, ringBuffer, epollRegistry, connections);
                }
            } // if
            io_uring_cqe_seen(ringBuffer, cqe);
        } // while
    }

    /**
     * @brief Decode MQTT message length based on https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html.
     *
     * @param packet packet to decode message from.
     * @return -1 on error, messageSize on success.
     */
    ssize_t DecodeMessageLength(connection_packet_t *packet)
    {
        size_t messageSize = 0;
        size_t multiplier = 1;
        int byteIdx = 1;

        uint8_t sizeByte;
        do
        {
            sizeByte = packet->buffer.at(byteIdx);
            messageSize += (sizeByte & 0x7F) * multiplier;
            byteIdx++;

            multiplier *= 128;
            if (multiplier > 0x200000)
            {
                std::cerr << "Malformed Remaining Length" << std::endl;
                return -1;
            }
        }
        while ((sizeByte & 0x80));

        #ifdef DEBUG
        std::cout << "\nNEW MESSAGE\n";
        std::cout << "message size  = " << messageSize + byteIdx << "\n";
        std::cout << "message len   = " << messageSize << "\n";
        std::cout << "message type  = " << packet->message.type << std::endl;
        #endif

        // Save remaining bytes as message
        while (byteIdx < mqttMessageHeaderSize)
        {
            packet->message.data.push_back(packet->buffer.at(byteIdx));
            packet->message.offset++;
            byteIdx++;
        }
        packet->initialized = true;

        return messageSize;
    }

    /**
     * @brief Checks packets Keep Alive time and closes client connection when times up.
     *
     * @param epollRegistry epoll status.
     * @param connections currently connected clients.
     */
    void ManagePacketLiveTime(
        epoll_registry_t *epollRegistry,
        connection_registry_t *connections
    )
    {
        std::vector<connection_packet_t*> expiredPackets = std::vector<connection_packet_t*>();

        // Measure connectons Keep Alive
        {
            std::lock_guard<std::mutex> lock(connections->mutex);
            for (auto &[fd, packet] : connections->entries)
                if (!CheckKeepAlive(packet)) expiredPackets.push_back(packet);
        }

        if (!expiredPackets.empty())
        {
            for (auto &packet : expiredPackets)
            {
                DisconnectClient(packet->fd, epollRegistry, connections);
                delete packet;
            }
        }
    }

}  // namespace broker
