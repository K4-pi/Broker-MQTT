#include "broker.hpp"
#include "packets.hpp"
#include "error.hpp"
#include "threadpool/pool.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
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

#ifdef DEBUG
#include "stdio.h"
#endif

namespace broker
{
    constexpr uint8_t  MQTT_MESSAGE_HEADER_SIZE = 6;   // MQTT max header size = 6
    constexpr uint16_t EPOLL_TIMEOUT_MS         = 250;
    constexpr uint16_t RING_ENTRIES             = 256;
    constexpr uint16_t MAX_EVENTS               = 16;

    /**
     * Better way to store epoll status variables
     */
    struct epoll_data
    {
        int epollfd;
        struct epoll_event event;
        struct epoll_event events[MAX_EVENTS];
    };

    /**
     * Store connections with mutex for it
     */
    struct connection_registry
    {
        std::map<int, ConnectionPacket*> entries; // Key = fd, Value = packet
        std::mutex mutex;
    };

    /**
     * @brief Fetch connection state for @p client_fd and submit initial read.
     *
     * Scheduled by worker threads after an epoll readability event is observed.
     *
     * @param client_fd Connected client socket file descriptor.
     */
    void fd_handler_submit(
        int client_fd,
        struct io_uring *ring_buffer,
        struct connection_registry *connections
    );

    /**
     * @brief Drain io_uring completion queue and advance packet parsing state.
     *
     * Handles both first header chunk and subsequent payload chunks per client.
     */
    void process_packets(
        struct io_uring *ring_buffer,
        struct epoll_data *e_data,
        struct connection_registry *connections
    );

    /**
     * @brief Initialize broker networking and event-loop resources.
     *
     * Creates and configures listening socket, epoll instance, and io_uring
     * queue for asynchronous reads.
     *
     * @param address IPv4 bind address in text form (e.g. "0.0.0.0").
     * @param port TCP port to bind.
     */
    void setup(
        char *address,
        int port,
        int listen_sock,
        sockaddr_in* server_addr,
        struct io_uring *ring_buffer,
        struct epoll_data *e_data
    );

    /**
     * @brief Submit async recv request for the next message chunk.
     *
     * @param packet Per-connection packet state to associate with completion.
     * @param message_size Requested number of bytes to read (capped to buffer size).
     */
    static void request_message(
        ConnectionPacket *packet,
        size_t message_size,
        struct io_uring *ring_buffer
    )
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring_buffer);
        if (sqe)
        {
            io_uring_prep_recv(
                sqe,
                packet->fd,
                packet->buffer.data(),
                std::min<std::size_t>(message_size, packet->buffer.size()), // Min so we don't exceed array size
                0
            );
            io_uring_sqe_set_data(sqe, packet);
            io_uring_submit(ring_buffer);
        }
    }

    /**
     * @brief Disconnects client from a server.
     *
     * @param client_fd client connection file descriptor.
     */
    static void disconnect_client(
        int client_fd,
        struct epoll_data *e_data,
        struct connection_registry *connections
    )
    {
        throw_if_error(epoll_ctl(e_data->epollfd, EPOLL_CTL_DEL, client_fd, nullptr), "epoll_ctl: del");
        close(client_fd);
        {
            std::lock_guard<std::mutex> lock(connections->mutex);
            connections->entries.erase(client_fd);
        }

        #ifdef DEBUG
        std::cout << "client disconnected" << std::endl;
        #endif
    }

    /**
     * @brief Configure listening socket, epoll, and io_uring resources.
     *
     * @param address IPv4 bind address in text form.
     * @param port TCP bind port.
     * @param server_addr pointer to sockaddr_in data where address will be saved
     */
    void setup(
        char *address,
        int port,
        int *listen_sock,
        sockaddr_in *server_addr,
        struct io_uring *ring_buffer,
        struct epoll_data *e_data
    )
    {
        try
        {
            memset(server_addr, 0, sizeof(*server_addr));

            server_addr->sin_family = AF_INET;
            server_addr->sin_port = htons(port);
            throw_if_error(inet_pton(server_addr->sin_family, address, &server_addr->sin_addr), "inet_pton");

            *listen_sock = socket(AF_INET, SOCK_STREAM, 0);
            throw_if_error(*listen_sock, "listen_sock");
            throw_if_error(bind(*listen_sock, (sockaddr *)server_addr, sizeof(*server_addr)), "bind: listen_sock");
            throw_if_error(listen(*listen_sock, 8), "listen");

            e_data->epollfd = epoll_create1(0);
            throw_if_error(e_data->epollfd, "epoll_create1");

            int ring_init_rc = io_uring_queue_init(RING_ENTRIES, ring_buffer, 0);
            if (ring_init_rc < 0)
                throw std::system_error(-ring_init_rc, std::generic_category(), "io_uring_queue_init");

            e_data->event.events = EPOLLIN;
            e_data->event.data.fd = *listen_sock;
            throw_if_error(epoll_ctl(e_data->epollfd, EPOLL_CTL_ADD, *listen_sock, &e_data->event), "epoll_ctl: listen_sock");
        } // try
        catch (const std::system_error &e)
        {
            std::cout << e.what() << "\n";
            exit(EXIT_FAILURE);
        }
    }

    /**
     * @brief Run the main broker loop with I/O handling.
     */
    void start(char *address, int port)
    {
        int listen_sock;
        struct sockaddr_in server_addr;
        struct io_uring ring_buffer;
        struct epoll_data e_data;
        setup(address, port, &listen_sock, &server_addr, &ring_buffer, &e_data);

        static boost::threadpool::pool workers(std::thread::hardware_concurrency());
        struct connection_registry connections;

        while (true)
        {
            int nfds = epoll_wait(e_data.epollfd, e_data.events, MAX_EVENTS, EPOLL_TIMEOUT_MS);
            try
            {
                throw_if_error(nfds, "epoll_wait");
            }
            catch (const std::system_error &e)
            {
                std::cout << e.what() << "\n";
                exit(EXIT_FAILURE);
            }

            for (int n = 0; n < nfds; ++n)
            {
                int fd = e_data.events[n].data.fd;
                if (fd == listen_sock)
                {
                    int connection_sock;
                    try
                    {
                        socklen_t server_addr_len = sizeof(server_addr);
                        connection_sock = accept4(listen_sock, (struct sockaddr *) &server_addr, &server_addr_len, SOCK_NONBLOCK);
                        throw_if_error(connection_sock, "accept4");

                        e_data.event.events = EPOLLIN | EPOLLET;
                        e_data.event.data.fd = connection_sock;
                        throw_if_error(epoll_ctl(e_data.epollfd, EPOLL_CTL_ADD, connection_sock, &e_data.event), "epoll_ctl: connection_sock");
                    }
                    catch (const std::system_error &e)
                    {
                        std::cout << e.what() << "\n";

                        // Close socket if accept4 succeded but epoll failed
                        if (connection_sock != -1) close(connection_sock);
                        continue;
                    }
                }
                else
                {
                    {
                        std::lock_guard<std::mutex> lock(connections.mutex);

                        if (connections.entries.contains(fd)) continue; // prevents sockets from having multiple messages at once
                        connections.entries.emplace(fd, new ConnectionPacket());
                    }

                    workers.schedule([fd, &ring_buffer, &connections]() {
                        fd_handler_submit(fd, &ring_buffer, &connections);
                    });
                }
            } // for

            process_packets(&ring_buffer, &e_data, &connections);
        } // while (true)
    }

    /**
     * @brief Print info.
     */
    void print_info()
    {
        std::cout << "Broker-MQTT, started!" << std::endl;
    }

    /**
     * @brief Handle new fd client.
     *
     * @param client_fd file descriptor of new client.
     */
    void fd_handler_submit(
        int client_fd,
        struct io_uring *ring_buffer,
        struct connection_registry *connections)
    {
        ConnectionPacket* packet;
        try
        {
            std::lock_guard<std::mutex> lock(connections->mutex);
            packet = connections->entries.at(client_fd);
        }
        catch (const std::out_of_range&) { return; }

        packet->fd = client_fd;
        packet->message.offset = 0;
        packet->message.size = 0;
        request_message(packet, MQTT_MESSAGE_HEADER_SIZE, ring_buffer);
    }

    /**
     * @brief Take actions based on ongoing message allocation.
     *
     * @param packet pointer to packet with processed message.
     * @param cqe IO Completion Queue Entry
     */
    void process_message(
        ConnectionPacket *packet,
        io_uring_cqe *cqe,
        struct io_uring *ring_buffer,
        struct epoll_data *e_data,
        struct connection_registry *connections
    )
    {
        auto bytes_to_copy = std::min<std::size_t>(packet->buffer.size(), static_cast<std::size_t>(cqe->res));
        auto it = packet->buffer.begin();
        packet->message.data.insert(packet->message.data.end(), it, it + bytes_to_copy);

        size_t remaining = (packet->message.size > 0) ? (packet->message.size - packet->message.data.size()) : 0;

        #ifdef DEBUG
        for (uint8_t b : packet->message.data) printf("%.02X ", b);
        std::cout << "\nremaining size = " << remaining << std::endl;
        #endif

        if (remaining > 0) request_message(packet, remaining, ring_buffer);
        else
        {
            MESSAGE_STATUS status = handle_message_data(packet->fd, &packet->message);
            if (status == OK)
            {
                packet->message.data.clear();
                packet->message.size = 0;
                packet->message.offset = 0;
                packet->initialized = false;
                request_message(packet, MQTT_MESSAGE_HEADER_SIZE, ring_buffer);
            }
            else
            {
                #ifdef DEBUG
                if (status == FINISHED) std::cout << "Client disconnected normally"   << std::endl;
                else if (status != OK)  std::cout << "Client disconnected with error" << std::endl;
                #endif

                disconnect_client(packet->fd, e_data, connections);
                delete packet;
            }
        }
    }

    /**
     * @brief Process packets in IO Completion Queue Entry.
     */
    void process_packets(
        struct io_uring *ring_buffer,
        struct epoll_data *e_data,
        struct connection_registry *connections
    )
    {
        struct io_uring_cqe *cqe;
        while (io_uring_peek_cqe(ring_buffer, &cqe) == 0)
        {
            ConnectionPacket *packet = (ConnectionPacket *)io_uring_cqe_get_data(cqe);
            if (cqe->res > 0 && packet) // cqe->res, read bytes
            {
                if (!packet->initialized) // Uninitialized message, read Header
                {
                    packet->message.type = static_cast<PACKET_TYPE>(packet->buffer.at(0) >> 4);

                    size_t message_size = 0;
                    size_t multiplier = 1;
                    int byte_idx = 1;

                    uint8_t size_byte;
                    do
                    {
                        size_byte = packet->buffer.at(byte_idx);
                        message_size += (size_byte & 0x7F) * multiplier;
                        byte_idx++;

                        multiplier *= 128;
                        if (multiplier > 0x200000)
                        {
                            #ifdef DEBUG
                            std::cout << "Malformed Remaining Length" << std::endl;
                            #endif

                            delete packet;
                            return;
                        }
                    }
                    while ((size_byte & 0x80));

                    #ifdef DEBUG
                    std::cout << "\nNEW MESSAGE\n";
                    std::cout << "message size  = " << message_size + byte_idx << "\n";
                    std::cout << "message len   = " << message_size << "\n";
                    std::cout << "message type  = " << packet->message.type << std::endl;
                    #endif

                    packet->message.size = message_size;

                    // Save remaining bytes as message
                    while (byte_idx < MQTT_MESSAGE_HEADER_SIZE)
                    {
                        packet->message.data.push_back(packet->buffer.at(byte_idx));
                        packet->message.offset++;
                        byte_idx++;
                    }
                    packet->initialized = true;

                    // Request the next part of a message
                    if (message_size > 0) request_message(packet, message_size, ring_buffer);
                    else process_message(packet, cqe, ring_buffer, e_data, connections);
                }
                else // Read message
                {
                    process_message(packet, cqe, ring_buffer, e_data, connections);
                }
            } // if
            io_uring_cqe_seen(ring_buffer, cqe);
        } // while
    }
}  // namespace broker
