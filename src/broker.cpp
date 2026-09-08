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
#include <string>
#include <system_error>
#include <thread>

#include "stdio.h"
#include "sys/epoll.h"
#include "sys/socket.h"
#include "netinet/in.h"
#include "linux/io_uring.h"
#include "liburing.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <unistd.h>

namespace broker
{
    void fd_handler_submit(int client_fd);
    void process_packets();

    constexpr uint16_t MAX_EVENTS = 16;
    constexpr uint16_t EPOLL_TIMEOUT_MS = 250;
    constexpr uint16_t RING_ENTRIES = 256;

    struct io_uring ring_buffer;

    struct epoll_event ev, events[MAX_EVENTS];
    int listen_sock, connection_sock, nfds, epollfd;

    sockaddr_in server_addr;

    std::map<int, ConnectionPacket*> connections; // Key = fd, Value = packet

    static boost::threadpool::pool workers(std::thread::hardware_concurrency());

    static inline void request_message(ConnectionPacket *packet, size_t message_size)
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_buffer);
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
            io_uring_submit(&ring_buffer);
        }
    }

    void setup(char *address, int port)
    {
        try
        {
            memset(&server_addr, 0, sizeof(server_addr));

            server_addr.sin_family = AF_INET;
            server_addr.sin_port = htons(port);

            throw_if_error(inet_pton(server_addr.sin_family, address, &server_addr.sin_addr), "inet_pton");

            listen_sock = socket(AF_INET, SOCK_STREAM, 0);
            throw_if_error(listen_sock, "listen_sock");

            throw_if_error(bind(listen_sock, (sockaddr *) &server_addr, sizeof(server_addr)), "bind: listen_sock");

            throw_if_error(listen(listen_sock, 8), "listen");

            epollfd = epoll_create1(0);
            throw_if_error(epollfd, "epoll_create1");

            int ring_init_rc = io_uring_queue_init(RING_ENTRIES, &ring_buffer, 0);
            if (ring_init_rc < 0)
                throw std::system_error(-ring_init_rc, std::generic_category(), "io_uring_queue_init");

            ev.events = EPOLLIN;
            ev.data.fd = listen_sock;
            throw_if_error(epoll_ctl(epollfd, EPOLL_CTL_ADD, listen_sock, &ev), "epoll_ctl: listen_sock");
        } // try
        catch (const std::system_error &e)
        {
            std::cout << e.what() << "\n";
            exit(EXIT_FAILURE);
        }
    }

    void start()
    {
        socklen_t server_addr_len = sizeof(server_addr);

        while (true)
        {
            nfds = epoll_wait(epollfd, events, MAX_EVENTS, EPOLL_TIMEOUT_MS);
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
                int fd = events[n].data.fd;

                if (fd == listen_sock)
                {
                    try
                    {
                        connection_sock = accept4(listen_sock, (struct sockaddr *) &server_addr, &server_addr_len, SOCK_NONBLOCK);
                        throw_if_error(connection_sock, "accept4");

                        ev.events = EPOLLIN | EPOLLET;
                        ev.data.fd = connection_sock;
                        throw_if_error(epoll_ctl(epollfd, EPOLL_CTL_ADD, connection_sock, &ev), "epoll_ctl: connection_sock");
                    }
                    catch (const std::system_error &e)
                    {
                        std::cout << e.what() << "\n";
                        exit(EXIT_FAILURE);
                    }
                }
                else if (!connections.contains(fd)) // prevents sockets from having multiple messages at once
                {
                    ConnectionPacket *packet = new ConnectionPacket();
                    connections.emplace(fd, packet);

                    workers.schedule([fd]() {
                        fd_handler_submit(fd);
                    });
                }
            } // for

            process_packets();
        } // while (true)
    }

    void print_info()
    {
        std::cout << "Broker-MQTT, Hello World!" << std::endl;
    }

    void fd_handler_submit(int client_fd)
    {
        ConnectionPacket* packet;
        try
        {
            packet = connections.at(client_fd);
        }
        catch (const std::out_of_range&) { return; }

        packet->fd = client_fd;
        packet->message.offset = 0;
        packet->message.size = 0;

        request_message(packet, 6); // Header size = 6
    }

    void process_packets()
    {
        struct io_uring_cqe *cqe;

        while (io_uring_peek_cqe(&ring_buffer, &cqe) == 0)
        {
            ConnectionPacket *packet = (ConnectionPacket *)io_uring_cqe_get_data(cqe);

            if (cqe->res > 0 && packet) // cqe->res, read bytes
            {
                if (packet->message.size == 0) // Uninitialized message, read Header
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
                    std::cout << "message size  = " << message_size + byte_idx << "\n";
                    std::cout << "message len   = " << message_size << "\n";
                    std::cout << "message type  = " << packet->message.type << std::endl;
                    #endif

                    packet->message.size = message_size;

                    // Save remaining bytes as message
                    while (byte_idx <= 5)
                    {
                        packet->message.data.push_back(packet->buffer.at(byte_idx));
                        packet->message.offset++;
                        byte_idx++;
                    }

                    // Request the next part of a message
                    request_message(packet, message_size);
                }
                else // Read message
                {
                    // TODO: mutex lock

                    auto bytes_to_copy = std::min<std::size_t>(packet->buffer.size(), static_cast<std::size_t>(cqe->res));
                    auto it = packet->buffer.begin();
                    packet->message.data.insert(packet->message.data.end(), it, it + bytes_to_copy);

                    #ifdef DEBUG
                    for (uint8_t b : packet->message.data)
                    {
                        printf("%.02X ", b);
                    }
                    printf("\n");
                    #endif

                    // TODO: Respond to message

                    size_t remaining = std::max(packet->message.size - packet->message.data.size(), (size_t)0);
                    if (remaining > 0) request_message(packet, remaining);
                    else
                    {
                        throw_if_error(epoll_ctl(epollfd, EPOLL_CTL_DEL, packet->fd, nullptr), "epoll_ctl: del");

                        close(packet->fd);
                        connections.erase(packet->fd);

                        delete packet;

                        #ifdef DEBUG
                        std::cout << "client removed" << std::endl;
                        #endif
                    }
                } // else
            }

            io_uring_cqe_seen(&ring_buffer, cqe);
        }
    }
}  // namespace broker
