#include "broker.hpp"
#include "packets.hpp"
#include "error.hpp"
#include "threadpool/pool.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
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

    static boost::threadpool::pool workers(std::thread::hardware_concurrency());

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
                else
                {
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
        constexpr int HEADER_SIZE = 6;

        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_buffer);
        if (!sqe) return;

        ConnectionPacket *packet = new ConnectionPacket();
        packet->fd = client_fd;
        packet->message.offset = 0;
        packet->message.size = 0;
        packet->buffer.assign(HEADER_SIZE, 0); // assign 6 because initial header of MQTT (1 byte protocol, 2 - 5 byte message lenght)

        io_uring_prep_recv(sqe, client_fd, packet->buffer.data(), HEADER_SIZE, 0);
        io_uring_sqe_set_data(sqe, packet);
        io_uring_submit(&ring_buffer);
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
                    int byte_idx = 1;
                    while (byte_idx <= 5)
                    {
                        uint8_t size_byte = packet->buffer.at(byte_idx);

                        if (!(size_byte & 100)) break; // Next byte is not length
                        message_size += (size_byte & 0x7F);
                        byte_idx++;
                    }

                    #ifdef DEBUG
                    std::cout << "message size  = " << message_size + byte_idx << "\n";
                    std::cout << "message len   = " << message_size << "\n";
                    std::cout << "message type  = " << packet->message.type << std::endl;
                    #endif

                    packet->message.size = message_size;

                    if (byte_idx != 5)
                    {
                        // Save remaining bytes as message
                        while (byte_idx <= 5)
                        {
                            packet->message.data.push_back(packet->buffer.at(byte_idx));
                            packet->message.offset++;
                            byte_idx++;
                        }
                    }

                    // Request the next part of a message
                    packet->buffer.resize(message_size + packet->buffer.size());

                    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_buffer);
                    if (sqe)
                    {
                        io_uring_prep_recv(sqe, packet->fd, packet->buffer.data(), message_size, 0);
                        io_uring_sqe_set_data(sqe, packet);
                        io_uring_submit(&ring_buffer);
                    }
                }
                else // Read message
                {
                    // TODO: mutex lock

                    auto it = packet->buffer.begin();
                    packet->message.data.insert(packet->message.data.end(), it, it + packet->message.size - packet->message.offset);

                    // for (size_t idx = 0; idx < packet->message.size - packet->message.offset; idx++)
                    // {
                    //     char c = packet->buffer.at(idx);

                    //     if (c == '\n')
                    //     {
                    //
                    //         break;
                    //     }

                    //     packet->message.data.push_back(c);
                    // }
                    #ifdef DEBUG
                    for (uint8_t b : packet->message.data)
                    {
                        printf("%.02X ", b);
                    }
                    printf("\n");
                    #endif

                    // TODO: Respond to message

                    delete packet;
                } // else
            }

            io_uring_cqe_seen(&ring_buffer, cqe);
        }
    }
}  // namespace broker
