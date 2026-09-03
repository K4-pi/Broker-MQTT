#include "broker.hpp"
#include "error.hpp"
#include "threadpool/pool.hpp"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
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

namespace broker
{
    void fd_handler_submit(int client_fd);
    void process_states();

    constexpr uint16_t STATE_BUFFER_SIZE = 64;
    constexpr uint16_t MAX_EVENTS = 16;
    constexpr uint16_t EPOLL_TIMEOUT_MS = 250;
    constexpr uint16_t RING_ENTRIES = 256;

    struct ConnectionState {
        int fd;
        char buffer[STATE_BUFFER_SIZE];
    };

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

            process_states();
        } // while (true)
    }

    void print_info()
    {
        std::cout << "Broker-MQTT, Hello World!" << std::endl;
    }

    void fd_handler_submit(int client_fd)
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_buffer);
        if (!sqe) return;

        ConnectionState *state = new ConnectionState();
        state->fd = client_fd;
        memset(state->buffer, 0, STATE_BUFFER_SIZE);
        // state->buffer[STATE_BUFFER_SIZE - 1] = '\0';

        io_uring_prep_recv(sqe, client_fd, state->buffer, STATE_BUFFER_SIZE, 0);

        io_uring_sqe_set_data(sqe, state);

        io_uring_submit(&ring_buffer);
    }

    void process_states()
    {
        struct io_uring_cqe *cqe;

        while (io_uring_peek_cqe(&ring_buffer, &cqe) == 0)
        {
            ConnectionState *state = (ConnectionState *)io_uring_cqe_get_data(cqe);

            bool is_free = true;

            if (cqe->res > 0 && state) // cqe->res > 0 if success, returns number of read bytes like in read()
            {
                #ifdef DEBUG
                std::cout << state->buffer << "\n";
                #endif

                /* we suspect that it isn't whole msg so we will
                 * reuse current state to store next part of the msg */
                if (cqe->res == STATE_BUFFER_SIZE)
                {
                    memset(state->buffer, 0, STATE_BUFFER_SIZE);

                    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_buffer);
                    io_uring_prep_recv(sqe, state->fd, state->buffer, STATE_BUFFER_SIZE, 0);
                    io_uring_sqe_set_data(sqe, state);
                    io_uring_submit(&ring_buffer);

                    is_free = false;
                }
            }

            if (is_free) delete state;
            io_uring_cqe_seen(&ring_buffer, cqe);
        }
    }   
}  // namespace broker
