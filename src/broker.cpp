#include <cstdlib>
#include <cstring>
#include <iostream>

#include "sys/epoll.h"
#include "sys/socket.h"
#include "netinet/in.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <system_error>

#include "broker.hpp"
#include "error.hpp"

constexpr int MAX_EVENTS = 10;

struct epoll_event ev, events[MAX_EVENTS];
int listen_sock, connection_sock, nfds, epollfd;

sockaddr_in server_addr;

namespace broker
{
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
            nfds = epoll_wait(epollfd, events, MAX_EVENTS, -1); // Change timeout
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
                if (events[n].data.fd == listen_sock)
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
                    // do_use_fd
                    std::cout << "do_use_fd\n";
                }
            }
        }
    }

    void print_info()
    {
        std::cout << "Broker-MQTT, Hello World!" << std::endl;
    }
}  // namespace broker
