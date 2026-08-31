#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sys/socket.h>

#include "sys/epoll.h"
#include "sys/socket.h"
#include "netinet/in.h"
#include <arpa/inet.h>

#include "broker.hpp"

constexpr int MAX_EVENTS = 10;

struct epoll_event ev, events[MAX_EVENTS];
int listen_sock, connection_sock, nfds, epollfd;

sockaddr_in server_addr;

namespace broker
{
    void setup(char *address, int port)
    {
        memset(&server_addr, 0, sizeof(server_addr));

        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);

        if (inet_pton(server_addr.sin_family, address, &server_addr.sin_addr) == -1)
        {
            perror("inet_pton: Error");
            exit(EXIT_FAILURE);
        }

        listen_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_sock == -1)
        {
            perror("listen_sock: socket");
            exit(EXIT_FAILURE);
        }

        if (bind(listen_sock, (sockaddr *) &server_addr, sizeof(server_addr)) == -1)
        {
            perror("bind: listen_sock");
            exit(EXIT_FAILURE);
        }

        if (listen(listen_sock, 8) == -1)
        {
            perror("listen: listen_sock");
            exit(EXIT_FAILURE);
        }

        epollfd = epoll_create1(0);
        if (epollfd == -1)
        {
            perror("epoll_create1: Error");
            exit(EXIT_FAILURE);
        }

        ev.events = EPOLLIN;
        ev.data.fd = listen_sock;
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, listen_sock, &ev) == -1)
        {
            perror("epoll_ctl: listen_sock");
            exit(EXIT_FAILURE);
        }
    }

    void start()
    {
        socklen_t server_addr_len = sizeof(server_addr);

        while (true)
        {
            nfds = epoll_wait(epollfd, events, MAX_EVENTS, -1); // Change timeout
            if (nfds == -1)
            {
                perror("epoll_wait: Error");
                exit(EXIT_FAILURE);
            }

            for (int n = 0; n < nfds; ++n)
            {
                if (events[n].data.fd == listen_sock)
                {
                    connection_sock = accept4(listen_sock, (struct sockaddr *) &server_addr, &server_addr_len, SOCK_NONBLOCK);
                    if (connection_sock == -1)
                    {
                        perror("accept4: Error");
                        exit(EXIT_FAILURE);
                    }

                    ev.events = EPOLLIN | EPOLLET;
                    ev.data.fd = connection_sock;
                    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, connection_sock, &ev) == -1)
                    {
                        perror("epoll_ctl: connection_sock");
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
