#pragma once

namespace broker
{
    /**
     * @brief Initialize broker networking and event-loop resources.
     *
     * Creates and configures listening socket, epoll instance, and io_uring
     * queue for asynchronous reads.
     *
     * @param address IPv4 bind address in text form (e.g. "0.0.0.0").
     * @param port TCP port to bind.
     */
    void setup(char *address, int port);

    /**
     * @brief Start the broker event loop.
     *
     * Waits for epoll events, accepts new connections, schedules read
     * submissions, and processes io_uring completions.
     */
    void start();

    /**
     * @brief Print startup/info message for the broker process.
     */
    void print_info();
}
