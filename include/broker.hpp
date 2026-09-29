#pragma once

namespace broker
{
    /**
     * @brief Start the broker event loop.
     *
     * Waits for epoll events, accepts new connections, schedules read
     * submissions, and processes io_uring completions.
     */
    void start(char *address, int port);

    /**
     * @brief Print startup/info message for the broker process.
     */
    void print_info();
}
