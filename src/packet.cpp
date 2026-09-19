#include "packets.hpp"

#include <cstdint>
#include <sys/socket.h>

#ifdef DEBUG
#include "stdio.h"
#endif

/**
 * @brief Send MQTT 3.1.1 CONNACK packet.
 *
 * @param fd Client socket file descriptor.
 * @return MESSAGE_STATUS OK on success, INVALID_VALUE on send failure.
 */
static MESSAGE_STATUS MQTT_connect(int fd)
{
    // CONNACK: packet type (0x20), remaining length (0x02)
    // connect acknowledge flags (0x00), reason code success (0x00)
    constexpr std::uint8_t connack[] = {0x20, 0x02, 0x00, 0x00};

    const ssize_t sent = send(fd, connack, sizeof(connack), 0);
    if (sent != static_cast<ssize_t>(sizeof(connack))) return INVALID_VALUE;

    #ifdef DEBUG
    printf("Sent CONNACK\n");
    #endif

    return OK;
}

/**
 * @brief Ping MQTT 3.1.1 PINGREQ packet.
 *
 * @param fd Client socket file descriptor.
 * @return MESSAGE_STATUS OK on success, INVALID_VALUE on send failure.
 */
static MESSAGE_STATUS MQTT_ping(int fd)
{
    // PINGRESP: packet type (0xD0), remaining length (0x00)
    constexpr std::uint8_t pingresp[] = {0xD0, 0x00};

    const ssize_t sent = send(fd, pingresp, sizeof(pingresp), 0);
    if (sent != static_cast<ssize_t>(sizeof(pingresp))) return INVALID_VALUE;

    #ifdef DEBUG
    printf("Sent PINGRESP\n");
    #endif

    return OK;
}

/**
 * @brief Process assembled MQTT message.
 *
 * @param client_fd Client socket file descriptor.
 * @return message pointer to handled message.
 */
MESSAGE_STATUS handle_message_data(int client_fd, MessageAccumulator *message)
{
    if (!message) return INVALID_VALUE;

    switch (message->type)
    {
        case CONNECT:
            return MQTT_connect(client_fd);

        case PINGREQ:
            return MQTT_ping(client_fd);

        case DISCONNECT:
            return FINISHED;

        default:
            return INVALID_TYPE;
    }
}
