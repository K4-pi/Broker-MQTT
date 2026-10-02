#include "packets.hpp"

#include <cstdint>
#include <sys/socket.h>

#ifdef DEBUG
#include "stdio.h"
#endif

constexpr int keepAliveTimeSec = 10;

bool CheckKeepAlive(connection_packet_t *packet);
static MESSAGE_STATUS MqttConnect(int fd);
static MESSAGE_STATUS MqttPing(int fd);
MESSAGE_STATUS HandleMessageData(int clientFd, message_accumulator_t *message);

/**
 * @brief Check Keep Alive time of a packet.
 *
 * @param packet Packet which has to be checked.
 * @return bool true if didn't excedeed Keep Alive time, false when exceeded.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
bool CheckKeepAlive(connection_packet_t *packet)
{
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds> (end - packet->keepAlive).count();

    #ifdef DEBUG
    printf("Elapsed = %ld\n", elapsed);
    #endif

    return (elapsed >= keepAliveTimeSec) ? false : true;
}

/**
 * @brief Send MQTT 3.1.1 CONNACK packet.
 *
 * @param fd Client socket file descriptor.
 * @return MESSAGE_STATUS OK on success, INVALID_VALUE on send failure.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
static MESSAGE_STATUS MqttConnect(int fd)
{
    // CONNACK: packet type (0x20), remaining length (0x02)
    // connect acknowledge flags (0x00), reason code success (0x00)
    constexpr std::uint8_t connack[] = {0x20, 0x02, 0x00, 0x00};

    const ssize_t sent = send(fd, connack, sizeof(connack), 0);
    if (sent != static_cast<ssize_t>(sizeof(connack))) return FAILURE;

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
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 */
static MESSAGE_STATUS MqttPing(int fd)
{
    // PINGRESP: packet type (0xD0), remaining length (0x00)
    constexpr std::uint8_t pingresp[] = {0xD0, 0x00};

    const ssize_t sent = send(fd, pingresp, sizeof(pingresp), 0);
    if (sent != static_cast<ssize_t>(sizeof(pingresp))) return FAILURE;

    #ifdef DEBUG
    printf("Sent PINGRESP\n");
    #endif

    return OK;
}

/**
 * @brief Process assembled MQTT message.
 *
 * @param clientFd Client socket file descriptor.
 * @return message pointer to handled message.
 */
MESSAGE_STATUS HandleMessageData(int clientFd, message_accumulator_t *message)
{
    if (!message) return FAILURE;

    switch (message->type)
    {
        case CONNECT:
            return MqttConnect(clientFd);

        case PINGREQ:
            return MqttPing(clientFd);

        case DISCONNECT:
            return FAILURE;

        default:
            return FAILURE;
    }
}
