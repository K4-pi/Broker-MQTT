#include "packets.hpp"
#include "topics.hpp"

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <sys/socket.h>

#ifdef DEBUG
#include <iostream>
#endif

constexpr int keepAliveTimeSec = 10;

bool CheckKeepAlive(connection_packet_t *packet);
static MESSAGE_STATUS MqttConnect(int fd);
static MESSAGE_STATUS MqttPing(int fd);
static MESSAGE_STATUS SendAck(int fd, PACKET_TYPE type, std::uint16_t packetId);
static MESSAGE_STATUS MqttPublish(int fd, message_accumulator_t *message, Topic *topicsRoot);
static MESSAGE_STATUS MqttPubrel(int fd, message_accumulator_t *message);
MESSAGE_STATUS HandleMessageData(int clientFd, message_accumulator_t *message, Topic *topicsRoot);

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
    std::cout << "[" << packet->fd << "] Elapsed = " << elapsed << std::endl;
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
    std::cout << "Received CONNECT\n";
    std::cout << "Sent CONNACK" << std::endl;
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
    std::cout << "Received PINGREQ\n";
    std::cout << "Sent PINGRESP" << std::endl;
    #endif

    return OK;
}

/**
 * @brief Send 4 byte acknowledge packet (PUBACK, PUBREC, PUBCOMP).
 *
 * @param fd Client socket file descriptor.
 * @param type Acknowledge packet type, its fixed header flags are always 0.
 * @param packetId Packet Identifier being acknowledged.
 * @return MESSAGE_STATUS OK on success, FAILURE on send failure.
 */
static MESSAGE_STATUS SendAck(int fd, PACKET_TYPE type, std::uint16_t packetId)
{
    // packet type in upper 4 bits, remaining length (0x02), packet identifier MSB, LSB
    const std::uint8_t ack[] = {
        static_cast<std::uint8_t>(type << 4),
        0x02,
        static_cast<std::uint8_t>(packetId >> 8),
        static_cast<std::uint8_t>(packetId & 0xFF)
    };

    const ssize_t sent = send(fd, ack, sizeof(ack), 0);
    if (sent != static_cast<ssize_t>(sizeof(ack))) return FAILURE;

    return OK;
}

/**
 * @brief Handle MQTT 3.1.1 PUBLISH packet.
 *
 * @param fd Client socket file descriptor.
 * @param message Assembled PUBLISH message (variable header + payload).
 * @param topicsRoot root of topics tree.
 * @return MESSAGE_STATUS OK on success, FAILURE on malformed packet or send failure.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718037
 */
static MESSAGE_STATUS MqttPublish(int fd, message_accumulator_t *message, Topic *topicsRoot)
{
    // Fixed header flags: bit 3 = DUP, bits 2-1 = QoS, bit 0 = RETAIN
    const bool dup          = (message->flags >> 3) & 0x01;
    const std::uint8_t qos  = (message->flags >> 1) & 0x03;
    const bool retain       = message->flags & 0x01;

    // QoS check: value 3 is reserved
    if (qos > 2) return FAILURE;

    // DUP check: has to be 0 for QoS 0 messages
    if (dup && qos == 0) return FAILURE;

    const std::size_t length = std::min(message->size, message->data.size());
    const std::uint8_t *data = message->data.data();
    std::size_t idx = 0;

    if (length < 2) return FAILURE;

    // Topic Name: 2 byte length followed by UTF-8 string
    const std::size_t topicLength = (data[0] << 8) | data[1]; // Make 2 bytes into one value high:low
    idx += 2;

    if (topicLength == 0 || idx + topicLength > length) return FAILURE;
    const std::string_view topic(reinterpret_cast<const char *>(data + idx), topicLength);
    idx += topicLength;

    // Wildcards are not allowed in PUBLISH Topic Name [MQTT-3.3.2-2]
    if (topic.find_first_of("+#") != std::string_view::npos) return FAILURE;

    std::uint16_t packetId = 0; // Packet Identifier: when QoS > 0
    if (qos > 0)
    {
        if (idx + 2 > length) return FAILURE;
        packetId = static_cast<std::uint16_t>((data[idx] << 8) | data[idx + 1]);
        idx += 2;

        if (packetId == 0) return FAILURE;
    }

    std::string payload(reinterpret_cast<const char *>(data + idx), length - idx);

    #ifdef DEBUG
    std::cout << "Received PUBLISH\n"
              << "topic=" << topic
              << "\nqos=" << static_cast<int>(qos)
              << " dup=" << dup
              << " retain=" << retain
              << " id=" << packetId
              << "\npayload=" << payload << std::endl;
    #endif

    // TODO: for QoS 2 a duplicate must not be delivered to subscribers twice.

    // Without RETAIN flag the message is not stored [MQTT-3.3.1-12].
    if (retain) SaveTopic(topicsRoot, topic, std::move(payload));

    // TODO: forward message to topic subscribers once SUBSCRIBE is implemented.

    // QoS check: pick acknowledge for the given level
    switch (qos)
    {
        case 0: // At most once, no response
            return OK;

        case 1: // At least once, respond with PUBACK
            #ifdef DEBUG
            std::cout << "Sent PUBACK" << std::endl;
            #endif
            return SendAck(fd, PUBACK, packetId);

        case 2: // Exactly once, respond with PUBREC, client continues with PUBREL
            #ifdef DEBUG
            std::cout << "Sent PUBREC" << std::endl;
            #endif
            return SendAck(fd, PUBREC, packetId);

        default:
            return FAILURE;
    }
}

/**
 * @brief Handle MQTT 3.1.1 PUBREL packet (QoS 2 flow, part 2) and respond with PUBCOMP.
 *
 * @param fd Client socket file descriptor.
 * @param message Assembled PUBREL message.
 * @return MESSAGE_STATUS OK on success, FAILURE on malformed packet or send failure.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718053
 */
static MESSAGE_STATUS MqttPubrel(int fd, message_accumulator_t *message)
{
    // Fixed header flags have to be 0b0010 [MQTT-3.6.1-1]
    if (message->flags != 0x02) return FAILURE;
    if (message->size != 2 || message->data.size() < 2) return FAILURE;

    const std::uint16_t packetId = static_cast<std::uint16_t>((message->data[0] << 8) | message->data[1]);

    #ifdef DEBUG
    std::cout << "Received PUBREL id=" << packetId << "\n";
    std::cout << "Sent PUBCOMP" << std::endl;
    #endif

    return SendAck(fd, PUBCOMP, packetId);
}

/**
 * @brief Process assembled MQTT message.
 *
 * @param clientFd Client socket file descriptor.
 * @return message pointer to handled message.
 */
MESSAGE_STATUS HandleMessageData(int clientFd, message_accumulator_t *message, Topic *topicsRoot)
{
    if (!message) return FAILURE;

    switch (message->type)
    {
        case CONNECT:
            return MqttConnect(clientFd);

        case PINGREQ:
            return MqttPing(clientFd);

        case PUBLISH:
            return MqttPublish(clientFd, message, topicsRoot);

        case PUBREL:
            return MqttPubrel(clientFd, message);

        case DISCONNECT:
            return FAILURE;

        default:
            return FAILURE;
    }
}
