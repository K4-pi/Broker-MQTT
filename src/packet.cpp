#include "packets.hpp"
#include "topics.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#ifdef DEBUG
#include <iostream>
#endif

constexpr int keepAliveTimeSec = 10;

// SUBACK return code telling the client a Topic Filter was rejected [MQTT-3.9.3-2]
constexpr std::uint8_t subackFailure = 0x80;

bool CheckKeepAlive(connection_packet_t *packet);
static std::uint16_t NextPacketId();
static void EncodeRemainingLength(std::vector<std::uint8_t> &out, std::size_t length);
static MESSAGE_STATUS MqttConnect(int fd, io_uring *ring);
static MESSAGE_STATUS MqttPing(int fd, io_uring *ring);
static MESSAGE_STATUS SendAck(int fd, io_uring *ring, PACKET_TYPE type, std::uint16_t packetId, std::uint8_t flags = 0);
static MESSAGE_STATUS SendSuback(int fd, io_uring *ring, std::uint16_t packetId, const std::vector<std::uint8_t> &returnCodes);
static MESSAGE_STATUS SendPublish(
    int fd,
    io_uring *ring,
    std::string_view topic,
    std::string_view payload,
    std::uint8_t qos,
    bool retain,
    std::uint16_t packetId
);
static MESSAGE_STATUS MqttPublish(int fd, io_uring *ring, message_accumulator_t *message, Topic *topicsRoot);
static MESSAGE_STATUS MqttPubrel(int fd, io_uring *ring, message_accumulator_t *message);
static MESSAGE_STATUS MqttPubrec(int fd, io_uring *ring, message_accumulator_t *message);
static MESSAGE_STATUS MqttDeliveryAck(message_accumulator_t *message);
static MESSAGE_STATUS MqttSubscribe(int fd, io_uring *ring, message_accumulator_t *message, Topic *topicsRoot);
static MESSAGE_STATUS MqttUnsubscribe(int fd, io_uring *ring, message_accumulator_t *message, Topic *topicsRoot);
MESSAGE_STATUS HandleMessageData(int clientFd, message_accumulator_t *message, Topic *topicsRoot, io_uring *ring);

/**
 * @brief Hand out the next Packet Identifier for broker originated messages.
 *
 * @return std::uint16_t identifier, never 0 since that value is reserved [MQTT-2.3.1-1].
 */
static std::uint16_t NextPacketId()
{
    static std::uint16_t packetId = 0;

    if (++packetId == 0) packetId = 1;

    return packetId;
}

/**
 * @brief Append a Remaining Length field in its variable length encoding.
 *
 * @param out packet being built.
 * @param length number of bytes following the fixed header.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718023
 */
static void EncodeRemainingLength(std::vector<std::uint8_t> &out, std::size_t length)
{
    do
    {
        std::uint8_t encoded = length % 128;
        length /= 128;
        if (length > 0) encoded |= 0x80;

        out.push_back(encoded);
    }
    while (length > 0);
}

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
static MESSAGE_STATUS MqttConnect(int fd, io_uring *ring)
{
    // CONNACK: packet type (0x20), remaining length (0x02)
    // connect acknowledge flags (0x00), reason code success (0x00)
    constexpr std::uint8_t connack[] = {0x20, 0x02, 0x00, 0x00};

    if (broker::QueueSend(ring, fd, {connack, connack + sizeof(connack)}) == FAILURE) return FAILURE;

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
static MESSAGE_STATUS MqttPing(int fd, io_uring *ring)
{
    // PINGRESP: packet type (0xD0), remaining length (0x00)
    constexpr std::uint8_t pingresp[] = {0xD0, 0x00};

    if (broker::QueueSend(ring, fd, {pingresp, pingresp + sizeof(pingresp)}) == FAILURE) return FAILURE;

    #ifdef DEBUG
    std::cout << "Received PINGREQ\n";
    std::cout << "Sent PINGRESP" << std::endl;
    #endif

    return OK;
}

/**
 * @brief Send 4 byte acknowledge packet (PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK).
 *
 * @param fd Client socket file descriptor.
 * @param type Acknowledge packet type.
 * @param packetId Packet Identifier being acknowledged.
 * @param flags Lower 4 bits of the fixed header, 0 for every type except PUBREL.
 * @return MESSAGE_STATUS OK on success, FAILURE on send failure.
 */
static MESSAGE_STATUS SendAck(int fd, io_uring *ring, PACKET_TYPE type, std::uint16_t packetId, std::uint8_t flags)
{
    // packet type in upper 4 bits, remaining length (0x02), packet identifier MSB, LSB
    const std::uint8_t ack[] = {
        static_cast<std::uint8_t>((type << 4) | (flags & 0x0F)),
        0x02,
        static_cast<std::uint8_t>(packetId >> 8),
        static_cast<std::uint8_t>(packetId & 0xFF)
    };

    return broker::QueueSend(ring, fd, {ack, ack + sizeof(ack)});
}

/**
 * @brief Send MQTT 3.1.1 SUBACK packet.
 *
 * @param fd Client socket file descriptor.
 * @param packetId Packet Identifier taken from the SUBSCRIBE packet.
 * @param returnCodes One granted QoS or failure code per requested Topic Filter.
 * @return MESSAGE_STATUS OK on success, FAILURE on send failure.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718068
 */
static MESSAGE_STATUS SendSuback(int fd, io_uring *ring, std::uint16_t packetId, const std::vector<std::uint8_t> &returnCodes)
{
    std::vector<std::uint8_t> packet;
    packet.push_back(SUBACK << 4);

    EncodeRemainingLength(packet, sizeof(packetId) + returnCodes.size());

    packet.push_back(static_cast<std::uint8_t>(packetId >> 8));
    packet.push_back(static_cast<std::uint8_t>(packetId & 0xFF));
    packet.insert(packet.end(), returnCodes.begin(), returnCodes.end());

    return broker::QueueSend(ring, fd, std::move(packet));
}

/**
 * @brief Send MQTT 3.1.1 PUBLISH packet to a subscriber.
 *
 * @param fd Subscriber socket file descriptor.
 * @param topic Topic Name the message was published on.
 * @param payload Application message.
 * @param qos QoS level the message is delivered with.
 * @param retain RETAIN flag, set only when replaying a stored message to a new subscription.
 * @param packetId Packet Identifier, used only when @p qos is greater than 0.
 * @return MESSAGE_STATUS OK on success, FAILURE on send failure.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718037
 */
static MESSAGE_STATUS SendPublish(
    int fd,
    io_uring *ring,
    std::string_view topic,
    std::string_view payload,
    std::uint8_t qos,
    bool retain,
    std::uint16_t packetId
)
{
    const std::size_t packetIdSize = (qos > 0) ? sizeof(packetId) : 0;

    std::vector<std::uint8_t> packet;
    packet.push_back(static_cast<std::uint8_t>((PUBLISH << 4) | (qos << 1) | (retain ? 0x01 : 0x00)));
    EncodeRemainingLength(packet, sizeof(std::uint16_t) + topic.size() + packetIdSize + payload.size());

    packet.push_back(static_cast<std::uint8_t>(topic.size() >> 8));
    packet.push_back(static_cast<std::uint8_t>(topic.size() & 0xFF));
    packet.insert(packet.end(), topic.begin(), topic.end());

    if (qos > 0)
    {
        packet.push_back(static_cast<std::uint8_t>(packetId >> 8));
        packet.push_back(static_cast<std::uint8_t>(packetId & 0xFF));
    }

    packet.insert(packet.end(), payload.begin(), payload.end());

    return broker::QueueSend(ring, fd, std::move(packet));
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
static MESSAGE_STATUS MqttPublish(int fd, io_uring *ring, message_accumulator_t *message, Topic *topicsRoot)
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

    for (const subscription_t &subscriber : MatchSubscribers(topicsRoot, topic))
    {
        // Delivery uses the lower of the published and the subscribed QoS [MQTT-3.8.4-6]
        const std::uint8_t deliveryQos = std::min(qos, subscriber.qos);
        const std::uint16_t deliveryId = (deliveryQos > 0) ? NextPacketId() : 0;

        // RETAIN is cleared on forwarded messages [MQTT-3.3.1-9], and one unreachable
        // subscriber must not stop delivery to the remaining ones
        SendPublish(subscriber.fd, ring, topic, payload, deliveryQos, false, deliveryId);
    }

    // Without RETAIN flag the message is not stored [MQTT-3.3.1-12].
    if (retain) SaveTopic(topicsRoot, topic, std::move(payload));

    // QoS check: pick acknowledge for the given level
    switch (qos)
    {
        case 0: // At most once, no response
            return OK;

        case 1: // At least once, respond with PUBACK
            #ifdef DEBUG
            std::cout << "Sent PUBACK" << std::endl;
            #endif
            return SendAck(fd, ring, PUBACK, packetId);

        case 2: // Exactly once, respond with PUBREC, client continues with PUBREL
            #ifdef DEBUG
            std::cout << "Sent PUBREC" << std::endl;
            #endif
            return SendAck(fd, ring, PUBREC, packetId);

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
static MESSAGE_STATUS MqttPubrel(int fd, io_uring *ring, message_accumulator_t *message)
{
    // Fixed header flags have to be 0b0010 [MQTT-3.6.1-1]
    if (message->flags != 0x02) return FAILURE;
    if (message->size != 2 || message->data.size() < 2) return FAILURE;

    const std::uint16_t packetId = static_cast<std::uint16_t>((message->data[0] << 8) | message->data[1]);

    #ifdef DEBUG
    std::cout << "Received PUBREL id=" << packetId << "\n";
    std::cout << "Sent PUBCOMP" << std::endl;
    #endif

    return SendAck(fd, ring, PUBCOMP, packetId);
}

/**
 * @brief Handle MQTT 3.1.1 PUBREC packet from a subscriber (QoS 2 flow) and respond with PUBREL.
 *
 * @param fd Client socket file descriptor.
 * @param message Assembled PUBREC message.
 * @return MESSAGE_STATUS OK on success, FAILURE on malformed packet or send failure.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718048
 */
static MESSAGE_STATUS MqttPubrec(int fd, io_uring *ring, message_accumulator_t *message)
{
    if (message->flags != 0) return FAILURE;
    if (message->size != 2 || message->data.size() < 2) return FAILURE;

    const std::uint16_t packetId = static_cast<std::uint16_t>((message->data[0] << 8) | message->data[1]);

    #ifdef DEBUG
    std::cout << "Received PUBREC id=" << packetId << "\n";
    std::cout << "Sent PUBREL" << std::endl;
    #endif

    // PUBREL fixed header flags have to be 0b0010 [MQTT-3.6.1-1]
    return SendAck(fd, ring, PUBREL, packetId, 0x02);
}

/**
 * @brief Accept PUBACK or PUBCOMP from a subscriber. In-flight ids are not tracked yet.
 *
 * @param message Assembled acknowledge message.
 * @return MESSAGE_STATUS OK on success, FAILURE on malformed packet.
 */
static MESSAGE_STATUS MqttDeliveryAck(message_accumulator_t *message)
{
    if (message->flags != 0) return FAILURE;
    if (message->size != 2 || message->data.size() < 2) return FAILURE;

    return OK;
}

/**
 * @brief Handle MQTT 3.1.1 SUBSCRIBE packet, respond with SUBACK, then send matching retained messages.
 *
 * @param fd Client socket file descriptor.
 * @param message Assembled SUBSCRIBE message.
 * @param topicsRoot root of topics tree.
 * @return MESSAGE_STATUS OK on success, FAILURE on malformed packet or send failure.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718063
 */
static MESSAGE_STATUS MqttSubscribe(int fd, io_uring *ring, message_accumulator_t *message, Topic *topicsRoot)
{
    // Fixed header flags have to be 0b0010 [MQTT-3.8.1-1]
    if (message->flags != 0x02) return FAILURE;

    const std::size_t length = std::min(message->size, message->data.size());
    const std::uint8_t *data = message->data.data();
    std::size_t idx = 0;

    if (length < 2) return FAILURE;
    const std::uint16_t packetId = static_cast<std::uint16_t>((data[0] << 8) | data[1]);
    idx += 2;

    if (packetId == 0) return FAILURE;

    // Payload must contain at least one Topic Filter [MQTT-3.8.3-3]
    if (idx >= length) return FAILURE;

    std::vector<std::uint8_t> returnCodes;
    std::vector<std::string> acceptedFilters;
    std::vector<std::uint8_t> acceptedQos;

    while (idx < length)
    {
        if (idx + 2 > length) return FAILURE;

        const std::size_t filterLength = (data[idx] << 8) | data[idx + 1];
        idx += 2;

        // Topic Filter followed by 1 byte Requested QoS
        if (filterLength == 0 || idx + filterLength + 1 > length) return FAILURE;

        const std::string_view filter(reinterpret_cast<const char *>(data + idx), filterLength);
        idx += filterLength;

        const std::uint8_t requested = data[idx++];
        // Bits 7-2 reserved, must be 0; bits 1-0 are QoS 0, 1 or 2 [MQTT-3.8.3-4]
        if ((requested & 0xFC) != 0 || (requested & 0x03) > 2) return FAILURE;

        const std::uint8_t qos = requested & 0x03;
        if (!IsValidTopicFilter(filter))
        {
            returnCodes.push_back(subackFailure);
            continue;
        }

        SubscribeTopic(topicsRoot, filter, fd, qos);
        returnCodes.push_back(qos);
        acceptedFilters.emplace_back(filter);
        acceptedQos.push_back(qos);
    }

    #ifdef DEBUG
    std::cout << "Received SUBSCRIBE id=" << packetId
              << " filters=" << returnCodes.size() << "\n";
    std::cout << "Sent SUBACK" << std::endl;
    #endif

    if (SendSuback(fd, ring, packetId, returnCodes) == FAILURE) return FAILURE;

    // Matching retained messages are sent with RETAIN
    for (std::size_t i = 0; i < acceptedFilters.size(); ++i)
    {
        for (const retained_message_t &retained : MatchRetained(topicsRoot, acceptedFilters[i]))
        {
            const std::uint16_t deliveryId = (acceptedQos[i] > 0) ? NextPacketId() : 0;
            SendPublish(fd, ring, retained.topic, retained.message, acceptedQos[i], true, deliveryId);
        }
    }

    return OK;
}

/**
 * @brief Handle MQTT 3.1.1 UNSUBSCRIBE packet and respond with UNSUBACK.
 *
 * @param fd Client socket file descriptor.
 * @param message Assembled UNSUBSCRIBE message.
 * @param topicsRoot root of topics tree.
 * @return MESSAGE_STATUS OK on success, FAILURE on malformed packet or send failure.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718072
 */
static MESSAGE_STATUS MqttUnsubscribe(int fd, io_uring *ring, message_accumulator_t *message, Topic *topicsRoot)
{
    // Fixed header flags have to be 0b0010 [MQTT-3.10.1-1]
    if (message->flags != 0x02) return FAILURE;

    const std::size_t length = std::min(message->size, message->data.size());
    const std::uint8_t *data = message->data.data();
    std::size_t idx = 0;

    if (length < 2) return FAILURE;
    const std::uint16_t packetId = static_cast<std::uint16_t>((data[0] << 8) | data[1]);
    idx += 2;

    if (packetId == 0) return FAILURE;

    // Payload must contain at least one Topic Filter [MQTT-3.10.3-2]
    if (idx >= length) return FAILURE;

    while (idx < length)
    {
        if (idx + 2 > length) return FAILURE;

        const std::size_t filterLength = (data[idx] << 8) | data[idx + 1];
        idx += 2;

        if (filterLength == 0 || idx + filterLength > length) return FAILURE;

        const std::string_view filter(reinterpret_cast<const char *>(data + idx), filterLength);
        idx += filterLength;

        UnsubscribeTopic(topicsRoot, filter, fd);
    }

    #ifdef DEBUG
    std::cout << "Received UNSUBSCRIBE id=" << packetId << "\n";
    std::cout << "Sent UNSUBACK" << std::endl;
    #endif

    return SendAck(fd, ring, UNSUBACK, packetId);
}

/**
 * @brief Process assembled MQTT message.
 *
 * @param clientFd Client socket file descriptor.
 * @return message pointer to handled message.
 */
MESSAGE_STATUS HandleMessageData(int clientFd, message_accumulator_t *message, Topic *topicsRoot, io_uring *ring)
{
    if (!message) return FAILURE;

    switch (message->type)
    {
        case CONNECT:
            return MqttConnect(clientFd, ring);

        case PINGREQ:
            return MqttPing(clientFd, ring);

        case PUBLISH:
            return MqttPublish(clientFd, ring, message, topicsRoot);

        case PUBREL:
            return MqttPubrel(clientFd, ring, message);

        case PUBREC:
            return MqttPubrec(clientFd, ring, message);

        case PUBACK:
        case PUBCOMP:
            return MqttDeliveryAck(message);

        case SUBSCRIBE:
            return MqttSubscribe(clientFd, ring, message, topicsRoot);

        case UNSUBSCRIBE:
            return MqttUnsubscribe(clientFd, ring, message, topicsRoot);

        case DISCONNECT:
            return FAILURE;

        default:
            return FAILURE;
    }
}
