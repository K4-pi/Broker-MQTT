#include "topics.hpp"
#include <boost/tokenizer.hpp>

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifdef DEBUG
#include <iostream>
#endif

Topic::Topic(std::string_view name) : mName( name ), mChilds(), mMessage(){}
Topic::Topic(std::string_view name, std::string message) : mName( name ), mChilds(), mMessage(message){}

Topic *Topic::AddTopic(std::string_view name, std::string msg)
{
    Topic *child = new Topic(name, msg);
    mChilds.push_back(child);
    return child;
}

Topic *InitTopicsTree()
{
    return new Topic("/");
}

static Topic *FindChild(Topic *parent, std::string_view name)
{
    for (auto &child : parent->mChilds)
        if (child->mName == name) return child;

    return nullptr;
}

/**
 * @brief Split a topic path or filter into its levels.
 *
 * @param path topic path, levels separated by '/'.
 * @return levels in the order they appear in @p path.
 */
static std::vector<std::string> SplitTopic(std::string_view path)
{
    boost::char_separator<char> sep("/");
    boost::tokenizer<boost::char_separator<char>> tokens(path, sep);

    return std::vector<std::string>(tokens.begin(), tokens.end());
}

/**
 * @brief Append a level to a topic path built during tree traversal.
 */
static std::string JoinPath(const std::string &path, const std::string &level)
{
    return path.empty() ? level : path + "/" + level;
}

/**
 * @brief Walk topic tree along @p path, creating missing levels.
 *
 * @param root root of topics tree.
 * @param path topic path, levels separated by '/'.
 * @return node of the last level of @p path.
 */
static Topic *FindOrCreateNode(Topic *root, std::string_view path)
{
    Topic *node = root;
    for (const std::string &level : SplitTopic(path))
    {
        Topic *child = FindChild(node, level);
        node = (child != nullptr) ? child : node->AddTopic(level, "");
    }

    return node;
}

/**
 * @brief Walk topic tree along @p path without creating missing levels.
 *
 * @param root root of topics tree.
 * @param path topic path, levels separated by '/'.
 * @return node of the last level of @p path, nullptr when the path does not exist.
 */
static Topic *FindNode(Topic *root, std::string_view path)
{
    Topic *node = root;
    for (const std::string &level : SplitTopic(path))
    {
        node = FindChild(node, level);
        if (node == nullptr) return nullptr;
    }

    return node;
}

/**
 * @brief Walk topic tree along @p path, creating missing levels, and store @p msg on the last level.
 *
 * @param root root of topics tree.
 * @param path topic path, levels separated by '/'.
 * @param msg message to store, empty string clears it.
 */
void SaveTopic(Topic *root, std::string_view path, std::string msg)
{
    Topic *node = FindOrCreateNode(root, path);
    node->mMessage = std::move(msg);

    #ifdef DEBUG
    std::cout << "retained topic: " << path << std::endl;
    #endif
}

/**
 * @brief Check that a Topic Filter uses wildcards the way MQTT 3.1.1 allows.
 *
 * @param filter Topic Filter from a SUBSCRIBE or UNSUBSCRIBE payload.
 * @return bool true when the filter is well formed.
 *
 * https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106
 */
bool IsValidTopicFilter(std::string_view filter)
{
    if (filter.empty()) return false;

    for (std::size_t i = 0; i < filter.size(); ++i)
    {
        const char level = filter[i];
        if (level != '+' && level != '#') continue;

        // A wildcard has to occupy an entire level [MQTT-4.7.1-2], [MQTT-4.7.1-3]
        const bool levelStart = (i == 0) || (filter[i - 1] == '/');
        const bool levelEnd   = (i + 1 == filter.size()) || (filter[i + 1] == '/');
        if (!levelStart || !levelEnd) return false;

        // The multi-level wildcard is only allowed as the last character [MQTT-4.7.1-2]
        if (level == '#' && i + 1 != filter.size()) return false;
    }

    return true;
}

/**
 * @brief Register a client on the node @p filter points at, wildcard levels included.
 *
 * @param root root of topics tree.
 * @param filter Topic Filter to subscribe to.
 * @param fd Client socket file descriptor.
 * @param qos QoS level the client requested.
 */
void SubscribeTopic(Topic *root, std::string_view filter, int fd, std::uint8_t qos)
{
    Topic *node = FindOrCreateNode(root, filter);

    // Resubscribing to the same filter replaces the earlier subscription [MQTT-3.8.4-3]
    for (subscription_t &subscription : node->mSubscribers)
    {
        if (subscription.fd != fd) continue;

        subscription.qos = qos;
        return;
    }

    node->mSubscribers.push_back({fd, qos});
}

/**
 * @brief Remove a client subscription from the node @p filter points at.
 *
 * @param root root of topics tree.
 * @param filter Topic Filter to unsubscribe from.
 * @param fd Client socket file descriptor.
 */
void UnsubscribeTopic(Topic *root, std::string_view filter, int fd)
{
    Topic *node = FindNode(root, filter);
    if (node == nullptr) return;

    std::vector<subscription_t> &subscribers = node->mSubscribers;
    subscribers.erase(
        std::remove_if(
            subscribers.begin(),
            subscribers.end(),
            [fd](const subscription_t &subscription) { return subscription.fd == fd; }
        ),
        subscribers.end()
    );
}

/**
 * @brief Drop every subscription of a client, used when its connection ends.
 *
 * File descriptors are reused by the kernel, so leftover subscriptions would
 * deliver messages to whichever client connects next on the same descriptor.
 *
 * @param root root of topics tree.
 * @param fd Client socket file descriptor.
 */
void RemoveSubscriber(Topic *root, int fd)
{
    std::vector<subscription_t> &subscribers = root->mSubscribers;
    subscribers.erase(
        std::remove_if(
            subscribers.begin(),
            subscribers.end(),
            [fd](const subscription_t &subscription) { return subscription.fd == fd; }
        ),
        subscribers.end()
    );

    for (Topic *child : root->mChilds) RemoveSubscriber(child, fd);
}

static void AppendSubscribers(std::vector<subscription_t> &out, const std::vector<subscription_t> &subscribers)
{
    out.insert(out.end(), subscribers.begin(), subscribers.end());
}

/**
 * @brief Collect subscribers whose filter matches @p levels, starting at @p depth.
 *
 * @param node currently visited node.
 * @param levels levels of the published Topic Name.
 * @param depth index of the level @p node has to be matched against.
 * @param out collected subscriptions.
 */
static void CollectSubscribers(
    Topic *node,
    const std::vector<std::string> &levels,
    std::size_t depth,
    std::vector<subscription_t> &out
)
{
    if (depth == levels.size())
    {
        AppendSubscribers(out, node->mSubscribers);

        // "sport/#" matches "sport" as well [MQTT-4.7.1-2]
        Topic *multiLevel = FindChild(node, "#");
        if (multiLevel != nullptr) AppendSubscribers(out, multiLevel->mSubscribers);

        return;
    }

    for (Topic *child : node->mChilds)
    {
        if (child->mName == "#")
        {
            AppendSubscribers(out, child->mSubscribers);
            continue;
        }

        if (child->mName == "+" || child->mName == levels[depth])
            CollectSubscribers(child, levels, depth + 1, out);
    }
}

/**
 * @brief List every client subscribed to a published Topic Name.
 *
 * @param root root of topics tree.
 * @param topic Topic Name of a PUBLISH packet, wildcards are not expected here.
 * @return subscriptions matching @p topic.
 */
std::vector<subscription_t> MatchSubscribers(Topic *root, std::string_view topic)
{
    std::vector<subscription_t> subscribers;
    CollectSubscribers(root, SplitTopic(topic), 0, subscribers);

    // Overlapping filters can match the same client more than once; keep the highest QoS
    std::vector<subscription_t> unique;
    for (const subscription_t &subscription : subscribers)
    {
        auto existing = std::find_if(
            unique.begin(),
            unique.end(),
            [fd = subscription.fd](const subscription_t &entry) { return entry.fd == fd; }
        );

        if (existing == unique.end()) unique.push_back(subscription);
        else existing->qos = std::max(existing->qos, subscription.qos);
    }

    return unique;
}

/**
 * @brief Collect retained messages stored below @p node that match @p levels.
 *
 * @param node currently visited node.
 * @param path topic path leading to @p node.
 * @param levels levels of the Topic Filter.
 * @param depth index of the filter level the children have to be matched against.
 * @param out collected retained messages.
 */
static void CollectRetained(
    Topic *node,
    const std::string &path,
    const std::vector<std::string> &levels,
    std::size_t depth,
    std::vector<retained_message_t> &out
)
{
    if (depth == levels.size())
    {
        if (!node->mMessage.empty()) out.push_back({path, node->mMessage});
        return;
    }

    const std::string &level = levels[depth];
    if (level == "#")
    {
        // The multi-level wildcard also matches the level it is attached to [MQTT-4.7.1-2]
        if (!path.empty() && !node->mMessage.empty()) out.push_back({path, node->mMessage});

        for (Topic *child : node->mChilds)
            CollectRetained(child, JoinPath(path, child->mName), levels, depth, out);

        return;
    }

    for (Topic *child : node->mChilds)
    {
        if (level != "+" && child->mName != level) continue;

        CollectRetained(child, JoinPath(path, child->mName), levels, depth + 1, out);
    }
}

/**
 * @brief List retained messages a new subscription has to receive.
 *
 * @param root root of topics tree.
 * @param filter Topic Filter of the subscription.
 * @return retained messages matching @p filter.
 */
std::vector<retained_message_t> MatchRetained(Topic *root, std::string_view filter)
{
    std::vector<retained_message_t> retained;
    CollectRetained(root, "", SplitTopic(filter), 0, retained);

    return retained;
}
