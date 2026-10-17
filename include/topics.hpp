#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

/**
 * Subscription of a single client on a single topic tree node
 */
struct subscription_t
{
    int fd;
    std::uint8_t qos;
};

/**
 * Retained message together with the topic path it was stored under
 */
struct retained_message_t
{
    std::string topic;
    std::string message;
};

class Topic
{
    public:
        std::string mName;
        std::vector<Topic*> mChilds;

        std::string mMessage;
        std::vector<subscription_t> mSubscribers;

        Topic(std::string_view name);
        Topic(std::string_view name, std::string message);
        Topic *AddTopic(std::string_view name, std::string msg);

        ~Topic()
        {
            for (auto &t : mChilds) delete t;
        };
};

Topic *InitTopicsTree();
void SaveTopic(Topic *root, std::string_view path, std::string msg="");

bool IsValidTopicFilter(std::string_view filter);
void SubscribeTopic(Topic *root, std::string_view filter, int fd, std::uint8_t qos);
void UnsubscribeTopic(Topic *root, std::string_view filter, int fd);
void RemoveSubscriber(Topic *root, int fd);
std::vector<subscription_t> MatchSubscribers(Topic *root, std::string_view topic);
std::vector<retained_message_t> MatchRetained(Topic *root, std::string_view filter);
