#include "topics.hpp"
#include <boost/tokenizer.hpp>

#include <string>
#include <string_view>
#include <utility>

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
 * @brief Walk topic tree along @p path, creating missing levels, and store @p msg on the last level.
 *
 * @param root root of topics tree.
 * @param path topic path, levels separated by '/'.
 * @param msg message to store, empty string clears it.
 */
void SaveTopic(Topic *root, std::string_view path, std::string msg)
{
    boost::char_separator<char> sep("/");
    boost::tokenizer<boost::char_separator<char>> tokens(path, sep);

    Topic *node = root;
    for (const std::string &token : tokens)
    {
        Topic *child = FindChild(node, token);
        node = (child != nullptr) ? child : node->AddTopic(token, "");
    }
    node->mMessage = std::move(msg);

    #ifdef DEBUG
    for (const std::string_view t : tokens)
        std::cout << "tok: " << t << std::endl;
    #endif
}
