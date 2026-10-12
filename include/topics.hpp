#pragma once

#include <string>
#include <string_view>
#include <vector>

class Topic
{
    public:
        std::string mName;
        std::vector<Topic*> mChilds;

        std::string mMessage;

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
