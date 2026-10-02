#include "broker.hpp"

int main()
{
    broker::PrintInfo();

    broker::Start((char*)"0.0.0.0", 8888);

    return 0;
}
