#include "broker.hpp"

int main()
{
    broker::print_info();

    broker::start((char*)"0.0.0.0", 8888);

    return 0;
}
