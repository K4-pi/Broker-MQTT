#include "broker.hpp"

int main() {

    broker::print_info();

    broker::setup((char*)"0.0.0.0", 8888);

    broker::start();

    return 0;
}
