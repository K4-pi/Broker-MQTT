#include "error.hpp"

#include <string>
#include <system_error>
#include <cerrno>

void throw_if_error(int return_code, const std::string description, int bad_code)
{
    if (return_code == bad_code)
        throw std::system_error(errno, std::generic_category(), description);
}
