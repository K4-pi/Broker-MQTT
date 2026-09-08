#include "error.hpp"

#include <system_error>
#include <cerrno>

/**
 * @brief Throw a system error when a call result indicates failure.
 *
 * @param return_code Value returned by a checked operation.
 * @param description Context string for the thrown error.
 * @param bad_code Sentinel return value treated as failure.
 */
void throw_if_error(int return_code, const std::string &description, int bad_code)
{
    if (return_code == bad_code)
        throw std::system_error(errno, std::generic_category(), description);
}
