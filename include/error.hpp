#pragma once

#include <string>

/**
 * @brief Throw a system error when a return code matches an error code.
 *
 * Helper for syscall-style APIs where failures are represented by a specific
 * numeric return value (default -1).
 *
 * @param return_code Value returned by a function call.
 * @param description Error context included in exception message.
 * @param bad_code Value that indicates failure.
 *
 * @throws std::system_error When @p return_code equals @p bad_code.
 */
void throw_if_error(int return_code, const std::string &description, int bad_code = -1);
