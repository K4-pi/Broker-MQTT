#pragma once

#include <string>

/**
 * @brief Throw a system error when a return code matches an error code.
 *
 * Helper for syscall-style APIs where failures are represented by a specific
 * numeric return value (default -1).
 *
 * @param returnCode Value returned by a function call.
 * @param description Error context included in exception message.
 * @param badCode Value that indicates failure.
 *
 * @throws std::system_error When @p returnCode equals @p badCode.
 */
void ThrowIfError(int returnCode, const std::string &description, int badCode = -1);
