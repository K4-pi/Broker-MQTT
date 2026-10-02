#include "error.hpp"

#include <system_error>
#include <cerrno>

void ThrowIfError(int returnCode, const std::string &description, int badCode);

/**
 * @brief Throw a system error when a call result indicates failure.
 *
 * @param returnCode Value returned by a checked operation.
 * @param description Context string for the thrown error.
 * @param badCode Sentinel return value treated as failure.
 */
void ThrowIfError(int returnCode, const std::string &description, int badCode)
{
    if (returnCode == badCode)
        throw std::system_error(errno, std::generic_category(), description);
}
