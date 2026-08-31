#pragma once

#include <string>

void throw_if_error(int return_code, const std::string description, int bad_code = -1);
