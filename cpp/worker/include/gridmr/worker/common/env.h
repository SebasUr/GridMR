// Lightweight env helper
#pragma once
#include <string>

namespace gridmr_worker {

// Returns the value of environment variable k, or default d if unset/empty.
std::string envOr(const char* k, const char* d);

}
