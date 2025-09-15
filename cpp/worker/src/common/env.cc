#include "gridmr/worker/common/env.h"
#include <cstdlib>

namespace gridmr_worker {

std::string envOr(const char* k, const char* d){
  const char* v = std::getenv(k);
  return (v && *v) ? std::string(v) : std::string(d);
}

} // namespace gridmr_worker
