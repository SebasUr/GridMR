#pragma once
#include <mutex>

namespace gridmr_worker {

// Thread-safe CPU and RAM usage helpers for Linux (/proc)
float sys_cpu_usage_percent();
float sys_ram_usage_percent();

} // namespace gridmr_worker
