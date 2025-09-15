#include "gridmr/worker/common/sysmetrics.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <string>

namespace gridmr_worker {

static long long g_prev_total = 0;
static long long g_prev_idle = 0;
static std::mutex g_cpu_mu;

float sys_cpu_usage_percent() {
    std::lock_guard<std::mutex> lk(g_cpu_mu);
    std::ifstream stat("/proc/stat");

    if (!stat) return 0.0f;

    std::string line;
    std::getline(stat, line);
    std::istringstream iss(line);
    std::string cpu_label;

    iss >> cpu_label;
    if (cpu_label != "cpu") return 0.0f;

    long long user=0, nice=0, system=0, idle=0, iowait=0, irq=0, softirq=0, steal=0;

    iss >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;

    long long total = user + nice + system + idle + iowait + irq + softirq + steal;
    long long idle_now = idle + iowait;

    if (g_prev_total == 0) {
        g_prev_total = total;
        g_prev_idle = idle_now;
        return 0.0f; 
    }

    long long total_diff = total - g_prev_total;
    long long idle_diff = idle_now - g_prev_idle;
    g_prev_total = total;
    g_prev_idle = idle_now;
    if (total_diff <= 0) return 0.0f;

    float usage = 100.0f * (total_diff - idle_diff) / (float)total_diff;
    usage = std::max(0.0f, std::min(100.0f, usage));

    return usage;
}

float sys_ram_usage_percent() {
    std::ifstream mem("/proc/meminfo");
    if (!mem) return 0.0f;

    long long total = 0, avail = 0;
    std::string line;

    while (std::getline(mem, line)) {
        std::istringstream iss(line);
        std::string key; long long val = 0; std::string unit;
        iss >> key >> val >> unit;
        if (key == "MemTotal:") total = val;
        else if (key == "MemAvailable:") avail = val;
    }
    
    if (total <= 0) return 0.0f;
    float usage = 100.0f * (total - avail) / (float)total;
    usage = std::max(0.0f, std::min(100.0f, usage));
    return usage;
}

} // namespace gridmr_worker
