#pragma once
#include <string>

namespace gridmr_worker {

void log_init();

void log_set_worker_id(const std::string& id);

void log_msg(const std::string& msg);

} // namespace gridmr_worker
