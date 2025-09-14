#pragma once
#include <string>

namespace gridmr_worker {

// Initialize logger using WORKER_ID or HOSTNAME env; creates log dirs/files.
void log_init();

// Optionally override the worker id used in prefixes.
void log_set_worker_id(const std::string& id);

// Append a message to local log and shared /shared/workerlogs.
void log_msg(const std::string& msg);

} // namespace gridmr_worker
