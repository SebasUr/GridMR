#pragma once
#include <string>

namespace gridmr_worker {

bool ensure_reducer_binary(const std::string& binary_uri, std::string& out_path);
std::string do_reduce_collect_output(const std::string& binary_uri, int split_count, int reducer_id, const std::string& job_id, const std::string& example_input_uri);

} // namespace gridmr_worker
