#pragma once
#include <string>

namespace gridmr_worker {

bool ensure_mapper_binary(const std::string& binary_uri, std::string& out_path);
void do_map(const std::string& s3_uri, const std::string& binary_uri, int reducer_id, int n_reducers);

}
