#pragma once
#include <string>

namespace gridmr_worker {

struct FsLoc { std::string root; std::string path; };

bool parse_fs_uri(const std::string& uri, FsLoc& out);

bool download_url_to_file(const std::string& url, const std::string& out_path);

bool upload_file_to_fs(const std::string& local_path, const std::string& dest_path);

} // namespace gridmr_worker
