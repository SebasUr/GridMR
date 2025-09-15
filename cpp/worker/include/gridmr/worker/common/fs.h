#pragma once
#include <string>

namespace gridmr_worker {

// Generic filesystem/URL helpers for local/NFS/EFS and HTTP sources.

struct FsLoc { std::string root; std::string path; };

// Optional parser for fs:// URIs (not used by main flow)
bool parse_fs_uri(const std::string& uri, FsLoc& out);

// Download/copy from local path (file:/// or absolute/relative) or http(s):// to a local file.
// - file:///path or /path -> copy
// - http(s):// -> curl
// Returns true on success.
bool download_url_to_file(const std::string& url, const std::string& out_path);

// Upload (copy) a local file to a destination path under a shared directory root.
// If `dest_path` is absolute, it will be used as-is; otherwise it will be
// resolved relative to SHARED_DATA_ROOT env var.
bool upload_file_to_fs(const std::string& local_path, const std::string& dest_path);

} // namespace gridmr_worker
