#pragma once
#include <string>

namespace gridmr_worker {

struct S3Loc { std::string bucket; std::string key; };

bool parse_s3_uri(const std::string& uri, S3Loc& out);

// Download from s3:// or http(s):// to a local file.
bool download_url_to_file(const std::string& url, const std::string& out_path);

// Upload a local file to an s3:// URI via awscli (MinIO-compatible).
bool upload_file_to_minio(const std::string& local_path, const std::string& s3_uri);

} // namespace gridmr_worker
