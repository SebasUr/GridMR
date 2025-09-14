#include "gridmr/worker/common/fs.h"
#include "gridmr/worker/common/env.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>
#include "gridmr/worker/common/logger.h"

namespace gridmr_worker {

bool parse_fs_uri(const std::string& uri, FsLoc& out){
  const std::string pre = "fs://";
  if (uri.rfind(pre, 0) != 0) return false;
  auto rest = uri.substr(pre.size());
  auto slash = rest.find('/');
  if (slash == std::string::npos) return false;
  out.root = rest.substr(0, slash);
  out.path = rest.substr(slash + 1);
  return !out.root.empty() && !out.path.empty();
}

static std::string shared_root() {
  return envOr("SHARED_DATA_ROOT", "/shared");
}

static bool is_http(const std::string& url){
  return url.rfind("http://", 0) == 0 || url.rfind("https://", 0) == 0;
}

static bool is_file_uri(const std::string& url){
  return url.rfind("file://", 0) == 0;
}

static std::string resolve_local_path(const std::string& uri){
  if (is_file_uri(uri)) return uri.substr(7); // strip file://
  if (!uri.empty() && uri[0] == '/') return uri; // absolute
  // Treat as relative to SHARED_DATA_ROOT
  return shared_root() + "/" + uri;
}

bool download_url_to_file(const std::string& url, const std::string& out_path){
  if (is_http(url)){
    std::string cmd = std::string("curl -fsSL ") + url + " -o '" + out_path + "'";
    log_msg(std::string("curl: ") + cmd);
    return std::system(cmd.c_str()) == 0;
  }
  // Local copy
  std::string src = resolve_local_path(url);
  std::string cmd = std::string("cp -f -- '") + src + "' '" + out_path + "'";
  log_msg(std::string("cp: ") + cmd);
  return std::system(cmd.c_str()) == 0;
}

bool upload_file_to_fs(const std::string& local_path, const std::string& dest_path){
  std::string dst = dest_path;
  if (dest_path.rfind("/", 0) != 0) {
    dst = shared_root() + "/" + dest_path;
  }
  // Ensure parent dir exists via mkdir -p
  std::string mkdirCmd = std::string("mkdir -p '") + dst.substr(0, dst.find_last_of('/')) + "'";
  std::system(mkdirCmd.c_str());
  std::string cmd = std::string("cp -f -- '") + local_path + "' '" + dst + "'";
  log_msg(std::string("cp upload: ") + cmd);
  return std::system(cmd.c_str()) == 0;
}

} // namespace gridmr_worker
