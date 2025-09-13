#include "gridmr/worker/mapreduce/reducer.h"
#include "gridmr/worker/common/fs.h"
#include "gridmr/worker/common/env.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <cstdlib>

namespace gridmr_worker {

static bool ends_with(const std::string& s, const std::string& suf){
  return s.size() >= suf.size() && s.compare(s.size()-suf.size(), suf.size(), suf) == 0;
}

bool ensure_reducer_binary(const std::string& binary_uri, std::string& out_path){
  if (binary_uri.empty()) return false;
  // reuse mapper compiler
  if (ends_with(binary_uri, ".cc") || ends_with(binary_uri, ".cpp")){
    std::string src = "/tmp/reduce_src.cc";
    if (!download_url_to_file(binary_uri, src)) return false;
    std::string bin = "/tmp/reduce_bin";
    std::string cmd = std::string("g++ -O2 -std=c++17 -static -static-libstdc++ -o ") + bin + " " + src;
    std::cerr << "[worker] compiling reducer: " << cmd << std::endl;
    if (std::system(cmd.c_str()) != 0){
      cmd = std::string("g++ -O2 -std=c++17 -o ") + bin + " " + src;
      std::cerr << "[worker] static link failed, retry dynamic: " << cmd << std::endl;
      if (std::system(cmd.c_str()) != 0) return false;
    }
    std::string chmodcmd = std::string("chmod +x ") + bin;
    std::system(chmodcmd.c_str());
    out_path = bin;
    return true;
  } else {
    std::string dest = "/tmp/reduce_bin";
    if (!download_url_to_file(binary_uri, dest)) return false;
    std::string chmodcmd = std::string("chmod +x ") + dest;
    if (std::system(chmodcmd.c_str()) != 0) return false;
    out_path = dest;
    return true;
  }
}

static std::string run_reducer_and_capture(const std::string& reducer, const std::string& input_path){
  std::string cmd = reducer + std::string(" < ") + input_path;
  std::cerr << "[worker] REDUCE exec: " << cmd << std::endl;
  FILE* pipe = popen(cmd.c_str(), "r");
  if (!pipe) { std::cerr << "[worker] Failed to run reducer" << std::endl; return {}; }
  std::ostringstream out;
  char buf[4096];
  while (fgets(buf, sizeof(buf), pipe)) out << buf;
  pclose(pipe);
  return out.str();
}

static std::string concat_reduce_inputs(int count){
  std::string tmp = "/tmp/reduce_concat.txt";
  std::ofstream ofs(tmp, std::ios::binary);
  for (int i = 0; i < count; ++i){
    std::string path = std::string("/tmp/reduce-input-") + std::to_string(i) + ".txt";
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) continue;
    ofs << ifs.rdbuf();
  }
  return tmp;
}

std::string do_reduce_collect_output(const std::string& binary_uri, int split_count, int reducer_id, const std::string& job_id, const std::string& example_input_uri){
  std::string reducer = "/usr/local/bin/reduce";
  std::string downloaded;
  std::string red_uri = envOr("MR_REDUCE_BIN_URI", binary_uri.c_str());
  if (!red_uri.empty() && ensure_reducer_binary(red_uri, downloaded)) reducer = downloaded;
  std::string concatPath = concat_reduce_inputs(split_count);
  std::string out = run_reducer_and_capture(reducer, concatPath);

  // Persist to local tmp
  std::string outLocal = std::string("/tmp/reduce-out-") + std::to_string(reducer_id) + ".txt";
  {
    std::ofstream ofs(outLocal, std::ios::binary);
    ofs.write(out.data(), (std::streamsize)out.size());
  }
  // Copy to shared filesystem (EFS/NFS)
  std::string root = envOr("SHARED_DATA_ROOT", "/shared");
  std::string dest = root + std::string("/results/") + job_id + "/part-" + std::to_string(reducer_id) + ".txt";
  // ensure dir exists
  std::string mkdirCmd = std::string("mkdir -p '") + root + "/results/" + job_id + "'";
  std::system(mkdirCmd.c_str());
  if (!upload_file_to_fs(outLocal, dest)) {
    std::cerr << "[worker] reduce output copy failed: " << dest << std::endl;
    return "";
  }
  return dest;
}

} // namespace gridmr_worker
