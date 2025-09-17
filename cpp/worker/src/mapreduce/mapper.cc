#include "gridmr/worker/mapreduce/mapper.h"
#include "gridmr/worker/common/fs.h"
#include "gridmr/worker/common/env.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <functional>
#include <cstdlib>
#include "gridmr/worker/common/logger.h"

namespace gridmr_worker {

static bool ends_with(const std::string& s, const std::string& suf){
  return s.size() >= suf.size() && s.compare(s.size()-suf.size(), suf.size(), suf) == 0;
}

bool ensure_mapper_binary(const std::string& binary_uri, std::string& out_path){
  if (binary_uri.empty()) return false;
  if (ends_with(binary_uri, ".cc") || ends_with(binary_uri, ".cpp")){
    std::string src = "/tmp/map_src.cc";
    if (!download_url_to_file(binary_uri, src)) return false;
    std::string bin = "/tmp/map_bin";
    std::string cmd = std::string("g++ -O2 -std=c++17 -static -static-libstdc++ -o ") + bin + " " + src;
  log_msg(std::string("compiling mapper: ") + cmd);
    if (std::system(cmd.c_str()) != 0){
      cmd = std::string("g++ -O2 -std=c++17 -o ") + bin + " " + src;
  log_msg(std::string("static link failed, retry dynamic: ") + cmd);
      if (std::system(cmd.c_str()) != 0) return false;
    }
    std::string chmodcmd = std::string("chmod +x ") + bin;
    std::system(chmodcmd.c_str());
    out_path = bin;
    return true;
  } else {
    std::string dest = "/tmp/map_bin";
    if (!download_url_to_file(binary_uri, dest)) return false;
    std::string chmodcmd = std::string("chmod +x ") + dest;
    if (std::system(chmodcmd.c_str()) != 0) return false;
    out_path = dest;
    return true;
  }
}

void do_map(const std::string& uri, const std::string& binary_uri, int /*reducer_id*/, int n_reducers) {
  std::string input_path = "/tmp/map_input.txt";
  if (!download_url_to_file(uri, input_path)){
  log_msg(std::string("input copy failed, trying SHARED_DATA_ROOT for: ") + uri);
    std::string prefix = envOr("SHARED_DATA_ROOT", "/shared");
    auto pos = uri.find_last_of('/');
    std::string file = (pos == std::string::npos) ? uri : uri.substr(pos + 1);
    input_path = prefix + "/" + file;
  }
  // Print input in single line
  {
    std::ifstream fin(input_path);
    if (fin) {
      std::ostringstream all;
      std::string line;
      bool first = true;
      while (std::getline(fin, line)) {
        if (!first) all << ' ';
        first = false;
        all << line;
      }
      std::cout << "[INPUT_ONE_LINE] " << all.str() << std::endl;
    }
  }
  std::string mapper = "/usr/local/bin/map";
  std::string downloaded;
  if (!binary_uri.empty() && ensure_mapper_binary(binary_uri, downloaded)) mapper = downloaded;
  std::string cmd = mapper + std::string(" < ") + input_path;
  log_msg(std::string("MAP exec: ") + cmd);
  FILE* pipe = popen(cmd.c_str(), "r");
  if (!pipe) { log_msg("Failed to run mapper"); return; }

  int R = std::max(1, n_reducers);
  std::vector<size_t> counts(R, 0);
  std::vector<std::ostringstream> parts(R);
  while (true) {
    char buf[4096];
    if (!fgets(buf, sizeof(buf), pipe)) break;
    std::string s(buf);
    if (!s.empty() && s.back() == '\n') s.pop_back();
    std::cout << s << std::endl; // echo
    s.push_back('\n');
    auto tab = s.find('\t');
    if (tab == std::string::npos) continue;
    std::string key = s.substr(0, tab);
    std::string val = s.substr(tab+1);
    std::hash<std::string> h;
    int pid = static_cast<int>(h(key) % R);
    counts[pid]++;
    parts[pid] << key << '\t' << val;
  }
  pclose(pipe);
  for (int i = 0; i < R; ++i) {
    std::string outPath = "/tmp/to-reduce-input-" + std::to_string(i) + ".txt";
    std::ofstream ofs(outPath, std::ios::binary);
    std::string data = parts[i].str();
    ofs.write(data.data(), (std::streamsize)data.size());
  }
}

}
