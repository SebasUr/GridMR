#include "gridmr/worker/common/logger.h"
#include <mutex>
#include <fstream>
#include <sstream>
#include <chrono>
#include <ctime>
#include <sys/stat.h>
#include <sys/types.h>
#include <cstdlib>
#include "gridmr/worker/common/env.h"

namespace gridmr_worker {

static std::mutex g_mu;
static std::string g_worker_id;
static std::string g_local_path = "/var/log/gridmr/worker.log";
static std::string g_shared_dir;

static std::string now_iso(){
  using namespace std::chrono;
  auto t = system_clock::to_time_t(system_clock::now());
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", std::localtime(&t));
  return std::string(buf);
}

static void ensure_dirs(){
  std::string cmd = std::string("mkdir -p /var/log/gridmr '") + g_shared_dir + "'";
  std::system(cmd.c_str());
}

void log_set_worker_id(const std::string& id){
  std::lock_guard<std::mutex> lk(g_mu);
  g_worker_id = id;
}

void log_init(){
  std::lock_guard<std::mutex> lk(g_mu);
  if (g_worker_id.empty()) {
    g_worker_id = envOr("WORKER_ID", "");
    if (g_worker_id.empty()) g_worker_id = envOr("HOSTNAME", "worker-1");
  }
  std::string shared_root = envOr("SHARED_DATA_ROOT", "/shared");
  g_shared_dir = shared_root + "/workerlogs";
  ensure_dirs();
}

void log_msg(const std::string& msg){
  std::lock_guard<std::mutex> lk(g_mu);
  if (g_worker_id.empty()) log_init();
  const std::string line = now_iso() + " [" + g_worker_id + "] " + msg + "\n";
  // Local
  {
    std::ofstream lf(g_local_path, std::ios::app);
    if (lf.good()) lf << line;
  }
  // Shared
  {
    std::ofstream sf(g_shared_dir + "/" + g_worker_id + ".log", std::ios::app);
    if (sf.good()) sf << line;
  }
}

} // namespace gridmr_worker
