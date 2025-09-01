#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <fstream>
#include <cstdio>
#include <sstream>

#ifdef GRIDMR_HAVE_AWSSDK
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#endif

#include "gridmr.pb.h"
#include "gridmr.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;

using gridmr::WorkerToMaster;
using gridmr::MasterToWorker;
using gridmr::ControlService;
using gridmr::WorkerInfo;
using gridmr::AssignTask;
using gridmr::TaskStatus;

static std::string envOr(const char* k, const char* d){
  const char* v = std::getenv(k);
  return (v && *v) ? std::string(v) : std::string(d);
}

struct S3Loc { std::string bucket; std::string key; };

static bool parse_s3_uri(const std::string& uri, S3Loc& out){
  const std::string pre = "s3://";
  if (uri.rfind(pre, 0) != 0) return false;
  auto rest = uri.substr(pre.size());
  auto slash = rest.find('/');
  if (slash == std::string::npos) return false;
  out.bucket = rest.substr(0, slash);
  out.key = rest.substr(slash + 1);
  return !out.bucket.empty() && !out.key.empty();
}

#ifdef GRIDMR_HAVE_AWSSDK
static void ensureAwsInit(){
  static bool inited = false;
  static Aws::SDKOptions options;
  if (!inited){
    Aws::InitAPI(options);
    std::atexit([](){ static Aws::SDKOptions options2; Aws::ShutdownAPI(options2); });
    inited = true;
  }
}

static std::string strip_scheme(const std::string& endpoint, Aws::Http::Scheme& scheme){
  std::string ep = endpoint;
  scheme = Aws::Http::Scheme::HTTPS;
  const std::string http = "http://";
  const std::string https = "https://";
  if (ep.rfind(http,0)==0){ scheme = Aws::Http::Scheme::HTTP; return ep.substr(http.size()); }
  if (ep.rfind(https,0)==0){ scheme = Aws::Http::Scheme::HTTPS; return ep.substr(https.size()); }
  return ep; // assume host:port
}

static bool s3_download_to_file(const std::string& s3_uri, const std::string& out_path){
  ensureAwsInit();
  S3Loc loc; if (!parse_s3_uri(s3_uri, loc)){ std::cerr << "[worker] Bad S3 URI: " << s3_uri << std::endl; return false; }
  std::string endpoint = envOr("MINIO_ENDPOINT", "http://minio:9000");
  std::string access = envOr("MINIO_ACCESS_KEY", "minioadmin");
  std::string secret = envOr("MINIO_SECRET_KEY", "minioadmin");

  Aws::Client::ClientConfiguration cfg;
  cfg.region = "us-east-1";
  cfg.useDualStack = false;
  cfg.enableHostPrefixInjection = false;
  cfg.followRedirects = true;
  cfg.verifySSL = false; // MinIO local in HTTP
  cfg.useVirtualAddressing = false; // path-style for MinIO
  Aws::Http::Scheme scheme;
  cfg.endpointOverride = strip_scheme(endpoint, scheme).c_str();
  cfg.scheme = scheme;

  Aws::Auth::AWSCredentials creds(access.c_str(), secret.c_str());
  Aws::S3::S3Client s3(creds, cfg, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, /*useVirtualAddressing*/ false);

  Aws::S3::Model::GetObjectRequest req;
  req.SetBucket(loc.bucket.c_str());
  req.SetKey(loc.key.c_str());
  auto outcome = s3.GetObject(req);
  if (!outcome.IsSuccess()){
    std::cerr << "[worker] S3 GetObject failed: " << outcome.GetError().GetMessage() << std::endl;
    return false;
  }
  auto& is = outcome.GetResult().GetBody();
  std::ofstream ofs(out_path, std::ios::binary);
  ofs << is.rdbuf();
  ofs.close();
  return true;
}
#endif

#ifndef GRIDMR_HAVE_AWSSDK
// Fallback: use awscli with endpoint override to download from MinIO
static bool s3_download_to_file(const std::string& s3_uri, const std::string& out_path){
  std::string endpoint = envOr("MINIO_ENDPOINT", "http://minio:9000");
  std::string access = envOr("MINIO_ACCESS_KEY", "minioadmin");
  std::string secret = envOr("MINIO_SECRET_KEY", "minioadmin");
  std::string cmd = std::string("AWS_ACCESS_KEY_ID=") + access +
                    " AWS_SECRET_ACCESS_KEY=" + secret +
                    " aws --no-cli-pager --endpoint-url " + endpoint +
                    " s3 cp '" + s3_uri + "' '" + out_path + "'";
  std::cerr << "[worker] awscli: " << cmd << std::endl;
  int rc = std::system(cmd.c_str());
  return rc == 0;
}
#endif

static bool download_url_to_file(const std::string& url, const std::string& out_path){
  if (url.rfind("s3://", 0) == 0) {
    // Use the same S3 downloader (MinIO-compatible)
    std::cerr << "[worker] download mapper from S3: " << url << std::endl;
    return s3_download_to_file(url, out_path);
  }
  std::string cmd = std::string("curl -fsSL ") + url + " -o " + out_path;
  std::cerr << "[worker] curl: " << cmd << std::endl;
  int rc = std::system(cmd.c_str());
  return rc == 0;
}

static bool ends_with(const std::string& s, const std::string& suf){
  return s.size() >= suf.size() && s.compare(s.size()-suf.size(), suf.size(), suf) == 0;
}

static bool ensure_mapper_binary(const std::string& binary_uri, std::string& out_path){
  if (binary_uri.empty()) return false;
  // If it's a C++ source, compile it locally; else treat as binary
  if (ends_with(binary_uri, ".cc") || ends_with(binary_uri, ".cpp")){
    std::string src = "/tmp/map_src.cc";
    if (!download_url_to_file(binary_uri, src)) return false;
    std::string bin = "/tmp/map_bin";
    std::string cmd = std::string("g++ -O2 -std=c++17 -static -static-libstdc++ -o ") + bin + " " + src;
    std::cerr << "[worker] compiling mapper: " << cmd << std::endl;
    if (std::system(cmd.c_str()) != 0){
      // Fallback to dynamic if static fails
      cmd = std::string("g++ -O2 -std=c++17 -o ") + bin + " " + src;
      std::cerr << "[worker] static link failed, retry dynamic: " << cmd << std::endl;
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

static void do_map(const std::string& s3_uri, const std::string& binary_uri, int /*reducer_id*/, int n_reducers) {
  std::string input_path;
  input_path = "/tmp/map_input.txt";
  if (!s3_download_to_file(s3_uri, input_path)){
    std::cerr << "[worker] S3 download failed, falling back to local testdata for URI: " << s3_uri << std::endl;
    std::string prefix = envOr("MAP_LOCAL_PREFIX", "/src/testdata");
    auto pos = s3_uri.find_last_of('/');
    std::string file = (pos == std::string::npos) ? s3_uri : s3_uri.substr(pos + 1);
    input_path = prefix + "/" + file;
  }
  // Dump input content to stdout (debug)
  {
    std::ifstream fin(input_path);
    if (fin) {
      std::cout << "[INPUT_BEGIN]" << std::endl;
      std::string line;
      while (std::getline(fin, line)) {
        std::cout << line << std::endl;
      }
      std::cout << "[INPUT_END]" << std::endl;
    } else {
      std::cerr << "[worker] Unable to open input file to print: " << input_path << std::endl;
    }
  }
  // If we have a binary_uri, download and use it; else use embedded /usr/local/bin/map
  std::string mapper = "/usr/local/bin/map";
  std::string downloaded;
  if (!binary_uri.empty() && ensure_mapper_binary(binary_uri, downloaded)) mapper = downloaded;
  std::string cmd = mapper + " < " + input_path;
  std::cerr << "[worker] MAP exec: " << cmd << std::endl;
  FILE* pipe = popen(cmd.c_str(), "r");
  if (!pipe) {
    std::cerr << "[worker] Failed to run mapper" << std::endl;
    return;
  }
  // Simple partition counters and buffers for demo
  int R = std::max(1, n_reducers);
  std::vector<size_t> counts(R, 0);
  std::vector<std::ostringstream> parts(R);
  char* line = nullptr; size_t len = 0; ssize_t nread;
  std::cout << "[MAPPER_OUT_BEGIN]" << std::endl;
  while (true) {
    char buf[4096];
    if (!fgets(buf, sizeof(buf), pipe)) break;
    std::string s(buf);
    // echo mapper output
    if (!s.empty() && s.back() == '\n') s.pop_back();
    std::cout << s << std::endl;
    // restore newline for parsing if needed
    s.push_back('\n');
    // Parse key\tvalue
    auto tab = s.find('\t');
    if (tab == std::string::npos) continue;
    std::string key = s.substr(0, tab);
    std::string val = s.substr(tab+1);
    // partition by hash
    std::hash<std::string> h;
    int pid = static_cast<int>(h(key) % R);
    counts[pid]++;
    parts[pid] << key << '\t' << val;
  }
  std::cout << "[MAPPER_OUT_END]" << std::endl;
  pclose(pipe);
  // Write per-partition files and log
  for (int i = 0; i < R; ++i) {
    std::string outPath = "/tmp/to-reduce-input-" + std::to_string(i) + ".txt";
    std::ofstream ofs(outPath, std::ios::binary);
    std::string data = parts[i].str();
    ofs.write(data.data(), (std::streamsize)data.size());
    ofs.close();
    std::cerr << "[worker] partition " << i << " records: " << counts[i] << std::endl;
  }
}

static bool upload_file_to_minio(const std::string& local_path, const std::string& s3_uri){
  // Simple upload via awscli to MinIO
  std::string endpoint = envOr("MINIO_ENDPOINT", "http://minio:9000");
  std::string access = envOr("MINIO_ACCESS_KEY", "minioadmin");
  std::string secret = envOr("MINIO_SECRET_KEY", "minioadmin");
  std::string cmd = std::string("AWS_ACCESS_KEY_ID=") + access +
                    " AWS_SECRET_ACCESS_KEY=" + secret +
                    " aws --no-cli-pager --endpoint-url " + endpoint +
                    " s3 cp '" + local_path + "' '" + s3_uri + "'";
  std::cerr << "[worker] awscli upload: " << cmd << std::endl;
  int rc = std::system(cmd.c_str());
  return rc == 0;
}

class WorkerClient {
 public:
  explicit WorkerClient(std::shared_ptr<Channel> channel)
      : stub_(ControlService::NewStub(channel)) {}

  void Run() {
    ClientContext ctx;
    std::shared_ptr<ClientReaderWriter<WorkerToMaster, MasterToWorker>> stream(
        stub_->WorkerStream(&ctx));

    // Send initial info
    WorkerToMaster hello;
    WorkerInfo* info = hello.mutable_info();
    info->set_worker_id(envOr("HOSTNAME", "worker-1"));
    info->set_host(envOr("HOSTNAME", "worker"));
    info->set_cpu(1);
    stream->Write(hello);

    MasterToWorker msg;
    while (stream->Read(&msg)) {
      std::cerr << "[worker] Received message from master\n";
      if (msg.has_assign()) {
        const AssignTask& t = msg.assign();
        std::cout << "[worker] Received task " << t.task_id() << " type=" << t.type() << std::endl;
        if (t.type() == AssignTask::MAP && t.split_uris_size() > 0) {
          for (int i = 0; i < t.split_uris_size(); ++i) {
            do_map(t.split_uris(i), t.binary_uri(), t.reducer_id(), t.n_reducers());
          }
          // After mapping, upload partition files and notify master
          int R = std::max(1, t.n_reducers());
          for (int pid = 0; pid < R; ++pid) {
            std::string local = "/tmp/to-reduce-input-" + std::to_string(pid) + ".txt";
            // Build s3 path: s3://<bucket>/intermediate/<jobid>/part-<pid>-<taskid>.txt
            // For MVP, derive bucket from the input split URI bucket
            std::string split = t.split_uris(0);
            S3Loc loc; if (!parse_s3_uri(split, loc)) loc.bucket = "gridmr";
            std::string dest = std::string("s3://") + loc.bucket + "/intermediate/" + t.job_id() + "/part-" + std::to_string(pid) + "-" + t.task_id() + ".txt";
            if (upload_file_to_minio(local, dest)) {
              WorkerToMaster partMsg;
              auto *p = partMsg.mutable_part();
              p->set_job_id(t.job_id());
              // try to parse map id from task_id like "map-<id>"
              int mid = 0; try { mid = std::stoi(std::string(t.task_id()).substr(4)); } catch (...) {}
              p->set_map_id(mid);
              p->set_partition_id(pid);
              p->set_uri(dest);
              stream->Write(partMsg);
            } else {
              std::cerr << "[worker] upload failed for partition " << pid << std::endl;
            }
          }
          WorkerToMaster statusMsg;
          TaskStatus* st = statusMsg.mutable_status();
          st->set_task_id(t.task_id());
          st->set_state(TaskStatus::COMPLETED);
          st->set_progress(100);
          st->set_message("done");
          stream->Write(statusMsg);
        }
        if (t.type() == AssignTask::REDUCE && t.split_uris_size() > 0) {
          // For now, only download inputs to verify integrity
          for (int i = 0; i < t.split_uris_size(); ++i) {
            std::string in = t.split_uris(i);
            std::string dest = std::string("/tmp/reduce-input-") + std::to_string(i) + ".txt";
            if (!s3_download_to_file(in, dest)) {
              std::cerr << "[worker] reduce input download failed: " << in << std::endl;
            } else {
              std::cerr << "[worker] reduce input downloaded: " << dest << std::endl;
            }
          }
          WorkerToMaster statusMsg;
          TaskStatus* st = statusMsg.mutable_status();
          st->set_task_id(t.task_id());
          st->set_state(TaskStatus::COMPLETED);
          st->set_progress(100);
          st->set_message("reduce fetched inputs");
          stream->Write(statusMsg);
        }
      }
    }

    Status s = stream->Finish();
    if (!s.ok()) {
      std::cerr << "Stream finished with error: " << s.error_message() << std::endl;
    }
  }

 private:
  std::unique_ptr<ControlService::Stub> stub_;
};

int main(int argc, char** argv) {
  std::string host = envOr("MASTER_HOST", "localhost");
  std::string port = envOr("MASTER_PORT", "50051");
  std::string target = host + ":" + port;
  auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  // wait 4 master
  int attempts = 0;
  while (true) {
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
    if (channel->WaitForConnected(deadline)) break;
    attempts++;
    std::cerr << "[worker] Waiting for master at " << target << " (attempt " << attempts << ")...\n";
    std::this_thread::sleep_for(std::chrono::seconds(std::min(5, attempts)));
  }

  WorkerClient c(channel);
  c.Run();
  return 0;
}
