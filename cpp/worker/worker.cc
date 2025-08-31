#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>
#include <thread>

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

static void do_map(const std::string& s3_uri, int reducer_id, int n_reducers) {
  // Placeholder: here you'd read from MinIO (S3 API) and produce partitions
  std::cout << "[worker] MAP reading " << s3_uri << ", reducers=" << n_reducers << "\n";
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
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
      if (msg.has_assign()) {
        const AssignTask& t = msg.assign();
        std::cout << "[worker] Received task " << t.task_id() << " type=" << t.type() << "\n";
        if (t.type() == AssignTask::MAP && t.split_uris_size() > 0) {
          do_map(t.split_uris(0), t.reducer_id(), t.n_reducers());
          WorkerToMaster statusMsg;
          TaskStatus* st = statusMsg.mutable_status();
          st->set_task_id(t.task_id());
          st->set_state(TaskStatus::COMPLETED);
          st->set_progress(100);
          st->set_message("done");
          stream->Write(statusMsg);
        }
      }
    }

    Status s = stream->Finish();
    if (!s.ok()) {
      std::cerr << "Stream finished with error: " << s.error_message() << "\n";
    }
  }

 private:
  std::unique_ptr<ControlService::Stub> stub_;
};

int main(int argc, char** argv) {
  std::string host = envOr("MASTER_HOST", "localhost");
  std::string port = envOr("MASTER_PORT", "50051");
  std::string target = host + ":" + port;
  WorkerClient c(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
  c.Run();
  return 0;
}
