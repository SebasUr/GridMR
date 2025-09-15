#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <chrono>
#include <thread>
#include <vector>
#include <mutex>
#include <atomic>
#include <fstream>
#include <string>
#include <sstream>

#include "gridmr.pb.h"
#include "gridmr.grpc.pb.h"

#include "gridmr/worker/common/env.h"
#include "gridmr/worker/common/fs.h"
#include "gridmr/worker/common/logger.h"
#include "gridmr/worker/common/sysmetrics.h"
#include "gridmr/worker/mapreduce/mapper.h"
#include "gridmr/worker/mapreduce/reducer.h"

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

using namespace gridmr_worker;

// Use shared sysmetrics helpers
static inline float get_cpu_usage() { return sys_cpu_usage_percent(); }
static inline float get_ram_usage() { return sys_ram_usage_percent(); }

class WorkerClient {
 public:
  explicit WorkerClient(std::shared_ptr<Channel> channel)
      : stub_(ControlService::NewStub(channel)) {}

    void Run() {
    ClientContext ctx;

    // Abrimos stream bidireccional entre el worker y master.
    auto stream = std::shared_ptr<ClientReaderWriter<WorkerToMaster, MasterToWorker>>(stub_->WorkerStream(&ctx));

    // Sincronización para todas las escrituras al stream (Write no reentrante desde múltiples hilos)
    std::mutex write_mu;
    std::atomic<bool> running{true};

    /* Explicación
    * Se envia mensaje de identificación Hello con
    * worker_id
    * host
    * cpu
    */

    log_init();
    WorkerToMaster hello;
    WorkerInfo* info = hello.mutable_info();
    std::string wid;
    {
        const char* env_wid = std::getenv("WORKER_ID");
        if (env_wid && *env_wid) wid = std::string(env_wid);
        else wid = envOr("HOSTNAME", "worker-1");
    }
    log_set_worker_id(wid);
    std::string host = envOr("HOSTNAME", "worker");
    info->set_worker_id(wid);
    info->set_host(host);
    info->set_cpu(1);
    log_msg(std::string("INFO sent: worker_id=") + wid + " host=" + host + " cpu=1");
    { std::lock_guard<std::mutex> lk(write_mu); stream->Write(hello); }

    // Hilo de heartbeats periódicos para evitar expiración por falta de latidos
    std::thread hb([&]{
    while (running.load()) {
        WorkerToMaster hbmsg;
        auto* hb = hbmsg.mutable_heartbeat();
        hb->set_worker_id(wid);
        float cpu = get_cpu_usage();
        float ram = get_ram_usage();
        hb->set_cpu_usage(cpu);
        hb->set_ram_usage(ram);
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        hb->set_timestamp(static_cast<long long>(now_ms));
        log_msg(std::string("HB sent: cpu=") + std::to_string(cpu) + "% ram=" + std::to_string(ram) + "% ts=" + std::to_string(now_ms));
        { std::lock_guard<std::mutex> lk(write_mu); stream->Write(hbmsg); }
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    }
    });

    // El worker escucha en loop tareas asignadas por el master.
    MasterToWorker msg;
    while (stream->Read(&msg)) {

        // Si tiene asignación
        if (msg.has_assign()) {

            // Mensaje tipo AssignTask (definido en el proto)
            const AssignTask& t = msg.assign();
            log_msg(std::string("Received task ") + t.task_id() + " type=" + std::to_string(t.type()));

            // Si la tarea es MAP y tiene splits
            if (t.type() == AssignTask::MAP && t.split_uris_size() > 0) {

                // Por cada split llamamos a do_map (mapper.cc)
                for (int i = 0; i < t.split_uris_size(); ++i) {
                    // Pasamos uris. Contenido de t, definido en gridmr.proto
                    do_map(t.split_uris(i), t.binary_uri(), t.reducer_id(), t.n_reducers());
                }

                // Creamos los archivos locales para cada split
                int R = std::max(1, t.n_reducers());
                for (int pid = 0; pid < R; ++pid) {
                std::string local = "/tmp/to-reduce-input-" + std::to_string(pid) + ".txt";
                std::string root = envOr("SHARED_DATA_ROOT", "/shared");
                std::string dest = root + std::string("/intermediate/") + t.job_id() + "/part-" + std::to_string(pid) + "-" + t.task_id() + ".txt";

                // Copiamos a shared FS y notificamos al master
                if (upload_file_to_fs(local, dest)) {
                    WorkerToMaster partMsg;
                    auto *p = partMsg.mutable_part();
                    p->set_job_id(t.job_id());
                    int mid = 0; try { mid = std::stoi(std::string(t.task_id()).substr(4)); } catch (...) {}
                    p->set_map_id(mid);
                    p->set_partition_id(pid);
                    p->set_uri(dest);
                    { std::lock_guard<std::mutex> lk(write_mu); stream->Write(partMsg); }
                    }   
                }

                // Finalmente preparamos un mensaje de tarea completada
                WorkerToMaster statusMsg;
                TaskStatus* st = statusMsg.mutable_status();
                st->set_task_id(t.task_id());
                st->set_state(TaskStatus::COMPLETED);
                st->set_progress(100);
                st->set_message("done");
                { std::lock_guard<std::mutex> lk(write_mu); stream->Write(statusMsg); }
                log_msg(std::string("Completed MAP task ") + t.task_id());
            }

            // En el caso de que la tarea sea REDUCE
            if (t.type() == AssignTask::REDUCE && t.split_uris_size() > 0) {

                // Copiamos los splits
                for (int i = 0; i < t.split_uris_size(); ++i) {
                    std::string in = t.split_uris(i);
                    std::string dest = std::string("/tmp/reduce-input-") + std::to_string(i) + ".txt";
                    download_url_to_file(in, dest);
                }

                // Ejecutamos el reducer y se recoge la URI del resultado
                std::string resultUri = do_reduce_collect_output(t.binary_uri(), t.split_uris_size(), t.reducer_id(), t.job_id(), t.split_uris(0));
                WorkerToMaster statusMsg;
                TaskStatus* st = statusMsg.mutable_status();
                st->set_task_id(t.task_id());
                st->set_state(TaskStatus::COMPLETED);
                st->set_progress(100);
                st->set_message(resultUri.empty()?"reduce_upload_failed":std::string("result_uri=")+resultUri);
                { std::lock_guard<std::mutex> lk(write_mu); stream->Write(statusMsg); }
                log_msg(std::string("Completed REDUCE task ") + t.task_id());
            }
        }
    }

    // Finalizamos el stream
    running.store(false);
    if (hb.joinable()) hb.join();
    Status s = stream->Finish();
    if (!s.ok()) log_msg(std::string("Stream finished with error: ") + s.error_message());
    }

 private:
  std::unique_ptr<ControlService::Stub> stub_;
};

int main(){

    // Buscamos el host y el puerto del master y se crea un canal gRPC.
    std::string host = envOr("MASTER_HOST", "localhost");
    std::string port = envOr("MASTER_PORT", "50051");
    std::string target = host + ":" + port;
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());

    // Intentamos conectarnos al canal gRPC, no arranca hasta que el master esté listo.
    int attempts = 0;
    while (true) {
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
        if (channel->WaitForConnected(deadline)) break;
        attempts++;
    log_msg(std::string("Waiting for master at ") + target + " (attempt " + std::to_string(attempts) + ")...");
        std::this_thread::sleep_for(std::chrono::seconds(std::min(5, attempts)));
    }

    // Creamos un objeto WorkerClient con el canal gRPC.
    WorkerClient c(channel);
    c.Run(); // Ciclo principal de vida del worker.
    return 0;
}
