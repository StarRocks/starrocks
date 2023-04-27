#include <gperftools/malloc_extension.h>
#include <unistd.h>

#if defined(LEAK_SANITIZER)
#include <sanitizer/lsan_interface.h>
#endif

#include "backend_service.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/pipeline/query_context.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "service/brpc.h"
#include "service/service.h"
#include "service/service_be/http_service.h"
#include "service/service_be/internal_service.h"
#ifdef USE_STAROS
#include "service/service_be/lake_service.h"
#endif
#include "storage/storage_engine.h"
#include "util/logging.h"
#include "util/thrift_server.h"

namespace brpc {

DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);

} // namespace brpc

void start_be() {
    using namespace starrocks;

    auto* exec_env = ExecEnv::GetInstance();

    // Begin to start services
    // 1. Start thrift server with 'be_port'.
    auto thrift_server = BackendService::create<BackendService>(exec_env, config::be_port);
    if (auto status = thrift_server->start(); !status.ok()) {
        LOG(ERROR) << "Fail to start BackendService thrift server on port " << config::be_port << ": " << status;
        shutdown_logging();
        exit(1);
    }

    // 2. Start brpc services.
    brpc::FLAGS_max_body_size = config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes = config::brpc_socket_max_unwritten_bytes;
    brpc::Server brpc_server;

    BackendInternalServiceImpl<PInternalService> internal_service(exec_env);
    BackendInternalServiceImpl<doris::PBackendService> backend_service(exec_env);

    brpc_server.AddService(&internal_service, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc_server.AddService(&backend_service, brpc::SERVER_DOESNT_OWN_SERVICE);
#ifdef USE_STAROS
    LakeServiceImpl lake_service(exec_env);
    brpc_server.AddService(&lake_service, brpc::SERVER_DOESNT_OWN_SERVICE);
#endif

    brpc::ServerOptions options;
    if (config::brpc_num_threads != -1) {
        options.num_threads = config::brpc_num_threads;
    }
#ifdef USE_STAROS
    brpc_server.MaxConcurrencyOf("starrocks.lake.LakeService", "publish_version") =
            config::lake_publish_version_max_concurrency;
    brpc_server.MaxConcurrencyOf("starrocks.lake.LakeService", "publish_log_version") =
            config::lake_publish_version_max_concurrency;
    brpc_server.MaxConcurrencyOf("starrocks.lake.LakeService", "compact") = config::lake_compaction_max_concurrency;

    int max_concurrency = config::lake_tablet_writer_max_concurrency;
    brpc_server.MaxConcurrencyOf("starrocks.PInternalService", "tablet_writer_open") = max_concurrency;
    brpc_server.MaxConcurrencyOf("starrocks.PInternalService", "tablet_writer_cancel") = max_concurrency;
    brpc_server.MaxConcurrencyOf("starrocks.PInternalService", "tablet_writer_add_chunk") = max_concurrency;
    brpc_server.MaxConcurrencyOf("starrocks.PInternalService", "tablet_writer_add_chunks") = max_concurrency;
    brpc_server.MaxConcurrencyOf("starrocks.PInternalService", "tablet_writer_add_segment") = max_concurrency;

    brpc_server.MaxConcurrencyOf("doris.PBackendService", "tablet_writer_open") = max_concurrency;
    brpc_server.MaxConcurrencyOf("doris.PBackendService", "tablet_writer_cancel") = max_concurrency;
    brpc_server.MaxConcurrencyOf("doris.PBackendService", "tablet_writer_add_chunk") = max_concurrency;
    brpc_server.MaxConcurrencyOf("doris.PBackendService", "tablet_writer_add_chunks") = max_concurrency;
    brpc_server.MaxConcurrencyOf("doris.PBackendService", "tablet_writer_add_segment") = max_concurrency;
#endif
    if (brpc_server.Start(config::brpc_port, &options) != 0) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting";
        shutdown_logging();
        exit(1);
    }

    // 3. Start HTTP service.
    std::unique_ptr<HttpServiceBE> http_service =
            std::make_unique<HttpServiceBE>(exec_env, config::be_http_port, config::be_http_num_workers);
    if (auto status = http_service->start(); !status.ok()) {
        LOG(ERROR) << "Internal Error:" << status.message();
        LOG(ERROR) << "StarRocks Be http service did not start correctly, exiting";
        shutdown_logging();
        exit(1);
    }

    while (!k_starrocks_exit.load()) {
        sleep(10);
    }

    wait_for_fragments_finish(exec_env, config::loop_count_wait_fragments_finish);

    http_service.reset();

    brpc_server.Stop(0);
    brpc_server.Join();

    thrift_server->stop();
    thrift_server->join();
}
