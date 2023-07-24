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
#include "service/service_be/lake_service.h"
#include "storage/storage_engine.h"
#include "util/logging.h"
#include "util/thrift_server.h"

namespace brpc {

DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);

} // namespace brpc

namespace starrocks {

void start_be() {
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
    auto brpc_server = std::make_unique<brpc::Server>();

    BackendInternalServiceImpl<PInternalService> internal_service(exec_env);
    BackendInternalServiceImpl<doris::PBackendService> backend_service(exec_env);
    LakeServiceImpl lake_service(exec_env);

    brpc_server->AddService(&internal_service, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc_server->AddService(&backend_service, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc_server->AddService(&lake_service, brpc::SERVER_DOESNT_OWN_SERVICE);

    brpc::ServerOptions options;
    if (config::brpc_num_threads != -1) {
        options.num_threads = config::brpc_num_threads;
    }
    if (brpc_server->Start(config::brpc_port, &options) != 0) {
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

    LOG(INFO) << "BE started successfully";

    while (!(k_starrocks_exit.load()) && !(k_starrocks_exit_quick.load())) {
        sleep(10);
    }

    exec_env->wait_for_finish();

    http_service->stop();
    brpc_server->Stop(0);
    thrift_server->stop();

    http_service->join();
    brpc_server->Join();
    thrift_server->join();

    http_service.reset();
    brpc_server.reset();
    thrift_server.reset();
}
} // namespace starrocks
