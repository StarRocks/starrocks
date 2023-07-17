#include <gperftools/malloc_extension.h>
#include <unistd.h>

#if defined(LEAK_SANITIZER)
#include <sanitizer/lsan_interface.h>
#endif

#include "agent/heartbeat_server.h"
#include "backend_service.h"
#include "block_cache/block_cache.h"
#include "block_cache/kv_cache.h"
#include "common/config.h"
#include "common/daemon.h"
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
#include "service/staros_worker.h"
#include "storage/storage_engine.h"
#include "util/logging.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_server.h"

namespace brpc {

DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);

} // namespace brpc

namespace starrocks {

void init_block_cache() {
#if !defined(WITH_CACHELIB) && !defined(WITH_STARCACHE)
    if (config::block_cache_enable) {
        config::block_cache_enable = false;
    }
#endif

    if (config::block_cache_enable) {
        BlockCache* cache = BlockCache::instance();
        starrocks::CacheOptions cache_options;
        cache_options.mem_space_size = config::block_cache_mem_size;

        std::vector<std::string> paths;
        auto parse_res = parse_conf_block_cache_paths(config::block_cache_disk_path, &paths);
        if (!parse_res.ok()) {
            LOG(FATAL) << "parse config block cache disk path failed, path=" << config::block_cache_disk_path;
            exit(-1);
        }
        for (auto& p : paths) {
            cache_options.disk_spaces.push_back(
                    {.path = p, .size = static_cast<size_t>(config::block_cache_disk_size)});
        }

        // Adjust the default engine based on build switches.
        if (config::block_cache_engine == "") {
#if defined(WITH_STARCACHE)
            config::block_cache_engine = "starcache";
#else
            config::block_cache_engine = "cachelib";
#endif
        }
        cache_options.meta_path = config::block_cache_meta_path;
        cache_options.block_size = config::block_cache_block_size;
        cache_options.checksum = config::block_cache_checksum_enable;
        cache_options.max_parcel_memory_mb = config::block_cache_max_parcel_memory_mb;
        cache_options.max_concurrent_inserts = config::block_cache_max_concurrent_inserts;
        cache_options.lru_insertion_point = config::block_cache_lru_insertion_point;
        cache_options.engine = config::block_cache_engine;
        EXIT_IF_ERROR(cache->init(cache_options));
    }
}

void start_be(ExecEnv* exec_env, StorageEngine* storage_engine, Daemon* deamon) {
#ifdef USE_STAROS
    init_staros_worker();
#endif

    init_block_cache();

    // Begin to start services
    // 1. Start thrift server
    auto thrift_server = BackendService::create<BackendService>(exec_env, config::be_port);
    if (auto status = thrift_server->start(); !status.ok()) {
        LOG(ERROR) << "Fail to start BackendService thrift server on port " << config::be_port << ": " << status;
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << "BE start thrift server success";

    // 2. Start brpc server
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
    if (auto ret = brpc_server->Start(config::brpc_port, &options); ret != 0) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting errcode: " << ret;
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << "BE start brpc server success";

    // 3. Start HTTP server
    auto http_server = std::make_unique<HttpServiceBE>(exec_env, config::be_http_port, config::be_http_num_workers);
    if (auto status = http_server->start(); !status.ok()) {
        LOG(ERROR) << "BE http server did not start correctly, exiting: " << status.message();
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << "BE start http server success";

    // 4. Start heartbeat server
    std::unique_ptr<ThriftServer> heartbeat_server;
    ThriftRpcHelper::setup(exec_env);
    if (auto ret = create_heartbeat_server(exec_env, config::heartbeat_service_port,
                                           config::heartbeat_service_thread_count);
        !ret.ok()) {
        LOG(ERROR) << "BE heartbeat server did not start correctly, exiting: " << ret.status().message();
        shutdown_logging();
        exit(1);
    } else {
        heartbeat_server = std::move(ret.value());
    }
    if (auto status = heartbeat_server->start(); status.ok()) {
        LOG(ERROR) << "BE heartbeat server dint not start correctlr, exiting: " << status.message();
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << "BE start heartbeat server success";

    LOG(INFO) << "BE started successfully";

    while (!(k_starrocks_exit.load()) && !(k_starrocks_exit_quick.load())) {
        sleep(10);
    }

    exec_env->wait_for_finish();

    heartbeat_server->stop();
    heartbeat_server->join();
    heartbeat_server.reset();
    LOG(INFO) << "BE heartbeat server exit success";

    http_server->stop();
    brpc_server->Stop(0);
    thrift_server->stop();

    deamon->stop();
    exec_env->stop();
    storage_engine->stop();

    http_server->join();
    LOG(INFO) << "BE http server exit success";
    brpc_server->Join();
    LOG(INFO) << "BE brpc server exit success";
    thrift_server->join();
    LOG(INFO) << "BE thrift server exit success";

    http_server.reset();
    brpc_server.reset();
    thrift_server.reset();

#ifdef USE_STAROS
    starrocks::shutdown_staros_worker();
#endif

#if defined(WITH_CACHELIB) || defined(WITH_STARCACHE)
    if (starrocks::config::block_cache_enable) {
        starrocks::BlockCache::instance()->shutdown();
    }
#endif
    exec_env->destroy();
    LOG(INFO) << "BE exec env destroy success";
}

} // namespace starrocks