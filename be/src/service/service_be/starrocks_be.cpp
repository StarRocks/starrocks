#include <gperftools/malloc_extension.h>
#include <unistd.h>

#include <utility>

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
#include "runtime/jdbc_driver_manager.h"
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

StorageEngine* init_storage_engine(GlobalEnv* exec_env, std::vector<StorePath> paths, bool as_cn) {
    // Init and open storage engine.
    starrocks::EngineOptions options;
    options.store_paths = std::move(paths);
    options.backend_uid = starrocks::UniqueId::gen_uid();
    options.compaction_mem_tracker = exec_env->compaction_mem_tracker();
    options.update_mem_tracker = exec_env->update_mem_tracker();
    options.need_write_cluster_id = !as_cn;
    starrocks::StorageEngine* engine = nullptr;

    auto st = starrocks::StorageEngine::open(options, &engine);
    if (!st.ok()) {
        LOG(FATAL) << "fail to open StorageEngine, res=" << st.get_error_msg();
        exit(-1);
    }

    // Start all background threads of storage engine.
    // SHOULD be called after exec env is initialized.
    EXIT_IF_ERROR(engine->start_bg_threads());
    return engine;
}

void start_be(const std::vector<StorePath>& paths, bool as_cn) {
    int start_step = 1;
    auto daemon = std::make_unique<Daemon>();
    daemon->init(as_cn, paths);
    LOG(INFO) << "BE start step " << start_step++ << ": daemon threads start successfully";

    // init jdbc driver manager
    EXIT_IF_ERROR(JDBCDriverManager::getInstance()->init(std::string(getenv("STARROCKS_HOME")) + "/lib/jdbc_drivers"));
    LOG(INFO) << "BE start step " << start_step++ << ": jdbc driver manager init successfully";

    if (!starrocks::BackendOptions::init()) {
        exit(-1);
    }
    LOG(INFO) << "BE start step " << start_step++ << ": backend network options init successfully";

    auto* global_env = GlobalEnv::GetInstance();
    EXIT_IF_ERROR(global_env->init());

    auto* storage_engine = init_storage_engine(global_env, paths, as_cn);
    LOG(INFO) << "BE start step " << start_step++ << ": storage engine init successfully";

    auto* exec_env = ExecEnv::GetInstance();
    EXIT_IF_ERROR(exec_env->init(paths, as_cn));
    LOG(INFO) << "BE start step " << start_step++ << ": exec engine init successfully";

#ifdef USE_STAROS
    init_staros_worker();
    LOG(INFO) << "BE start step" << start_step++ << ": staros worker init successfully";
#endif

    init_block_cache();
    LOG(INFO) << "BE start step " << start_step++ << ": block cache init successfully";

    // Begin to start services
    // 1. Start thrift server
    auto thrift_server = BackendService::create<BackendService>(exec_env, config::be_port);
    if (auto status = thrift_server->start(); !status.ok()) {
        LOG(ERROR) << "Fail to start BackendService thrift server on port " << config::be_port << ": " << status;
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << "BE start step " << start_step++ << ": start thrift server successfully";

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
    LOG(INFO) << "BE start step " << start_step++ << ": start brpc server successfully";

    // 3. Start HTTP server
    auto http_server = std::make_unique<HttpServiceBE>(exec_env, config::be_http_port, config::be_http_num_workers);
    if (auto status = http_server->start(); !status.ok()) {
        LOG(ERROR) << "BE http server did not start correctly, exiting: " << status.message();
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << "BE start step " << start_step++ << ": start http server successfully";

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
    if (auto status = heartbeat_server->start(); !status.ok()) {
        LOG(ERROR) << "BE heartbeat server dint not start correctlr, exiting: " << status.message();
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << "BE start step " << start_step++ << ": start heartbeat server successfully";

    LOG(INFO) << "BE start successfully";

    while (!(k_starrocks_exit.load()) && !(k_starrocks_exit_quick.load())) {
        sleep(10);
    }

    int exit_step = 1;
    exec_env->wait_for_finish();
    LOG(INFO) << "BE exit step " << exit_step++ << ": wait exec engine tasks finish successfully";

    heartbeat_server->stop();
    heartbeat_server->join();
    heartbeat_server.reset();
    LOG(INFO) << "BE exit step " << exit_step++ << ": heartbeat server exit successfully";

    http_server->stop();
    brpc_server->Stop(0);
    thrift_server->stop();

    daemon->stop();
    exec_env->stop();
    storage_engine->stop();

    http_server->join();
    LOG(INFO) << "BE exit step " << exit_step++ << ": http server exit successfully";

    brpc_server->Join();
    LOG(INFO) << "BE exit step " << exit_step++ << ": brpc server exit successfully";

    thrift_server->join();
    LOG(INFO) << "BE exit step " << exit_step++ << ": thrift server exit successfully";

    http_server.reset();
    brpc_server.reset();
    thrift_server.reset();

#ifdef USE_STAROS
    starrocks::shutdown_staros_worker();
    LOG(INFO) << "BE exit step " << exit_step++ << ": staros worker exit successfully";
#endif

#if defined(WITH_CACHELIB) || defined(WITH_STARCACHE)
    if (starrocks::config::block_cache_enable) {
        starrocks::BlockCache::instance()->shutdown();
        LOG(INFO) << "BE exit step " << exit_step++ << ": block cache shutdown successfully";
    }
#endif

    daemon.reset();
    LOG(INFO) << "BE exit step " << exit_step++ << ": daemon threads exit successfully";

    exec_env->destroy();
    LOG(INFO) << "BE exit step " << exit_step++ << ": exec engine destroy successfully";

    delete storage_engine;
    LOG(INFO) << "BE exit step " << exit_step++ << ": storage engine exit successfully";

    global_env->stop();

    LOG(INFO) << "BE exit success";
}

} // namespace starrocks