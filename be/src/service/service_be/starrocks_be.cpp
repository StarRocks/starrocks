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

void start_be() {
    using starrocks::BackendService;

    auto* exec_env = starrocks::ExecEnv::GetInstance();

<<<<<<< HEAD
    // Begin to start services
    // 1. Start thrift server with 'be_port'.
    auto thrift_server = BackendService::create<BackendService>(exec_env, starrocks::config::be_port);
=======
    if (config::block_cache_enable) {
        BlockCache* cache = BlockCache::instance();
        CacheOptions cache_options;
        cache_options.mem_space_size = config::block_cache_mem_size;

        std::vector<std::string> paths;
        EXIT_IF_ERROR(parse_conf_block_cache_paths(config::block_cache_disk_path, &paths));

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
        cache_options.max_parcel_memory_mb = config::block_cache_max_parcel_memory_mb;
        cache_options.max_concurrent_inserts = config::block_cache_max_concurrent_inserts;
        cache_options.lru_insertion_point = config::block_cache_lru_insertion_point;
        cache_options.enable_checksum = config::block_cache_checksum_enable;
        cache_options.enable_direct_io = config::block_cache_direct_io_enable;
        cache_options.engine = config::block_cache_engine;
        EXIT_IF_ERROR(cache->init(cache_options));
    }
}

StorageEngine* init_storage_engine(GlobalEnv* global_env, std::vector<StorePath> paths, bool as_cn) {
    // Init and open storage engine.
    EngineOptions options;
    options.store_paths = std::move(paths);
    options.backend_uid = UniqueId::gen_uid();
    options.compaction_mem_tracker = global_env->compaction_mem_tracker();
    options.update_mem_tracker = global_env->update_mem_tracker();
    options.need_write_cluster_id = !as_cn;
    StorageEngine* engine = nullptr;

    EXIT_IF_ERROR(StorageEngine::open(options, &engine));

    return engine;
}

void start_be(const std::vector<StorePath>& paths, bool as_cn) {
    std::string process_name = as_cn ? "CN" : "BE";

    int start_step = 1;

    auto daemon = std::make_unique<Daemon>();
    daemon->init(as_cn, paths);
    LOG(INFO) << process_name << " start step " << start_step++ << ": daemon threads start successfully";

    // init jdbc driver manager
    EXIT_IF_ERROR(JDBCDriverManager::getInstance()->init(std::string(getenv("STARROCKS_HOME")) + "/lib/jdbc_drivers"));
    LOG(INFO) << process_name << " start step " << start_step++ << ": jdbc driver manager init successfully";

    // init network option
    if (!BackendOptions::init()) {
        exit(-1);
    }
    LOG(INFO) << process_name << " start step " << start_step++ << ": backend network options init successfully";

    // init global env
    auto* global_env = GlobalEnv::GetInstance();
    EXIT_IF_ERROR(global_env->init());
    LOG(INFO) << process_name << " start step " << start_step++ << ": global env init successfully";

    auto* storage_engine = init_storage_engine(global_env, paths, as_cn);
    LOG(INFO) << process_name << " start step " << start_step++ << ": storage engine init successfully";

    auto* exec_env = ExecEnv::GetInstance();
    EXIT_IF_ERROR(exec_env->init(paths, as_cn));
    LOG(INFO) << process_name << " start step " << start_step++ << ": exec engine init successfully";

    // Start all background threads of storage engine.
    // SHOULD be called after exec env is initialized.
    EXIT_IF_ERROR(storage_engine->start_bg_threads());
    LOG(INFO) << process_name << " start step " << start_step++ << ": storage engine start bg threads successfully";

#ifdef USE_STAROS
    init_staros_worker();
    LOG(INFO) << process_name << " start step" << start_step++ << ": staros worker init successfully";
#endif

    init_block_cache();
    LOG(INFO) << process_name << " start step " << start_step++ << ": block cache init successfully";

    // Start thrift server
    auto thrift_server = BackendService::create<BackendService>(exec_env, config::be_port);
>>>>>>> dfd1f52e80 ([Enhancement] print "start cn succ" when it is started as cn  (#30020))
    if (auto status = thrift_server->start(); !status.ok()) {
        LOG(ERROR) << "Fail to start BackendService thrift server on port " << starrocks::config::be_port << ": "
                   << status;
        starrocks::shutdown_logging();
        exit(1);
    }
<<<<<<< HEAD
=======
    LOG(INFO) << process_name << " start step " << start_step++ << ": start thrift server successfully";
>>>>>>> dfd1f52e80 ([Enhancement] print "start cn succ" when it is started as cn  (#30020))

    // 2. Start brpc services.
    brpc::FLAGS_max_body_size = starrocks::config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes = starrocks::config::brpc_socket_max_unwritten_bytes;
    brpc::Server brpc_server;

    starrocks::BackendInternalServiceImpl<starrocks::PInternalService> internal_service(exec_env);
    starrocks::BackendInternalServiceImpl<doris::PBackendService> backend_service(exec_env);
    starrocks::LakeServiceImpl lake_service(exec_env, exec_env->lake_tablet_manager());

    brpc_server.AddService(&internal_service, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc_server.AddService(&backend_service, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc_server.AddService(&lake_service, brpc::SERVER_DOESNT_OWN_SERVICE);

    brpc::ServerOptions options;
    if (starrocks::config::brpc_num_threads != -1) {
        options.num_threads = starrocks::config::brpc_num_threads;
    }
    const auto lake_service_max_concurrency = starrocks::config::lake_service_max_concurrency;
    const auto service_name = "starrocks.lake.LakeService";
    const auto methods = {"abort_txn",           "abort_compaction", "compact",          "drop_table",
                          "delete_data",         "delete_tablet",    "get_tablet_stats", "publish_version",
                          "publish_log_version", "vacuum",           "vacuum_full"};
    for (auto method : methods) {
        brpc_server.MaxConcurrencyOf(service_name, method) = lake_service_max_concurrency;
    }

    if (auto ret = brpc_server.Start(starrocks::config::brpc_port, &options); ret != 0) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting errcoe: " << ret;
        starrocks::shutdown_logging();
        exit(1);
    }
<<<<<<< HEAD

    // 3. Start HTTP service.
    std::unique_ptr<starrocks::HttpServiceBE> http_service = std::make_unique<starrocks::HttpServiceBE>(
            exec_env, starrocks::config::be_http_port, starrocks::config::be_http_num_workers);
    if (auto status = http_service->start(); !status.ok()) {
        LOG(ERROR) << "Internal Error:" << status.message();
        LOG(ERROR) << "StarRocks Be http service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }
=======
    LOG(INFO) << process_name << " start step " << start_step++ << ": start brpc server successfully";

    // Start HTTP server
    auto http_server = std::make_unique<HttpServiceBE>(exec_env, config::be_http_port, config::be_http_num_workers);
    if (auto status = http_server->start(); !status.ok()) {
        LOG(ERROR) << process_name << " http server did not start correctly, exiting: " << status.message();
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << process_name << " start step " << start_step++ << ": start http server successfully";

    // Start heartbeat server
    std::unique_ptr<ThriftServer> heartbeat_server;
    ThriftRpcHelper::setup(exec_env);
    if (auto ret = create_heartbeat_server(exec_env, config::heartbeat_service_port,
                                           config::heartbeat_service_thread_count);
        !ret.ok()) {
        LOG(ERROR) << process_name << " heartbeat server did not start correctly, exiting: " << ret.status().message();
        shutdown_logging();
        exit(1);
    } else {
        heartbeat_server = std::move(ret.value());
    }
    if (auto status = heartbeat_server->start(); !status.ok()) {
        LOG(ERROR) << process_name << " heartbeat server dint not start correctlr, exiting: " << status.message();
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << process_name << " start step " << start_step++ << ": start heartbeat server successfully";
>>>>>>> dfd1f52e80 ([Enhancement] print "start cn succ" when it is started as cn  (#30020))

    LOG(INFO) << process_name << " started successfully";

    while (!(starrocks::k_starrocks_exit.load()) && !(starrocks::k_starrocks_exit_quick.load())) {
        sleep(1);
    }

    starrocks::wait_for_fragments_finish(exec_env, starrocks::config::loop_count_wait_fragments_finish);

<<<<<<< HEAD
    http_service.reset();

    brpc_server.Stop(0);
    brpc_server.Join();
=======
    exec_env->wait_for_finish();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": wait exec engine tasks finish successfully";

    heartbeat_server->stop();
    heartbeat_server->join();
    heartbeat_server.reset();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": heartbeat server exit successfully";
>>>>>>> dfd1f52e80 ([Enhancement] print "start cn succ" when it is started as cn  (#30020))

    thrift_server->stop();
<<<<<<< HEAD
    thrift_server->join();
=======

    http_server->join();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": http server exit successfully";

    brpc_server->Join();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": brpc server exit successfully";

    thrift_server->join();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": thrift server exit successfully";

    http_server.reset();
    brpc_server.reset();
    thrift_server.reset();

    daemon->stop();
    daemon.reset();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": daemon threads exit successfully";

    exec_env->stop();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": exec engine destroy successfully";

    storage_engine->stop();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": storage engine exit successfully";

#ifdef USE_STAROS
    shutdown_staros_worker();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": staros worker exit successfully";
#endif

#if defined(WITH_CACHELIB) || defined(WITH_STARCACHE)
    if (config::block_cache_enable) {
        BlockCache::instance()->shutdown();
        LOG(INFO) << process_name << " exit step " << exit_step++ << ": block cache shutdown successfully";
    }
#endif

    exec_env->destroy();
    delete storage_engine;

    // Unbind with MemTracker
    tls_mem_tracker = nullptr;

    global_env->stop();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": global env stop successfully";

    LOG(INFO) << process_name << " exited successfully";
>>>>>>> dfd1f52e80 ([Enhancement] print "start cn succ" when it is started as cn  (#30020))
}
