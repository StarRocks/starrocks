// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "butil/file_util.h"
#include "column/column_helper.h"
#include "column/column_pool.h"
#include "common/config.h"
#include "exec/pipeline/query_context.h"
#include "gtest/gtest.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/chunk_allocator.h"
#include "runtime/time_types.h"
#include "runtime/user_function_cache.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/timezone_utils.h"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    if (getenv("STARROCKS_HOME") == nullptr) {
        fprintf(stderr, "you need set STARROCKS_HOME environment variable.\n");
        exit(-1);
    }
    std::string conffile = std::string(getenv("STARROCKS_HOME")) + "/conf/be.conf";
    if (!starrocks::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    butil::FilePath curr_dir(std::filesystem::current_path());
    butil::FilePath storage_root;
    CHECK(butil::CreateNewTempDirectory("tmp_ut_", &storage_root));
    starrocks::config::storage_root_path = storage_root.value();

    starrocks::init_glog("be_test", true);
    starrocks::CpuInfo::init();
    starrocks::DiskInfo::init();
    starrocks::MemInfo::init();
    starrocks::UserFunctionCache::instance()->init(starrocks::config::user_function_dir);

    starrocks::vectorized::date::init_date_cache();
    starrocks::TimezoneUtils::init_time_zones();

    std::vector<starrocks::StorePath> paths;
    paths.emplace_back(starrocks::config::storage_root_path);

    std::unique_ptr<starrocks::MemTracker> metadata_mem_tracker = std::make_unique<starrocks::MemTracker>();
    std::unique_ptr<starrocks::MemTracker> schema_change_mem_tracker = std::make_unique<starrocks::MemTracker>();
    std::unique_ptr<starrocks::MemTracker> compaction_mem_tracker = std::make_unique<starrocks::MemTracker>();
    std::unique_ptr<starrocks::MemTracker> update_mem_tracker = std::make_unique<starrocks::MemTracker>();
    starrocks::StorageEngine* engine = nullptr;
    starrocks::EngineOptions options;
    options.store_paths = paths;
    options.metadata_mem_tracker = metadata_mem_tracker.get();
    options.schema_change_mem_tracker = schema_change_mem_tracker.get();
    options.compaction_mem_tracker = compaction_mem_tracker.get();
    options.update_mem_tracker = update_mem_tracker.get();
    starrocks::Status s = starrocks::StorageEngine::open(options, &engine);
    if (!s.ok()) {
        butil::DeleteFile(storage_root, true);
        fprintf(stderr, "storage engine open failed, path=%s, msg=%s\n", starrocks::config::storage_root_path.c_str(),
                s.to_string().c_str());
        return -1;
    }
    auto* exec_env = starrocks::ExecEnv::GetInstance();
    exec_env->init_mem_tracker();
    starrocks::ExecEnv::init(exec_env, paths);

    int r = RUN_ALL_TESTS();

    // clear some trash objects kept in tablet_manager so mem_tracker checks will not fail
    starrocks::StorageEngine::instance()->tablet_manager()->start_trash_sweep();
    (void)butil::DeleteFile(storage_root, true);
    starrocks::vectorized::TEST_clear_all_columns_this_thread();
    // delete engine
    engine->stop();
    delete engine;
    exec_env->set_storage_engine(nullptr);
    // destroy exec env
    starrocks::tls_thread_status.set_mem_tracker(nullptr);
    starrocks::ExecEnv::destroy(exec_env);

    return r;
}
