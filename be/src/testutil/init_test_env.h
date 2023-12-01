// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "butil/file_util.h"
#include "column/column_helper.h"
#include "column/column_pool.h"
#include "common/config.h"
#include "exec/pipeline/query_context.h"
#include "gtest/gtest.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/mem_chunk_allocator.h"
#include "runtime/time_types.h"
#include "runtime/user_function_cache.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/update_manager.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/timezone_utils.h"

namespace starrocks {

extern void shutdown_tracer();

int init_test_env(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    if (getenv("STARROCKS_HOME") == nullptr) {
        fprintf(stderr, "you need set STARROCKS_HOME environment variable.\n");
        exit(-1);
    }
    std::string conffile = std::string(getenv("STARROCKS_HOME")) + "/conf/be_test.conf";
    if (!config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    butil::FilePath curr_dir(std::filesystem::current_path());
    butil::FilePath storage_root;
    CHECK(butil::CreateNewTempDirectory("tmp_ut_", &storage_root));
    butil::FilePath spill_path = storage_root.Append("spill");
    CHECK(butil::CreateDirectory(spill_path));
    config::storage_root_path = storage_root.value();
    config::enable_event_based_compaction_framework = false;
    config::l0_snapshot_size = 1048576;
    config::storage_flood_stage_left_capacity_bytes = 10485600;
    config::spill_local_storage_dir = spill_path.value();

    FLAGS_alsologtostderr = true;
    init_glog("be_test", true);
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();
    CHECK(UserFunctionCache::instance()->init(config::user_function_dir).ok());

    date::init_date_cache();
    TimezoneUtils::init_time_zones();

    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path);

    auto metadata_mem_tracker = std::make_unique<MemTracker>();
    auto tablet_schema_mem_tracker = std::make_unique<MemTracker>(-1, "tablet_schema", metadata_mem_tracker.get());
    auto schema_change_mem_tracker = std::make_unique<MemTracker>();
    auto compaction_mem_tracker = std::make_unique<MemTracker>();
    auto update_mem_tracker = std::make_unique<MemTracker>();
    StorageEngine* engine = nullptr;
    EngineOptions options;
    options.store_paths = paths;
    options.compaction_mem_tracker = compaction_mem_tracker.get();
    options.update_mem_tracker = update_mem_tracker.get();
    Status s = StorageEngine::open(options, &engine);
    if (!s.ok()) {
        butil::DeleteFile(storage_root, true);
        fprintf(stderr, "storage engine open failed, path=%s, msg=%s\n", config::storage_root_path.c_str(),
                s.to_string().c_str());
        return -1;
    }
    auto* global_env = GlobalEnv::GetInstance();
    config::disable_storage_page_cache = true;
    auto st = global_env->init();
    CHECK(st.ok()) << st;
    auto* exec_env = ExecEnv::GetInstance();
    // Pagecache is turned on by default, and some test cases require cache to be turned on,
    // and some test cases do not. For easy management, we turn cache off during unit test
    // initialization. If there are test cases that require Pagecache, it must be responsible
    // for managing it.
    st = exec_env->init(paths);
    CHECK(st.ok()) << st;

    int r = RUN_ALL_TESTS();

    // clear some trash objects kept in tablet_manager so mem_tracker checks will not fail
    CHECK(StorageEngine::instance()->tablet_manager()->start_trash_sweep().ok());
    (void)butil::DeleteFile(storage_root, true);
    TEST_clear_all_columns_this_thread();
    // delete engine
    StorageEngine::instance()->stop();
    // destroy exec env
    tls_thread_status.set_mem_tracker(nullptr);
    exec_env->stop();
    exec_env->destroy();
    global_env->stop();

    shutdown_tracer();

    shutdown_logging();

    return r;
}

} // namespace starrocks
