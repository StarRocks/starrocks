// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "butil/file_util.h"
#include "column/column_helper.h"
#include "column/column_pool.h"
#include "common/config.h"
#include "exprs/bitmap_function.h"
#include "exprs/cast_functions.h"
#include "exprs/compound_predicate.h"
#include "exprs/decimal_operators.h"
#include "exprs/decimalv2_operators.h"
#include "exprs/encryption_functions.h"
#include "exprs/es_functions.h"
#include "exprs/grouping_sets_functions.h"
#include "exprs/hash_functions.h"
#include "exprs/hll_function.h"
#include "exprs/hll_hash_function.h"
#include "exprs/is_null_predicate.h"
#include "exprs/json_functions.h"
#include "exprs/like_predicate.h"
#include "exprs/math_functions.h"
#include "exprs/new_in_predicate.h"
#include "exprs/operators.h"
#include "exprs/percentile_function.h"
#include "exprs/string_functions.h"
#include "exprs/time_operators.h"
#include "exprs/timestamp_functions.h"
#include "exprs/utility_functions.h"
#include "geo/geo_functions.h"
#include "gtest/gtest.h"
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/chunk_allocator.h"
#include "runtime/user_function_cache.h"
#include "runtime/vectorized/time_types.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/mem_info.h"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
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
    starrocks::Operators::init();
    starrocks::IsNullPredicate::init();
    starrocks::LikePredicate::init();
    starrocks::StringFunctions::init();
    starrocks::CastFunctions::init();
    starrocks::InPredicate::init();
    starrocks::MathFunctions::init();
    starrocks::EncryptionFunctions::init();
    starrocks::TimestampFunctions::init();
    starrocks::DecimalOperators::init();
    starrocks::DecimalV2Operators::init();
    starrocks::TimeOperators::init();
    starrocks::UtilityFunctions::init();
    starrocks::CompoundPredicate::init();
    starrocks::JsonFunctions::init();
    starrocks::HllHashFunctions::init();
    starrocks::ESFunctions::init();
    starrocks::GeoFunctions::init();
    starrocks::GroupingSetsFunctions::init();
    starrocks::BitmapFunctions::init();
    starrocks::HllFunctions::init();
    starrocks::HashFunctions::init();
    starrocks::PercentileFunctions::init();

    starrocks::vectorized::ColumnHelper::init_static_variable();
    starrocks::vectorized::date::init_date_cache();

    starrocks::ChunkAllocator::init_instance(starrocks::config::chunk_reserved_bytes_limit);

    std::vector<starrocks::StorePath> paths;
    paths.emplace_back(starrocks::config::storage_root_path, -1);

    std::unique_ptr<starrocks::MemTracker> table_meta_mem_tracker = std::make_unique<starrocks::MemTracker>();
    std::unique_ptr<starrocks::MemTracker> schema_change_mem_tracker = std::make_unique<starrocks::MemTracker>();
    std::unique_ptr<starrocks::MemTracker> compaction_mem_tracker = std::make_unique<starrocks::MemTracker>();
    std::unique_ptr<starrocks::MemTracker> update_mem_tracker = std::make_unique<starrocks::MemTracker>();
    starrocks::StorageEngine* engine = nullptr;
    starrocks::EngineOptions options;
    options.store_paths = paths;
    options.tablet_meta_mem_tracker = table_meta_mem_tracker.get();
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
    int r = RUN_ALL_TESTS();
    // clear some trash objects kept in tablet_manager so mem_tracker checks will not fail
    starrocks::StorageEngine::instance()->tablet_manager()->start_trash_sweep();
    // clear caches in update manager so mem_tracker checks will not fail
    starrocks::StorageEngine::instance()->update_manager()->clear_cache();
    (void)butil::DeleteFile(storage_root, true);
    starrocks::vectorized::TEST_clear_all_columns_this_thread();

    return r;
}
