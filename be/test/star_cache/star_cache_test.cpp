// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/star_cache.h"

#include <gtest/gtest.h>
#include <cstring>
#include <butil/fast_rand.h>
#include "common/logging.h"
#include "common/statusor.h"
#include "util/logging.h"
#include "fs/fs_util.h"
#include "star_cache/types.h"
#include "star_cache/common/config.h"
#include "star_cache/sharded_lock_manager.h"

namespace starrocks::starcache {

IOBuf gen_iobuf(size_t size, char ch) {
    IOBuf buf;
    buf.resize(size, ch);
    return buf;
}

class StarCacheTest : public ::testing::Test {
protected:
    void SetUp() override { ASSERT_TRUE(fs::create_directories("./ut_dir/star_disk_cache").ok()); }
    void TearDown() override { /*ASSERT_TRUE(fs::remove_all("./ut_dir").ok());*/ }
};

TEST_F(StarCacheTest, hybrid_cache) {
    std::unique_ptr<StarCache> cache(new StarCache);

    CacheOptions options;
    options.mem_quota_bytes = 20 * 1024 * 1024;
    size_t quota = 500 * 1024 * 1024;
    options.disk_dir_spaces.push_back({.path = "./ut_dir/star_disk_cache", .quota_bytes = quota});
    Status status = cache->init(options);
    ASSERT_TRUE(status.ok());

    const size_t obj_size = 4 * config::FLAGS_block_size + 123;
    const size_t rounds = 10;
    const std::string cache_key = "test_file";

    // write cache
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        IOBuf buf = gen_iobuf(obj_size, ch);
        Status st = cache->set(cache_key + std::to_string(i), buf);
        ASSERT_TRUE(st.ok()) << st.get_error_msg();
    }

    // get cache
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        IOBuf expect_buf = gen_iobuf(obj_size, ch);
        IOBuf buf;
        Status st = cache->get(cache_key + std::to_string(i), &buf);
        ASSERT_TRUE(st.ok()) << st.get_error_msg();
        ASSERT_EQ(buf, expect_buf);
    }

    // read cache
    size_t batch_size = 2 * config::FLAGS_block_size;
    off_t offset = config::FLAGS_block_size;
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        IOBuf expect_buf = gen_iobuf(batch_size, ch);
        IOBuf buf;
        Status st = cache->read(cache_key + std::to_string(i), offset, batch_size, &buf);
        ASSERT_TRUE(st.ok()) << st.get_error_msg();
        ASSERT_EQ(buf, expect_buf);
    }
    for (size_t i = 0; i < rounds; ++i) {
        off_t off = butil::fast_rand_less_than(obj_size);
        size_t size = butil::fast_rand_less_than(obj_size - off);
        LOG(INFO) << "random read, offset: " << off << ", size: " << size;
        if (size == 0) {
            continue;
        }
        char ch = 'a' + i % 26;
        IOBuf expect_buf = gen_iobuf(size, ch);
        IOBuf buf;
        Status st = cache->read(cache_key + std::to_string(i), off, size, &buf);
        ASSERT_TRUE(st.ok()) << st.get_error_msg();
        ASSERT_EQ(buf, expect_buf);
    }

    // remove cache
    std::string key_to_remove = cache_key + std::to_string(0);
    status = cache->remove(key_to_remove);
    ASSERT_TRUE(status.ok()) << status.get_error_msg();

    IOBuf buf;
    status = cache->get(key_to_remove, &buf);
    ASSERT_TRUE(status.is_not_found());
}

} // namespace starrocks::starcache

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);    
    starrocks::init_glog("starcache_test", true);

    int r = RUN_ALL_TESTS();

    //starrocks::shutdown_logging();
    return r;
}

/*
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

    auto metadata_mem_tracker = std::make_unique<starrocks::MemTracker>();
    auto tablet_schema_mem_tracker =
            std::make_unique<starrocks::MemTracker>(-1, "tablet_schema", metadata_mem_tracker.get());
    auto schema_change_mem_tracker = std::make_unique<starrocks::MemTracker>();
    auto compaction_mem_tracker = std::make_unique<starrocks::MemTracker>();
    auto update_mem_tracker = std::make_unique<starrocks::MemTracker>();
    starrocks::StorageEngine* engine = nullptr;
    starrocks::EngineOptions options;
    options.store_paths = paths;
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
    // Pagecache is turned on by default, and some test cases require cache to be turned on,
    // and some test cases do not. For easy management, we turn cache off during unit test
    // initialization. If there are test cases that require Pagecache, it must be responsible
    // for managing it.
    starrocks::config::disable_storage_page_cache = true;
    exec_env->init_mem_tracker();
    starrocks::ExecEnv::init(exec_env, paths);

    int r = RUN_ALL_TESTS();

    // clear some trash objects kept in tablet_manager so mem_tracker checks will not fail
    starrocks::StorageEngine::instance()->tablet_manager()->start_trash_sweep();
    (void)butil::DeleteFile(storage_root, true);
    starrocks::vectorized::TEST_clear_all_columns_this_thread();
    // delete engine
    starrocks::StorageEngine::instance()->stop();
    // destroy exec env
    starrocks::tls_thread_status.set_mem_tracker(nullptr);
    starrocks::ExecEnv::destroy(exec_env);

    starrocks::shutdown_logging();

    return r;
}
*/
