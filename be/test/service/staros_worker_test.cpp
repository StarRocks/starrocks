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

#ifdef USE_STAROS
#include "service/staros_worker.h"

#include <fslib/configuration.h>
#include <fslib/fslib_all_initializer.h>
#include <gtest/gtest.h>

#include <functional>

#include "common/config.h"

namespace starrocks {

// Simulated g_starlet object
class MockStarlet {
public:
    static absl::StatusOr<staros::starlet::ShardInfo> get_shard_info(ShardId id) {
        if (_shard_map.count(id)) {
            return _shard_map[id];
        }
        return absl::NotFoundError("Shard not found");
    }

    static bool is_ready() { return true; }

    static void add_shard(const staros::starlet::ShardInfo& info) { _shard_map[info.id] = info; }

private:
    static std::map<ShardId, staros::starlet::ShardInfo> _shard_map;
};

std::map<ShardId, staros::starlet::ShardInfo> MockStarlet::_shard_map;

extern std::unique_ptr<staros::starlet::Starlet> g_starlet;

struct ScopedMockStarlet {
    ScopedMockStarlet() {
        original_starlet = std::move(g_starlet);
        g_starlet = std::make_unique<MockStarlet>();
    }

    ~ScopedMockStarlet() { g_starlet = std::move(original_starlet); }

private:
    std::unique_ptr<staros::starlet::Starlet> original_starlet;
};

static void add_shard_listener(std::vector<StarOSWorker::ShardId>* shardIds, int* counter, StarOSWorker::ShardId id) {
    shardIds->push_back(id);
    ++*counter;
}

TEST(StarOSWorkerTest, test_add_listener) {
    int counter = 0;
    std::vector<StarOSWorker::ShardId> ids;

    auto worker = std::make_unique<StarOSWorker>();

    StarOSWorker::ShardInfo info;

    EXPECT_EQ(0, counter);
    EXPECT_TRUE(ids.empty());

    info.id = 1;
    EXPECT_TRUE(worker->add_shard(info).ok());

    // no shard registered, counter and ids will not be modified
    EXPECT_EQ(0, counter);
    EXPECT_TRUE(ids.empty());

    // register the counter;
    worker->register_add_shard_listener(std::bind(&add_shard_listener, &ids, &counter, std::placeholders::_1));

    info.id = 2;
    EXPECT_TRUE(worker->add_shard(info).ok());

    // shard:2 added
    EXPECT_EQ(1, counter);
    EXPECT_EQ(1, ids.size());
    EXPECT_EQ(2, ids[0]);

    // add it again
    EXPECT_TRUE(worker->add_shard(info).ok());
    // no change, the shard:2 is already added
    EXPECT_EQ(1, counter);
    EXPECT_EQ(1, ids.size());
}

TEST(StarOSWorkerTest, test_fs_cache) {
    staros::starlet::fslib::register_builtin_filesystems();
    staros::starlet::ShardInfo shard_info;
    shard_info.id = 1;
    auto fs_info = shard_info.path_info.mutable_fs_info();
    fs_info->set_fs_type(staros::FileStoreType::S3);
    auto s3_fs_info = fs_info->mutable_s3_fs_info();
    s3_fs_info->set_bucket("test_bucket");
    s3_fs_info->set_endpoint("test_endpoint");
    s3_fs_info->set_region("us-east-1");
    auto credential = s3_fs_info->mutable_credential();
    auto simple_credential = credential->mutable_simple_credential();
    simple_credential->set_access_key("test_ak");
    simple_credential->set_access_key_secret("test_sk");
    // set full path
    shard_info.path_info.set_full_path(absl::StrFormat("s3://%s/%d/", s3_fs_info->bucket(), time(NULL)));

    // cache settings
    shard_info.cache_info.set_enable_cache(false);
    shard_info.cache_info.set_async_write_back(false);

    auto schema_or = StarOSWorker::build_scheme_from_shard_info(shard_info);
    EXPECT_TRUE(schema_or.ok());
    auto schema = schema_or.value();

    auto conf_or = shard_info.fslib_conf_from_this(false, "");
    EXPECT_TRUE(conf_or.ok());
    auto conf = conf_or.value();

    auto cache_key = StarOSWorker::get_cache_key(schema, conf);

    auto worker = std::make_shared<StarOSWorker>();
    g_worker = worker;

    EXPECT_TRUE(worker->add_shard(shard_info).ok());

    EXPECT_FALSE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->get_shard_filesystem(shard_info.id, conf).ok());

    EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->remove_shard(shard_info.id).ok());

    EXPECT_FALSE(worker->lookup_fs_cache(cache_key));
}

TEST(StarOSWorkerTest, test_build_scheme_from_shard_info) {
    staros::starlet::ShardInfo shard_info;
    shard_info.id = 1;

    // Set the file system type to GS
    auto fs_info = shard_info.path_info.mutable_fs_info();
    fs_info->set_fs_type(staros::FileStoreType::GS);

    // Call the function and verify the result
    auto scheme_or = StarOSWorker::build_scheme_from_shard_info(shard_info);
    EXPECT_TRUE(scheme_or.ok());
    EXPECT_EQ("gs://", scheme_or.value());
}

TEST(StarOSWorkerTest, test_shard_info_cache_hit) {
    ScopedMockStarlet mock_starlet_scope;

    config::starlet_shard_info_cache_capacity = 1000;
    config::starlet_shard_info_cache_ttl_sec = 300;

    auto worker = std::make_shared<StarOSWorker>();

    // Prepare a shard info
    staros::starlet::ShardInfo info;
    info.id = 12345;
    info.properties["tableId"] = "1001";
    MockStarlet::add_shard(info);

    // First call should trigger RPC
    auto result1 = worker->_fetch_shard_info_from_remote(info.id);
    ASSERT_TRUE(result1.ok());
    EXPECT_EQ(result1->id, info.id);

    // Second call should hit cache
    auto result2 = worker->_fetch_shard_info_from_remote(info.id);
    ASSERT_TRUE(result2.ok());
    EXPECT_EQ(result2->id, info.id);
}

TEST(StarOSWorkerTest, test_shard_info_cache_miss) {
    ScopedMockStarlet mock_starlet_scope;

    config::starlet_shard_info_cache_capacity = 1000;
    config::starlet_shard_info_cache_ttl_sec = 300;

    auto worker = std::make_shared<StarOSWorker>();

    ShardId id = 98765;

    // This shard does not exist in mock, so it should return error
    auto result = worker->_fetch_shard_info_from_remote(id);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), absl::StatusCode::kNotFound);
}

TEST(StarOSWorkerTest, test_shard_info_passive_ttl_expiry) {
    ScopedMockStarlet mock_starlet_scope;

    config::starlet_shard_info_cache_capacity = 1000;
    config::starlet_shard_info_cache_ttl_sec = 2; // short TTL

    auto worker = std::make_shared<StarOSWorker>();

    staros::starlet::ShardInfo info;
    info.id = 54321;
    info.properties["tableId"] = "1002";
    MockStarlet::add_shard(info);

    // First fetch: should work and populate cache
    auto result1 = worker->_fetch_shard_info_from_remote(info.id);
    ASSERT_TRUE(result1.ok());

    // Wait longer than TTL
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Second fetch: should miss due to expiry and re-fetch
    auto result2 = worker->_fetch_shard_info_from_remote(info.id);
    ASSERT_TRUE(result2.ok());
    EXPECT_EQ(result2->id, info.id);
}

TEST(StarOSWorkerTest, test_shard_info_active_ttl_cleanup) {
    ScopedMockStarlet mock_starlet_scope;

    config::starlet_shard_info_cache_capacity = 1000;
    config::starlet_shard_info_cache_ttl_sec = 2;              // short TTL
    config::starlet_shard_info_cache_cleanup_interval_sec = 1; // clean every second

    auto worker = std::make_shared<StarOSWorker>();

    staros::starlet::ShardInfo info;
    info.id = 67890;
    info.properties["tableId"] = "1003";
    MockStarlet::add_shard(info);

    // First fetch: should populate cache
    auto result1 = worker->_fetch_shard_info_from_remote(info.id);
    ASSERT_TRUE(result1.ok());

    // Wait for active cleaner to run
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Check that cache no longer contains this entry
    {
        std::shared_lock l(worker->_mtx);
        auto cached = worker->_shard_info_cache->get(info.id);
        EXPECT_FALSE(cached.has_value());
    }

    // Fetch again: should trigger RPC again
    auto result2 = worker->_fetch_shard_info_from_remote(info.id);
    ASSERT_TRUE(result2.ok());
    EXPECT_EQ(result2->id, info.id);
}

} // namespace starrocks
#endif
