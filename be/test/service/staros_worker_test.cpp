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

#include <aws/core/Aws.h>
#include <fslib/configuration.h>
#include <fslib/fslib_all_initializer.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <manager.grpc.pb.h>
#include <shard.pb.h>

#include <algorithm>
#include <condition_variable>
#include <functional>
#include <limits>

#include "common/config.h"
#include "common/shutdown_hook.h"
#include "testutil/scoped_updater.h"
#include "util/defer_op.h"
#include "util/lru_cache.h"
#include "util/starrocks_metrics.h"
#include "util/stopwatch.hpp"

namespace starrocks {

static void add_shard_listener(std::vector<StarOSWorker::ShardId>* shardIds, int* counter, StarOSWorker::ShardId id) {
    shardIds->push_back(id);
    ++*counter;
}

TEST(StarletRequestTimeoutTest, PreserveTimeoutSemanticsForEachHttpClient) {
    auto positive = starlet_request_timeout_ms(10000, true);
    ASSERT_TRUE(positive);
    EXPECT_EQ(10000, *positive);

    auto disabled = starlet_request_timeout_ms(0, true);
    ASSERT_TRUE(disabled);
    EXPECT_EQ(0, *disabled);

    auto poco_unset = starlet_request_timeout_ms(-1, true);
    ASSERT_TRUE(poco_unset);
    EXPECT_EQ(-1, *poco_unset);

    auto curl_unset = starlet_request_timeout_ms(-1, false);
    ASSERT_TRUE(curl_unset);
    EXPECT_EQ(0, *curl_unset);

    auto max = starlet_request_timeout_ms(std::numeric_limits<int32_t>::max(), true);
    ASSERT_TRUE(max);
    EXPECT_EQ(std::numeric_limits<int32_t>::max(), *max);
    EXPECT_FALSE(starlet_request_timeout_ms(static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1, true));
}

static Aws::SDKOptions _s_options;

class StarOSWorkerTest : public ::testing::Test {
public:
    static void SetUpTestCase() { Aws::InitAPI(_s_options); }

    static void TearDownTestCase() {
        staros::starlet::common::ShutdownHook::shutdown();
        Aws::ShutdownAPI(_s_options);
    }
};

TEST_F(StarOSWorkerTest, test_add_listener) {
    int counter = 0;
    std::vector<StarOSWorker::ShardId> ids;

    auto worker = std::make_unique<StarOSWorker>();

    StarOSWorker::ShardInfo info;

    EXPECT_EQ(0, counter);
    EXPECT_TRUE(ids.empty());

    info.id = 1;
    EXPECT_TRUE(worker->add_shard(info).ok());
    EXPECT_EQ(1, worker->shard_ids().size());

    // no shard registered, counter and ids will not be modified
    EXPECT_EQ(0, counter);
    EXPECT_TRUE(ids.empty());

    // register the counter;
    worker->register_add_shard_listener(std::bind(&add_shard_listener, &ids, &counter, std::placeholders::_1));

    info.id = 2;
    EXPECT_TRUE(worker->add_shard(info).ok());
    EXPECT_EQ(2, worker->shard_ids().size());

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

TEST_F(StarOSWorkerTest, test_fs_cache) {
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

TEST_F(StarOSWorkerTest, test_build_scheme_from_shard_info) {
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

TEST_F(StarOSWorkerTest, test_fs_cache_concurrent) {
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
    shard_info.path_info.set_full_path(absl::StrFormat("s3://%s/%d/", s3_fs_info->bucket(), time(NULL)));

    shard_info.cache_info.set_enable_cache(true);
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

    std::shared_ptr<std::string> key1, key2;
    std::mutex mtx;
    std::condition_variable cv;
    bool ready = false;
    int ready_count = 0;

    EXPECT_FALSE(worker->lookup_fs_cache(cache_key));

    auto thread_func = [&](std::shared_ptr<std::string>& key) {
        {
            std::unique_lock<std::mutex> lock(mtx);
            ready_count++;
            cv.notify_all();
            cv.wait(lock, [&] { return ready; });
        }

        auto result = worker->build_filesystem_from_shard_info(shard_info, conf);
        EXPECT_TRUE(result.ok());
        key = result->first;
    };

    std::thread t1(thread_func, std::ref(key1));
    std::thread t2(thread_func, std::ref(key2));

    {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&] { return ready_count == 2; });
        ready = true;
    }
    cv.notify_all();

    t1.join();
    t2.join();

    EXPECT_EQ(key1.get(), key2.get());

    EXPECT_EQ(*key1, *key2);

    EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->get_shard_filesystem(shard_info.id, conf).ok());

    EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->remove_shard(shard_info.id).ok());

    EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    key1.reset();
    key2.reset();

    EXPECT_FALSE(worker->lookup_fs_cache(cache_key));
}

static StarOSWorker::ShardInfo make_s3_shard_info(StarOSWorker::ShardId id, const std::string& full_path) {
    StarOSWorker::ShardInfo info;
    info.id = id;
    info.hash_code = 0;
    auto* fs_info = info.path_info.mutable_fs_info();
    fs_info->set_fs_type(staros::FileStoreType::S3);
    auto* s3_fs_info = fs_info->mutable_s3_fs_info();
    s3_fs_info->set_bucket("test_bucket");
    s3_fs_info->set_endpoint("test_endpoint");
    s3_fs_info->set_region("us-east-1");
    auto* simple_credential = s3_fs_info->mutable_credential()->mutable_simple_credential();
    simple_credential->set_access_key("test_ak");
    simple_credential->set_access_key_secret("test_sk");
    info.path_info.set_full_path(full_path);
    info.cache_info.set_enable_cache(false);
    return info;
}

// Builds a filesystem for each of |count| partitions of one table and returns the keys of their filesystem
// cache entries. FE gives every partition its own path, so each partition gets its own entry. A caller
// holds the keys for as long as the entries must stay: releasing a key erases its entry.
static std::vector<std::shared_ptr<std::string>> build_partition_filesystems(StarOSWorker* worker, int count) {
    std::vector<std::shared_ptr<std::string>> keys;
    keys.reserve(count);
    for (int i = 0; i < count; ++i) {
        auto info = make_s3_shard_info(20000 + i, absl::StrFormat("s3://test_bucket/db/tbl/p%d", i));
        auto built = worker->build_filesystem_from_shard_info(info, {});
        EXPECT_TRUE(built.ok()) << built.status();
        if (built.ok()) {
            keys.push_back(std::move(built->first));
        }
    }
    return keys;
}

// Whether |key| is still the key of a live filesystem cache entry.
static bool is_live_fs_cache_key(StarOSWorker* worker, const std::shared_ptr<std::string>& key) {
    if (key == nullptr) {
        return false;
    }
    auto value_or = worker->find_fs_cache(*key);
    return value_or.ok() && value_or->first == key;
}

static int count_live_fs_cache_keys(StarOSWorker* worker, const std::vector<std::shared_ptr<std::string>>& keys) {
    return std::count_if(keys.begin(), keys.end(),
                         [&](const std::shared_ptr<std::string>& key) { return is_live_fs_cache_key(worker, key); });
}

// What the filesystem cache charges a filesystem: the 1 it is inserted with plus its handle and key.
static size_t fs_cache_entry_charge(const std::string& key) {
    return 1 + LRUCache::key_handle_size(CacheKey(key));
}

// `starlet_filesystem_instance_cache_capacity` is a number of filesystems. A node that reads more
// partitions than that number misses the cache on almost every lookup and builds a new filesystem each time,
// so a number of partitions well below the capacity must all stay cached, although the cache itself charges each
// entry its handle and key as well.
TEST_F(StarOSWorkerTest, fs_cache_capacity_counts_filesystems) {
    staros::starlet::fslib::register_builtin_filesystems();
    SCOPED_UPDATE(int32_t, config::starlet_filesystem_instance_cache_capacity, 10000);
    // A released key erases its entry through the global worker.
    auto origin = g_worker;
    auto worker = std::make_shared<StarOSWorker>();
    g_worker = worker;
    DeferOp restore([&] { g_worker = origin; });

    // Each LRU shard holds a 32nd of the capacity, so stay well below it.
    constexpr int kPartitions = 1000;
    auto keys = build_partition_filesystems(worker.get(), kPartitions);
    ASSERT_EQ(kPartitions, keys.size());
    EXPECT_EQ(kPartitions, count_live_fs_cache_keys(worker.get(), keys));
    // Every filesystem is charged the same, which the conversion of the capacity relies on.
    const size_t entry_charge = fs_cache_entry_charge(*keys.front());
    EXPECT_EQ(kPartitions * entry_charge, worker->_fs_cache->get_memory_usage());
    EXPECT_EQ(10000 * entry_charge, worker->_fs_cache->get_capacity());
}

// Each LRU shard of the filesystem cache gets a 32nd of its capacity, so a multiple of 32 divides evenly.
static constexpr int kExactFsCacheCapacity = 4 * kNumShards;

// The exact bound: once every LRU shard overflows, the cache holds exactly as many filesystems as its capacity.
// One fewer per shard means each filesystem is charged more than the capacity was converted for; the usage check
// catches a charge that is off in either direction.
TEST_F(StarOSWorkerTest, fs_cache_holds_exactly_its_capacity) {
    staros::starlet::fslib::register_builtin_filesystems();
    SCOPED_UPDATE(int32_t, config::starlet_filesystem_instance_cache_capacity, kExactFsCacheCapacity);
    auto origin = g_worker;
    auto worker = std::make_shared<StarOSWorker>();
    g_worker = worker;
    DeferOp restore([&] { g_worker = origin; });

    // About 31 partitions per LRU shard, well above the 4 each one holds.
    constexpr int kPartitions = 1000;
    auto keys = build_partition_filesystems(worker.get(), kPartitions);
    ASSERT_EQ(kPartitions, keys.size());
    EXPECT_EQ(kExactFsCacheCapacity, count_live_fs_cache_keys(worker.get(), keys));
    const size_t entry_charge = fs_cache_entry_charge(*keys.front());
    EXPECT_EQ(kExactFsCacheCapacity * entry_charge, worker->_fs_cache->get_memory_usage());
    EXPECT_EQ(kExactFsCacheCapacity * entry_charge, worker->_fs_cache->get_capacity());
}

// The config is mutable: raising it through `set_fs_cache_capacity` makes room for that many more filesystems,
// and lowering it evicts down to exactly that many.
TEST_F(StarOSWorkerTest, fs_cache_capacity_update_counts_filesystems) {
    staros::starlet::fslib::register_builtin_filesystems();
    SCOPED_UPDATE(int32_t, config::starlet_filesystem_instance_cache_capacity, 64);
    auto origin = g_worker;
    auto worker = std::make_shared<StarOSWorker>();
    g_worker = worker;
    DeferOp restore([&] { g_worker = origin; });

    constexpr int kPartitions = 1000;
    auto evicted = build_partition_filesystems(worker.get(), kPartitions);
    ASSERT_EQ(kPartitions, evicted.size());
    // 64 filesystems split over 32 LRU shards: 2 per shard.
    const size_t entry_charge = fs_cache_entry_charge(*evicted.front());
    EXPECT_GT(count_live_fs_cache_keys(worker.get(), evicted), 0);
    EXPECT_LE(worker->_fs_cache->get_memory_usage(), 64 * entry_charge);

    worker->set_fs_cache_capacity(10000);
    auto keys = build_partition_filesystems(worker.get(), kPartitions);
    ASSERT_EQ(kPartitions, keys.size());
    EXPECT_EQ(kPartitions, count_live_fs_cache_keys(worker.get(), keys));
    EXPECT_EQ(kPartitions * entry_charge, worker->_fs_cache->get_memory_usage());

    worker->set_fs_cache_capacity(kExactFsCacheCapacity);
    EXPECT_EQ(kExactFsCacheCapacity, count_live_fs_cache_keys(worker.get(), keys));
    EXPECT_EQ(kExactFsCacheCapacity * entry_charge, worker->_fs_cache->get_memory_usage());
}

// Verify that a cache hit in retrieve_shard_info() does not trigger the fallback path
// and therefore does not increment the fallback counters.
TEST_F(StarOSWorkerTest, test_fallback_metric_not_incremented_on_cache_hit) {
    auto* metrics = StarRocksMetrics::instance();
    int64_t before_total = metrics->staros_shard_info_fallback_total.value();
    int64_t before_failed = metrics->staros_shard_info_fallback_failed_total.value();

    auto worker = std::make_unique<StarOSWorker>();
    StarOSWorker::ShardInfo info;
    info.id = 7;
    ASSERT_TRUE(worker->add_shard(info).ok());

    auto got = worker->retrieve_shard_info(7);
    ASSERT_TRUE(got.ok());
    EXPECT_EQ(7u, got.value().id);
    // Cache hit -- neither counter should move.
    EXPECT_EQ(before_total, metrics->staros_shard_info_fallback_total.value());
    EXPECT_EQ(before_failed, metrics->staros_shard_info_fallback_failed_total.value());
}

// Mock starmgr gRPC service that returns an error for every GetShard request.
class ErrorStarMgrService : public staros::StarManager::Service {
public:
    ::grpc::Status GetShard(::grpc::ServerContext* /*context*/, const staros::GetShardRequest* /*req*/,
                            staros::GetShardResponse* /*reply*/) override {
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, "mock starmgr error");
    }
    ::grpc::Status WorkerHeartbeat(::grpc::ServerContext* /*context*/, const staros::WorkerHeartbeatRequest* /*req*/,
                                   staros::WorkerHeartbeatResponse* /*reply*/) override {
        return ::grpc::Status::OK;
    }
};

TEST_F(StarOSWorkerTest, test_fallback_metric_increments_on_cache_miss_failure) {
    // Start a mock starmgr gRPC server on localhost that returns error for GetShard.
    ErrorStarMgrService mock_service;
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&mock_service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);
    ASSERT_GT(port, 0);

    // Save original g_starlet and set up a temporary one pointing at our mock.
    // Use DeferOp to guarantee cleanup on all exit paths (including ASSERT_* failures).
    auto orig_starlet = std::move(g_starlet);
    DeferOp restore_starlet([&orig_starlet, &server] {
        if (g_starlet) {
            g_starlet->stop();
        }
        g_starlet = std::move(orig_starlet);
        server->Shutdown();
    });

    auto worker = std::make_shared<StarOSWorker>();
    g_starlet = std::make_unique<staros::starlet::Starlet>(worker);
    staros::starlet::StarletConfig config;
    config.rpc_port = 0;
    config.heartbeat_interval = 10;
    g_starlet->init(config);
    g_starlet->start();
    g_starlet->set_star_mgr_addr("127.0.0.1:" + std::to_string(port));
    ASSERT_TRUE(g_starlet->is_ready());

    auto* metrics = StarRocksMetrics::instance();
    int64_t before_total = metrics->staros_shard_info_fallback_total.value();
    int64_t before_failed = metrics->staros_shard_info_fallback_failed_total.value();

    // Shard 99 is not in the local cache, so retrieve_shard_info triggers the real
    // _fetch_shard_info_from_remote -> g_starlet->get_shard_info() -> mock starmgr -> error.
    auto got = worker->retrieve_shard_info(99);
    ASSERT_FALSE(got.ok());
    EXPECT_EQ(before_total + 1, metrics->staros_shard_info_fallback_total.value());
    EXPECT_EQ(before_failed + 1, metrics->staros_shard_info_fallback_failed_total.value());
}

// `shutdown_staros_worker()` releases the starlet runtime while an in-flight load may still be
// walking a StarOS-backed path. Every call that reaches starlet after that must report a status
// instead of dereferencing the released runtime. See issue #78883.
TEST_F(StarOSWorkerTest, starlet_calls_fail_after_runtime_release) {
    auto orig_starlet = std::move(g_starlet);
    DeferOp restore_starlet([&orig_starlet] { g_starlet = std::move(orig_starlet); });
    ASSERT_EQ(nullptr, get_starlet());

    StarOSWorker worker;

    // A cache miss falls back to the remote fetch, which waits for starlet readiness. With no
    // starlet there is nothing to wait for, so the call must give up right away rather than burn
    // the full 5s readiness timeout.
    MonotonicStopWatch watch;
    watch.start();
    EXPECT_FALSE(worker.retrieve_shard_info(987654321).ok());
    EXPECT_LT(watch.elapsed_time(), 3L * 1000 * 1000 * 1000);
}

// `shutdown_staros_worker()` drops the process-wide starlet reference while an in-flight operation
// may still be using it. The reference that operation already holds must keep the runtime alive: a
// raw pointer would dangle across the blocking starmgr RPC behind `get_shard_info()`, and the
// use-after-free lands in the same `Starlet::_mutex` that a point-in-time null check cannot
// protect. See issue #78883.
TEST_F(StarOSWorkerTest, retained_starlet_outlives_shutdown_release) {
    auto worker = std::make_shared<StarOSWorker>();
    auto orig_starlet = std::move(g_starlet);
    DeferOp restore_starlet([&orig_starlet] { g_starlet = std::move(orig_starlet); });
    g_starlet = std::make_shared<staros::starlet::Starlet>(worker);

    // An in-flight operation resolves the runtime before shutdown retires it.
    auto held = get_starlet();
    ASSERT_NE(nullptr, held);

    {
        // Stands in for shutdown: stop the runtime and drop the process-wide reference. `held` is
        // the only remaining owner, so the object must survive this scope.
        auto retired = std::move(g_starlet);
        ASSERT_EQ(held.get(), retired.get());
        retired->stop();
    }
    ASSERT_EQ(nullptr, get_starlet());

    // Touches Starlet::_mutex, the member a use-after-free would have corrupted. Under ASAN this
    // is what fails if the retained reference stops keeping the runtime alive.
    EXPECT_FALSE(held->is_ready());
}

} // namespace starrocks
#endif
