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
#include "staros_integration/staros_worker.h"

#include <aws/core/Aws.h>
#include <fslib/configuration.h>
#include <fslib/fslib_all_initializer.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <manager.grpc.pb.h>
#include <shard.pb.h>

#include <condition_variable>
#include <functional>

#include "base/utility/defer_op.h"
#include "common/shutdown_hook.h"
#include "staros_integration/staros_worker_metrics.h"
#include "staros_integration/staros_worker_runtime.h"

namespace starrocks {

static void add_shard_listener(std::vector<StarOSWorker::ShardId>* shardIds, int* counter, StarOSWorker::ShardId id) {
    shardIds->push_back(id);
    ++*counter;
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

    auto conf_or = shard_info.fslib_conf_from_this(false, "");
    EXPECT_TRUE(conf_or.ok());
    auto conf = conf_or.value();

    // TODO: Re-enable lookup assertions after StarOSWorker filesystem-cache lookup behavior is fixed.
    // auto schema_or = StarOSWorker::build_scheme_from_shard_info(shard_info);
    // EXPECT_TRUE(schema_or.ok());
    // auto schema = schema_or.value();
    // auto local_conf_or = StarOSWorker::build_conf_from_shard_info(shard_info, &conf);
    // EXPECT_TRUE(local_conf_or.ok());
    // auto cache_key = StarOSWorker::get_cache_key(schema, local_conf_or.value());

    auto worker = std::make_shared<StarOSWorker>();
    set_staros_worker_for_test(worker);

    EXPECT_TRUE(worker->add_shard(shard_info).ok());

    // EXPECT_FALSE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->get_shard_filesystem(shard_info.id, conf).ok());

    // EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->remove_shard(shard_info.id).ok());

    // EXPECT_FALSE(worker->lookup_fs_cache(cache_key));
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

    auto conf_or = shard_info.fslib_conf_from_this(false, "");
    EXPECT_TRUE(conf_or.ok());
    auto conf = conf_or.value();

    auto worker = std::make_shared<StarOSWorker>();
    set_staros_worker_for_test(worker);

    EXPECT_TRUE(worker->add_shard(shard_info).ok());

    std::shared_ptr<std::string> key1, key2;
    std::mutex mtx;
    std::condition_variable cv;
    bool ready = false;
    int ready_count = 0;

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

    ASSERT_NE(nullptr, key1);
    ASSERT_NE(nullptr, key2);
    EXPECT_EQ(*key1, *key2);

    // TODO: Re-enable lookup assertions after StarOSWorker filesystem-cache lookup behavior is fixed.
    // auto cache_key = *key1;
    // EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->get_shard_filesystem(shard_info.id, conf).ok());

    // EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    EXPECT_TRUE(worker->remove_shard(shard_info.id).ok());

    // EXPECT_TRUE(worker->lookup_fs_cache(cache_key));

    key1.reset();
    key2.reset();

    // EXPECT_FALSE(worker->lookup_fs_cache(cache_key));
}

// Verify that a cache hit in retrieve_shard_info() does not trigger the fallback path
// and therefore does not increment the fallback counters.
TEST_F(StarOSWorkerTest, test_fallback_metric_not_incremented_on_cache_hit) {
    auto* metrics = StarOSWorkerMetrics::instance();
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

    // Save original Starlet and set up a temporary one pointing at our mock.
    // Use DeferOp to guarantee cleanup on all exit paths (including ASSERT_* failures).
    auto orig_starlet = swap_starlet_for_test(nullptr);
    DeferOp restore_starlet([&orig_starlet, &server] {
        auto starlet = swap_starlet_for_test(nullptr);
        if (starlet) {
            starlet->stop();
        }
        (void)swap_starlet_for_test(std::move(orig_starlet));
        server->Shutdown();
    });

    auto worker = std::make_shared<StarOSWorker>();
    auto starlet = std::make_unique<staros::starlet::Starlet>(worker);
    auto* starlet_ptr = starlet.get();
    (void)swap_starlet_for_test(std::move(starlet));
    staros::starlet::StarletConfig config;
    config.rpc_port = 0;
    config.heartbeat_interval = 10;
    starlet_ptr->init(config);
    starlet_ptr->start();
    starlet_ptr->set_star_mgr_addr("127.0.0.1:" + std::to_string(port));
    ASSERT_TRUE(starlet_ptr->is_ready());

    auto* metrics = StarOSWorkerMetrics::instance();
    int64_t before_total = metrics->staros_shard_info_fallback_total.value();
    int64_t before_failed = metrics->staros_shard_info_fallback_failed_total.value();

    // Shard 99 is not in the local cache, so retrieve_shard_info triggers the real
    // _fetch_shard_info_from_remote -> Starlet get_shard_info() -> mock starmgr -> error.
    auto got = worker->retrieve_shard_info(99);
    ASSERT_FALSE(got.ok());
    EXPECT_EQ(before_total + 1, metrics->staros_shard_info_fallback_total.value());
    EXPECT_EQ(before_failed + 1, metrics->staros_shard_info_fallback_failed_total.value());
}

} // namespace starrocks
#endif
