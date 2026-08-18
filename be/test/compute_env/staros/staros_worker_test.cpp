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
#include "compute_env/staros/staros_worker.h"

#include <aws/core/Aws.h>
#include <fslib/configuration.h>
#include <fslib/fslib_all_initializer.h>
#include <gflags/gflags.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <manager.grpc.pb.h>
#include <shard.pb.h>

#include <algorithm>
#include <condition_variable>
#include <functional>
<<<<<<< HEAD
#include <mutex>
#include <string>
#include <utility>
#include <vector>
=======
#include <iterator>
>>>>>>> a16411accce ([Enhancement] Make starlet object-store upload thresholds configurable at runtime (#60610))

#include "base/testutil/scoped_updater.h"
#include "base/utility/defer_op.h"
<<<<<<< HEAD
#include "common/config_metrics_fwd.h"
#include "common/logging.h"
=======
#include "common/config_staros_worker_fwd.h"
#include "common/configbase.h"
>>>>>>> a16411accce ([Enhancement] Make starlet object-store upload thresholds configurable at runtime (#60610))
#include "common/shutdown_hook.h"
#include "common/util/table_metrics.h"
#include "compute_env/staros/staros_worker_metrics.h"
#include "compute_env/staros/staros_worker_runtime.h"

DECLARE_int64(fslib_s3_max_single_part_size);
DECLARE_int64(fslib_s3_min_upload_part_size);
DECLARE_int64(fslib_gs_max_single_part_size);
DECLARE_int64(fslib_azure_storage_max_single_part_size);
DECLARE_int64(fslib_azure_storage_min_upload_part_size);

namespace starrocks {

static void add_shard_listener(std::vector<StarOSWorker::ShardId>* shardIds, int* counter, StarOSWorker::ShardId id) {
    shardIds->push_back(id);
    ++*counter;
}

static Aws::SDKOptions _s_options;

class RecordingLogSink final : public google::LogSink {
public:
    void send(google::LogSeverity severity, const char* full_filename, const char* base_filename, int line,
              const google::LogMessageTime& time, const char* message, size_t message_len) override {
        std::lock_guard lock(_mutex);
        _messages.emplace_back(severity, std::string(message, message_len));
    }

    size_t count(google::LogSeverity severity, const std::string& needle) const {
        std::lock_guard lock(_mutex);
        return std::count_if(_messages.begin(), _messages.end(), [&](const auto& entry) {
            return entry.first == severity && entry.second.find(needle) != std::string::npos;
        });
    }

private:
    mutable std::mutex _mutex;
    std::vector<std::pair<google::LogSeverity, std::string>> _messages;
};

class StarOSWorkerTest : public ::testing::Test {
public:
    static void SetUpTestCase() { Aws::InitAPI(_s_options); }

    static void TearDownTestCase() {
        staros::starlet::common::ShutdownHook::shutdown();
        Aws::ShutdownAPI(_s_options);
    }

    static void expect_malformed_table_id(std::string table_id, StarOSWorker::ShardId shard_id) {
        TableMetricsManager table_metrics_mgr;
        StarOSWorker worker(&table_metrics_mgr);
        RecordingLogSink sink;
        google::AddLogSink(&sink);
        DeferOp remove_sink([&] { google::RemoveLogSink(&sink); });

        StarOSWorker::ShardInfo shard;
        shard.id = shard_id;
        shard.properties.emplace("tableId", table_id);
        EXPECT_TRUE(worker.add_shard(shard).ok());
        EXPECT_EQ(0, table_metrics_mgr.size());
        EXPECT_TRUE(worker.remove_shard(shard.id).ok());
        EXPECT_EQ(0, table_metrics_mgr.size());
        EXPECT_EQ(2, sink.count(google::GLOG_WARNING, "failed to parse tableId: " + table_id));
    }
};

TEST_F(StarOSWorkerTest, test_add_listener) {
    int counter = 0;
    std::vector<StarOSWorker::ShardId> ids;

    auto worker = std::make_unique<StarOSWorker>();

    StarOSWorker::ShardInfo info;

    EXPECT_EQ(0, counter);
    EXPECT_TRUE(ids.empty());

    auto& shard_count_metric = StarOSWorkerMetrics::instance()->staros_shard_count;

    info.id = 1;
    EXPECT_TRUE(worker->add_shard(info).ok());
    EXPECT_EQ(1, worker->shard_ids().size());
    EXPECT_EQ(1, shard_count_metric.value());

    // no shard registered, counter and ids will not be modified
    EXPECT_EQ(0, counter);
    EXPECT_TRUE(ids.empty());

    // register the counter;
    worker->register_add_shard_listener(std::bind(&add_shard_listener, &ids, &counter, std::placeholders::_1));

    info.id = 2;
    EXPECT_TRUE(worker->add_shard(info).ok());
    EXPECT_EQ(2, worker->shard_ids().size());
    EXPECT_EQ(2, shard_count_metric.value());

    // shard:2 added
    EXPECT_EQ(1, counter);
    EXPECT_EQ(1, ids.size());
    EXPECT_EQ(2, ids[0]);

    // add it again — insert_or_assign keeps the count flat
    EXPECT_TRUE(worker->add_shard(info).ok());
    EXPECT_EQ(1, counter);
    EXPECT_EQ(1, ids.size());
    EXPECT_EQ(2, shard_count_metric.value());

    EXPECT_TRUE(worker->remove_shard(1).ok());
    EXPECT_EQ(1, shard_count_metric.value());

    // remove a shard that does not exist — count unchanged
    EXPECT_TRUE(worker->remove_shard(999).ok());
    EXPECT_EQ(1, shard_count_metric.value());

    EXPECT_TRUE(worker->remove_shard(2).ok());
    EXPECT_EQ(0, shard_count_metric.value());
}

TEST_F(StarOSWorkerTest, TableMetricsIgnoreVirtualShard) {
    const bool old_enable_table_metrics = config::enable_table_metrics;
    DeferOp restore_config([&] { config::enable_table_metrics = old_enable_table_metrics; });
    config::enable_table_metrics = true;

    TableMetricsManager table_metrics_mgr;
    StarOSWorker worker(&table_metrics_mgr);
    RecordingLogSink sink;
    google::AddLogSink(&sink);
    DeferOp remove_sink([&] { google::RemoveLogSink(&sink); });

    StarOSWorker::ShardInfo shard;
    shard.id = 101;
    EXPECT_TRUE(worker.add_shard(shard).ok());
    EXPECT_EQ(0, table_metrics_mgr.size());
    EXPECT_TRUE(worker.remove_shard(shard.id).ok());
    EXPECT_EQ(0, table_metrics_mgr.size());
    EXPECT_EQ(0, sink.count(google::GLOG_WARNING, "tableId"));
}

TEST_F(StarOSWorkerTest, TableMetricsRejectPartiallyParsedTableIds) {
    const bool old_enable_table_metrics = config::enable_table_metrics;
    DeferOp restore_config([&] { config::enable_table_metrics = old_enable_table_metrics; });
    config::enable_table_metrics = true;
    expect_malformed_table_id("-1", 102);
    expect_malformed_table_id("42junk", 103);
}

TEST_F(StarOSWorkerTest, TableMetricsRejectEmptyTableId) {
    const bool old_enable_table_metrics = config::enable_table_metrics;
    DeferOp restore_config([&] { config::enable_table_metrics = old_enable_table_metrics; });
    config::enable_table_metrics = true;
    expect_malformed_table_id("", 104);
}

TEST_F(StarOSWorkerTest, TableMetricsRejectOverflowingTableId) {
    const bool old_enable_table_metrics = config::enable_table_metrics;
    DeferOp restore_config([&] { config::enable_table_metrics = old_enable_table_metrics; });
    config::enable_table_metrics = true;
    expect_malformed_table_id("18446744073709551616", 105);
}

TEST_F(StarOSWorkerTest, TableMetricsRejectEmbeddedNullTableId) {
    const bool old_enable_table_metrics = config::enable_table_metrics;
    DeferOp restore_config([&] { config::enable_table_metrics = old_enable_table_metrics; });
    config::enable_table_metrics = true;
    expect_malformed_table_id(std::string("42\0junk", 7), 108);
}

TEST_F(StarOSWorkerTest, TableMetricsPreserveTableShardReferenceCounts) {
    const bool old_enable_table_metrics = config::enable_table_metrics;
    DeferOp restore_config([&] { config::enable_table_metrics = old_enable_table_metrics; });
    config::enable_table_metrics = true;

    TableMetricsManager table_metrics_mgr;
    StarOSWorker worker(&table_metrics_mgr);
    RecordingLogSink sink;
    google::AddLogSink(&sink);
    DeferOp remove_sink([&] { google::RemoveLogSink(&sink); });

    StarOSWorker::ShardInfo first;
    first.id = 106;
    first.properties.emplace("tableId", "42");
    StarOSWorker::ShardInfo second;
    second.id = 107;
    second.properties.emplace("tableId", " 42 ");

    EXPECT_TRUE(worker.add_shard(first).ok());
    EXPECT_TRUE(worker.add_shard(second).ok());
    ASSERT_EQ(1, table_metrics_mgr.size());
    EXPECT_EQ(2, table_metrics_mgr.get_table_metrics(42)->ref_count);
    EXPECT_TRUE(worker.remove_shard(first.id).ok());
    EXPECT_EQ(1, table_metrics_mgr.get_table_metrics(42)->ref_count);
    EXPECT_TRUE(worker.remove_shard(second.id).ok());
    EXPECT_EQ(0, table_metrics_mgr.get_table_metrics(42)->ref_count);
    EXPECT_EQ(0, sink.count(google::GLOG_WARNING, "tableId"));
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

namespace {

struct UploadThresholdMapping {
    const char* be_config;
    const char* starlet_flag;
};

const UploadThresholdMapping kUploadThresholdMappings[] = {
        {"starlet_fslib_s3_max_single_part_size", "fslib_s3_max_single_part_size"},
        {"starlet_fslib_s3_min_upload_part_size", "fslib_s3_min_upload_part_size"},
        {"starlet_fslib_gs_max_single_part_size", "fslib_gs_max_single_part_size"},
        {"starlet_fslib_azure_storage_max_single_part_size", "fslib_azure_storage_max_single_part_size"},
        {"starlet_fslib_azure_storage_min_upload_part_size", "fslib_azure_storage_min_upload_part_size"},
};

} // namespace

// The BE config default must equal the starlet gflag's registered default, so that merging this
// feature changes no behavior. Both sides read declared defaults, never mutable current values.
TEST_F(StarOSWorkerTest, upload_threshold_config_defaults_match_starlet) {
    auto configs = config::list_configs();
    for (const auto& mapping : kUploadThresholdMappings) {
        auto it = std::find_if(configs.begin(), configs.end(),
                               [&](const config::ConfigInfo& info) { return info.name == mapping.be_config; });
        ASSERT_NE(configs.end(), it) << "missing BE config " << mapping.be_config;

        gflags::CommandLineFlagInfo flag_info;
        ASSERT_TRUE(gflags::GetCommandLineFlagInfo(mapping.starlet_flag, &flag_info))
                << "missing starlet gflag " << mapping.starlet_flag;

        EXPECT_EQ(flag_info.default_value, it->defval)
                << mapping.be_config << " default drifted from " << mapping.starlet_flag;
    }

    // Direct typed references: a wrong DECLARE_ type or a misspelled flag name fails to build.
    // Binding to `const int64_t*` is the whole check; no current value is read.
    [[maybe_unused]] const int64_t* const typed_flags[] = {
            &FLAGS_fslib_s3_max_single_part_size, &FLAGS_fslib_s3_min_upload_part_size,
            &FLAGS_fslib_gs_max_single_part_size, &FLAGS_fslib_azure_storage_max_single_part_size,
            &FLAGS_fslib_azure_storage_min_upload_part_size};
    static_assert(std::size(kUploadThresholdMappings) == std::size(typed_flags),
                  "every mapped config needs a typed flag reference above");
}

// Distinct values per flag, so deleting one assignment or swapping two fails.
TEST_F(StarOSWorkerTest, upload_threshold_configs_applied_at_startup) {
    gflags::FlagSaver flag_saver;
    SCOPED_UPDATE(int64_t, config::starlet_fslib_s3_max_single_part_size, 11L << 20);
    SCOPED_UPDATE(int64_t, config::starlet_fslib_s3_min_upload_part_size, 12L << 20);
    SCOPED_UPDATE(int64_t, config::starlet_fslib_gs_max_single_part_size, 13L << 20);
    SCOPED_UPDATE(int64_t, config::starlet_fslib_azure_storage_max_single_part_size, 14L << 20);
    SCOPED_UPDATE(int64_t, config::starlet_fslib_azure_storage_min_upload_part_size, 15L << 20);

    apply_starlet_upload_threshold_configs();

    EXPECT_EQ(11L << 20, FLAGS_fslib_s3_max_single_part_size);
    EXPECT_EQ(12L << 20, FLAGS_fslib_s3_min_upload_part_size);
    EXPECT_EQ(13L << 20, FLAGS_fslib_gs_max_single_part_size);
    EXPECT_EQ(14L << 20, FLAGS_fslib_azure_storage_max_single_part_size);
    EXPECT_EQ(15L << 20, FLAGS_fslib_azure_storage_min_upload_part_size);
}

// A non-positive config value must not be applied; whatever was already effective stays.
// Covers all five mappings, with a distinct sentinel prior value per flag, so a crossed pair fails.
TEST_F(StarOSWorkerTest, upload_threshold_configs_reject_non_positive_at_startup) {
    gflags::FlagSaver flag_saver;
    FLAGS_fslib_s3_max_single_part_size = 7L << 20;
    FLAGS_fslib_s3_min_upload_part_size = 8L << 20;
    FLAGS_fslib_gs_max_single_part_size = 9L << 20;
    FLAGS_fslib_azure_storage_max_single_part_size = 10L << 20;
    FLAGS_fslib_azure_storage_min_upload_part_size = 11L << 20;

    {
        SCOPED_UPDATE(int64_t, config::starlet_fslib_s3_max_single_part_size, 0);
        SCOPED_UPDATE(int64_t, config::starlet_fslib_s3_min_upload_part_size, -1);
        SCOPED_UPDATE(int64_t, config::starlet_fslib_gs_max_single_part_size, 0);
        SCOPED_UPDATE(int64_t, config::starlet_fslib_azure_storage_max_single_part_size, -1);
        SCOPED_UPDATE(int64_t, config::starlet_fslib_azure_storage_min_upload_part_size, 0);
        apply_starlet_upload_threshold_configs();
    }

    EXPECT_EQ(7L << 20, FLAGS_fslib_s3_max_single_part_size);
    EXPECT_EQ(8L << 20, FLAGS_fslib_s3_min_upload_part_size);
    EXPECT_EQ(9L << 20, FLAGS_fslib_gs_max_single_part_size);
    EXPECT_EQ(10L << 20, FLAGS_fslib_azure_storage_max_single_part_size);
    EXPECT_EQ(11L << 20, FLAGS_fslib_azure_storage_min_upload_part_size);
}

} // namespace starrocks
#endif
