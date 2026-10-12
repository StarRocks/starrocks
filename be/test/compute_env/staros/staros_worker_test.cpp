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
#include <gmock/gmock.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <manager.grpc.pb.h>
#include <shard.pb.h>

#include <algorithm>
#include <condition_variable>
#include <functional>
#include <iterator>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "base/concurrency/stopwatch.hpp"
#include "base/container/lru_cache.h"
#include "base/testutil/assert.h"
#include "base/testutil/scoped_updater.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/config_metrics_fwd.h"
#include "common/config_staros_worker_fwd.h"
#include "common/configbase.h"
#include "common/logging.h"
#include "common/shutdown_hook.h"
#include "common/util/table_metrics.h"
#include "compute_env/staros/staros_worker_metrics.h"
#include "compute_env/staros/staros_worker_runtime.h"

DECLARE_int64(fslib_s3_max_single_part_size);
DECLARE_int64(fslib_s3_min_upload_part_size);
DECLARE_int64(fslib_gs_max_single_part_size);
DECLARE_int64(fslib_azure_storage_max_single_part_size);
DECLARE_int64(fslib_azure_storage_min_upload_part_size);
DECLARE_bool(fslib_s3_multipart_equal_part_size);

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
// A worker whose starmgr fallback is observable, so a test can require that it never happens.
class RemoteCountingStarOSWorker : public StarOSWorker {
public:
    MOCK_METHOD((absl::StatusOr<staros::starlet::ShardInfo>), _fetch_shard_info_from_remote,
                (staros::starlet::ShardId id));
};

static StarOSWorker::ShardInfo make_s3_shard_info(StarOSWorker::ShardId id, const std::string& full_path,
                                                  int64_t fs_version) {
    StarOSWorker::ShardInfo info;
    info.id = id;
    info.hash_code = 0;
    auto* fs_info = info.path_info.mutable_fs_info();
    fs_info->set_fs_type(staros::FileStoreType::S3);
    fs_info->set_version(fs_version);
    auto* s3_fs_info = fs_info->mutable_s3_fs_info();
    s3_fs_info->set_bucket("test_bucket");
    s3_fs_info->set_endpoint("test_endpoint");
    s3_fs_info->set_region("us-east-1");
    auto* simple_credential = s3_fs_info->mutable_credential()->mutable_simple_credential();
    simple_credential->set_access_key("test_ak");
    simple_credential->set_access_key_secret("test_sk");
    info.path_info.set_full_path(full_path);
    info.cache_info.set_enable_cache(false);
    info.properties.emplace("indexId", "42");
    return info;
}

// The load coordinator writes a combined txn log under the directory of a tablet it does not own. With
// the shard info the owner handed over, that resolves without a starmgr RPC.
TEST_F(StarOSWorkerTest, borrowed_shard_info_resolves_without_starmgr) {
    staros::starlet::fslib::register_builtin_filesystems();
    StarOSWorker owner;
    auto info = make_s3_shard_info(1001, "s3://test_bucket/db/tbl/p1", 1);
    ASSERT_TRUE(owner.add_shard(info).ok());
    EXPECT_TRUE(absl::IsNotFound(owner.export_shard_info(1002).status()));
    auto exported = owner.export_shard_info(1001);
    ASSERT_TRUE(exported.ok()) << exported.status();

    RemoteCountingStarOSWorker borrower;
    EXPECT_CALL(borrower, _fetch_shard_info_from_remote(::testing::_)).Times(0);
    ASSERT_TRUE(borrower.borrow_shard_info(*exported).ok());
    EXPECT_TRUE(borrower.has_borrowed_shard_info(1001));
    // Borrowing does not make the shard owned, so no ownership check changes its answer.
    EXPECT_TRUE(absl::IsNotFound(borrower.get_shard_info(1001).status()));

    auto* metrics = StarOSWorkerMetrics::instance();
    const int64_t borrowed_before = metrics->lake_tablet_location_handoff_hits_total.value();
    const int64_t fallback_before = metrics->staros_shard_info_fallback_total.value();

    auto retrieved = borrower.retrieve_shard_info(1001);
    ASSERT_TRUE(retrieved.ok()) << retrieved.status();
    EXPECT_EQ(info.path_info.full_path(), retrieved->path_info.full_path());
    EXPECT_EQ("42", retrieved->properties.at("indexId"));

    auto first = borrower.get_shard_filesystem(1001, {});
    ASSERT_TRUE(first.ok()) << first.status();
    // The next load writes through the same filesystem instead of building another one.
    auto second = borrower.get_shard_filesystem(1001, {});
    ASSERT_TRUE(second.ok()) << second.status();
    EXPECT_EQ(first->get(), second->get());

    EXPECT_EQ(borrowed_before + 3, metrics->lake_tablet_location_handoff_hits_total.value());
    EXPECT_EQ(fallback_before, metrics->staros_shard_info_fallback_total.value());
}

TEST_F(StarOSWorkerTest, borrowed_shard_info_never_shadows_an_owned_shard) {
    StarOSWorker owner;
    auto info = make_s3_shard_info(2001, "s3://test_bucket/db/tbl/p2", 1);
    ASSERT_TRUE(owner.add_shard(info).ok());
    auto exported = owner.export_shard_info(2001);
    ASSERT_TRUE(exported.ok()) << exported.status();

    // A worker that owns the shard ignores a handed-over copy.
    StarOSWorker also_owner;
    ASSERT_TRUE(also_owner.add_shard(info).ok());
    ASSERT_TRUE(also_owner.borrow_shard_info(*exported).ok());
    EXPECT_FALSE(also_owner.has_borrowed_shard_info(2001));

    // And a borrowed copy goes away once the shard is assigned here.
    StarOSWorker borrower;
    ASSERT_TRUE(borrower.borrow_shard_info(*exported).ok());
    EXPECT_TRUE(borrower.has_borrowed_shard_info(2001));
    ASSERT_TRUE(borrower.add_shard(info).ok());
    EXPECT_FALSE(borrower.has_borrowed_shard_info(2001));
}

// An owner that has not seen a storage update yet must not roll back the copy of one that has.
TEST_F(StarOSWorkerTest, borrowed_shard_info_keeps_the_newer_storage_version) {
    StarOSWorker stale_owner;
    ASSERT_TRUE(stale_owner.add_shard(make_s3_shard_info(3001, "s3://test_bucket/v1", 1)).ok());
    StarOSWorker fresh_owner;
    ASSERT_TRUE(fresh_owner.add_shard(make_s3_shard_info(3001, "s3://test_bucket/v2", 2)).ok());
    auto stale = stale_owner.export_shard_info(3001);
    auto fresh = fresh_owner.export_shard_info(3001);
    ASSERT_TRUE(stale.ok() && fresh.ok());

    StarOSWorker borrower;
    ASSERT_TRUE(borrower.borrow_shard_info(*fresh).ok());
    ASSERT_TRUE(borrower.borrow_shard_info(*stale).ok());
    auto retrieved = borrower.retrieve_shard_info(3001);
    ASSERT_TRUE(retrieved.ok()) << retrieved.status();
    EXPECT_EQ("s3://test_bucket/v2", retrieved->path_info.full_path());
}

// A credential rotation reaches a borrower only with the next handoff, which must then stop the
// filesystem built from the old credentials from being reused.
TEST_F(StarOSWorkerTest, borrowed_shard_info_rebuilds_the_filesystem_on_a_storage_update) {
    staros::starlet::fslib::register_builtin_filesystems();
    StarOSWorker old_owner;
    auto old_info = make_s3_shard_info(6001, "s3://test_bucket/db/tbl/p6", 1);
    ASSERT_TRUE(old_owner.add_shard(old_info).ok());
    StarOSWorker new_owner;
    auto new_info = make_s3_shard_info(6001, "s3://test_bucket/db/tbl/p6", 2);
    new_info.path_info.mutable_fs_info()
            ->mutable_s3_fs_info()
            ->mutable_credential()
            ->mutable_simple_credential()
            ->set_access_key("rotated_ak");
    ASSERT_TRUE(new_owner.add_shard(new_info).ok());
    auto old_exported = old_owner.export_shard_info(6001);
    auto new_exported = new_owner.export_shard_info(6001);
    ASSERT_TRUE(old_exported.ok() && new_exported.ok());

    RemoteCountingStarOSWorker borrower;
    EXPECT_CALL(borrower, _fetch_shard_info_from_remote(::testing::_)).Times(0);
    ASSERT_TRUE(borrower.borrow_shard_info(*old_exported).ok());
    auto before = borrower.get_shard_filesystem(6001, {});
    ASSERT_TRUE(before.ok()) << before.status();
    ASSERT_TRUE(borrower.borrow_shard_info(*new_exported).ok());
    auto after = borrower.get_shard_filesystem(6001, {});
    ASSERT_TRUE(after.ok()) << after.status();
    EXPECT_NE(before->get(), after->get());
}

// If no filesystem can be built from a handed-over shard info, the lookup falls back to starmgr, which
// is what it did before the handoff existed, instead of failing the write.
TEST_F(StarOSWorkerTest, borrowed_shard_info_falls_back_when_no_filesystem_can_be_built) {
    staros::starlet::fslib::register_builtin_filesystems();
    StarOSWorker owner;
    StarOSWorker::ShardInfo unusable;
    unusable.id = 7001;
    unusable.hash_code = 0;
    unusable.path_info.set_full_path("s3://test_bucket/db/tbl/p7"); // fs type left INVALID
    ASSERT_TRUE(owner.add_shard(unusable).ok());
    auto exported = owner.export_shard_info(7001);
    ASSERT_TRUE(exported.ok()) << exported.status();

    RemoteCountingStarOSWorker borrower;
    ASSERT_TRUE(borrower.borrow_shard_info(*exported).ok());
    ASSERT_TRUE(borrower.has_borrowed_shard_info(7001));
    EXPECT_CALL(borrower, _fetch_shard_info_from_remote(7001))
            .WillOnce(::testing::Return(make_s3_shard_info(7001, "s3://test_bucket/db/tbl/p7", 1)));
    auto handle = borrower.get_shard_filesystem(7001, {});
    ASSERT_TRUE(handle.ok()) << handle.status();
}

TEST_F(StarOSWorkerTest, expired_borrowed_shard_info_falls_back_to_starmgr) {
    StarOSWorker owner;
    auto info = make_s3_shard_info(4001, "s3://test_bucket/db/tbl/p4", 1);
    ASSERT_TRUE(owner.add_shard(info).ok());
    auto exported = owner.export_shard_info(4001);
    ASSERT_TRUE(exported.ok()) << exported.status();

    RemoteCountingStarOSWorker borrower;
    borrower._borrowed_shard_info_ttl_sec = 0;
    ASSERT_TRUE(borrower.borrow_shard_info(*exported).ok());
    EXPECT_FALSE(borrower.has_borrowed_shard_info(4001));
    EXPECT_CALL(borrower, _fetch_shard_info_from_remote(4001)).WillOnce(::testing::Return(info));
    auto retrieved = borrower.retrieve_shard_info(4001);
    ASSERT_TRUE(retrieved.ok()) << retrieved.status();
}

// The switch is a kill switch: off, it stops using what was borrowed before too, not only new borrowing.
TEST_F(StarOSWorkerTest, borrowed_shard_info_unused_while_handoff_disabled) {
    StarOSWorker owner;
    auto info = make_s3_shard_info(5001, "s3://test_bucket/db/tbl/p5", 1);
    ASSERT_TRUE(owner.add_shard(info).ok());
    auto exported = owner.export_shard_info(5001);
    ASSERT_TRUE(exported.ok()) << exported.status();

    RemoteCountingStarOSWorker borrower;
    ASSERT_TRUE(borrower.borrow_shard_info(*exported).ok());
    ASSERT_TRUE(borrower.has_borrowed_shard_info(5001));
    {
        SCOPED_UPDATE(bool, config::lake_enable_tablet_location_handoff, false);
        EXPECT_FALSE(borrower.has_borrowed_shard_info(5001));
        EXPECT_CALL(borrower, _fetch_shard_info_from_remote(5001)).WillOnce(::testing::Return(info));
        ASSERT_TRUE(borrower.retrieve_shard_info(5001).ok());

        // Nothing handed over while off is kept.
        StarOSWorker other_borrower;
        ASSERT_TRUE(other_borrower.borrow_shard_info(*exported).ok());
        SCOPED_UPDATE(bool, config::lake_enable_tablet_location_handoff, true);
        EXPECT_FALSE(other_borrower.has_borrowed_shard_info(5001));
    }
    EXPECT_TRUE(borrower.has_borrowed_shard_info(5001));
}

// lake_tablet_location_handoff_entries is the number of borrowed shard infos held: a handoff adds one, a refresh
// replaces it, and an assignment to this worker, the LRU's eviction or the worker going away drops it.
TEST_F(StarOSWorkerTest, borrowed_shard_count_tracks_the_held_entries) {
    auto& count = StarOSWorkerMetrics::instance()->lake_tablet_location_handoff_entries;
    const int64_t base = count.value();
    StarOSWorker owner;
    std::vector<std::string> exported;
    for (int64_t id : {8001, 8002}) {
        ASSERT_TRUE(owner.add_shard(make_s3_shard_info(id, "s3://test_bucket/db/tbl/p8", 1)).ok());
        auto info = owner.export_shard_info(id);
        ASSERT_TRUE(info.ok()) << info.status();
        exported.push_back(std::move(info).value());
    }
    {
        StarOSWorker borrower;
        ASSERT_TRUE(borrower.borrow_shard_info(exported[0]).ok());
        ASSERT_TRUE(borrower.borrow_shard_info(exported[1]).ok());
        EXPECT_EQ(base + 2, count.value());
        // A refresh replaces the entry.
        ASSERT_TRUE(borrower.borrow_shard_info(exported[1]).ok());
        EXPECT_EQ(base + 2, count.value());
        // Assigned to this worker: no longer borrowed.
        ASSERT_TRUE(borrower.add_shard(make_s3_shard_info(8001, "s3://test_bucket/db/tbl/p8", 1)).ok());
        EXPECT_EQ(base + 1, count.value());
        EXPECT_FALSE(borrower.has_borrowed_shard_info(8001));
        EXPECT_TRUE(borrower.has_borrowed_shard_info(8002));
    }
    EXPECT_EQ(base, count.value());
}

// The borrowed shard infos live in a capacity-bounded LRU: what does not fit is evicted, with no sweep
// and no load needed to make room.
TEST_F(StarOSWorkerTest, borrowed_shard_infos_are_bounded_by_the_cache_capacity) {
    auto& count = StarOSWorkerMetrics::instance()->lake_tablet_location_handoff_entries;
    const int64_t base = count.value();
    StarOSWorker owner;
    ASSERT_TRUE(owner.add_shard(make_s3_shard_info(8101, "s3://test_bucket/db/tbl/p81", 1)).ok());
    auto exported = owner.export_shard_info(8101);
    ASSERT_TRUE(exported.ok()) << exported.status();

    StarOSWorker borrower;
    borrower._borrowed_cache.reset(new_lru_cache(1)); // smaller than any entry
    ASSERT_TRUE(borrower.borrow_shard_info(*exported).ok());
    EXPECT_FALSE(borrower.has_borrowed_shard_info(8101));
    EXPECT_EQ(base, count.value());
}

// The shard gets assigned to this worker while a handoff of it is between its ownership check and its
// insertion. The handoff must not leave a borrowed copy of a shard this worker owns behind.
TEST_F(StarOSWorkerTest, borrow_shard_info_racing_add_shard_keeps_no_copy_of_an_owned_shard) {
    StarOSWorker owner;
    auto info = make_s3_shard_info(9001, "s3://test_bucket/db/tbl/p9", 1);
    ASSERT_TRUE(owner.add_shard(info).ok());
    auto exported = owner.export_shard_info(9001);
    ASSERT_TRUE(exported.ok()) << exported.status();

    const int64_t base = StarOSWorkerMetrics::instance()->lake_tablet_location_handoff_entries.value();
    StarOSWorker worker;
    std::thread assign;
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&] {
        SyncPoint::GetInstance()->ClearCallBack("StarOSWorker::borrow_shard_info:ownership_checked");
        SyncPoint::GetInstance()->DisableProcessing();
        if (assign.joinable()) {
            assign.join();
        }
    });
    SyncPoint::GetInstance()->SetCallBack("StarOSWorker::borrow_shard_info:ownership_checked", [&](void*) {
        // StarMgr assigns the shard right after the check found it not owned. Wait until the
        // assignment is visible, so it lands between the check and the insertion.
        assign = std::thread([&] { ASSERT_TRUE(worker.add_shard(info).ok()); });
        while (!worker.get_shard_info(9001).ok()) {
            std::this_thread::yield();
        }
    });
    ASSERT_TRUE(worker.borrow_shard_info(*exported).ok());
    assign.join();

    ASSERT_TRUE(worker.get_shard_info(9001).ok());
    EXPECT_FALSE(worker.has_borrowed_shard_info(9001));
    EXPECT_EQ(base, StarOSWorkerMetrics::instance()->lake_tablet_location_handoff_entries.value());
}

TEST_F(StarOSWorkerTest, borrow_shard_info_rejects_malformed_input) {
    StarOSWorker worker;
    EXPECT_TRUE(absl::IsInvalidArgument(worker.borrow_shard_info("\xff\xff\xff")));
}

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
    auto starlet = std::make_shared<staros::starlet::Starlet>(worker);
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
        {"starlet_fslib_gcs_max_single_part_size", "fslib_gs_max_single_part_size"},
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
    SCOPED_UPDATE(int64_t, config::starlet_fslib_gcs_max_single_part_size, 13L << 20);
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
        SCOPED_UPDATE(int64_t, config::starlet_fslib_gcs_max_single_part_size, 0);
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

// s3_multipart_equal_part_size is shared with the BE S3 filesystem, so it has no `starlet_` prefix,
// but it reaches starlet through the same startup hook as the thresholds above.
TEST_F(StarOSWorkerTest, s3_multipart_equal_part_size_applied_at_startup) {
    gflags::FlagSaver flag_saver;

    auto configs = config::list_configs();
    auto it = std::find_if(configs.begin(), configs.end(),
                           [](const config::ConfigInfo& info) { return info.name == "s3_multipart_equal_part_size"; });
    ASSERT_NE(configs.end(), it);
    gflags::CommandLineFlagInfo flag_info;
    ASSERT_TRUE(gflags::GetCommandLineFlagInfo("fslib_s3_multipart_equal_part_size", &flag_info));
    EXPECT_EQ(flag_info.default_value, it->defval);

    // Start from the opposite value each time, so both directions are really assigned.
    for (bool value : {true, false}) {
        SCOPED_UPDATE(bool, config::s3_multipart_equal_part_size, value);
        FLAGS_fslib_s3_multipart_equal_part_size = !value;
        apply_starlet_upload_threshold_configs();
        EXPECT_EQ(value, FLAGS_fslib_s3_multipart_equal_part_size);
    }
}

// `shutdown_staros_worker()` releases the starlet runtime while an in-flight load may still be
// walking a StarOS-backed path. Every call that reaches starlet after that must report a status
// instead of dereferencing the released runtime. See issue #78883.
TEST_F(StarOSWorkerTest, starlet_calls_fail_after_runtime_release) {
    auto orig_starlet = swap_starlet_for_test(nullptr);
    DeferOp restore_starlet([&orig_starlet] { (void)swap_starlet_for_test(std::move(orig_starlet)); });
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
    auto orig_starlet = swap_starlet_for_test(std::make_shared<staros::starlet::Starlet>(worker));
    DeferOp restore_starlet([&orig_starlet] { (void)swap_starlet_for_test(std::move(orig_starlet)); });

    // An in-flight operation resolves the runtime before shutdown retires it.
    auto held = get_starlet();
    ASSERT_NE(nullptr, held);

    {
        // Stands in for shutdown: stop the runtime and drop the process-wide reference. `held` is
        // the only remaining owner, so the object must survive this scope.
        auto retired = swap_starlet_for_test(nullptr);
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
