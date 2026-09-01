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

#include "storage/lake/lake_replication_txn_manager.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <optional>
#include <random>
#include <thread>

#include "agent/master_info.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"
#include "util/countdown_latch.h"
#include "util/failpoint/fail_point.h"
#include "util/threadpool.h"
#ifdef USE_STAROS
#include <fslib/file.h>
#include <fslib/file_system.h>
#include <fslib/stat.h>
#endif
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "fs/bundle_file.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "fs/fs_starlet.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gutil/strings/join.h"
#include "runtime/exec_env.h"
#include "service/staros_worker.h"
#include "storage/chunk_helper.h"
#include "storage/file_stream_converter.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_reshard.h"
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
#include "storage/options.h"
#include "storage/protobuf_file.h"
#include "storage/replication_utils.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

// UT for shared data cross-cluster replication
class SharedDataReplicationTxnManagerTest : public testing::TestWithParam<KeysType> {
public:
    SharedDataReplicationTxnManagerTest() { _test_dir = kTestDirectory; }

    ~SharedDataReplicationTxnManagerTest() override = default;

protected:
    void SetUp() override {
        (void)fs::remove_all(_test_dir);
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kTxnLogDirectoryName)));
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_mgr = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 16384);
        _replication_txn_manager = std::make_unique<lake::LakeReplicationTxnManager>(_tablet_mgr.get());

        _src_tablet_metadata = generate_tablet_metadata(GetParam());
        _target_tablet_metadata = generate_tablet_metadata(GetParam());

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_src_tablet_metadata));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_target_tablet_metadata));

        _src_tablet_id = _src_tablet_metadata->id();
        _target_tablet_id = _target_tablet_metadata->id();
        // target visible version
        _version = _target_tablet_metadata->version();

        PFailPointTriggerMode trigger_mode;
        trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
        auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get(
                "table_schema_service_disable_remote_schema_for_load");
        if (fp != nullptr) {
            fp->setMode(trigger_mode);
        }
    }

    void TearDown() override {
        PFailPointTriggerMode trigger_mode;
        trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
        auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get(
                "table_schema_service_disable_remote_schema_for_load");
        if (fp != nullptr) {
            fp->setMode(trigger_mode);
        }

#ifdef USE_STAROS
        if (config::starlet_cache_dir.compare(0, 5, std::string("/tmp/")) == 0) {
            // Clean cache directory
            std::string cmd = fmt::format("rm -rf {}", config::starlet_cache_dir);
            ::system(cmd.c_str());
        }
#endif

        config::enable_transparent_data_encryption = false;

        // check primary index cache's ref
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        // check trash files already removed
        for (const auto& file : _trash_files) {
            EXPECT_FALSE(fs::path_exist(file));
        }
        ASSERT_OK(fs::remove_all(_test_dir));
    }

    std::shared_ptr<TabletMetadataPB> generate_tablet_metadata(KeysType keys_type) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(next_id());
        metadata->set_version(1);
        metadata->set_cumulative_point(0);
        metadata->set_next_rowset_id(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = metadata->mutable_schema();
        schema->set_keys_type(keys_type);
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            c1->set_aggregation(keys_type == DUP_KEYS ? "NONE" : "REPLACE");
        }
        return metadata;
    }

    Chunk generate_data(int64_t chunk_size, int shift, int update_ratio) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        std::vector<int> v2(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i + shift * chunk_size;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * update_ratio;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));

        for (int i = 0; i < chunk_size; i++) {
            v2[i] = v0[i] * 4;
        }
        auto c2 = Int32Column::create();
        c2->append_numbers(v2.data(), v2.size() * sizeof(int));
        return Chunk({std::move(c0), std::move(c1), std::move(c2)}, _slot_cid_map);
    }

    void write_src_tablet_data() {
        auto chunk0 = generate_data(kChunkSize, 0, 3);
        auto chunk1 = generate_data(kChunkSize, 0, 3);
        auto indexes = std::vector<uint32_t>(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) {
            indexes[i] = i;
        }

        auto version = 1;
        // normal write
        for (int i = 0; i < 3; i++) {
            auto txn_id = next_id();
            ASSIGN_OR_ABORT(auto delta_writer, lake::DeltaWriterBuilder()
                                                       .set_tablet_manager(_tablet_mgr.get())
                                                       .set_tablet_id(_src_tablet_id)
                                                       .set_txn_id(txn_id)
                                                       .set_partition_id(_src_partition_id)
                                                       .set_mem_tracker(_mem_tracker.get())
                                                       .set_schema_id(_src_tablet_metadata->schema().id())
                                                       .build());
            ASSERT_OK(delta_writer->open());
            ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
            ASSERT_OK(delta_writer->finish_with_txnlog());
            delta_writer->close();
            // Publish version
            auto txn_info = TxnInfoPB();
            txn_info.set_txn_id(txn_id);
            txn_info.set_combined_txn_log(false);
            txn_info.set_commit_time(0);
            auto txn_info_span = std::span<const TxnInfoPB>(&txn_info, 1);
            ASSERT_OK(lake::publish_version(_tablet_mgr.get(), PublishTabletInfo(_src_tablet_id), version, version + 1,
                                            txn_info_span, false));
            version++;
        }
        ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(_src_tablet_id, version));
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
        EXPECT_EQ(new_tablet_metadata->version(), 4);
        // src visible version
        _src_version = new_tablet_metadata->version();
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_replication";
    constexpr static int kChunkSize = 12;

    std::unique_ptr<TabletManager> _tablet_mgr;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    std::unique_ptr<lake::LakeReplicationTxnManager> _replication_txn_manager;

    int64_t _src_tablet_id = 10000;
    int64_t _target_tablet_id = 20000;

    std::shared_ptr<TabletMetadata> _src_tablet_metadata;
    std::shared_ptr<TabletMetadata> _target_tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    std::vector<std::string> _trash_files;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;

    std::string _test_dir;

    int64_t _transaction_id = 300;
    int64_t _table_id = 30001;
    int64_t _partition_id = 30002;
    int64_t _version = 1;
    int64_t _src_version = 1;
    int32_t _schema_hash = 368169781;
    int64_t _virtual_tablet_id = 40001;
    int64_t _src_db_id = 40002;
    int64_t _src_table_id = 40003;
    int64_t _src_partition_id = 40004;
};

TEST_P(SharedDataReplicationTxnManagerTest, test_replicate_no_missing_versions) {
    TReplicateSnapshotRequest request;
    request.__set_transaction_id(_transaction_id);
    request.__set_table_id(_table_id);
    request.__set_partition_id(_partition_id);
    request.__set_tablet_id(_target_tablet_id);
    request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    request.__set_schema_hash(_schema_hash);
    request.__set_visible_version(_version);
    request.__set_data_version(_version);
    // src tablet
    request.__set_src_tablet_id(_src_tablet_id);
    request.__set_src_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    request.__set_src_visible_version(_version); // same as `data_version`
    request.__set_src_db_id(_src_db_id);
    request.__set_src_table_id(_src_tablet_id);
    request.__set_src_partition_id(_src_partition_id);

    // virtual tablet
    request.__set_virtual_tablet_id(_virtual_tablet_id);

    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);
    EXPECT_FALSE(status.ok());
}

// Tests for LakeReplicationTxnManager::copy_non_segment_file_with_retry
class CopyNonSegmentFileWithRetryTest : public testing::Test {
protected:
    void SetUp() override {
        (void)fs::remove_all(_test_dir);
        CHECK_OK(fs::create_directories(_test_dir));
        SyncPoint::GetInstance()->EnableProcessing();
    }

    void TearDown() override {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
        (void)fs::remove_all(_test_dir);
    }

    Status create_test_file(const std::string& path, const std::string& content) {
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
        ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(opts, path));
        RETURN_IF_ERROR(wf->append(content));
        return wf->close();
    }

    static constexpr const char* kTestDirectory = "test_non_segment_copy_retry";
    std::string _test_dir = kTestDirectory;
};

TEST_F(CopyNonSegmentFileWithRetryTest, test_copy_success_no_retry_needed) {
    std::string src_path = lake::join_path(_test_dir, "test.sst");
    std::string dst_path = lake::join_path(_test_dir, "test_copy.sst");
    std::string content(4096, 'A');
    ASSERT_OK(create_test_file(src_path, content));

    ASSIGN_OR_ABORT(auto src_fs, FileSystem::CreateSharedFromString(src_path));
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};

    auto result = LakeReplicationTxnManager::copy_non_segment_file_with_retry(src_path, src_fs, dst_path, opts, 3);
    ASSERT_OK(result.status());
    EXPECT_EQ(*result, content.size());
}

TEST_F(CopyNonSegmentFileWithRetryTest, test_copy_error_retry_succeeds) {
    std::string src_path = lake::join_path(_test_dir, "test.sst");
    std::string dst_path = lake::join_path(_test_dir, "test_copy.sst");
    std::string content(4096, 'A');
    ASSERT_OK(create_test_file(src_path, content));

    int call_count = 0;
    SyncPoint::GetInstance()->SetCallBack("fs::copy_file", [&](void* arg) {
        auto* st = static_cast<Status*>(arg);
        if (call_count++ == 0) {
            *st = Status::IOError("Injected transient copy error");
        }
    });

    ASSIGN_OR_ABORT(auto src_fs, FileSystem::CreateSharedFromString(src_path));
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};

    auto result = LakeReplicationTxnManager::copy_non_segment_file_with_retry(src_path, src_fs, dst_path, opts, 3);
    ASSERT_OK(result.status());
    EXPECT_EQ(*result, content.size());
    EXPECT_EQ(call_count, 2);
}

TEST_F(CopyNonSegmentFileWithRetryTest, test_copy_error_exhausts_all_retries) {
    std::string src_path = lake::join_path(_test_dir, "test.delvec");
    std::string dst_path = lake::join_path(_test_dir, "test_copy.delvec");
    std::string content(4096, 'C');
    ASSERT_OK(create_test_file(src_path, content));

    SyncPoint::GetInstance()->SetCallBack("fs::copy_file", [&](void* arg) {
        auto* st = static_cast<Status*>(arg);
        *st = Status::IOError("Persistent copy error");
    });

    ASSIGN_OR_ABORT(auto src_fs, FileSystem::CreateSharedFromString(src_path));
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};

    auto result = LakeReplicationTxnManager::copy_non_segment_file_with_retry(src_path, src_fs, dst_path, opts, 3);
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_io_error()) << result.status();
}

TEST_F(CopyNonSegmentFileWithRetryTest, test_copy_size_mismatch_exhausts_retries) {
    std::string src_path = lake::join_path(_test_dir, "test.delvec");
    std::string dst_path = lake::join_path(_test_dir, "test_copy.delvec");
    std::string content(8192, 'B');
    ASSERT_OK(create_test_file(src_path, content));

    SyncPoint::GetInstance()->SetCallBack("lake_replication_non_segment_copy_size", [&](void* arg) {
        auto* size = static_cast<size_t*>(arg);
        *size = *size / 2;
    });

    ASSIGN_OR_ABORT(auto src_fs, FileSystem::CreateSharedFromString(src_path));
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};

    int max_retry = std::max(1, config::lake_replication_max_file_copy_retry);
    auto result =
            LakeReplicationTxnManager::copy_non_segment_file_with_retry(src_path, src_fs, dst_path, opts, max_retry);
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
    EXPECT_NE(std::string::npos, result.status().message().find("File size mismatch after copy"));
}

TEST_F(CopyNonSegmentFileWithRetryTest, test_copy_size_mismatch_then_succeeds) {
    std::string src_path = lake::join_path(_test_dir, "test.cols");
    std::string dst_path = lake::join_path(_test_dir, "test_copy.cols");
    std::string content(2048, 'D');
    ASSERT_OK(create_test_file(src_path, content));

    int call_count = 0;
    SyncPoint::GetInstance()->SetCallBack("lake_replication_non_segment_copy_size", [&](void* arg) {
        if (call_count++ == 0) {
            auto* size = static_cast<size_t*>(arg);
            *size = *size / 2;
        }
    });

    ASSIGN_OR_ABORT(auto src_fs, FileSystem::CreateSharedFromString(src_path));
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};

    auto result = LakeReplicationTxnManager::copy_non_segment_file_with_retry(src_path, src_fs, dst_path, opts, 3);
    ASSERT_OK(result.status());
    EXPECT_EQ(*result, content.size());
    EXPECT_EQ(call_count, 2);
}

TEST_F(CopyNonSegmentFileWithRetryTest, test_max_retry_clamped_to_at_least_one) {
    std::string src_path = lake::join_path(_test_dir, "test.del");
    std::string dst_path = lake::join_path(_test_dir, "test_copy.del");
    std::string content(1024, 'E');
    ASSERT_OK(create_test_file(src_path, content));

    ASSIGN_OR_ABORT(auto src_fs, FileSystem::CreateSharedFromString(src_path));
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};

    auto result = LakeReplicationTxnManager::copy_non_segment_file_with_retry(src_path, src_fs, dst_path, opts, 0);
    ASSERT_OK(result.status());
    EXPECT_EQ(*result, content.size());
}

class Int32ConfigGuard {
public:
    explicit Int32ConfigGuard(int32_t* config_ptr) : _config_ptr(config_ptr), _old_value(*config_ptr) {}
    ~Int32ConfigGuard() { *_config_ptr = _old_value; }

private:
    int32_t* _config_ptr;
    int32_t _old_value;
};

class BoolConfigGuard {
public:
    explicit BoolConfigGuard(bool* config_ptr) : _config_ptr(config_ptr), _old_value(*config_ptr) {}
    ~BoolConfigGuard() { *_config_ptr = _old_value; }

private:
    bool* _config_ptr;
    bool _old_value;
};

class Int64ConfigGuard {
public:
    explicit Int64ConfigGuard(int64_t* config_ptr) : _config_ptr(config_ptr), _old_value(*config_ptr) {}
    ~Int64ConfigGuard() { *_config_ptr = _old_value; }

private:
    int64_t* _config_ptr;
    int64_t _old_value;
};

TEST(LakeReplicationTaskRunnerTest, test_should_use_parallel_copy_basic_gate) {
    Int32ConfigGuard min_file_guard(&config::lake_replication_parallel_copy_min_file_count);
    config::lake_replication_parallel_copy_min_file_count = 2;
    EXPECT_FALSE(LakeReplicationTxnManager::should_use_parallel_copy(2, nullptr));

    std::unique_ptr<ThreadPool> pool;
    ASSERT_OK(ThreadPoolBuilder("lake_repl_parallel_gate")
                      .set_min_threads(1)
                      .set_max_threads(1)
                      .set_max_queue_size(8)
                      .build(&pool));
    EXPECT_FALSE(LakeReplicationTxnManager::should_use_parallel_copy(1, pool.get()));
    EXPECT_TRUE(LakeReplicationTxnManager::should_use_parallel_copy(2, pool.get()));
    pool->shutdown();
}

TEST(LakeReplicationTaskRunnerTest, test_should_use_parallel_copy_queue_overloaded) {
    Int32ConfigGuard min_file_guard(&config::lake_replication_parallel_copy_min_file_count);
    config::lake_replication_parallel_copy_min_file_count = 2;
    std::unique_ptr<ThreadPool> pool;
    ASSERT_OK(ThreadPoolBuilder("lake_repl_parallel_overload")
                      .set_min_threads(1)
                      .set_max_threads(1)
                      .set_max_queue_size(32)
                      .build(&pool));

    CountDownLatch block(1);
    ASSERT_OK(pool->submit_func([&]() { block.wait(); }));
    for (int i = 0; i < 9; ++i) {
        ASSERT_OK(pool->submit_func([&]() { block.wait(); }));
    }

    EXPECT_FALSE(LakeReplicationTxnManager::should_use_parallel_copy(20, pool.get()));
    block.count_down();
    pool->wait();
    pool->shutdown();
}

TEST(LakeReplicationTaskRunnerTest, test_should_use_parallel_copy_can_disable_by_config) {
    Int32ConfigGuard min_file_guard(&config::lake_replication_parallel_copy_min_file_count);
    config::lake_replication_parallel_copy_min_file_count = 0;

    std::unique_ptr<ThreadPool> pool;
    ASSERT_OK(ThreadPoolBuilder("lake_repl_parallel_disable")
                      .set_min_threads(1)
                      .set_max_threads(1)
                      .set_max_queue_size(8)
                      .build(&pool));

    EXPECT_FALSE(LakeReplicationTxnManager::should_use_parallel_copy(100, pool.get()));
    pool->shutdown();
}

// Regression test for the self-deadlock fix: an outer task running on one pool must be
// able to submit work into a DISTINCT inner pool and call ThreadPoolToken::wait() on it.
// This mirrors how the REPLICATE_SNAPSHOT agent task (outer pool) drives per-file copy
// sub-tasks on the dedicated `replicate_file` pool. If both ends were the same pool,
// ThreadPool::check_not_pool_thread_unlocked() would LOG(FATAL) and abort the process.
TEST(LakeReplicationTaskRunnerTest, test_outer_pool_can_wait_on_distinct_inner_pool) {
    std::unique_ptr<ThreadPool> outer_pool;
    ASSERT_OK(ThreadPoolBuilder("repl_outer_pool")
                      .set_min_threads(1)
                      .set_max_threads(1)
                      .set_max_queue_size(8)
                      .build(&outer_pool));
    std::unique_ptr<ThreadPool> file_pool;
    ASSERT_OK(ThreadPoolBuilder("repl_file_pool")
                      .set_min_threads(1)
                      .set_max_threads(2)
                      .set_max_queue_size(16)
                      .build(&file_pool));

    constexpr int kNumFiles = 8;
    std::atomic<int> done{0};
    // Capture per-iteration submit results in the worker and assert on the main thread.
    // gtest ASSERT_*/EXPECT_* from a non-test thread does not reliably fail the test —
    // gtest prints to stderr but the test process can still report success.
    std::vector<Status> inner_submit_status(kNumFiles);
    std::atomic<bool> outer_body_completed{false};
    ASSERT_OK(outer_pool->submit_func([&]() {
        auto token = file_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
        for (int i = 0; i < kNumFiles; ++i) {
            inner_submit_status[i] = token->submit_func([&]() { done.fetch_add(1, std::memory_order_relaxed); });
        }
        token->wait();
        outer_body_completed.store(true);
    }));
    outer_pool->wait();
    EXPECT_TRUE(outer_body_completed.load());
    for (const auto& s : inner_submit_status) {
        EXPECT_OK(s);
    }
    EXPECT_EQ(kNumFiles, done.load());
    outer_pool->shutdown();
    file_pool->shutdown();
}

#ifdef USE_STAROS
TEST(LakeReplicationTxnManagerTest, test_convert_s3_path_to_starlet_uri) {
    // Test case from user: convert S3 path to starlet URI
    std::string s3_path =
            "s3://cdp-hangzhou/cdp-hangzhou/5/186d104c-7078-4d21-ae3f-087873046b97/db135540/135542/135541/meta/"
            "0000000000021178_0000000000000002.meta";
    int64_t shard_id = 12345;

    std::string expected_uri =
            "staros://12345/cdp-hangzhou/5/186d104c-7078-4d21-ae3f-087873046b97/db135540/135542/135541/meta/"
            "0000000000021178_0000000000000002.meta";
    std::string actual_uri = lake::convert_s3_path_to_starlet_uri(s3_path, shard_id);

    EXPECT_EQ(expected_uri, actual_uri);
}

TEST(LakeReplicationTxnManagerTest, test_convert_s3_path_to_starlet_uri_edge_cases) {
    int64_t shard_id = 99999;

    // Edge case 1: S3 path without slash after bucket name (e.g., "s3://bucket")
    // Should produce starlet URI with empty path
    {
        std::string s3_path = "s3://bucket";
        std::string actual_uri = lake::convert_s3_path_to_starlet_uri(s3_path, shard_id);
        std::string expected_uri = build_starlet_uri(shard_id, "");
        EXPECT_EQ(expected_uri, actual_uri);
    }

    // Edge case 2: Path that doesn't start with "s3://"
    // Should fallback to using the entire path as-is
    {
        std::string non_s3_path = "hdfs://namenode/path/to/data";
        std::string actual_uri = lake::convert_s3_path_to_starlet_uri(non_s3_path, shard_id);
        std::string expected_uri = build_starlet_uri(shard_id, non_s3_path);
        EXPECT_EQ(expected_uri, actual_uri);
    }

    // Edge case 3: Empty S3 path
    {
        std::string empty_path;
        std::string actual_uri = lake::convert_s3_path_to_starlet_uri(empty_path, shard_id);
        std::string expected_uri = build_starlet_uri(shard_id, "");
        EXPECT_EQ(expected_uri, actual_uri);
    }
}
#endif

class TryBuildSourceTabletMetaWithFallbackTest : public testing::Test {
public:
    TryBuildSourceTabletMetaWithFallbackTest() = default;
    ~TryBuildSourceTabletMetaWithFallbackTest() override = default;

protected:
    void SetUp() override {
        (void)fs::remove_all(_test_dir);
        CHECK_OK(fs::create_directories(_test_dir));
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_mgr = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 16384);
        _replication_txn_manager = std::make_unique<lake::LakeReplicationTxnManager>(_tablet_mgr.get());

        // Create a simple tablet metadata for testing
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(_src_tablet_id);
        _tablet_metadata->set_version(_version);
        _tablet_metadata->set_next_rowset_id(1);
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_keys_type(DUP_KEYS);
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        auto c0 = schema->add_column();
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        // Create shared filesystem for testing
        auto fs_or = FileSystem::CreateSharedFromString(_test_dir);
        CHECK(fs_or.ok());
        _shared_fs = fs_or.value();
    }

    void TearDown() override {
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        ASSERT_OK(fs::remove_all(_test_dir));
    }

    // Helper to create metadata file at a specific path (for test purpose)
    // Uses ProtobufFile directly since TabletManager::put_tablet_metadata with custom path is private
    Status create_metadata_at_path(const std::string& meta_dir) {
        RETURN_IF_ERROR(fs::create_directories(meta_dir));
        auto filename = lake::tablet_metadata_filename(_src_tablet_id, _version);
        auto filepath = lake::join_path(meta_dir, filename);
        ProtobufFile file(filepath);
        return file.save(*_tablet_metadata);
    }

    // Build path formats for testing
    // Current format: {base}/db{db_id}/{table_id}/{partition_id}/meta
    std::string build_current_format_meta_dir() {
        return lake::join_path(_test_dir, fmt::format("db{}/{}/{}/meta", _src_db_id, _src_table_id, _src_partition_id));
    }
    std::string build_current_format_data_dir() {
        return lake::join_path(_test_dir, fmt::format("db{}/{}/{}/data", _src_db_id, _src_table_id, _src_partition_id));
    }

    // Legacy format 1: {base}/{table_id}/{partition_id}/meta (without db_id)
    std::string build_legacy1_format_meta_dir() {
        return lake::join_path(_test_dir, fmt::format("{}/{}/meta", _src_table_id, _src_partition_id));
    }
    std::string build_legacy1_format_data_dir() {
        return lake::join_path(_test_dir, fmt::format("{}/{}/data", _src_table_id, _src_partition_id));
    }

    // Legacy format 2: {base}/{table_id}/meta (without db_id and partition_id)
    std::string build_legacy2_format_meta_dir() {
        return lake::join_path(_test_dir, fmt::format("{}/meta", _src_table_id));
    }
    std::string build_legacy2_format_data_dir() {
        return lake::join_path(_test_dir, fmt::format("{}/data", _src_table_id));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_fallback_meta";

    std::unique_ptr<TabletManager> _tablet_mgr;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    std::unique_ptr<lake::LakeReplicationTxnManager> _replication_txn_manager;
    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<FileSystem> _shared_fs;

    std::string _test_dir = kTestDirectory;
    int64_t _src_tablet_id = 63457;
    int64_t _src_db_id = 56764;
    int64_t _src_table_id = 56970;
    int64_t _src_partition_id = 63453;
    int64_t _version = 2;
    TTransactionId _txn_id = 12345;
};

struct SourceBundleReadCounters {
    int read_all_calls = 0;
    int64_t read_at_fully_bytes = 0;
    int open_calls = 0;
    bool all_opens_skip_disk_cache = true;
};

class CountingSeekableInputStream final : public io::SeekableInputStreamWrapper {
public:
    CountingSeekableInputStream(std::shared_ptr<io::SeekableInputStream> stream, SourceBundleReadCounters* counters)
            : io::SeekableInputStreamWrapper(stream.get(), kDontTakeOwnership),
              _stream(std::move(stream)),
              _counters(counters) {}

    StatusOr<std::string> read_all() override {
        ++_counters->read_all_calls;
        return _stream->read_all();
    }

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        RETURN_IF_ERROR(_stream->read_at_fully(offset, out, count));
        _counters->read_at_fully_bytes += count;
        return Status::OK();
    }

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    SourceBundleReadCounters* _counters;
};

class CountingMemoryFileSystem final : public MemoryFileSystem {
public:
    explicit CountingMemoryFileSystem(SourceBundleReadCounters* counters) : _counters(counters) {}

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& url) override {
        ++_counters->open_calls;
        _counters->all_opens_skip_disk_cache &= opts.skip_disk_cache;
        ASSIGN_OR_RETURN(auto file, MemoryFileSystem::new_random_access_file(opts, url));
        auto stream = std::make_shared<CountingSeekableInputStream>(file->stream(), _counters);
        return std::make_unique<RandomAccessFile>(std::move(stream), url);
    }

private:
    SourceBundleReadCounters* _counters;
};

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_fallback_to_legacy2_format_success) {
    // Create metadata ONLY at legacy2 format path (without db_id and partition_id)
    // This forces all three attempts to be tried
    std::string legacy2_meta_dir = build_legacy2_format_meta_dir();
    std::string legacy2_data_dir = build_legacy2_format_data_dir();
    ASSERT_OK(create_metadata_at_path(legacy2_meta_dir));

    // Start with current format paths (which don't exist)
    std::string test_meta_dir = build_current_format_meta_dir();
    std::string test_data_dir = build_current_format_data_dir();

    // Verify initial paths contain db_id and partition_id
    EXPECT_NE(std::string::npos, test_meta_dir.find(fmt::format("db{}", _src_db_id)));
    EXPECT_NE(std::string::npos, test_meta_dir.find(std::to_string(_src_partition_id)));

    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, _version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    // Verify success and paths updated to legacy2 format
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(legacy2_meta_dir, test_meta_dir);
    EXPECT_EQ(legacy2_data_dir, test_data_dir);
    EXPECT_EQ(_src_tablet_id, result.value()->id());
    EXPECT_EQ(_version, result.value()->version());

    // Verify final paths don't contain db_id or partition_id
    EXPECT_EQ(std::string::npos, test_meta_dir.find(fmt::format("db{}", _src_db_id)));
    EXPECT_EQ(std::string::npos, test_meta_dir.find(std::to_string(_src_partition_id)));
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, test_all_attempts_fail_not_found) {
    // Don't create any metadata files - all attempts should fail
    std::string test_meta_dir = build_current_format_meta_dir();
    std::string test_data_dir = build_current_format_data_dir();

    auto result = _replication_txn_manager->try_build_source_tablet_meta_with_fallback(
            _src_tablet_id, _version, _src_db_id, _txn_id, test_meta_dir, test_data_dir, _shared_fs);

    // Verify failure with NotFound error
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_not_found()) << result.status();
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, reads_source_tablet_from_bundle) {
    ASSERT_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
    std::map<int64_t, TabletMetadataPB> tablet_metas;
    tablet_metas.emplace(_src_tablet_id, *_tablet_metadata);
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas));

    const std::string meta_dir = lake::join_path(_test_dir, lake::kMetadataDirectoryName);
    auto result = _replication_txn_manager->build_source_tablet_meta(_src_tablet_id, _version, meta_dir, _shared_fs);

    ASSERT_OK(result.status());
    EXPECT_EQ(_src_tablet_id, (*result)->id());
    EXPECT_EQ(_version, (*result)->version());
    ASSERT_TRUE((*result)->has_schema());
    EXPECT_EQ(1, (*result)->schema().column_size());
    EXPECT_EQ("c0", (*result)->schema().column(0).name());
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, reads_only_requested_source_bundle_page) {
    BoolConfigGuard checksum_guard(&config::lake_enable_protobuf_file_checksum);
    config::lake_enable_protobuf_file_checksum = false;

    ASSERT_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
    auto other_metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    other_metadata->set_id(_src_tablet_id + 1);
    auto* rowset = other_metadata->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(100);
    rowset->set_data_size(4096);
    for (int i = 0; i < 32; ++i) {
        rowset->add_segment_metas()->set_filename(fmt::format("segment-{}", i));
    }

    std::map<int64_t, TabletMetadataPB> tablet_metas;
    tablet_metas.emplace(_src_tablet_id, *_tablet_metadata);
    tablet_metas.emplace(other_metadata->id(), *other_metadata);
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas));

    const auto bundle_path = _tablet_mgr->bundle_tablet_metadata_location(_src_tablet_id, _version);
    ASSIGN_OR_ABORT(auto local_bundle_file, _shared_fs->new_random_access_file(bundle_path));
    ASSIGN_OR_ABORT(auto bundle_content, local_bundle_file->read_all());

    SourceBundleReadCounters counters;
    auto source_fs = std::make_shared<CountingMemoryFileSystem>(&counters);
    const std::string meta_dir = "/remote/source/meta";
    const auto source_bundle_path = lake::join_path(meta_dir, lake::tablet_metadata_filename(0, _version));
    ASSERT_OK(source_fs->create_dir_recursive(meta_dir));
    ASSERT_OK(source_fs->create_file(source_bundle_path));
    ASSERT_OK(source_fs->append_file(source_bundle_path, Slice(bundle_content)));

    auto result = _replication_txn_manager->build_source_tablet_meta(_src_tablet_id, _version, meta_dir, source_fs);

    ASSERT_OK(result.status());
    EXPECT_EQ(_src_tablet_id, (*result)->id());
    EXPECT_EQ(0, counters.read_all_calls);
    EXPECT_GT(counters.read_at_fully_bytes, 0);
    EXPECT_LT(counters.read_at_fully_bytes, bundle_content.size());
    EXPECT_GT(counters.open_calls, 0);
    EXPECT_TRUE(counters.all_opens_skip_disk_cache);
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, standalone_source_read_bypasses_disk_cache) {
    SourceBundleReadCounters counters;
    auto source_fs = std::make_shared<CountingMemoryFileSystem>(&counters);
    const std::string meta_dir = "/remote/source/meta";
    const auto metadata_path = lake::join_path(meta_dir, lake::tablet_metadata_filename(_src_tablet_id, _version));
    std::string content;
    ASSERT_TRUE(_tablet_metadata->SerializeToString(&content));
    ASSERT_OK(source_fs->create_dir_recursive(meta_dir));
    ASSERT_OK(source_fs->create_file(metadata_path));
    ASSERT_OK(source_fs->append_file(metadata_path, Slice(content)));

    auto result = _replication_txn_manager->build_source_tablet_meta(_src_tablet_id, _version, meta_dir, source_fs);

    ASSERT_OK(result.status());
    EXPECT_EQ(_src_tablet_id, (*result)->id());
    EXPECT_GT(counters.open_calls, 0);
    EXPECT_TRUE(counters.all_opens_skip_disk_cache);
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, rejects_malformed_source_bundle_envelope) {
    std::string oversized_footer(sizeof(uint64_t), '\0');
    oversized_footer[0] = 1;
    const std::vector<std::pair<std::string, std::string>> cases = {
            {"tiny", "is too small"},
            {oversized_footer, "Invalid source metadata bundle footer"},
    };

    const std::string meta_dir = "/remote/source/meta";
    const auto bundle_path = lake::join_path(meta_dir, lake::tablet_metadata_filename(0, _version));
    for (const auto& [content, expected_message] : cases) {
        SCOPED_TRACE(expected_message);
        auto source_fs = std::make_shared<MemoryFileSystem>();
        ASSERT_OK(source_fs->create_dir_recursive(meta_dir));
        ASSERT_OK(source_fs->create_file(bundle_path));
        ASSERT_OK(source_fs->append_file(bundle_path, Slice(content)));

        auto result = _replication_txn_manager->build_source_tablet_meta(_src_tablet_id, _version, meta_dir, source_fs);

        ASSERT_TRUE(result.status().is_corruption()) << result.status();
        EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr(expected_message));
    }
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, reports_missing_source_tablet_in_bundle) {
    ASSERT_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
    auto other_metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    other_metadata->set_id(_src_tablet_id + 1);
    std::map<int64_t, TabletMetadataPB> tablet_metas;
    tablet_metas.emplace(other_metadata->id(), *other_metadata);
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas));

    const std::string meta_dir = lake::join_path(_test_dir, lake::kMetadataDirectoryName);
    auto result = _replication_txn_manager->build_source_tablet_meta(_src_tablet_id, _version, meta_dir, _shared_fs);

    ASSERT_TRUE(result.status().is_not_found()) << result.status();
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("absent from source metadata bundle"));
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, rejects_source_bundle_page_with_mismatched_checksum) {
    BoolConfigGuard checksum_guard(&config::lake_enable_protobuf_file_checksum);
    config::lake_enable_protobuf_file_checksum = true;

    ASSERT_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
    std::map<int64_t, TabletMetadataPB> tablet_metas;
    tablet_metas.emplace(_src_tablet_id, *_tablet_metadata);
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas));

    const auto bundle_path = _tablet_mgr->bundle_tablet_metadata_location(_src_tablet_id, _version);
    ASSIGN_OR_ABORT(auto input_file, _shared_fs->new_random_access_file(bundle_path));
    ASSIGN_OR_ABORT(auto content, input_file->read_all());
    ASSERT_FALSE(content.empty());
    content[0] ^= 0xFF;
    WritableFileOptions opts{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_ABORT(auto output_file, _shared_fs->new_writable_file(opts, bundle_path));
    ASSERT_OK(output_file->append(Slice(content)));
    ASSERT_OK(output_file->close());

    const std::string meta_dir = lake::join_path(_test_dir, lake::kMetadataDirectoryName);
    auto result = _replication_txn_manager->build_source_tablet_meta(_src_tablet_id, _version, meta_dir, _shared_fs);

    ASSERT_TRUE(result.status().is_corruption()) << result.status();
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("Mismatched checksum"));
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, rejects_mismatched_source_tablet_id_in_bundle_page) {
    ASSERT_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
    auto mismatched_metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    mismatched_metadata->set_id(_src_tablet_id + 1);
    std::map<int64_t, TabletMetadataPB> tablet_metas;
    tablet_metas.emplace(_src_tablet_id, *mismatched_metadata);
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas));

    const std::string meta_dir = lake::join_path(_test_dir, lake::kMetadataDirectoryName);
    auto result = _replication_txn_manager->build_source_tablet_meta(_src_tablet_id, _version, meta_dir, _shared_fs);

    ASSERT_TRUE(result.status().is_corruption()) << result.status();
    EXPECT_THAT(std::string(result.status().message()), testing::HasSubstr("Tablet ID mismatch"));
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, does_not_hide_corrupt_source_metadata_with_bundle_fallback) {
    ASSERT_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
    std::map<int64_t, TabletMetadataPB> tablet_metas;
    tablet_metas.emplace(_src_tablet_id, *_tablet_metadata);
    ASSERT_OK(_tablet_mgr->put_bundle_tablet_metadata(tablet_metas));

    const std::string meta_dir = lake::join_path(_test_dir, lake::kMetadataDirectoryName);
    const auto metadata_path = lake::join_path(meta_dir, lake::tablet_metadata_filename(_src_tablet_id, _version));
    ASSIGN_OR_ABORT(auto output_file, _shared_fs->new_writable_file(metadata_path));
    ASSERT_OK(output_file->append(Slice("\xff", 1)));
    ASSERT_OK(output_file->close());

    auto result = _replication_txn_manager->build_source_tablet_meta(_src_tablet_id, _version, meta_dir, _shared_fs);

    EXPECT_TRUE(result.status().is_corruption()) << result.status();
}

TEST_F(TryBuildSourceTabletMetaWithFallbackTest, replication_source_read_bypasses_metacache) {
    _replication_txn_manager.reset();
    _tablet_mgr = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 1024 * 1024);
    _replication_txn_manager = std::make_unique<lake::LakeReplicationTxnManager>(_tablet_mgr.get());

    const std::string meta_dir = "/remote/source/meta";
    const auto metadata_path = lake::join_path(meta_dir, lake::tablet_metadata_filename(_src_tablet_id, _version));
    auto stale_metadata = std::make_shared<TabletMetadata>();
    stale_metadata->set_id(_src_tablet_id);
    stale_metadata->set_version(_version);
    stale_metadata->set_gtid(101);
    _tablet_mgr->metacache()->cache_tablet_metadata(metadata_path, stale_metadata);

    auto source_fs_a = std::make_shared<MemoryFileSystem>();
    auto source_fs_b = std::make_shared<MemoryFileSystem>();
    auto write_source_metadata = [&](const std::shared_ptr<MemoryFileSystem>& source_fs, int64_t gtid) {
        auto source_metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
        source_metadata->set_gtid(gtid);
        std::string content;
        CHECK(source_metadata->SerializeToString(&content));
        CHECK_OK(source_fs->create_dir_recursive(meta_dir));
        CHECK_OK(source_fs->create_file(metadata_path));
        CHECK_OK(source_fs->append_file(metadata_path, Slice(content)));
    };
    write_source_metadata(source_fs_a, 202);
    write_source_metadata(source_fs_b, 303);

    auto result_a = _replication_txn_manager->build_source_tablet_meta(_src_tablet_id, _version, meta_dir, source_fs_a);
    auto result_b = _replication_txn_manager->build_source_tablet_meta(_src_tablet_id, _version, meta_dir, source_fs_b);
    ASSERT_OK(result_a);
    ASSERT_OK(result_b);
    EXPECT_EQ(202, result_a.value()->gtid());
    EXPECT_EQ(303, result_b.value()->gtid());
    auto cached = _tablet_mgr->metacache()->lookup_tablet_metadata(metadata_path);
    ASSERT_NE(nullptr, cached);
    EXPECT_EQ(101, cached->gtid());
}

class LakeReplicationMetadataConversionTest : public testing::Test {
protected:
    void SetUp() override {
        (void)fs::remove_all(_test_dir);
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kTxnLogDirectoryName)));
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_mgr = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 16384);
        _replication_txn_manager = std::make_unique<lake::LakeReplicationTxnManager>(_tablet_mgr.get());
    }

    void TearDown() override {
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        ASSERT_OK(fs::remove_all(_test_dir));
    }

    std::shared_ptr<TabletMetadata> make_metadata(int64_t tablet_id, int64_t version, bool range_table = false) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(version);
        metadata->set_next_rowset_id(1);
        auto* schema = metadata->mutable_schema();
        schema->set_keys_type(DUP_KEYS);
        schema->set_id(1);
        schema->set_num_short_key_columns(1);
        auto* column = schema->add_column();
        column->set_unique_id(1);
        column->set_name("c0");
        column->set_type("INT");
        column->set_is_key(true);
        column->set_is_nullable(false);
        if (range_table) {
            metadata->mutable_range();
        }
        return metadata;
    }

    static std::string file_name(int id, std::string_view extension) {
        return fmt::format("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-{:012d}.{}", id, extension);
    }

    static void add_file_set(TabletMetadata* metadata, int base, bool shared) {
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(base + 1);
        rowset->set_overlapped(false);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(file_name(base + 1, "dat"));
        segment->set_size(11);
        segment->set_shared(shared);
        auto* del = rowset->add_del_files();
        del->set_name(file_name(base + 2, "del"));
        del->set_shared(shared);

        auto* sst = metadata->mutable_sstable_meta()->add_sstables();
        sst->set_filename(file_name(base + 3, "sst"));
        sst->set_shared(shared);

        auto& delvec = (*metadata->mutable_delvec_meta()->mutable_version_to_file())[base + 4];
        delvec.set_name(file_name(base + 4, "delvec"));
        delvec.set_shared(shared);

        auto& dcg = (*metadata->mutable_dcg_meta()->mutable_dcgs())[base + 5];
        dcg.add_column_files(file_name(base + 5, "cols"));
        dcg.add_shared_files(shared);
    }

    static void expect_file_set_shared(const TabletMetadataPB& metadata, int set_index, int base, bool shared) {
        EXPECT_EQ(shared, metadata.rowsets(set_index).segment_metas(0).shared());
        EXPECT_EQ(shared, metadata.rowsets(set_index).del_files(0).shared());
        EXPECT_EQ(shared, metadata.sstable_meta().sstables(set_index).shared());
        EXPECT_EQ(shared, metadata.delvec_meta().version_to_file().at(base + 4).shared());
        EXPECT_EQ(shared, metadata.dcg_meta().dcgs().at(base + 5).shared_files(0));
    }

    static void set_file_set_encryption_meta(TabletMetadata* metadata, int set_index, int base,
                                             std::string_view prefix) {
        auto* rowset = metadata->mutable_rowsets(set_index);
        rowset->mutable_segment_metas(0)->set_encryption_meta(fmt::format("{}-segment", prefix));
        rowset->mutable_del_files(0)->set_encryption_meta(fmt::format("{}-del", prefix));
        metadata->mutable_sstable_meta()->mutable_sstables(set_index)->set_encryption_meta(
                fmt::format("{}-sst", prefix));
        (*metadata->mutable_delvec_meta()->mutable_version_to_file())[base + 4].set_encryption_meta(
                fmt::format("{}-delvec", prefix));
        auto& dcg = (*metadata->mutable_dcg_meta()->mutable_dcgs())[base + 5];
        dcg.clear_encryption_metas();
        dcg.add_encryption_metas(fmt::format("{}-dcg", prefix));
    }

    static std::array<std::string, 5> file_set_encryption_meta(const TabletMetadataPB& metadata, int set_index,
                                                               int base) {
        return {metadata.rowsets(set_index).segment_metas(0).encryption_meta(),
                metadata.rowsets(set_index).del_files(0).encryption_meta(),
                metadata.sstable_meta().sstables(set_index).encryption_meta(),
                metadata.delvec_meta().version_to_file().at(base + 4).encryption_meta(),
                metadata.dcg_meta().dcgs().at(base + 5).encryption_metas(0)};
    }

    static void seed_test_encryption_keys() {
        EncryptionKeyPB pb;
        pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
        pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
        pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
        pb.set_plain_key("0000000000000000");
        std::unique_ptr<EncryptionKey> root_encryption_key = EncryptionKey::create_from_pb(pb).value();
        auto val_st = root_encryption_key->generate_key();
        ASSERT_TRUE(val_st.ok());
        std::unique_ptr<EncryptionKey> encryption_key = std::move(val_st.value());
        encryption_key->set_id(2);
        KeyCache::instance().add_key(root_encryption_key);
        KeyCache::instance().add_key(encryption_key);
    }

    StatusOr<std::shared_ptr<TabletMetadataPB>> convert(
            const TabletMetadataPtr& source, const TabletMetadataPtr& target, int64_t data_version,
            const std::string& source_data_dir, std::unordered_map<std::string, size_t>* segment_sizes = nullptr,
            std::map<std::string, std::string>* file_locations_out = nullptr,
            std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>* filename_map_out = nullptr,
            LakeReplicationTxnManager::SourceEncryptionMetaMap* source_encryption_metas_out = nullptr) {
        std::unordered_map<std::string, size_t> local_segment_sizes;
        std::map<std::string, std::string> local_file_locations;
        std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> local_filename_map;
        LakeReplicationTxnManager::SourceEncryptionMetaMap local_source_encryption_metas;
        return _replication_txn_manager->convert_and_build_new_tablet_meta(
                source, target, source->id(), target->id(), 70001, data_version, source_data_dir,
                segment_sizes != nullptr ? *segment_sizes : local_segment_sizes,
                file_locations_out != nullptr ? *file_locations_out : local_file_locations,
                filename_map_out != nullptr ? *filename_map_out : local_filename_map,
                source_encryption_metas_out != nullptr ? *source_encryption_metas_out : local_source_encryption_metas);
    }

    static Status write_file(const std::string& path, std::string_view content) {
        ASSIGN_OR_RETURN(auto local_fs, FileSystem::CreateSharedFromString(path));
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto output, local_fs->new_writable_file(opts, path));
        RETURN_IF_ERROR(output->append(content));
        return output->close();
    }

    static StatusOr<std::string> read_file(const std::string& path) {
        ASSIGN_OR_RETURN(auto local_fs, FileSystem::CreateSharedFromString(path));
        ASSIGN_OR_RETURN(auto input, local_fs->new_random_access_file(path));
        ASSIGN_OR_RETURN(auto size, input->get_size());
        std::string content(size, '\0');
        RETURN_IF_ERROR(input->read_at_fully(0, content.data(), size));
        return content;
    }

    static constexpr const char* kTestDirectory = "test_lake_replication_metadata_conversion";
    std::string _test_dir = kTestDirectory;
    std::shared_ptr<lake::FixedLocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    std::unique_ptr<lake::TabletManager> _tablet_mgr;
    std::unique_ptr<lake::LakeReplicationTxnManager> _replication_txn_manager;
};

TEST_F(LakeReplicationMetadataConversionTest, target_split_child_without_data_version_metadata) {
    auto source = make_metadata(51001, 3);
    auto target = make_metadata(51002, 2, true);
    const std::string source_filename = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000081.dat";
    const std::string target_filename = "00000000000000ff_aaaaaaaa-bbbb-cccc-dddd-000000000081.dat";
    auto* source_rowset = source->add_rowsets();
    source_rowset->set_id(1);
    source_rowset->add_segment_metas()->set_filename(source_filename);
    auto* target_rowset = target->add_rowsets();
    target_rowset->set_id(1);
    auto* target_segment = target_rowset->add_segment_metas();
    target_segment->set_filename(target_filename);
    target_segment->set_shared(true);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));

    std::map<std::string, std::string> file_locations;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;
    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"), nullptr, &file_locations,
                          &filename_map);
    ASSERT_OK(result.status());
    ASSERT_EQ(1, (*result)->rowsets_size());
    ASSERT_EQ(1, (*result)->rowsets(0).segment_metas_size());
    EXPECT_EQ(target_filename, (*result)->rowsets(0).segment_metas(0).filename());
    EXPECT_TRUE((*result)->rowsets(0).segment_metas(0).shared());
    EXPECT_TRUE(file_locations.empty());
    EXPECT_TRUE(filename_map.empty());
}

TEST_F(LakeReplicationMetadataConversionTest,
       target_split_child_without_data_version_metadata_current_version_not_newer) {
    auto source = make_metadata(51101, 3);
    auto target = make_metadata(51102, 1, true);

    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_not_found()) << result.status();
}

TEST_F(LakeReplicationMetadataConversionTest, target_hash_tablet_without_data_version_metadata) {
    auto source = make_metadata(52001, 3);
    auto target = make_metadata(52002, 2);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));

    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_not_found()) << result.status();
}

TEST_F(LakeReplicationMetadataConversionTest, shared_file_ownership_matrix) {
    auto source = make_metadata(53001, 2);
    auto target = make_metadata(53002, 1);
    add_file_set(target.get(), 0, true);
    add_file_set(target.get(), 20, false);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));

    // Reuse two destination file sets with opposite ownership, then add a source-shared set
    // that must become private because it is copied into this destination for the first time.
    add_file_set(source.get(), 0, false);
    add_file_set(source.get(), 20, true);
    add_file_set(source.get(), 40, true);

    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
    ASSERT_OK(result.status());
    expect_file_set_shared(**result, 0, 0, true);
    expect_file_set_shared(**result, 1, 20, false);
    expect_file_set_shared(**result, 2, 40, false);
    EXPECT_TRUE((*result)->sstable_meta().sstables(2).filename().ends_with(".sst"));
}

TEST_F(LakeReplicationMetadataConversionTest, records_exact_private_source_encryption_metadata_for_all_file_types) {
    auto source = make_metadata(53005, 2);
    auto target = make_metadata(53006, 1);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));
    add_file_set(source.get(), 0, false);
    set_file_set_encryption_meta(source.get(), 0, 0, "source");
    add_file_set(source.get(), 20, false);

    LakeReplicationTxnManager::SourceEncryptionMetaMap source_encryption_metas;
    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"), nullptr, nullptr, nullptr,
                          &source_encryption_metas);
    ASSERT_OK(result.status());
    ASSERT_EQ(10, source_encryption_metas.size());
    const std::array<std::string, 5> encrypted_metas = {"source-segment", "source-del", "source-sst", "source-delvec",
                                                        "source-dcg"};
    const std::array<std::string, 5> encrypted_filenames = {file_name(1, "dat"), file_name(2, "del"),
                                                            file_name(3, "sst"), file_name(4, "delvec"),
                                                            file_name(5, "cols")};
    for (size_t i = 0; i < encrypted_filenames.size(); ++i) {
        EXPECT_EQ(encrypted_metas[i], source_encryption_metas.at(encrypted_filenames[i]));
    }
    for (const auto& filename : {file_name(21, "dat"), file_name(22, "del"), file_name(23, "sst"),
                                 file_name(24, "delvec"), file_name(25, "cols")}) {
        EXPECT_EQ("", source_encryption_metas.at(filename));
    }
}

TEST_F(LakeReplicationMetadataConversionTest, rejects_empty_and_encrypted_metadata_for_one_private_file) {
    auto source = make_metadata(53007, 2);
    auto target = make_metadata(53008, 1);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));
    const auto filename = file_name(7, "dat");
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    rowset->add_segment_metas()->set_filename(filename);
    auto& delvec = (*source->mutable_delvec_meta()->mutable_version_to_file())[2];
    delvec.set_name(filename);
    delvec.set_encryption_meta("encrypted");

    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
}

TEST_F(LakeReplicationMetadataConversionTest, rejects_distinct_encrypted_metadata_for_one_private_file) {
    auto source = make_metadata(53009, 2);
    auto target = make_metadata(53010, 1);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));
    const auto filename = file_name(9, "dat");
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(filename);
    segment->set_encryption_meta("encrypted-a");
    auto& delvec = (*source->mutable_delvec_meta()->mutable_version_to_file())[2];
    delvec.set_name(filename);
    delvec.set_encryption_meta("encrypted-b");

    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
}

TEST_F(LakeReplicationMetadataConversionTest, range_shared_aggregate_conflicting_source_declarations_are_corruption) {
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;

    for (bool private_encrypted_first : {true, false}) {
        SCOPED_TRACE(fmt::format("private_encrypted_first={}", private_encrypted_first));
        auto source = make_metadata(private_encrypted_first ? 53041 : 53043, 2, true);
        auto target = make_metadata(private_encrypted_first ? 53042 : 53044, 1, true);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));
        const auto filename = file_name(private_encrypted_first ? 41 : 43, "dat");
        auto add_segment = [&](bool shared, const std::string& encryption_meta) {
            auto* rowset = source->add_rowsets();
            rowset->set_id(source->rowsets_size());
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(filename);
            segment->set_size(11);
            segment->set_shared(shared);
            segment->set_encryption_meta(encryption_meta);
        };
        if (private_encrypted_first) {
            add_segment(false, "private-encrypted-meta");
            add_segment(true, "");
        } else {
            add_segment(true, "");
            add_segment(false, "private-encrypted-meta");
        }

        auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_corruption()) << result.status();
    }
}

TEST_F(LakeReplicationMetadataConversionTest, bundled_aggregate_conflicting_source_declarations_are_corruption) {
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;

    for (bool private_encrypted_first : {true, false}) {
        SCOPED_TRACE(fmt::format("private_encrypted_first={}", private_encrypted_first));
        auto source = make_metadata(private_encrypted_first ? 53045 : 53047, 2);
        auto target = make_metadata(private_encrypted_first ? 53046 : 53048, 1);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));
        const auto filename = file_name(private_encrypted_first ? 45 : 47, "dat");
        auto add_segment = [&](bool bundled, const std::string& encryption_meta) {
            auto* rowset = source->add_rowsets();
            rowset->set_id(source->rowsets_size());
            auto* segment = rowset->add_segment_metas();
            segment->set_filename(filename);
            segment->set_size(11);
            if (bundled) {
                segment->set_bundle_file_offset(0);
            }
            segment->set_encryption_meta(encryption_meta);
        };
        if (private_encrypted_first) {
            add_segment(false, "private-encrypted-meta");
            add_segment(true, "");
        } else {
            add_segment(true, "");
            add_segment(false, "private-encrypted-meta");
        }

        auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_corruption()) << result.status();
    }
}

TEST_F(LakeReplicationMetadataConversionTest, range_shared_files_remain_shared_after_copy) {
    auto source = make_metadata(53011, 2, true);
    auto target = make_metadata(53012, 1, true);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));

    // A split range tablet can reference only a slice of a physical segment. The shared bit
    // also tells the reader to apply the tablet range, so clearing it after copying the file
    // would make every split child read the complete physical segment.
    add_file_set(source.get(), 0, true);

    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
    ASSERT_OK(result.status());
    expect_file_set_shared(**result, 0, 0, true);
}

TEST_F(LakeReplicationMetadataConversionTest, range_new_shared_files_with_tde_are_rejected) {
    seed_test_encryption_keys();
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = true;

    auto source = make_metadata(53021, 2, true);
    auto target = make_metadata(53022, 1, true);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));
    add_file_set(source.get(), 0, true);

    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_not_supported()) << result.status();
}

TEST_F(LakeReplicationMetadataConversionTest, range_new_source_encrypted_shared_file_is_rejected_without_target_tde) {
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;
    auto source = make_metadata(53023, 2, true);
    auto target = make_metadata(53024, 1, true);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(file_name(23, "dat"));
    segment->set_shared(true);
    segment->set_encryption_meta("deliberately-unresolvable-source-meta");

    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_not_supported()) << result.status();
}

TEST_F(LakeReplicationMetadataConversionTest, new_bundled_segments_with_tde_are_rejected) {
    seed_test_encryption_keys();
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = true;

    for (bool range_table : {true, false}) {
        auto source = make_metadata(range_table ? 53025 : 53027, 2, range_table);
        auto target = make_metadata(range_table ? 53026 : 53028, 1, range_table);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));
        auto* rowset = source->add_rowsets();
        rowset->set_id(1);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(file_name(25, "dat"));
        segment->set_size(11);
        segment->set_bundle_file_offset(0);
        segment->set_shared(false);

        auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
        ASSERT_FALSE(result.ok()) << "range_table=" << range_table;
        EXPECT_TRUE(result.status().is_not_supported()) << result.status();
    }
}

TEST_F(LakeReplicationMetadataConversionTest, new_source_encrypted_bundle_is_rejected_without_target_tde) {
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;
    auto source = make_metadata(53029, 2);
    auto target = make_metadata(53030, 1);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(file_name(29, "dat"));
    segment->set_size(11);
    segment->set_bundle_file_offset(0);
    segment->set_encryption_meta("deliberately-unresolvable-source-meta");

    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_not_supported()) << result.status();
}

TEST_F(LakeReplicationMetadataConversionTest, range_existing_encrypted_shared_files_reuse_when_target_tde_is_off) {
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;

    auto source = make_metadata(53031, 2, true);
    auto target = make_metadata(53032, 1, true);
    add_file_set(target.get(), 0, true);
    set_file_set_encryption_meta(target.get(), 0, 0, "target-reused");
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));

    add_file_set(source.get(), 0, true);
    set_file_set_encryption_meta(source.get(), 0, 0, "source-reused");

    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;
    LakeReplicationTxnManager::SourceEncryptionMetaMap source_encryption_metas;
    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"), nullptr, nullptr, &filename_map,
                          &source_encryption_metas);
    ASSERT_OK(result.status());
    EXPECT_TRUE(filename_map.empty());
    EXPECT_TRUE(source_encryption_metas.empty());
    const std::array<std::string, 5> expected_reused = {"target-reused-segment", "target-reused-del",
                                                        "target-reused-sst", "target-reused-delvec",
                                                        "target-reused-dcg"};
    EXPECT_EQ(expected_reused, file_set_encryption_meta(**result, 0, 0));
    expect_file_set_shared(**result, 0, 0, true);
}

TEST_F(LakeReplicationMetadataConversionTest, existing_encrypted_bundle_reuses_target_without_source_unwrap) {
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;
    auto source = make_metadata(53033, 2);
    auto target = make_metadata(53034, 1);
    const auto source_filename = file_name(33, "dat");
    const auto target_filename = fmt::format("00000000000000ff_{}", source_filename.substr(17));
    auto* target_rowset = target->add_rowsets();
    target_rowset->set_id(1);
    auto* target_segment = target_rowset->add_segment_metas();
    target_segment->set_filename(target_filename);
    target_segment->set_bundle_file_offset(0);
    target_segment->set_shared(true);
    target_segment->set_encryption_meta("target-encryption-meta");
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));

    auto* source_rowset = source->add_rowsets();
    source_rowset->set_id(1);
    auto* source_segment = source_rowset->add_segment_metas();
    source_segment->set_filename(source_filename);
    source_segment->set_bundle_file_offset(0);
    source_segment->set_size(11);
    source_segment->set_encryption_meta("deliberately-unresolvable-source-meta");

    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;
    LakeReplicationTxnManager::SourceEncryptionMetaMap source_encryption_metas;
    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"), nullptr, nullptr, &filename_map,
                          &source_encryption_metas);
    ASSERT_OK(result.status());
    EXPECT_TRUE(filename_map.empty());
    EXPECT_TRUE(source_encryption_metas.empty());
    ASSERT_EQ(1, (*result)->rowsets_size());
    ASSERT_EQ(1, (*result)->rowsets(0).segment_metas_size());
    EXPECT_EQ(target_filename, (*result)->rowsets(0).segment_metas(0).filename());
    EXPECT_EQ("target-encryption-meta", (*result)->rowsets(0).segment_metas(0).encryption_meta());
}

TEST_F(LakeReplicationMetadataConversionTest, existing_encrypted_bundle_reuses_per_slice_target_metadata) {
    seed_test_encryption_keys();
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;
    ASSIGN_OR_ABORT(auto target_pair0, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    ASSIGN_OR_ABORT(auto target_pair1, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    ASSERT_NE(target_pair0.info.key, target_pair1.info.key);

    auto source = make_metadata(53037, 2);
    auto target = make_metadata(53038, 1);
    const auto source_bundle_filename = file_name(37, "dat");
    const auto target_bundle_filename = fmt::format("00000000000000ff_{}", source_bundle_filename.substr(17));
    const auto target_bundle_path = _tablet_mgr->segment_location(target->id(), target_bundle_filename);
    const std::array<std::string, 2> plaintexts = {"first-target-bundle-slice", "second-target-bundle-slice"};

    BundleWritableFileContext bundle_context;
    WritableFileOptions bundle_opts{.sync_on_close = true,
                                    .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                    .encryption_info = target_pair0.info};
    ASSERT_OK(bundle_context.try_create_bundle_file(
            [&]() { return fs::new_writable_file(bundle_opts, target_bundle_path); }));
    BundleWritableFile writer0(&bundle_context, target_pair0.info);
    BundleWritableFile writer1(&bundle_context, target_pair1.info);
    bundle_context.increase_active_writers();
    bundle_context.increase_active_writers();
    ASSERT_OK(writer0.append(plaintexts[0]));
    ASSERT_OK(writer1.append(plaintexts[1]));
    ASSERT_OK(writer0.close());
    ASSERT_OK(writer1.close());
    ASSERT_OK(bundle_context.decrease_active_writers());
    ASSERT_OK(bundle_context.decrease_active_writers());
    ASSERT_EQ(0, writer0.bundle_file_offset());
    ASSERT_EQ(plaintexts[0].size(), writer1.bundle_file_offset());

    auto* target_rowset = target->add_rowsets();
    target_rowset->set_id(1);
    for (int i = 0; i < 2; ++i) {
        auto* segment = target_rowset->add_segment_metas();
        segment->set_filename(target_bundle_filename);
        segment->set_size(plaintexts[i].size());
        segment->set_bundle_file_offset(i == 0 ? writer0.bundle_file_offset() : writer1.bundle_file_offset());
        segment->set_encryption_meta(i == 0 ? target_pair0.encryption_meta : target_pair1.encryption_meta);
    }
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));

    auto* source_rowset = source->add_rowsets();
    source_rowset->set_id(1);
    for (int i = 0; i < 2; ++i) {
        auto* segment = source_rowset->add_segment_metas();
        segment->set_filename(source_bundle_filename);
        segment->set_size(plaintexts[i].size());
        segment->set_bundle_file_offset(i == 0 ? writer0.bundle_file_offset() : writer1.bundle_file_offset());
        segment->set_encryption_meta(fmt::format("deliberately-unresolvable-source-slice-{}", i));
    }

    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;
    LakeReplicationTxnManager::SourceEncryptionMetaMap source_encryption_metas;
    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"), nullptr, nullptr, &filename_map,
                          &source_encryption_metas);
    ASSERT_OK(result.status());
    EXPECT_TRUE(filename_map.empty());
    EXPECT_TRUE(source_encryption_metas.empty());

    const auto& output_segments = (*result)->rowsets(0).segment_metas();
    ASSERT_EQ(2, output_segments.size());
    EXPECT_EQ(target_bundle_filename, output_segments.Get(0).filename());
    EXPECT_EQ(target_bundle_filename, output_segments.Get(1).filename());
    EXPECT_EQ(target_pair0.encryption_meta, output_segments.Get(0).encryption_meta());
    EXPECT_EQ(target_pair1.encryption_meta, output_segments.Get(1).encryption_meta());

    ASSIGN_OR_ABORT(auto target_fs, FileSystem::CreateSharedFromString(target_bundle_path));
    for (int i = 0; i < 2; ++i) {
        ASSIGN_OR_ABORT(auto output_info,
                        KeyCache::instance().unwrap_encryption_meta(output_segments.Get(i).encryption_meta()));
        RandomAccessFileOptions read_opts{.encryption_info = output_info};
        FileInfo file_info{.path = target_bundle_path,
                           .size = output_segments.Get(i).size(),
                           .bundle_file_offset = output_segments.Get(i).bundle_file_offset()};
        ASSIGN_OR_ABORT(auto reader, target_fs->new_random_access_file_with_bundling(read_opts, file_info));
        ASSIGN_OR_ABORT(auto plaintext, reader->read_all());
        EXPECT_EQ(plaintexts[i], plaintext);
    }
}

TEST_F(LakeReplicationMetadataConversionTest, mixed_dcg_encryption_metadata_preserves_column_file_positions) {
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;
    auto source = make_metadata(53035, 2);
    auto target = make_metadata(53036, 1);

    const auto new_source_filename = file_name(35, "cols");
    const auto existing_source_filename = file_name(36, "cols");
    const auto existing_target_filename = fmt::format("00000000000000ff_{}", existing_source_filename.substr(17));
    auto& target_dcg = (*target->mutable_dcg_meta()->mutable_dcgs())[1];
    target_dcg.add_column_files(existing_target_filename);
    target_dcg.add_shared_files(false);
    target_dcg.add_encryption_metas("target-existing-dcg-meta");
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));

    auto& source_dcg = (*source->mutable_dcg_meta()->mutable_dcgs())[1];
    source_dcg.add_column_files(new_source_filename);
    source_dcg.add_column_files(existing_source_filename);
    source_dcg.add_shared_files(false);
    source_dcg.add_shared_files(false);
    source_dcg.add_encryption_metas("");
    source_dcg.add_encryption_metas("deliberately-unresolvable-existing-source-meta");

    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;
    LakeReplicationTxnManager::SourceEncryptionMetaMap source_encryption_metas;
    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"), nullptr, nullptr, &filename_map,
                          &source_encryption_metas);
    ASSERT_OK(result.status());
    const auto& result_dcg = (*result)->dcg_meta().dcgs().at(1);
    ASSERT_EQ(result_dcg.column_files_size(), result_dcg.encryption_metas_size());
    ASSERT_EQ(2, result_dcg.encryption_metas_size());
    EXPECT_EQ("", result_dcg.encryption_metas(0));
    EXPECT_EQ("target-existing-dcg-meta", result_dcg.encryption_metas(1));
    ASSERT_EQ(1, source_encryption_metas.size());
    EXPECT_EQ("", source_encryption_metas.at(new_source_filename));
    EXPECT_FALSE(source_encryption_metas.contains(existing_source_filename));
    ASSERT_EQ(1, filename_map.size());
    EXPECT_TRUE(filename_map.contains(new_source_filename));
}

TEST_F(LakeReplicationMetadataConversionTest, shared_file_ownership_matrix_tde_metadata) {
    seed_test_encryption_keys();
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = true;

    auto source = make_metadata(53101, 2);
    auto target = make_metadata(53102, 1);
    add_file_set(target.get(), 0, true);
    set_file_set_encryption_meta(target.get(), 0, 0, "target-reused");
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));

    add_file_set(source.get(), 0, false);
    set_file_set_encryption_meta(source.get(), 0, 0, "source-reused");
    add_file_set(source.get(), 40, true);
    set_file_set_encryption_meta(source.get(), 1, 40, "source-new");

    auto result = convert(source, target, 1, lake::join_path(_test_dir, "source_data"));
    ASSERT_OK(result.status());
    const std::array<std::string, 5> expected_reused = {"target-reused-segment", "target-reused-del",
                                                        "target-reused-sst", "target-reused-delvec",
                                                        "target-reused-dcg"};
    EXPECT_EQ(expected_reused, file_set_encryption_meta(**result, 0, 0));

    const auto new_encryption_meta = file_set_encryption_meta(**result, 1, 40);
    const std::array<std::string, 5> source_encryption_meta = {"source-new-segment", "source-new-del", "source-new-sst",
                                                               "source-new-delvec", "source-new-dcg"};
    for (size_t i = 0; i < new_encryption_meta.size(); ++i) {
        EXPECT_FALSE(new_encryption_meta[i].empty());
        EXPECT_NE(source_encryption_meta[i], new_encryption_meta[i]);
    }
    expect_file_set_shared(**result, 0, 0, true);
    expect_file_set_shared(**result, 1, 40, false);
}

TEST_F(LakeReplicationMetadataConversionTest, copies_complete_bundle_object) {
    auto source = make_metadata(54001, 2);
    auto target = make_metadata(54002, 1);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*target));

    const std::string bundle_name = file_name(61, "dat");
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    for (const auto [logical_size, offset] : {std::pair<int64_t, int64_t>{5, 0}, {7, 5}}) {
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(bundle_name);
        segment->set_size(logical_size);
        segment->set_bundle_file_offset(offset);
        segment->set_shared(true);
    }

    const std::string source_data_dir = lake::join_path(_test_dir, "source_data");
    ASSERT_OK(fs::create_directories(source_data_dir));
    const std::string source_path = lake::join_path(source_data_dir, bundle_name);
    const std::string physical_contents = "AAAAABBBBBBB-physical-tail";
    ASSERT_OK(write_file(source_path, physical_contents));

    std::unordered_map<std::string, size_t> segment_sizes;
    std::map<std::string, std::string> file_locations;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;
    auto result = convert(source, target, 1, source_data_dir, &segment_sizes, &file_locations, &filename_map);
    ASSERT_OK(result.status());
    ASSERT_EQ(1, filename_map.size());
    ASSERT_EQ(1, file_locations.size());
    EXPECT_EQ(5, (*result)->rowsets(0).segment_metas(0).size());
    EXPECT_EQ(0, (*result)->rowsets(0).segment_metas(0).bundle_file_offset());
    EXPECT_EQ(7, (*result)->rowsets(0).segment_metas(1).size());
    EXPECT_EQ(5, (*result)->rowsets(0).segment_metas(1).bundle_file_offset());

    const size_t source_size_for_copy = segment_sizes.contains(bundle_name) ? segment_sizes.at(bundle_name) : 0;
    ASSIGN_OR_ABORT(auto source_fs, FileSystem::CreateSharedFromString(source_path));
    const auto& target_path = file_locations.begin()->second;
    FileConverterCreatorFunc converter = [target_path](
                                                 const std::string& file_name,
                                                 uint64_t file_size) -> StatusOr<std::unique_ptr<FileStreamConverter>> {
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto output, fs::new_writable_file(opts, target_path));
        return std::make_unique<FileStreamConverter>(file_name, file_size, std::move(output));
    };
    size_t copied_size = 0;
    ASSERT_OK(ReplicationUtils::download_lake_file_with_converter(source_path, bundle_name, source_size_for_copy,
                                                                  source_fs, converter, &copied_size));
    EXPECT_EQ(physical_contents.size(), copied_size);
    ASSIGN_OR_ABORT(auto copied_contents, read_file(target_path));
    EXPECT_EQ(physical_contents, copied_contents);
}

#ifdef USE_STAROS
class InMemoryStarletInputStreamForReplication : public staros::starlet::fslib::InputStream {
public:
    explicit InMemoryStarletInputStreamForReplication(std::string contents, bool fail_read = false)
            : _contents(std::move(contents)), _fail_read(fail_read) {}

    bool support_seek() override { return true; }
    bool support_tell() override { return true; }
    bool support_size() override { return true; }

    absl::StatusOr<size_t> seek(int64_t offset, Anchor anchor) override {
        int64_t base = 0;
        if (anchor == CURRENT) {
            base = static_cast<int64_t>(_position);
        } else if (anchor == END) {
            base = static_cast<int64_t>(_contents.size());
        }
        const int64_t next = base + offset;
        if (next < 0 || next > static_cast<int64_t>(_contents.size())) {
            return absl::InvalidArgumentError("seek outside in-memory file");
        }
        _position = static_cast<size_t>(next);
        return _position;
    }

    absl::StatusOr<size_t> tell() override { return _position; }
    absl::StatusOr<size_t> size() override { return _contents.size(); }
    absl::Status close() override { return absl::OkStatus(); }

    absl::StatusOr<size_t> read(void* data, size_t length) override {
        if (_fail_read) {
            return absl::InternalError("injected source read failure");
        }
        const size_t bytes_to_read = std::min(length, _contents.size() - _position);
        std::memcpy(data, _contents.data() + _position, bytes_to_read);
        _position += bytes_to_read;
        return bytes_to_read;
    }

private:
    std::string _contents;
    size_t _position = 0;
    bool _fail_read = false;
};

class InMemoryStarletReadOnlyFileForReplication : public staros::starlet::fslib::ReadOnlyFile {
public:
    explicit InMemoryStarletReadOnlyFileForReplication(std::string contents, bool fail_read = false)
            : _stream(std::make_unique<InMemoryStarletInputStreamForReplication>(std::move(contents), fail_read)) {}

    const std::string& name() override { return _name; }
    absl::StatusOr<size_t> size() override { return _stream->size(); }
    absl::StatusOr<std::string> get_meta(std::string_view) override {
        return absl::UnimplementedError("get_meta not implemented");
    }
    absl::Status set_meta(std::string_view, std::string_view) override {
        return absl::UnimplementedError("set_meta not implemented");
    }
    absl::Status remove_meta(std::string_view) override {
        return absl::UnimplementedError("remove_meta not implemented");
    }
    absl::Status close() override { return absl::OkStatus(); }
    absl::StatusOr<staros::starlet::fslib::InputStream*> stream() override { return _stream.get(); }

private:
    std::unique_ptr<InMemoryStarletInputStreamForReplication> _stream;
    std::string _name = "in-memory-replication-source";
};

// Mock staros::starlet::fslib::FileSystem for SyncPoint injection
class MockStarletFileSystemForReplication : public staros::starlet::fslib::FileSystem {
public:
    MockStarletFileSystemForReplication() : staros::starlet::fslib::FileSystem() {}
    explicit MockStarletFileSystemForReplication(std::string contents)
            : staros::starlet::fslib::FileSystem(), _contents(std::move(contents)) {}
    MockStarletFileSystemForReplication(std::string contents, bool fail_read)
            : staros::starlet::fslib::FileSystem(), _contents(std::move(contents)), _fail_read(fail_read) {}
    ~MockStarletFileSystemForReplication() override = default;

    std::string_view scheme() override { return "mock"; }

    absl::StatusOr<std::unique_ptr<staros::starlet::fslib::ReadOnlyFile>> open(
            std::string_view path, const staros::starlet::fslib::ReadOptions& opts) override {
        if (_contents.has_value()) {
            ++_open_count;
            return std::unique_ptr<staros::starlet::fslib::ReadOnlyFile>(
                    new InMemoryStarletReadOnlyFileForReplication(*_contents, _fail_read));
        }
        return absl::UnimplementedError("MockStarletFileSystemForReplication::open not implemented");
    }

    int open_count() const { return _open_count; }

    absl::StatusOr<std::unique_ptr<staros::starlet::fslib::WritableFile>> create(
            std::string_view path, const staros::starlet::fslib::WriteOptions& opts) override {
        return absl::UnimplementedError("MockStarletFileSystemForReplication::create not implemented");
    }

    absl::StatusOr<bool> exists(std::string_view path) override { return false; }

    absl::Status rename_file(std::string_view src, std::string_view dest) override {
        return absl::UnimplementedError("not implemented");
    }

    absl::Status rename_dir(std::string_view src, std::string_view dest) override {
        return absl::UnimplementedError("not implemented");
    }

    absl::Status delete_file(std::string_view path) override { return absl::UnimplementedError("not implemented"); }

    absl::Status delete_files(absl::Span<const std::string> paths) override {
        return absl::UnimplementedError("not implemented");
    }

    absl::Status delete_dir(std::string_view path, bool recursive) override {
        return absl::UnimplementedError("not implemented");
    }

    absl::StatusOr<staros::starlet::fslib::Stat> stat(std::string_view path) override {
        if (_contents.has_value()) {
            staros::starlet::fslib::Stat result{};
            result.size = _contents->size();
            return result;
        }
        return absl::UnimplementedError("not implemented");
    }

    absl::Status hard_link(std::string_view src, std::string_view dest) override {
        return absl::UnimplementedError("not implemented");
    }

    absl::Status mkdir(std::string_view path, bool create_parent) override {
        return absl::UnimplementedError("not implemented");
    }

    absl::Status list_dir(std::string_view path, bool recursive,
                          std::function<bool(staros::starlet::fslib::EntryStat)> visitor,
                          std::string_view name_prefix) override {
        return absl::UnimplementedError("not implemented");
    }

protected:
    absl::Status initialize(const staros::starlet::fslib::Configuration& conf) override { return absl::OkStatus(); }

private:
    std::optional<std::string> _contents;
    bool _fail_read = false;
    int _open_count = 0;
};

// Test fixture for testing the USE_STAROS code path in replicate_lake_remote_storage
class LakeReplicationRemoteStorageTest : public testing::Test {
public:
    LakeReplicationRemoteStorageTest() { _test_dir = kTestDirectory; }
    ~LakeReplicationRemoteStorageTest() override = default;

protected:
    void SetUp() override {
        (void)fs::remove_all(_test_dir);
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kTxnLogDirectoryName)));
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_mgr = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 1024 * 1024);
        _replication_txn_manager = std::make_unique<lake::LakeReplicationTxnManager>(_tablet_mgr.get());

        _src_tablet_metadata = generate_simple_tablet_metadata(_src_tablet_id);
        _target_tablet_metadata = generate_simple_tablet_metadata(_target_tablet_id);

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_src_tablet_metadata));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_target_tablet_metadata));

        // Enable SyncPoint processing
        SyncPoint::GetInstance()->EnableProcessing();
    }

    void TearDown() override {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
        // Clearing the callbacks is not enough: new_fs_starlet() caches the filesystem this test
        // injected, keyed by shard id, for the life of the process. A later test using the same shard
        // id gets a cache HIT and returns before its own callback runs, so it silently inherits this
        // test's mock. Drop the cache with the callbacks that populated it.
        TEST_clear_shard_fs_cache();

        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        ASSERT_OK(fs::remove_all(_test_dir));
    }

    std::shared_ptr<TabletMetadataPB> generate_simple_tablet_metadata(int64_t tablet_id) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        metadata->set_cumulative_point(0);
        metadata->set_next_rowset_id(1);
        auto schema = metadata->mutable_schema();
        schema->set_keys_type(DUP_KEYS);
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_num_rows_per_row_block(65535);
        // Cross-cluster replication of the same logical table normally shares the column
        // unique-id space between source and target.
        auto c0 = schema->add_column();
        c0->set_unique_id(1);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        auto c1 = schema->add_column();
        c1->set_unique_id(2);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation("NONE");
        return metadata;
    }

    TReplicateSnapshotRequest build_request(bool with_full_path) {
        TReplicateSnapshotRequest request;
        request.__set_transaction_id(_transaction_id);
        request.__set_table_id(_table_id);
        request.__set_partition_id(_partition_id);
        request.__set_tablet_id(_target_tablet_id);
        request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
        request.__set_schema_hash(_schema_hash);
        request.__set_visible_version(1);
        // data_version < src_visible_version to ensure missed_versions is not empty
        request.__set_data_version(1);
        request.__set_src_tablet_id(_src_tablet_id);
        request.__set_src_tablet_type(TTabletType::TABLET_TYPE_LAKE);
        request.__set_src_visible_version(2); // > data_version so missed_versions is not empty
        request.__set_src_db_id(_src_db_id);
        request.__set_src_table_id(_src_table_id);
        request.__set_src_partition_id(_src_partition_id);
        request.__set_virtual_tablet_id(_virtual_tablet_id);

        if (with_full_path) {
            request.__set_src_partition_full_path("s3://test-bucket/path/to/db123/456/789");
        }

        return request;
    }

    static void seed_test_encryption_keys() {
        EncryptionKeyPB root_pb;
        root_pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
        root_pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
        root_pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
        root_pb.set_plain_key("0000000000000000");
        auto root_key = EncryptionKey::create_from_pb(root_pb).value();
        ASSIGN_OR_ABORT(auto kek, root_key->generate_key());
        kek->set_id(2);
        KeyCache::instance().add_key(root_key);
        KeyCache::instance().add_key(kek);
    }

    static FileEncryptionPair create_test_encryption_pair(const std::string& root_plain_key, int64_t kek_id) {
        KeyCache source_cache;
        EncryptionKeyPB root_pb;
        root_pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
        root_pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
        root_pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
        root_pb.set_plain_key(root_plain_key);
        auto root_key = EncryptionKey::create_from_pb(root_pb).value();
        auto kek = root_key->generate_key().value();
        kek->set_id(kek_id);
        source_cache.add_key(root_key);
        source_cache.add_key(kek);
        return source_cache.create_encryption_meta_pair_using_current_kek().value();
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_replication_remote";

    std::unique_ptr<TabletManager> _tablet_mgr;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    std::unique_ptr<lake::LakeReplicationTxnManager> _replication_txn_manager;

    std::shared_ptr<TabletMetadata> _src_tablet_metadata;
    std::shared_ptr<TabletMetadata> _target_tablet_metadata;

    int64_t _src_tablet_id = 50001;
    int64_t _target_tablet_id = 50002;
    int64_t _transaction_id = 60001;
    int64_t _table_id = 70001;
    int64_t _partition_id = 70002;
    int32_t _schema_hash = 368169781;
    int64_t _virtual_tablet_id = 80001;
    int64_t _src_db_id = 90001;
    int64_t _src_table_id = 90002;
    int64_t _src_partition_id = 90003;
    std::string _test_dir;
};

TEST_F(LakeReplicationRemoteStorageTest, EncryptedPrivateSourceSegmentIsReencrypted) {
    seed_test_encryption_keys();
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = true;
    auto& target_key_cache = KeyCache::instance();
    const auto target_cache_size = target_key_cache.size();
    ASSIGN_OR_ABORT(auto target_pair_before, target_key_cache.create_encryption_meta_pair_using_current_kek());
    EncryptionMetaPB target_meta_before;
    ASSERT_TRUE(target_meta_before.ParseFromString(target_pair_before.encryption_meta));
    ASSERT_EQ(3, target_meta_before.key_hierarchy_size());
    auto source_pair = create_test_encryption_pair("source-root-key!", 100);

    const std::string plaintext = "independent-source-segment-plaintext";
    const std::string encrypted_source_path = lake::join_path(_test_dir, "encrypted-source-segment");
    ASSIGN_OR_ABORT(auto local_fs, FileSystem::CreateSharedFromString(encrypted_source_path));
    WritableFileOptions source_write_opts{.sync_on_close = true,
                                          .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                          .encryption_info = source_pair.info};
    ASSIGN_OR_ABORT(auto source_output, local_fs->new_writable_file(source_write_opts, encrypted_source_path));
    ASSERT_OK(source_output->append(plaintext));
    ASSERT_OK(source_output->close());
    ASSIGN_OR_ABORT(auto raw_source_input, local_fs->new_random_access_file(encrypted_source_path));
    ASSIGN_OR_ABORT(auto source_ciphertext, raw_source_input->read_all());
    ASSERT_NE(plaintext, source_ciphertext);

    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(source_ciphertext);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    const std::string source_segment = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000e1.dat";
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(1);
    rowset->set_data_size(plaintext.size());
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(source_segment);
    segment->set_size(plaintext.size());
    segment->set_encryption_meta(source_pair.encryption_meta);
    src_meta_v2->set_next_rowset_id(2);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) { *static_cast<TabletMetadataPtr*>(arg) = src_meta_v2; });

    const auto original_master_info = get_master_info();
    TMasterInfo active_master_info = original_master_info;
    active_master_info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(active_master_info));
    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(_virtual_tablet_id + 81);
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request);
    (void)update_master_info(original_master_info);
    ASSERT_OK(status);

    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id));
    const auto& target_segment = txn_log->op_replication().tablet_metadata().rowsets(0).segment_metas(0);
    ASSERT_FALSE(target_segment.encryption_meta().empty());
    ASSIGN_OR_ABORT(auto target_info, KeyCache::instance().unwrap_encryption_meta(target_segment.encryption_meta()));
    RandomAccessFileOptions target_read_opts{.encryption_info = target_info};
    const auto target_path = _tablet_mgr->segment_location(_target_tablet_id, target_segment.filename());
    ASSIGN_OR_ABORT(auto target_input, local_fs->new_random_access_file(target_read_opts, target_path));
    ASSIGN_OR_ABORT(auto target_plaintext, target_input->read_all());
    EXPECT_EQ(plaintext, target_plaintext);

    EXPECT_EQ(target_cache_size, target_key_cache.size());
    ASSIGN_OR_ABORT(auto target_pair_after, target_key_cache.create_encryption_meta_pair_using_current_kek());
    EncryptionMetaPB target_meta_after;
    ASSERT_TRUE(target_meta_after.ParseFromString(target_pair_after.encryption_meta));
    ASSERT_EQ(3, target_meta_after.key_hierarchy_size());
    EXPECT_EQ(target_meta_before.key_hierarchy(0).SerializeAsString(),
              target_meta_after.key_hierarchy(0).SerializeAsString());
    EXPECT_EQ(target_meta_before.key_hierarchy(1).SerializeAsString(),
              target_meta_after.key_hierarchy(1).SerializeAsString());
}

TEST_F(LakeReplicationRemoteStorageTest, EncryptedPrivateSequentialSidecarsAreReencrypted) {
    seed_test_encryption_keys();
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = true;
    ASSIGN_OR_ABORT(auto source_pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());

    const std::string plaintext = "encrypted-private-sequential-plaintext";
    const std::string encrypted_source_path = lake::join_path(_test_dir, "encrypted-source-sidecar");
    ASSIGN_OR_ABORT(auto local_fs, FileSystem::CreateSharedFromString(encrypted_source_path));
    WritableFileOptions source_write_opts{.sync_on_close = true,
                                          .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                          .encryption_info = source_pair.info};
    ASSIGN_OR_ABORT(auto source_output, local_fs->new_writable_file(source_write_opts, encrypted_source_path));
    ASSERT_OK(source_output->append(plaintext));
    ASSERT_OK(source_output->close());
    ASSIGN_OR_ABORT(auto raw_source_input, local_fs->new_random_access_file(encrypted_source_path));
    ASSIGN_OR_ABORT(auto source_ciphertext, raw_source_input->read_all());

    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(source_ciphertext);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        *static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg) = mock_fs;
    });
    const std::string source_delvec = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000e2.delvec";
    const std::string source_sst = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000e3.sst";
    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    auto& delvec = (*source->mutable_delvec_meta()->mutable_version_to_file())[2];
    delvec.set_name(source_delvec);
    delvec.set_encryption_meta(source_pair.encryption_meta);
    auto* sst = source->mutable_sstable_meta()->add_sstables();
    sst->set_filename(source_sst);
    sst->set_encryption_meta(source_pair.encryption_meta);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) { *static_cast<TabletMetadataPtr*>(arg) = source; });

    const auto original_master_info = get_master_info();
    TMasterInfo active_master_info = original_master_info;
    active_master_info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(active_master_info));
    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(_virtual_tablet_id + 82);
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request);
    (void)update_master_info(original_master_info);
    ASSERT_OK(status);

    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id));
    const auto& replicated = txn_log->op_replication().tablet_metadata();
    const auto& target_delvec = replicated.delvec_meta().version_to_file().at(2);
    const auto& target_sst = replicated.sstable_meta().sstables(0);
    auto assert_reencrypted = [&](const auto& target_meta, const std::string& target_filename) {
        ASSERT_FALSE(target_meta.encryption_meta().empty());
        EXPECT_NE(source_pair.encryption_meta, target_meta.encryption_meta());
        ASSIGN_OR_ABORT(auto target_info, KeyCache::instance().unwrap_encryption_meta(target_meta.encryption_meta()));
        RandomAccessFileOptions target_read_opts{.encryption_info = target_info};
        const auto target_path = _tablet_mgr->segment_location(_target_tablet_id, target_filename);
        ASSIGN_OR_ABORT(auto target_input, local_fs->new_random_access_file(target_read_opts, target_path));
        ASSIGN_OR_ABORT(auto target_plaintext, target_input->read_all());
        EXPECT_EQ(plaintext, target_plaintext);
    };
    assert_reencrypted(target_delvec, target_delvec.name());
    assert_reencrypted(target_sst, target_sst.filename());
}

TEST_F(LakeReplicationRemoteStorageTest, UnencryptedPrivateSourceIsEncryptedAtTarget) {
    seed_test_encryption_keys();
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = true;
    const std::string plaintext = "unencrypted-source-encrypted-target";
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(plaintext);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        *static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg) = mock_fs;
    });
    const std::string source_segment = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000e4.dat";
    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(source_segment);
    segment->set_size(plaintext.size());
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) { *static_cast<TabletMetadataPtr*>(arg) = source; });

    const auto original_master_info = get_master_info();
    TMasterInfo active_master_info = original_master_info;
    active_master_info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(active_master_info));
    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(_virtual_tablet_id + 84);
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request);
    (void)update_master_info(original_master_info);
    ASSERT_OK(status);

    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id));
    const auto& target_segment = txn_log->op_replication().tablet_metadata().rowsets(0).segment_metas(0);
    ASSERT_FALSE(target_segment.encryption_meta().empty());
    ASSIGN_OR_ABORT(auto target_info, KeyCache::instance().unwrap_encryption_meta(target_segment.encryption_meta()));
    RandomAccessFileOptions target_read_opts{.encryption_info = target_info};
    const auto target_path = _tablet_mgr->segment_location(_target_tablet_id, target_segment.filename());
    ASSIGN_OR_ABORT(auto target_fs, FileSystem::CreateSharedFromString(target_path));
    ASSIGN_OR_ABORT(auto target_input, target_fs->new_random_access_file(target_read_opts, target_path));
    ASSIGN_OR_ABORT(auto target_plaintext, target_input->read_all());
    EXPECT_EQ(plaintext, target_plaintext);
}

TEST_F(LakeReplicationRemoteStorageTest, MalformedSourceEncryptionMetadataFailsBeforeCopyOrTxnLog) {
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>("unused-source");
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        *static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg) = mock_fs;
    });
    const std::string source_segment = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000e5.dat";
    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(source_segment);
    segment->set_size(13);
    segment->set_encryption_meta("not-an-encryption-meta-protobuf");
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) { *static_cast<TabletMetadataPtr*>(arg) = source; });

    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(_virtual_tablet_id + 85);
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(0, mock_fs->open_count());
    const auto target_path =
            _tablet_mgr->segment_location(_target_tablet_id, gen_filename_from(_transaction_id, source_segment));
    EXPECT_FALSE(fs::path_exist(target_path));
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
}

TEST_F(LakeReplicationRemoteStorageTest, MissingParentSourceEncryptionKeyFailsBeforeCopyOrTxnLog) {
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;
    EncryptionMetaPB meta_pb;
    auto* missing_parent = meta_pb.add_key_hierarchy();
    missing_parent->set_id(990001);
    missing_parent->set_parent_id(990000);
    missing_parent->set_type(EncryptionKeyTypePB::NORMAL_KEY);
    missing_parent->set_algorithm(EncryptionAlgorithmPB::AES_128);
    missing_parent->set_encrypted_key("0123456789abcdef");
    auto* child = meta_pb.add_key_hierarchy();
    child->set_parent_id(990001);
    child->set_type(EncryptionKeyTypePB::NORMAL_KEY);
    child->set_algorithm(EncryptionAlgorithmPB::AES_128);
    child->set_encrypted_key("fedcba9876543210");
    std::string missing_parent_meta;
    ASSERT_TRUE(meta_pb.SerializeToString(&missing_parent_meta));

    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>("unused-source");
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        *static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg) = mock_fs;
    });
    const std::string source_segment = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000e6.dat";
    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(source_segment);
    segment->set_size(13);
    segment->set_encryption_meta(missing_parent_meta);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) { *static_cast<TabletMetadataPtr*>(arg) = source; });

    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(_virtual_tablet_id + 86);
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(0, mock_fs->open_count());
    const auto target_path =
            _tablet_mgr->segment_location(_target_tablet_id, gen_filename_from(_transaction_id, source_segment));
    EXPECT_FALSE(fs::path_exist(target_path));
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
}

TEST_F(LakeReplicationRemoteStorageTest, PrivateSequentialSourceReadFailureCleansTarget) {
    Int32ConfigGuard retry_guard(&config::lake_replication_max_file_copy_retry);
    config::lake_replication_max_file_copy_retry = 1;
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>("source-sidecar", true);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        *static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg) = mock_fs;
    });
    const std::string source_sst = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000e7.sst";
    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    source->mutable_sstable_meta()->add_sstables()->set_filename(source_sst);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) { *static_cast<TabletMetadataPtr*>(arg) = source; });

    const auto original_master_info = get_master_info();
    TMasterInfo active_master_info = original_master_info;
    active_master_info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(active_master_info));
    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(_virtual_tablet_id + 87);
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request);
    (void)update_master_info(original_master_info);
    EXPECT_FALSE(status.ok());

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    const auto target_path =
            _tablet_mgr->segment_location(_target_tablet_id, gen_filename_from(_transaction_id, source_sst));
    EXPECT_FALSE(fs::path_exist(target_path));
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
}

TEST_F(LakeReplicationRemoteStorageTest, PrivateSequentialTargetCloseFailureCleansTarget) {
    Int32ConfigGuard retry_guard(&config::lake_replication_max_file_copy_retry);
    config::lake_replication_max_file_copy_retry = 1;
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>("source-sidecar");
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        *static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg) = mock_fs;
    });
    const std::string source_sst = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000e8.sst";
    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    source->mutable_sstable_meta()->add_sstables()->set_filename(source_sst);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) { *static_cast<TabletMetadataPtr*>(arg) = source; });
    TEST_ENABLE_ERROR_POINT("PosixFileSystem::close", Status::IOError("injected target close failure"));

    const auto original_master_info = get_master_info();
    TMasterInfo active_master_info = original_master_info;
    active_master_info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(active_master_info));
    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(_virtual_tablet_id + 88);
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request);
    (void)update_master_info(original_master_info);
    TEST_DISABLE_ERROR_POINT("PosixFileSystem::close");
    EXPECT_FALSE(status.ok());

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    const auto target_path =
            _tablet_mgr->segment_location(_target_tablet_id, gen_filename_from(_transaction_id, source_sst));
    EXPECT_FALSE(fs::path_exist(target_path));
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
}

TEST_F(LakeReplicationRemoteStorageTest, PrivateSequentialCopyIsCleanedWhenTxnLogWriteFails) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>("source-sidecar");
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        *static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg) = mock_fs;
    });
    const std::string source_sst = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000e9.sst";
    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    source->mutable_sstable_meta()->add_sstables()->set_filename(source_sst);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) { *static_cast<TabletMetadataPtr*>(arg) = source; });

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    auto* fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("put_txn_log_fail");
    ASSERT_NE(nullptr, fp);
    fp->setMode(trigger_mode);
    const auto original_master_info = get_master_info();
    TMasterInfo active_master_info = original_master_info;
    active_master_info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(active_master_info));
    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(_virtual_tablet_id + 89);
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request);
    (void)update_master_info(original_master_info);
    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(trigger_mode);
    EXPECT_TRUE(status.is_internal_error()) << status;

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    const auto target_path =
            _tablet_mgr->segment_location(_target_tablet_id, gen_filename_from(_transaction_id, source_sst));
    EXPECT_FALSE(fs::path_exist(target_path));
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
}

// Test Case 1: has_full_path=true, new_fs_starlet returns nullptr
TEST_F(LakeReplicationRemoteStorageTest, test_has_full_path_fs_creation_failure) {
    // SyncPoint makes new_fs_starlet return nullptr by setting an error status
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = absl::InternalError("Mock: failed to get shard filesystem");
    });

    auto request = build_request(true /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_NE(std::string::npos, status.message().find("Failed to create virtual starlet filesystem"));
}

TEST_F(LakeReplicationRemoteStorageTest, raw_s3_uses_virtual_shard_uri) {
    std::string captured_uri;
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::src_partition_starlet_uri",
                                          [&](void* arg) { captured_uri = *static_cast<std::string*>(arg); });
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = absl::InternalError("stop after URI construction");
    });

    auto request = build_request(true /* with_full_path */);
    ASSERT_NE(request.src_tablet_id, request.virtual_tablet_id);
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request);
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_EQ(convert_s3_path_to_starlet_uri(request.src_partition_full_path, request.virtual_tablet_id), captured_uri);
    EXPECT_NE(convert_s3_path_to_starlet_uri(request.src_partition_full_path, request.src_tablet_id), captured_uri);
}

TEST_F(LakeReplicationRemoteStorageTest, non_s3_uses_virtual_shard_uri_authority) {
    std::string captured_meta_dir;
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::src_meta_dir",
                                          [&](void* arg) { captured_meta_dir = *static_cast<std::string*>(arg); });
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = absl::InternalError("stop after source path construction");
    });

    auto request = build_request(false /* with_full_path */);
    ASSERT_NE(request.src_tablet_id, request.virtual_tablet_id);
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request);
    EXPECT_TRUE(status.is_corruption()) << status;

    RemoteStarletLocationProvider provider;
    const auto expected_meta_dir = provider.metadata_root_location(request.virtual_tablet_id, request.src_db_id,
                                                                   request.src_table_id, request.src_partition_id);
    const auto legacy_meta_dir = provider.metadata_root_location(request.src_tablet_id, request.src_db_id,
                                                                 request.src_table_id, request.src_partition_id);
    EXPECT_EQ(expected_meta_dir, captured_meta_dir);
    EXPECT_NE(legacy_meta_dir, captured_meta_dir);
    ASSIGN_OR_ABORT(auto expected_parsed, parse_starlet_uri(expected_meta_dir));
    ASSIGN_OR_ABORT(auto legacy_parsed, parse_starlet_uri(legacy_meta_dir));
    EXPECT_EQ(expected_parsed.first, legacy_parsed.first);
    EXPECT_EQ(request.virtual_tablet_id, expected_parsed.second);
    EXPECT_EQ(request.src_tablet_id, legacy_parsed.second);
}

// Test Case 2: has_full_path=false, new_fs_starlet returns nullptr
TEST_F(LakeReplicationRemoteStorageTest, test_no_full_path_fs_creation_failure) {
    // SyncPoint makes new_fs_starlet return nullptr by setting an error status
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = absl::InternalError("Mock: failed to get shard filesystem");
    });

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_NE(std::string::npos, status.message().find("Failed to create virtual starlet filesystem"));
}

// Test Case 3: has_full_path=true, new_fs_starlet returns valid fs, meta build fails
TEST_F(LakeReplicationRemoteStorageTest, test_has_full_path_meta_build_failure) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();

    // SyncPoint makes new_fs_starlet return a valid (but mock) filesystem
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto request = build_request(true /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    // The filesystem creation succeeds (not nullptr), but reading tablet metadata
    // via the mock filesystem will fail. The error should NOT be
    // "Failed to create virtual starlet filesystem".
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(std::string::npos, status.message().find("Failed to create virtual starlet filesystem"))
            << "Should have passed the nullptr check, error: " << status;
}

// Test Case 4: has_full_path=false, new_fs_starlet returns valid fs, meta build fails
TEST_F(LakeReplicationRemoteStorageTest, test_no_full_path_meta_build_failure) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();

    // SyncPoint makes new_fs_starlet return a valid (but mock) filesystem
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    // The filesystem creation succeeds (not nullptr), but reading tablet metadata
    // via the mock filesystem will fail. The error should NOT be
    // "Failed to create virtual starlet filesystem".
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(std::string::npos, status.message().find("Failed to create virtual starlet filesystem"))
            << "Should have passed the nullptr check, error: " << status;
}
// Test Case 5: has_full_path=true with non-S3 path should fail with InvalidArgument
TEST_F(LakeReplicationRemoteStorageTest, test_has_full_path_non_s3_type_rejected) {
    auto request = build_request(false /* with_full_path */);
    // Manually set a non-S3 full path (e.g., HDFS path)
    request.__set_src_partition_full_path("hdfs://namenode/path/to/data");

    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_invalid_argument()) << status;
    EXPECT_NE(std::string::npos, status.message().find("Full path must be S3 type"));
}

// Test Case 6: Fast cancel - when min_active_txn_id > txn_id, replication should abort
// before copying any files.
TEST_F(LakeReplicationRemoteStorageTest, test_fast_cancel_txn_aborted_before_copy) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();

    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    // Create source tablet metadata at version 2 with a rowset containing segment files.
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(1024);
    rowset->add_segment_metas()->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
    rowset->add_segment_metas()->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000002.dat");
    src_meta_v2->set_next_rowset_id(2);

    // Inject source tablet metadata via SyncPoint to avoid metacache dependency
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    // Save original master info and set min_active_txn_id > txn_id to trigger fast cancel
    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(_transaction_id + 1);
    ASSERT_TRUE(update_master_info(info));

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    // Restore original master info
    (void)update_master_info(original_master_info);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_aborted()) << status;
    EXPECT_NE(std::string::npos, status.message().find("Lake replication cancelled, transaction is aborted"));
}

// Test Case 7: Fast cancel - when min_active_txn_id advances during file copy (between
// iterations), replication should abort after copying some files but before all files.
TEST_F(LakeReplicationRemoteStorageTest, test_fast_cancel_txn_aborted_during_copy) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();

    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    // Create source tablet metadata at version 2 with a rowset containing segment files
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(1024);
    rowset->add_segment_metas()->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
    rowset->add_segment_metas()->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000002.dat");
    src_meta_v2->set_next_rowset_id(2);

    // Inject source tablet metadata via SyncPoint to avoid metacache dependency
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    // Save original master info. Start with min_active_txn_id <= txn_id (no abort yet).
    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    // In the before_copy SyncPoint callback, advance min_active_txn_id past txn_id
    // after the first file copy iteration. The next iteration's fast cancel check
    // will detect the abort.
    int before_copy_count = 0;
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_lake_remote_storage::before_copy",
                                          [&](void*) {
                                              before_copy_count++;
                                              if (before_copy_count == 1) {
                                                  TMasterInfo updated_info = get_master_info();
                                                  updated_info.__set_min_active_txn_id(_transaction_id + 1);
                                                  (void)update_master_info(updated_info);
                                              }
                                          });

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    // Restore original master info
    (void)update_master_info(original_master_info);

    // The first iteration's before_copy callback runs (file copy attempt happens but may
    // fail due to mock filesystem). Either:
    // a) The first file copy fails (IOError from mock fs) - this is acceptable, OR
    // b) If we somehow get past the first copy, the second iteration detects the abort.
    EXPECT_FALSE(status.ok());
    EXPECT_GE(before_copy_count, 1) << "SyncPoint before_copy should have been invoked at least once";
}

// Test Case 8: No fast cancel - when min_active_txn_id <= txn_id, the fast cancel check
// should NOT abort the replication (it should proceed to the file copy step).
TEST_F(LakeReplicationRemoteStorageTest, test_no_fast_cancel_when_txn_active) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();

    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    // Create source tablet metadata at version 2 with a rowset containing a segment file
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(1024);
    rowset->add_segment_metas()->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
    src_meta_v2->set_next_rowset_id(2);

    // Inject source tablet metadata via SyncPoint to avoid metacache dependency
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    // Set min_active_txn_id <= txn_id so fast cancel does NOT trigger
    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(_transaction_id); // equal, not greater
    ASSERT_TRUE(update_master_info(info));

    bool before_copy_invoked = false;
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_lake_remote_storage::before_copy",
                                          [&](void*) { before_copy_invoked = true; });

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    // Restore original master info
    (void)update_master_info(original_master_info);

    // The fast cancel check should NOT have triggered. The function should proceed past
    // the fast cancel check to the before_copy SyncPoint and then to file copy.
    // The file copy will fail due to the mock filesystem, but that's expected.
    EXPECT_TRUE(before_copy_invoked) << "before_copy SyncPoint should have been reached (fast cancel did not trigger)";
    EXPECT_FALSE(status.is_aborted()) << "Should not abort when min_active_txn_id <= txn_id, status: " << status;
}

// Test Case 9: Sequential copy with mocked file operations - covers task lambda body,
// segment download path, non-segment copy path, size tracking, encryption, slow log.
TEST_F(LakeReplicationRemoteStorageTest, test_sequential_copy_with_mocked_file_operations) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    // Create source metadata with segments (with segment_size) and delvec
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(4096);
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
        sm->set_size(1024); // src_file_size for segment 1
    }
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000002.dat");
        sm->set_size(2048); // src_file_size for segment 2
    }
    // Add a delvec for non-segment path
    auto* delvec_meta = src_meta_v2->mutable_delvec_meta();
    auto& delvec_entry = (*delvec_meta->mutable_version_to_file())[2];
    delvec_entry.set_name("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000003.delvec");
    src_meta_v2->set_next_rowset_id(2);

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    // Mock segment download: set final_file_size=2048 so seg1 triggers size_changes (1024!=2048)
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::download_segment",
                                          [&](void* arg) {
                                              auto* file_size = static_cast<size_t*>(arg);
                                              *file_size = 2048;
                                          });

    // Mock non-segment copy
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::copy_non_segment",
                                          [&](void* arg) {
                                              auto* file_size = static_cast<size_t*>(arg);
                                              *file_size = 512;
                                          });

    // Set slow log threshold to 0 to cover slow log path
    Int64ConfigGuard slow_log_guard(&config::lake_replication_slow_log_ms);
    config::lake_replication_slow_log_ms = 0;

    // Disable parallel to ensure sequential path
    Int32ConfigGuard min_file_guard(&config::lake_replication_parallel_copy_min_file_count);
    config::lake_replication_parallel_copy_min_file_count = 0;

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    (void)update_master_info(original_master_info);

    ASSERT_OK(status);
}

// Test Case 10: Parallel copy with mocked file operations - covers parallel branch,
// mutex-guarded segment_size_changes and files_to_delete paths.
TEST_F(LakeReplicationRemoteStorageTest, test_parallel_copy_with_mocked_file_operations) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(4096);
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
        sm->set_size(1024);
    }
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000002.dat");
        sm->set_size(2048);
    }
    auto* delvec_meta = src_meta_v2->mutable_delvec_meta();
    auto& delvec_entry = (*delvec_meta->mutable_version_to_file())[2];
    delvec_entry.set_name("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000003.delvec");
    src_meta_v2->set_next_rowset_id(2);

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::download_segment",
                                          [&](void* arg) {
                                              auto* file_size = static_cast<size_t*>(arg);
                                              *file_size = 2048;
                                          });

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::copy_non_segment",
                                          [&](void* arg) {
                                              auto* file_size = static_cast<size_t*>(arg);
                                              *file_size = 512;
                                          });

    // Enable parallel copy: min_file_count=2, we have 3 files (2 segments + 1 delvec)
    Int32ConfigGuard min_file_guard(&config::lake_replication_parallel_copy_min_file_count);
    config::lake_replication_parallel_copy_min_file_count = 2;

    // Create thread pool and assign to replication manager
    std::unique_ptr<ThreadPool> pool;
    ASSERT_OK(ThreadPoolBuilder("lake_repl_test_pool")
                      .set_min_threads(2)
                      .set_max_threads(4)
                      .set_max_queue_size(16)
                      .build(&pool));
    _replication_txn_manager->_replicate_file_thread_pool = pool.get();

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    (void)update_master_info(original_master_info);
    _replication_txn_manager->_replicate_file_thread_pool = nullptr;

    ASSERT_OK(status);
    pool->shutdown();
}

// Test Case 11: Parallel copy error handling - covers L370-376 (parallel error logging/return).
TEST_F(LakeReplicationRemoteStorageTest, test_parallel_copy_error_handling) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(4096);
    rowset->add_segment_metas()->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
    rowset->add_segment_metas()->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000002.dat");
    src_meta_v2->set_next_rowset_id(2);

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    // Do NOT register download_segment callback - actual download will fail with mock FS

    Int32ConfigGuard min_file_guard(&config::lake_replication_parallel_copy_min_file_count);
    config::lake_replication_parallel_copy_min_file_count = 2;

    std::unique_ptr<ThreadPool> pool;
    ASSERT_OK(ThreadPoolBuilder("lake_repl_test_err_pool")
                      .set_min_threads(2)
                      .set_max_threads(4)
                      .set_max_queue_size(16)
                      .build(&pool));
    _replication_txn_manager->_replicate_file_thread_pool = pool.get();

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);

    (void)update_master_info(original_master_info);
    _replication_txn_manager->_replicate_file_thread_pool = nullptr;

    // Parallel copy should fail because download_lake_file_with_converter fails with mock FS
    EXPECT_FALSE(status.ok());
    pool->shutdown();
}

TEST_F(LakeReplicationRemoteStorageTest, copies_complete_bundle_object_through_full_replication) {
    const std::string physical_contents = "AAAAABBBBBBB-physical-tail";
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(physical_contents);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    const std::string bundle_name = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000072.dat";
    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    for (const auto [logical_size, offset] : {std::pair<int64_t, int64_t>{5, 0}, {7, 5}}) {
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(bundle_name);
        segment->set_size(logical_size);
        segment->set_bundle_file_offset(offset);
        segment->set_shared(true);
    }
    source->set_next_rowset_id(2);

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = source;
                                          });
    int copy_count = 0;
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_lake_remote_storage::before_copy",
                                          [&](void*) { ++copy_count; });

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));
    auto request = build_request(false /* with_full_path */);
    // new_fs_starlet caches by shard id across tests in this process. Use a dedicated id so
    // earlier error-path tests cannot leave an unimplemented filesystem in this test's slot.
    request.__set_virtual_tablet_id(_virtual_tablet_id + 72);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);
    (void)update_master_info(original_master_info);
    ASSERT_OK(status);

    EXPECT_EQ(1, copy_count);
    EXPECT_EQ(1, mock_fs->open_count());
    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id));
    ASSERT_TRUE(txn_log->op_replication().has_tablet_metadata());
    const auto& built_meta = txn_log->op_replication().tablet_metadata();
    ASSERT_EQ(1, built_meta.rowsets_size());
    ASSERT_EQ(2, built_meta.rowsets(0).segment_metas_size());
    const auto& first_slice = built_meta.rowsets(0).segment_metas(0);
    const auto& second_slice = built_meta.rowsets(0).segment_metas(1);
    EXPECT_EQ(first_slice.filename(), second_slice.filename());
    EXPECT_NE(bundle_name, first_slice.filename());
    EXPECT_EQ(5, first_slice.size());
    EXPECT_EQ(0, first_slice.bundle_file_offset());
    EXPECT_EQ(7, second_slice.size());
    EXPECT_EQ(5, second_slice.bundle_file_offset());

    const std::string target_path = _tablet_mgr->segment_location(_target_tablet_id, first_slice.filename());
    ASSIGN_OR_ABORT(auto target_fs, FileSystem::CreateSharedFromString(target_path));
    ASSIGN_OR_ABORT(auto target_file, target_fs->new_random_access_file(target_path));
    ASSIGN_OR_ABORT(auto target_size, target_file->get_size());
    std::string copied_contents(target_size, '\0');
    ASSERT_OK(target_file->read_at_fully(0, copied_contents.data(), target_size));
    EXPECT_EQ(physical_contents, copied_contents);
}

TEST_F(LakeReplicationRemoteStorageTest, bundle_siblings_reuse_shared_file_without_failed_cleanup) {
    const std::string contents = "shared-bundle-segment";
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(contents);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    const std::string segment_name = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000074.dat";
    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(segment_name);
    segment->set_size(contents.size());
    segment->set_bundle_file_offset(0);
    segment->set_shared(false);
    source->set_next_rowset_id(2);

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = source;
                                          });
    bool hide_existing_file = false;
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::get_existing_file_size", [&](void* arg) {
        if (hide_existing_file) {
            static_cast<std::optional<size_t>*>(arg)->reset();
        }
    });

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto first_request = build_request(false /* with_full_path */);
    first_request.__set_virtual_tablet_id(_virtual_tablet_id + 74);
    ASSERT_OK(_replication_txn_manager->replicate_lake_remote_storage(first_request));

    ASSIGN_OR_ABORT(auto first_txn_log, _tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id));
    const auto& target_segment = first_txn_log->op_replication().tablet_metadata().rowsets(0).segment_metas(0);
    const std::string target_path = _tablet_mgr->segment_location(_target_tablet_id, target_segment.filename());
    ASSERT_TRUE(fs::path_exist(target_path));

    const int64_t sibling_tablet_id = _target_tablet_id + 1;
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*generate_simple_tablet_metadata(sibling_tablet_id)));

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    auto* fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("put_txn_log_fail");
    ASSERT_NE(nullptr, fp);
    fp->setMode(trigger_mode);

    auto sibling_request = first_request;
    sibling_request.__set_tablet_id(sibling_tablet_id);
    hide_existing_file = true;
    Status sibling_status = _replication_txn_manager->replicate_lake_remote_storage(sibling_request);
    hide_existing_file = false;

    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(trigger_mode);

    ASSERT_TRUE(sibling_status.is_internal_error()) << sibling_status;
    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    EXPECT_TRUE(fs::path_exist(target_path));
    EXPECT_EQ(2, mock_fs->open_count());

    const int64_t third_tablet_id = sibling_tablet_id + 1;
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*generate_simple_tablet_metadata(third_tablet_id)));
    auto third_request = first_request;
    third_request.__set_tablet_id(third_tablet_id);
    ASSERT_OK(_replication_txn_manager->replicate_lake_remote_storage(third_request));
    EXPECT_EQ(2, mock_fs->open_count());

    (void)update_master_info(original_master_info);
}

TEST_F(LakeReplicationRemoteStorageTest, range_siblings_reuse_shared_file_without_failed_cleanup) {
    const std::string contents = "shared-range-segment";
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(contents);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    const std::string segment_name = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000073.dat";
    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    source->mutable_range();
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(segment_name);
    segment->set_size(contents.size());
    segment->set_shared(true);
    source->set_next_rowset_id(2);

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = source;
                                          });
    bool hide_existing_file = false;
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::get_existing_file_size", [&](void* arg) {
        if (hide_existing_file) {
            static_cast<std::optional<size_t>*>(arg)->reset();
        }
    });

    _target_tablet_metadata->mutable_range();
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*_target_tablet_metadata));

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto first_request = build_request(false /* with_full_path */);
    first_request.__set_virtual_tablet_id(_virtual_tablet_id + 73);
    ASSERT_OK(_replication_txn_manager->replicate_lake_remote_storage(first_request));

    ASSIGN_OR_ABORT(auto first_txn_log, _tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id));
    const auto& target_segment = first_txn_log->op_replication().tablet_metadata().rowsets(0).segment_metas(0);
    const std::string target_path = _tablet_mgr->segment_location(_target_tablet_id, target_segment.filename());
    ASSERT_TRUE(fs::path_exist(target_path));

    const int64_t sibling_tablet_id = _target_tablet_id + 1;
    auto sibling = generate_simple_tablet_metadata(sibling_tablet_id);
    sibling->mutable_range();
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*sibling));

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    auto* fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("put_txn_log_fail");
    ASSERT_NE(nullptr, fp);
    fp->setMode(trigger_mode);

    auto sibling_request = first_request;
    sibling_request.__set_tablet_id(sibling_tablet_id);
    // Model the concurrent interleaving where this sibling checked the target before the first
    // copy became visible. It must copy independently but must not acquire cleanup ownership.
    hide_existing_file = true;
    Status sibling_status = _replication_txn_manager->replicate_lake_remote_storage(sibling_request);
    hide_existing_file = false;

    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp->setMode(trigger_mode);

    ASSERT_TRUE(sibling_status.is_internal_error()) << sibling_status;
    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    EXPECT_TRUE(fs::path_exist(target_path));
    EXPECT_EQ(2, mock_fs->open_count());

    const int64_t third_tablet_id = sibling_tablet_id + 1;
    auto third = generate_simple_tablet_metadata(third_tablet_id);
    third->mutable_range();
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*third));
    auto third_request = first_request;
    third_request.__set_tablet_id(third_tablet_id);
    ASSERT_OK(_replication_txn_manager->replicate_lake_remote_storage(third_request));
    EXPECT_EQ(2, mock_fs->open_count());

    (void)update_master_info(original_master_info);
}

TEST_F(LakeReplicationRemoteStorageTest, rejects_conflicting_bundled_encryption_metadata_before_copy) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    const std::string bundle_name = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000073.dat";
    for (const auto [offset, encryption_meta] :
         {std::pair<int64_t, const char*>{0, "slice-zero-encryption"}, {11, "slice-one-encryption"}}) {
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(bundle_name);
        segment->set_size(11);
        segment->set_bundle_file_offset(offset);
        segment->set_encryption_meta(encryption_meta);
    }
    source->set_next_rowset_id(2);

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = source;
                                          });
    bool copy_started = false;
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_lake_remote_storage::before_copy",
                                          [&](void*) { copy_started = true; });
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::download_segment",
                                          [&](void* arg) { *static_cast<size_t*>(arg) = 22; });

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));
    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);
    (void)update_master_info(original_master_info);

    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_FALSE(copy_started);
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
}

TEST_F(LakeReplicationRemoteStorageTest, rejects_bundled_fast_schema_conversion) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto source = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    source->set_version(2);
    source->mutable_schema()->mutable_column(1)->set_unique_id(9999);
    auto* rowset = source->add_rowsets();
    rowset->set_id(1);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000071.dat");
    segment->set_size(17);
    segment->set_bundle_file_offset(0);
    source->set_next_rowset_id(2);

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = source;
                                          });
    bool copy_started = false;
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::download_segment",
                                          [&](void* arg) {
                                              copy_started = true;
                                              *static_cast<size_t*>(arg) = 17;
                                          });

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));
    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request);
    (void)update_master_info(original_master_info);

    ASSERT_TRUE(status.is_not_supported()) << status;
    EXPECT_FALSE(copy_started);
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
}

#endif // USE_STAROS

INSTANTIATE_TEST_SUITE_P(SharedDataReplicationTxnManagerTest, SharedDataReplicationTxnManagerTest,
                         testing::Values(KeysType::DUP_KEYS, KeysType::AGG_KEYS, KeysType::PRIMARY_KEYS));

} // namespace starrocks::lake
