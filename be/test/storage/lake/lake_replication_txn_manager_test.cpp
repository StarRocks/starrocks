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

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <random>
#include <thread>

#include "base/concurrency/countdown_latch.h"
#include "base/failpoint/fail_point.h"
#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "common/system/master_info.h"
#include "storage/lake/replication_txn_manager.h"
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
#include "common/config_lake_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/config_starlet_fwd.h"
#include "common/thread/threadpool.h"
#include "compute_env/staros/starlet_filesystem.h"
#include "compute_env/staros/staros_worker.h"
#include "compute_env/staros/staros_worker_runtime.h"
#include "exec/exec_env.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gutil/strings/join.h"
#include "platform/key_cache.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"
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
        StorageEngine::instance()->wait_storage_cleanup_tasks();
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

    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);
    EXPECT_FALSE(status.ok());
}

TEST_P(SharedDataReplicationTxnManagerTest, test_target_split_child_without_data_version_metadata) {
    auto src_metadata = generate_tablet_metadata(GetParam());
    src_metadata->set_version(3);
    src_metadata->mutable_range();

    constexpr const char* kSourceSegment = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat";
    constexpr const char* kTargetSegment = "0000000000000002_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat";
    constexpr const char* kNewSourceSegment = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000002.dat";
    constexpr const char* kNewSourceDel = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000009.del";
    constexpr const char* kNewSourceSst = "aaaaaaaa-bbbb-cccc-dddd-000000000010.sst";
    constexpr const char* kNewSourceDelvec = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000011.delvec";
    constexpr const char* kNewSourceDcg = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000012.cols";
    constexpr const char* kNewSourceIdg = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000013.idx";
    constexpr const char* kSourceBundledSegment = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000008.dat";
    constexpr const char* kTargetBundledSegment = "0000000000000002_aaaaaaaa-bbbb-cccc-dddd-000000000008.dat";
    constexpr const char* kSourceDel = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000003.del";
    constexpr const char* kTargetDel = "0000000000000002_aaaaaaaa-bbbb-cccc-dddd-000000000003.del";
    constexpr const char* kSst = "aaaaaaaa-bbbb-cccc-dddd-000000000004.sst";
    constexpr const char* kSourceDelvec = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000005.delvec";
    constexpr const char* kTargetDelvec = "0000000000000002_aaaaaaaa-bbbb-cccc-dddd-000000000005.delvec";
    constexpr const char* kSourceDcg = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000006.cols";
    constexpr const char* kTargetDcg = "0000000000000002_aaaaaaaa-bbbb-cccc-dddd-000000000006.cols";
    constexpr const char* kSourceIdg = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000007.idx";
    constexpr const char* kTargetIdg = "0000000000000002_aaaaaaaa-bbbb-cccc-dddd-000000000007.idx";

    auto* src_rowset = src_metadata->add_rowsets();
    src_rowset->set_id(1);
    src_rowset->add_segment_metas()->set_filename(kSourceSegment);
    src_rowset->add_del_files()->set_name(kSourceDel);
    src_metadata->mutable_sstable_meta()->add_sstables()->set_filename(kSst);
    (*src_metadata->mutable_delvec_meta()->mutable_version_to_file())[1].set_name(kSourceDelvec);
    auto& src_dcg = (*src_metadata->mutable_dcg_meta()->mutable_dcgs())[1];
    src_dcg.add_column_files(kSourceDcg);
    src_dcg.add_shared_files(false);
    auto* src_idg_entry = (*src_metadata->mutable_idg_meta()->mutable_idgs())[1].add_entries();
    src_idg_entry->set_index_file(kSourceIdg);
    auto* src_bundled_rowset = src_metadata->add_rowsets();
    src_bundled_rowset->set_id(2);
    auto* src_bundled_segment = src_bundled_rowset->add_segment_metas();
    src_bundled_segment->set_filename(kSourceBundledSegment);
    src_bundled_segment->set_size(1234);
    src_bundled_segment->set_bundle_file_offset(4096);

    auto target_child_id = generate_tablet_metadata(GetParam())->id();
    auto target_child_metadata = std::make_shared<TabletMetadataPB>(*src_metadata);
    target_child_metadata->set_id(target_child_id);
    target_child_metadata->set_version(2);
    auto* target_rowset = target_child_metadata->mutable_rowsets(0);
    target_rowset->mutable_segment_metas(0)->set_filename(kTargetSegment);
    target_rowset->mutable_segment_metas(0)->set_shared(true);
    target_rowset->mutable_del_files(0)->set_name(kTargetDel);
    target_rowset->mutable_del_files(0)->set_shared(true);
    target_child_metadata->mutable_sstable_meta()->mutable_sstables(0)->set_shared(true);
    auto& target_delvec = (*target_child_metadata->mutable_delvec_meta()->mutable_version_to_file())[1];
    target_delvec.set_name(kTargetDelvec);
    target_delvec.set_shared(true);
    auto& target_dcg = (*target_child_metadata->mutable_dcg_meta()->mutable_dcgs())[1];
    target_dcg.set_column_files(0, kTargetDcg);
    target_dcg.set_shared_files(0, true);
    auto* target_idg_entry = (*target_child_metadata->mutable_idg_meta()->mutable_idgs())[1].mutable_entries(0);
    target_idg_entry->set_index_file(kTargetIdg);
    target_idg_entry->set_shared_file(true);
    auto* target_bundled_segment = target_child_metadata->mutable_rowsets(1)->mutable_segment_metas(0);
    target_bundled_segment->set_filename(kTargetBundledSegment);
    // Bundled segments are effectively shared even when the explicit shared flag is absent.
    target_bundled_segment->set_shared(false);

    // This source-shared file was not present in the target split-child baseline. Replication must
    // preserve the shared ownership flag because another source child can reference the same object
    // and will deterministically map it to the same target object.
    auto* new_source_segment = src_rowset->add_segment_metas();
    new_source_segment->set_filename(kNewSourceSegment);
    new_source_segment->set_shared(true);
    auto* new_source_del = src_rowset->add_del_files();
    new_source_del->set_name(kNewSourceDel);
    new_source_del->set_shared(true);
    auto* new_source_sst = src_metadata->mutable_sstable_meta()->add_sstables();
    new_source_sst->set_filename(kNewSourceSst);
    new_source_sst->set_shared(true);
    auto& new_source_delvec = (*src_metadata->mutable_delvec_meta()->mutable_version_to_file())[2];
    new_source_delvec.set_name(kNewSourceDelvec);
    new_source_delvec.set_shared(true);
    auto& new_source_dcg = (*src_metadata->mutable_dcg_meta()->mutable_dcgs())[2];
    new_source_dcg.add_column_files(kNewSourceDcg);
    new_source_dcg.add_shared_files(true);
    auto* new_source_idg = (*src_metadata->mutable_idg_meta()->mutable_idgs())[2].add_entries();
    new_source_idg->set_index_file(kNewSourceIdg);
    new_source_idg->set_shared_file(true);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*target_child_metadata));
    ASSERT_TRUE(_tablet_mgr->get_tablet_metadata(target_child_metadata->id(), 1).status().is_not_found());

    std::unordered_map<std::string, size_t> segment_name_to_size_map;
    std::map<std::string, std::string> file_locations;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;
    auto result = _replication_txn_manager->convert_and_build_new_tablet_meta(
            src_metadata, target_child_metadata, src_metadata->id(), target_child_metadata->id(), _transaction_id, 1,
            lake::join_path(_test_dir, lake::kSegmentDirectoryName), segment_name_to_size_map, file_locations,
            filename_map);

    ASSERT_OK(result);
    EXPECT_EQ(target_child_metadata->id(), result.value()->id());
    EXPECT_EQ(2, result.value()->version());
    ASSERT_EQ(2, result.value()->rowsets_size());
    const auto& built_rowset = result.value()->rowsets(0);
    ASSERT_EQ(2, built_rowset.segment_metas_size());
    EXPECT_EQ(kTargetSegment, built_rowset.segment_metas(0).filename());
    EXPECT_TRUE(built_rowset.segment_metas(0).shared());
    EXPECT_TRUE(built_rowset.segment_metas(1).shared());
    ASSERT_EQ(2, built_rowset.del_files_size());
    EXPECT_EQ(kTargetDel, built_rowset.del_files(0).name());
    EXPECT_TRUE(built_rowset.del_files(0).shared());
    EXPECT_TRUE(built_rowset.del_files(1).shared());
    ASSERT_EQ(2, result.value()->sstable_meta().sstables_size());
    EXPECT_TRUE(result.value()->sstable_meta().sstables(0).shared());
    EXPECT_TRUE(result.value()->sstable_meta().sstables(1).shared());
    EXPECT_EQ(kTargetDelvec, result.value()->delvec_meta().version_to_file().at(1).name());
    EXPECT_TRUE(result.value()->delvec_meta().version_to_file().at(1).shared());
    EXPECT_TRUE(result.value()->delvec_meta().version_to_file().at(2).shared());
    EXPECT_EQ(kTargetDcg, result.value()->dcg_meta().dcgs().at(1).column_files(0));
    EXPECT_TRUE(result.value()->dcg_meta().dcgs().at(1).shared_files(0));
    EXPECT_TRUE(result.value()->dcg_meta().dcgs().at(2).shared_files(0));
    EXPECT_EQ(kTargetIdg, result.value()->idg_meta().idgs().at(1).entries(0).index_file());
    EXPECT_TRUE(result.value()->idg_meta().idgs().at(1).entries(0).shared_file());
    EXPECT_TRUE(result.value()->idg_meta().idgs().at(2).entries(0).shared_file());
    const auto& built_bundled_segment = result.value()->rowsets(1).segment_metas(0);
    EXPECT_EQ(kTargetBundledSegment, built_bundled_segment.filename());
    ASSERT_TRUE(built_bundled_segment.has_bundle_file_offset());
    EXPECT_EQ(4096, built_bundled_segment.bundle_file_offset());
    EXPECT_TRUE(built_bundled_segment.shared());
    ASSERT_TRUE(segment_name_to_size_map.contains(kSourceBundledSegment));
    EXPECT_EQ(0, segment_name_to_size_map.at(kSourceBundledSegment));
    EXPECT_EQ(6, file_locations.size());
    EXPECT_EQ(6, filename_map.size());
}

TEST_P(SharedDataReplicationTxnManagerTest, test_target_hash_tablet_without_data_version_metadata) {
    auto src_metadata = generate_tablet_metadata(GetParam());
    src_metadata->set_version(3);

    auto target_metadata = generate_tablet_metadata(GetParam());
    target_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*target_metadata));
    ASSERT_TRUE(_tablet_mgr->get_tablet_metadata(target_metadata->id(), 1).status().is_not_found());

    std::unordered_map<std::string, size_t> segment_name_to_size_map;
    std::map<std::string, std::string> file_locations;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;
    auto result = _replication_txn_manager->convert_and_build_new_tablet_meta(
            src_metadata, target_metadata, src_metadata->id(), target_metadata->id(), _transaction_id, 1,
            lake::join_path(_test_dir, lake::kSegmentDirectoryName), segment_name_to_size_map, file_locations,
            filename_map);

    ASSERT_TRUE(result.status().is_not_found());
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
        ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(path));
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

    ASSIGN_OR_ABORT(auto src_fs, FileSystemFactory::CreateSharedFromString(src_path));
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

    ASSIGN_OR_ABORT(auto src_fs, FileSystemFactory::CreateSharedFromString(src_path));
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

    ASSIGN_OR_ABORT(auto src_fs, FileSystemFactory::CreateSharedFromString(src_path));
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

    ASSIGN_OR_ABORT(auto src_fs, FileSystemFactory::CreateSharedFromString(src_path));
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

    ASSIGN_OR_ABORT(auto src_fs, FileSystemFactory::CreateSharedFromString(src_path));
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

    ASSIGN_OR_ABORT(auto src_fs, FileSystemFactory::CreateSharedFromString(src_path));
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
    ASSERT_OK(ThreadPoolBuilder("lake_par_gate")
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
    ASSERT_OK(ThreadPoolBuilder("lake_par_overld")
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
    ASSERT_OK(
            ThreadPoolBuilder("lake_par_dis").set_min_threads(1).set_max_threads(1).set_max_queue_size(8).build(&pool));

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
        auto fs_or = FileSystemFactory::CreateSharedFromString(_test_dir);
        CHECK(fs_or.ok());
        _shared_fs = fs_or.value();
    }

    void TearDown() override {
        StorageEngine::instance()->wait_storage_cleanup_tasks();
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

#ifdef USE_STAROS
class InMemoryInputStreamForReplication : public staros::starlet::fslib::InputStream {
public:
    explicit InMemoryInputStreamForReplication(std::shared_ptr<const std::string> content)
            : _content(std::move(content)) {}

    bool support_seek() override { return true; }
    bool support_tell() override { return true; }
    bool support_size() override { return true; }

    absl::StatusOr<size_t> seek(int64_t offset, Anchor anchor) override {
        int64_t base = 0;
        switch (anchor) {
        case BEGIN:
            break;
        case CURRENT:
            base = static_cast<int64_t>(_offset);
            break;
        case END:
            base = static_cast<int64_t>(_content->size());
            break;
        }
        const int64_t position = base + offset;
        if (position < 0 || position > static_cast<int64_t>(_content->size())) {
            return absl::InvalidArgumentError("seek outside in-memory source file");
        }
        _offset = static_cast<size_t>(position);
        return _offset;
    }

    absl::StatusOr<size_t> tell() override { return _offset; }
    absl::StatusOr<size_t> size() override { return _content->size(); }
    absl::Status close() override {
        _closed = true;
        return absl::OkStatus();
    }
    bool closed() const override { return _closed; }

    absl::StatusOr<size_t> read(void* data, size_t length) override {
        const size_t bytes_to_read = std::min(length, _content->size() - _offset);
        std::memcpy(data, _content->data() + _offset, bytes_to_read);
        _offset += bytes_to_read;
        return bytes_to_read;
    }

private:
    std::shared_ptr<const std::string> _content;
    size_t _offset = 0;
    bool _closed = false;
};

class InMemoryReadOnlyFileForReplication : public staros::starlet::fslib::ReadOnlyFile {
public:
    InMemoryReadOnlyFileForReplication(std::string name, std::shared_ptr<const std::string> content)
            : _name(std::move(name)), _content(std::move(content)), _stream(_content) {}

    const std::string& name() override { return _name; }
    absl::StatusOr<size_t> size() override { return _content->size(); }
    absl::StatusOr<std::string> get_meta(std::string_view) override {
        return absl::UnimplementedError("get_meta is not needed by this test");
    }
    absl::Status set_meta(std::string_view, std::string_view) override {
        return absl::UnimplementedError("set_meta is not needed by this test");
    }
    absl::Status remove_meta(std::string_view) override {
        return absl::UnimplementedError("remove_meta is not needed by this test");
    }
    absl::Status close() override { return _stream.close(); }
    absl::StatusOr<staros::starlet::fslib::InputStream*> stream() override { return &_stream; }

private:
    std::string _name;
    std::shared_ptr<const std::string> _content;
    InMemoryInputStreamForReplication _stream;
};

// Mock staros::starlet::fslib::FileSystem for SyncPoint injection
class MockStarletFileSystemForReplication : public staros::starlet::fslib::FileSystem {
public:
    MockStarletFileSystemForReplication() : staros::starlet::fslib::FileSystem() {}
    explicit MockStarletFileSystemForReplication(std::string file_content)
            : staros::starlet::fslib::FileSystem(),
              _file_content(std::make_shared<const std::string>(std::move(file_content))) {}
    ~MockStarletFileSystemForReplication() override = default;

    std::string_view scheme() override { return "mock"; }

    absl::StatusOr<std::unique_ptr<staros::starlet::fslib::ReadOnlyFile>> open(
            std::string_view path, const staros::starlet::fslib::ReadOptions& opts) override {
        {
            std::lock_guard lock(_mutex);
            _opened_paths.emplace_back(path);
            if (_fail_open_count > 0) {
                --_fail_open_count;
                return absl::InternalError("injected source open failure");
            }
        }
        if (_file_content != nullptr) {
            return std::make_unique<InMemoryReadOnlyFileForReplication>(std::string(path), _file_content);
        }
        return absl::UnimplementedError("MockStarletFileSystemForReplication::open not implemented");
    }

    std::vector<std::string> opened_paths() const {
        std::lock_guard lock(_mutex);
        return _opened_paths;
    }

    void set_stat_size(size_t size) {
        _stat_size = size;
        _has_stat_size = true;
    }

    void set_fail_open_count(int count) {
        std::lock_guard lock(_mutex);
        _fail_open_count = count;
    }

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
        if (_file_content != nullptr) {
            return staros::starlet::fslib::Stat{.size = _file_content->size(), .mode = S_IFREG};
        }
        if (_has_stat_size) {
            return staros::starlet::fslib::Stat{.size = _stat_size, .mode = S_IFREG};
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
    mutable std::mutex _mutex;
    std::vector<std::string> _opened_paths;
    std::shared_ptr<const std::string> _file_content;
    size_t _stat_size = 0;
    bool _has_stat_size = false;
    int _fail_open_count = 0;
};

// Models different Starlet shard URIs that resolve to one physical partition root. The virtual
// paths retain a tablet-specific alias component, while local filesystem traversal and
// real_location() both collapse the aliases to the same files under _root.
class AliasedLocalLocationProvider : public lake::LocationProvider {
public:
    explicit AliasedLocalLocationProvider(std::string root) : _root(std::move(root)) {}

    std::string root_location(int64_t tablet_id) const override {
        return fmt::format("{}/aliases/{}/../..", _root, tablet_id);
    }

    StatusOr<std::string> real_location(const std::string& virtual_path) const override {
        return std::filesystem::path(virtual_path).lexically_normal().string();
    }

private:
    std::string _root;
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

        StorageEngine::instance()->wait_storage_cleanup_tasks();
        ASSERT_OK(fs::remove_all(_test_dir));
    }

    void use_aliased_local_location_provider(std::initializer_list<int64_t> tablet_ids) {
        _replication_txn_manager.reset();
        _tablet_mgr.reset();
        _update_manager.reset();
        for (int64_t tablet_id : tablet_ids) {
            CHECK_OK(fs::create_directories(fmt::format("{}/aliases/{}", _test_dir, tablet_id)));
        }
        _location_provider = std::make_shared<AliasedLocalLocationProvider>(_test_dir);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_mgr = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 1024 * 1024);
        _replication_txn_manager = std::make_unique<lake::LakeReplicationTxnManager>(_tablet_mgr.get());
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
        // Fixed, tablet-independent column unique ids: cross-cluster replication of the SAME
        // logical table normally shares the column unique-id space between source and target,
        // so calc_column_unique_id_map() is empty and the IDG (.idx) fast-path indexes replicate
        // verbatim. (A per-tablet next_id() here would fabricate a divergent id space that does
        // not model real same-table CCR and would trip the divergent-id skip guard.)
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

TEST_F(LakeReplicationRemoteStorageTest, test_has_full_path_uses_virtual_shard_uri) {
    std::string src_partition_starlet_uri;
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = absl::InternalError("stop after observing the source partition URI");
    });
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::src_partition_starlet_uri", [&](void* arg) {
        src_partition_starlet_uri = *static_cast<std::string*>(arg);
    });

    auto request = build_request(true /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ("staros://80001/path/to/db123/456/789", src_partition_starlet_uri);
    EXPECT_NE(_src_tablet_id, _virtual_tablet_id);
}

// Test Case 1: has_full_path=true, new_fs_starlet returns nullptr
TEST_F(LakeReplicationRemoteStorageTest, test_has_full_path_fs_creation_failure) {
    // SyncPoint makes new_fs_starlet return nullptr by setting an error status
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = absl::InternalError("Mock: failed to get shard filesystem");
    });

    auto request = build_request(true /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_NE(std::string::npos, status.message().find("Failed to create virtual starlet filesystem"));
}

// Test Case 2: has_full_path=false, new_fs_starlet returns nullptr
TEST_F(LakeReplicationRemoteStorageTest, test_no_full_path_fs_creation_failure) {
    // SyncPoint makes new_fs_starlet return nullptr by setting an error status
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = absl::InternalError("Mock: failed to get shard filesystem");
    });

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);

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
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);

    // The filesystem creation succeeds (not nullptr), but reading tablet metadata
    // via the mock filesystem will fail. The error should NOT be
    // "Failed to create virtual starlet filesystem".
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(std::string::npos, status.message().find("Failed to create virtual starlet filesystem"))
            << "Should have passed the nullptr check, error: " << status;
    ASSERT_FALSE(mock_fs->opened_paths().empty());
    EXPECT_EQ("path/to/db123/456/789/meta/000000000000C351_0000000000000002.meta", mock_fs->opened_paths().front());
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
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);

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

    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);

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
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);

    // Restore original master info
    (void)update_master_info(original_master_info);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_aborted()) << status;
    EXPECT_NE(std::string::npos, status.message().find("Lake replication transaction has been aborted"));
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
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);

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
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);

    // Restore original master info
    (void)update_master_info(original_master_info);

    // The fast cancel check should NOT have triggered. The function should proceed past
    // the fast cancel check to the before_copy SyncPoint and then to file copy.
    // The file copy will fail due to the mock filesystem, but that's expected.
    EXPECT_TRUE(before_copy_invoked) << "before_copy SyncPoint should have been reached (fast cancel did not trigger)";
    EXPECT_FALSE(status.is_aborted()) << "Should not abort when min_active_txn_id <= txn_id, status: " << status;
}

TEST_F(LakeReplicationRemoteStorageTest, test_copy_complete_bundle_once_for_multiple_logical_segments) {
    const std::string unreferenced_prefix = "bundle-prefix-for-another-tablet";
    const std::string logical_segment1 = "logical-segment-one";
    const std::string logical_segment2 = "logical-segment-two-with-more-bytes";
    const std::string physical_bundle = unreferenced_prefix + logical_segment1 + logical_segment2;
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(physical_bundle);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    constexpr const char* kBundleFilename = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000009.dat";
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(20);
    rowset->set_data_size(logical_segment1.size() + logical_segment2.size());
    auto* segment1 = rowset->add_segment_metas();
    segment1->set_filename(kBundleFilename);
    segment1->set_size(logical_segment1.size());
    segment1->set_bundle_file_offset(unreferenced_prefix.size());
    auto* segment2 = rowset->add_segment_metas();
    segment2->set_filename(kBundleFilename);
    segment2->set_size(logical_segment2.size());
    segment2->set_bundle_file_offset(unreferenced_prefix.size() + logical_segment1.size());
    src_meta_v2->set_next_rowset_id(2);

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto request = build_request(false /* with_full_path */);
    // new_fs_starlet caches filesystems by virtual shard id across fixture instances. Use a
    // process-unique id so this test cannot inherit a mock filesystem from an earlier/repeated test.
    request.__set_virtual_tablet_id(next_id());
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);

    (void)update_master_info(original_master_info);
    ASSERT_OK(status);

    // The two logical segments share one source object, so replication must open and copy it once.
    ASSERT_EQ(1, mock_fs->opened_paths().size());

    auto txn_log = _tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id);
    ASSERT_OK(txn_log);
    const auto& replicated_rowsets = txn_log.value()->op_replication().tablet_metadata().rowsets();
    ASSERT_EQ(1, replicated_rowsets.size());
    ASSERT_EQ(2, replicated_rowsets.Get(0).segment_metas_size());
    const auto& replicated_segment1 = replicated_rowsets.Get(0).segment_metas(0);
    const auto& replicated_segment2 = replicated_rowsets.Get(0).segment_metas(1);
    EXPECT_EQ(replicated_segment1.filename(), replicated_segment2.filename());
    EXPECT_TRUE(replicated_segment1.shared());
    EXPECT_TRUE(replicated_segment2.shared());
    EXPECT_EQ(logical_segment1.size(), replicated_segment1.size());
    EXPECT_EQ(logical_segment2.size(), replicated_segment2.size());
    EXPECT_EQ(unreferenced_prefix.size(), replicated_segment1.bundle_file_offset());
    EXPECT_EQ(unreferenced_prefix.size() + logical_segment1.size(), replicated_segment2.bundle_file_offset());

    const auto target_bundle_path = _tablet_mgr->segment_location(_target_tablet_id, replicated_segment1.filename());
    ASSIGN_OR_ABORT(auto target_bundle_file, fs::new_random_access_file(target_bundle_path));
    ASSIGN_OR_ABORT(auto target_bundle_size, target_bundle_file->get_size());
    ASSERT_EQ(physical_bundle.size(), target_bundle_size);
    std::string target_bundle(target_bundle_size, '\0');
    ASSERT_OK(target_bundle_file->read_at_fully(0, target_bundle.data(), target_bundle.size()));
    EXPECT_EQ(physical_bundle, target_bundle);

    ASSIGN_OR_ABORT(auto target_fs, FileSystemFactory::CreateSharedFromString(target_bundle_path));
    RandomAccessFileOptions opts;
    FileInfo segment1_info{.path = target_bundle_path,
                           .size = replicated_segment1.size(),
                           .bundle_file_offset = replicated_segment1.bundle_file_offset()};
    ASSIGN_OR_ABORT(auto segment1_file, target_fs->new_random_access_file_with_bundling(opts, segment1_info));
    std::string target_segment1(logical_segment1.size(), '\0');
    ASSERT_OK(segment1_file->read_at_fully(0, target_segment1.data(), target_segment1.size()));
    EXPECT_EQ(logical_segment1, target_segment1);

    FileInfo segment2_info{.path = target_bundle_path,
                           .size = replicated_segment2.size(),
                           .bundle_file_offset = replicated_segment2.bundle_file_offset()};
    ASSIGN_OR_ABORT(auto segment2_file, target_fs->new_random_access_file_with_bundling(opts, segment2_info));
    std::string target_segment2(logical_segment2.size(), '\0');
    ASSERT_OK(segment2_file->read_at_fully(0, target_segment2.data(), target_segment2.size()));
    EXPECT_EQ(logical_segment2, target_segment2);

    // Publishing removes the PREPARED cleanup intent but must not delete the now-visible bundle.
    ASSERT_OK(_tablet_mgr->get_txn_slog(_target_tablet_id, _transaction_id));
    TxnInfoPB txn_info;
    txn_info.set_txn_id(_transaction_id);
    txn_info.set_txn_type(TxnTypePB::TXN_REPLICATION);
    txn_info.set_combined_txn_log(false);
    txn_info.set_commit_time(0);
    std::vector<TxnInfoPB> txns{txn_info};
    ASSIGN_OR_ABORT(auto published,
                    publish_version(_tablet_mgr.get(), PublishTabletInfo(_target_tablet_id), 1, 2, txns, false));
    StorageEngine::instance()->wait_storage_cleanup_tasks();
    EXPECT_TRUE(fs::path_exist(target_bundle_path));
    EXPECT_TRUE(_tablet_mgr->get_txn_slog(_target_tablet_id, _transaction_id).status().is_not_found());
    for (const auto& file : published->orphan_files()) {
        EXPECT_NE(kReplicationCleanupFileVersion, file.version());
    }
}

TEST_F(LakeReplicationRemoteStorageTest, test_reuse_complete_shared_segment_across_sequential_tablet_tasks) {
    const std::string physical_segment = "one-cross-boundary-segment-shared-by-split-children";
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(physical_segment);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    constexpr const char* kSegmentFilename = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000017.dat";
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(10);
    rowset->set_data_size(physical_segment.size());
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(kSegmentFilename);
    segment->set_size(physical_segment.size());
    segment->set_shared(true);
    src_meta_v2->set_next_rowset_id(2);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    const int64_t second_target_tablet_id = _target_tablet_id + 1;
    use_aliased_local_location_provider({_target_tablet_id, second_target_tablet_id});
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_target_tablet_metadata));
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*generate_simple_tablet_metadata(second_target_tablet_id)));

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto first_request = build_request(false /* with_full_path */);
    first_request.__set_virtual_tablet_id(next_id());
    ASSERT_OK(_replication_txn_manager->replicate_lake_remote_storage(first_request, nullptr));
    ASSERT_EQ(1, mock_fs->opened_paths().size());

    auto second_request = first_request;
    second_request.__set_tablet_id(second_target_tablet_id);
    second_request.__set_src_tablet_id(_src_tablet_id + 1);
    ASSERT_OK(_replication_txn_manager->replicate_lake_remote_storage(second_request, nullptr));
    EXPECT_EQ(1, mock_fs->opened_paths().size());

    const auto target_filename = gen_filename_from(_transaction_id, kSegmentFilename);
    for (int64_t tablet_id : {_target_tablet_id, second_target_tablet_id}) {
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, _transaction_id));
        const auto& replicated_segment = txn_log->op_replication().tablet_metadata().rowsets(0).segment_metas(0);
        EXPECT_EQ(target_filename, replicated_segment.filename());
        EXPECT_TRUE(replicated_segment.shared());
        EXPECT_FALSE(replicated_segment.has_bundle_file_offset());
        EXPECT_EQ(physical_segment.size(), replicated_segment.size());
    }

    const auto target_path = _tablet_mgr->segment_location(_target_tablet_id, target_filename);
    ASSIGN_OR_ABORT(auto target_segment_file, fs::new_random_access_file(target_path));
    std::string target_segment(physical_segment.size(), '\0');
    ASSERT_OK(target_segment_file->read_at_fully(0, target_segment.data(), target_segment.size()));
    EXPECT_EQ(physical_segment, target_segment);

    (void)update_master_info(original_master_info);
}

TEST_F(LakeReplicationRemoteStorageTest, test_singleflight_shared_bundle_across_tablet_tasks_and_retry) {
    const std::string physical_bundle = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(physical_bundle);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    constexpr const char* kBundleFilename = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000012.dat";
    auto make_source_meta = [&](size_t offset, size_t size) {
        auto metadata = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
        metadata->set_version(2);
        auto* rowset = metadata->add_rowsets();
        rowset->set_id(1);
        rowset->set_num_rows(10);
        rowset->set_data_size(size);
        auto* segment = rowset->add_segment_metas();
        segment->set_filename(kBundleFilename);
        segment->set_size(size);
        segment->set_bundle_file_offset(offset);
        segment->set_shared(true);
        metadata->set_next_rowset_id(2);
        return metadata;
    };
    auto first_source_meta = make_source_meta(5, 11);
    auto second_source_meta = make_source_meta(23, 13);
    auto retry_source_meta = make_source_meta(40, 12);
    std::atomic<int> injected_metadata_count{0};
    SyncPoint::GetInstance()->SetCallBack(
            "LakeReplicationTxnManager::build_source_tablet_meta::inject", [&](void* arg) {
                auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                int index = injected_metadata_count.fetch_add(1);
                *meta_ptr = index == 0 ? first_source_meta : index == 1 ? second_source_meta : retry_source_meta;
            });

    const int64_t second_target_tablet_id = _target_tablet_id + 1;
    const int64_t mismatch_target_tablet_id = _target_tablet_id + 2;
    const int64_t reuse_target_tablet_id = _target_tablet_id + 3;
    use_aliased_local_location_provider(
            {_target_tablet_id, second_target_tablet_id, mismatch_target_tablet_id, reuse_target_tablet_id});
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_target_tablet_metadata));
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*generate_simple_tablet_metadata(second_target_tablet_id)));
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*generate_simple_tablet_metadata(mismatch_target_tablet_id)));
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*generate_simple_tablet_metadata(reuse_target_tablet_id)));

    const auto target_filename = gen_filename_from(_transaction_id, kBundleFilename);
    const auto first_virtual_path = _tablet_mgr->segment_location(_target_tablet_id, target_filename);
    const auto second_virtual_path = _tablet_mgr->segment_location(second_target_tablet_id, target_filename);
    ASSERT_NE(first_virtual_path, second_virtual_path);
    ASSIGN_OR_ABORT(auto first_real_path, _location_provider->real_location(first_virtual_path));
    ASSIGN_OR_ABORT(auto second_real_path, _location_provider->real_location(second_virtual_path));
    ASSERT_EQ(first_real_path, second_real_path);

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    // Hold the single-flight leader after it has published its Future until the second
    // tablet task has joined that same key. This makes the concurrency assertion deterministic.
    CountDownLatch leader_started(1);
    CountDownLatch follower_joined(1);
    std::atomic<bool> leader_wait_timed_out{false};
    SyncPoint::GetInstance()->SetCallBack("singleflight::Group::Do:2", [&](void*) {
        leader_started.count_down();
        if (!follower_joined.wait_for(std::chrono::seconds(5))) {
            leader_wait_timed_out = true;
        }
    });
    SyncPoint::GetInstance()->SetCallBack("singleflight::Group::Do:1", [&](void*) { follower_joined.count_down(); });

    auto first_request = build_request(false /* with_full_path */);
    first_request.__set_virtual_tablet_id(next_id());
    auto second_request = first_request;
    second_request.__set_tablet_id(second_target_tablet_id);
    second_request.__set_src_tablet_id(_src_tablet_id + 1);

    Status first_status;
    Status second_status;
    std::thread first(
            [&]() { first_status = _replication_txn_manager->replicate_lake_remote_storage(first_request, nullptr); });
    bool observed_leader = leader_started.wait_for(std::chrono::seconds(5));
    std::thread second([&]() {
        second_status = _replication_txn_manager->replicate_lake_remote_storage(second_request, nullptr);
    });
    first.join();
    second.join();

    SyncPoint::GetInstance()->ClearCallBack("singleflight::Group::Do:1");
    SyncPoint::GetInstance()->ClearCallBack("singleflight::Group::Do:2");
    EXPECT_TRUE(observed_leader);
    EXPECT_FALSE(leader_wait_timed_out.load());
    ASSERT_OK(first_status);
    ASSERT_OK(second_status);
    ASSERT_EQ(1, mock_fs->opened_paths().size());

    auto verify_replicated_segment = [&](int64_t tablet_id, size_t expected_offset, size_t expected_size) {
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, _transaction_id));
        const auto& replicated_segment = txn_log->op_replication().tablet_metadata().rowsets(0).segment_metas(0);
        EXPECT_EQ(target_filename, replicated_segment.filename());
        EXPECT_TRUE(replicated_segment.shared());
        ASSERT_TRUE(replicated_segment.has_bundle_file_offset());
        EXPECT_EQ(expected_offset, replicated_segment.bundle_file_offset());
        EXPECT_EQ(expected_size, replicated_segment.size());
    };
    verify_replicated_segment(_target_tablet_id, 5, 11);
    verify_replicated_segment(second_target_tablet_id, 23, 13);

    // Corrupt the completed target's physical size. A later tablet must detect the mismatch and
    // recopy the complete immutable bundle rather than treating mere existence as completion.
    WritableFileOptions truncate_opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_ABORT(auto truncated_file, fs::new_writable_file(truncate_opts, first_virtual_path));
    ASSERT_OK(truncated_file->append(Slice("short")));
    ASSERT_OK(truncated_file->close());

    auto mismatch_request = first_request;
    mismatch_request.__set_tablet_id(mismatch_target_tablet_id);
    mismatch_request.__set_src_tablet_id(_src_tablet_id + 2);
    ASSERT_OK(_replication_txn_manager->replicate_lake_remote_storage(mismatch_request, nullptr));
    EXPECT_EQ(2, mock_fs->opened_paths().size());
    verify_replicated_segment(mismatch_target_tablet_id, 40, 12);

    ASSIGN_OR_ABORT(auto target_bundle_file, fs::new_random_access_file(first_virtual_path));
    ASSIGN_OR_ABORT(auto target_bundle_size, target_bundle_file->get_size());
    ASSERT_EQ(physical_bundle.size(), target_bundle_size);
    std::string target_bundle(target_bundle_size, '\0');
    ASSERT_OK(target_bundle_file->read_at_fully(0, target_bundle.data(), target_bundle.size()));
    EXPECT_EQ(physical_bundle, target_bundle);

    // Once the physical size is complete again, a non-overlapping retry reuses it without a third
    // source read and still publishes its own slice metadata.
    auto reuse_request = first_request;
    reuse_request.__set_tablet_id(reuse_target_tablet_id);
    reuse_request.__set_src_tablet_id(_src_tablet_id + 3);
    ASSERT_OK(_replication_txn_manager->replicate_lake_remote_storage(reuse_request, nullptr));
    EXPECT_EQ(2, mock_fs->opened_paths().size());
    verify_replicated_segment(reuse_target_tablet_id, 40, 12);

    (void)update_master_info(original_master_info);
}

TEST_F(LakeReplicationRemoteStorageTest, test_shared_bundle_final_log_failure_is_reclaimed_by_abort) {
    const std::string physical_bundle = "complete-bundle-after-retry";
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(physical_bundle);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    constexpr const char* kBundleFilename = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000014.dat";
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(10);
    rowset->set_data_size(physical_bundle.size() - 3);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(kBundleFilename);
    segment->set_size(physical_bundle.size() - 3);
    segment->set_bundle_file_offset(3);
    segment->set_shared(true);
    src_meta_v2->set_next_rowset_id(2);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    const int64_t second_target_tablet_id = _target_tablet_id + 1;
    use_aliased_local_location_provider({_target_tablet_id, second_target_tablet_id});
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_target_tablet_metadata));
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*generate_simple_tablet_metadata(second_target_tablet_id)));

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    // Fail only the final txn-log write, after PREPARED cleanup intents exist and the shared object
    // has been copied. Abort must recover the manifest without relying on process memory or full
    // vacuum.
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::before_final_txn_log", [&](void* arg) {
        *static_cast<Status*>(arg) = Status::InternalError("injected final txn log failure");
    });

    CountDownLatch leader_started(1);
    CountDownLatch follower_joined(1);
    std::atomic<bool> leader_wait_timed_out{false};
    SyncPoint::GetInstance()->SetCallBack("singleflight::Group::Do:2", [&](void*) {
        leader_started.count_down();
        if (!follower_joined.wait_for(std::chrono::seconds(5))) {
            leader_wait_timed_out = true;
        }
    });
    SyncPoint::GetInstance()->SetCallBack("singleflight::Group::Do:1", [&](void*) { follower_joined.count_down(); });

    auto first_request = build_request(false /* with_full_path */);
    first_request.__set_virtual_tablet_id(next_id());
    auto second_request = first_request;
    second_request.__set_tablet_id(second_target_tablet_id);
    second_request.__set_src_tablet_id(_src_tablet_id + 1);
    Status first_status;
    Status second_status;
    std::thread first(
            [&]() { first_status = _replication_txn_manager->replicate_lake_remote_storage(first_request, nullptr); });
    bool observed_leader = leader_started.wait_for(std::chrono::seconds(5));
    std::thread second([&]() {
        second_status = _replication_txn_manager->replicate_lake_remote_storage(second_request, nullptr);
    });
    first.join();
    second.join();
    SyncPoint::GetInstance()->ClearCallBack("singleflight::Group::Do:1");
    SyncPoint::GetInstance()->ClearCallBack("singleflight::Group::Do:2");
    SyncPoint::GetInstance()->ClearCallBack("LakeReplicationTxnManager::before_final_txn_log");

    EXPECT_TRUE(observed_leader);
    EXPECT_FALSE(leader_wait_timed_out.load());
    ASSERT_FALSE(first_status.ok());
    ASSERT_FALSE(second_status.ok());
    EXPECT_EQ(first_status.to_string(), second_status.to_string());
    EXPECT_EQ(1, mock_fs->opened_paths().size());

    const auto target_filename = gen_filename_from(_transaction_id, kBundleFilename);
    const auto target_path = _tablet_mgr->segment_location(_target_tablet_id, target_filename);
    EXPECT_TRUE(fs::path_exist(target_path));
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
    EXPECT_TRUE(_tablet_mgr->get_txn_log(second_target_tablet_id, _transaction_id).status().is_not_found());

    auto verify_prepared_manifest = [&](int64_t tablet_id) {
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_slog(tablet_id, _transaction_id));
        EXPECT_EQ(ReplicationTxnStatePB::TXN_PREPARED, txn_log->op_replication().txn_meta().txn_state());
        const auto& manifest = txn_log->op_replication().tablet_metadata().orphan_files();
        ASSERT_EQ(1, manifest.size());
        EXPECT_EQ(target_filename, manifest.Get(0).name());
        EXPECT_EQ(kReplicationCleanupFileVersion, manifest.Get(0).version());
    };
    verify_prepared_manifest(_target_tablet_id);
    verify_prepared_manifest(second_target_tablet_id);

    TxnInfoPB txn_info;
    txn_info.set_txn_id(_transaction_id);
    txn_info.set_txn_type(TxnTypePB::TXN_REPLICATION);
    std::vector<TxnInfoPB> txns{txn_info};
    abort_txn(_tablet_mgr.get(), _target_tablet_id, txns);
    abort_txn(_tablet_mgr.get(), second_target_tablet_id, txns);
    StorageEngine::instance()->wait_storage_cleanup_tasks();
    EXPECT_FALSE(fs::path_exist(target_path));
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
    EXPECT_TRUE(_tablet_mgr->get_txn_log(second_target_tablet_id, _transaction_id).status().is_not_found());
    EXPECT_TRUE(_tablet_mgr->get_txn_slog(_target_tablet_id, _transaction_id).status().is_not_found());
    EXPECT_TRUE(_tablet_mgr->get_txn_slog(second_target_tablet_id, _transaction_id).status().is_not_found());

    // A retry after abort performs a fresh copy and publishes a readable final log.
    ASSERT_OK(_replication_txn_manager->replicate_lake_remote_storage(first_request, nullptr));
    EXPECT_EQ(2, mock_fs->opened_paths().size());
    ASSIGN_OR_ABORT(auto target_bundle_file, fs::new_random_access_file(target_path));
    ASSIGN_OR_ABORT(auto target_bundle_size, target_bundle_file->get_size());
    ASSERT_EQ(physical_bundle.size(), target_bundle_size);
    std::string target_bundle(target_bundle_size, '\0');
    ASSERT_OK(target_bundle_file->read_at_fully(0, target_bundle.data(), target_bundle.size()));
    EXPECT_EQ(physical_bundle, target_bundle);

    (void)update_master_info(original_master_info);
}

TEST_F(LakeReplicationRemoteStorageTest, test_abort_fence_after_copy_removes_late_files_and_final_log) {
    const std::string physical_segment = "late-file-written-before-abort-fence";
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(physical_segment);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    constexpr const char* kSegmentFilename = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000015.dat";
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(10);
    rowset->set_data_size(physical_segment.size());
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(kSegmentFilename);
    segment->set_size(physical_segment.size());
    src_meta_v2->set_next_rowset_id(2);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    std::atomic<bool> aborted{false};
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::before_final_txn_log",
                                          [&](void*) { aborted = true; });

    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(next_id());
    Status status =
            _replication_txn_manager->replicate_lake_remote_storage(request, nullptr, [&]() { return aborted.load(); });
    SyncPoint::GetInstance()->ClearCallBack("LakeReplicationTxnManager::before_final_txn_log");
    (void)update_master_info(original_master_info);

    ASSERT_TRUE(status.is_aborted()) << status;
    const auto target_filename = gen_filename_from(_transaction_id, kSegmentFilename);
    const auto target_path = _tablet_mgr->segment_location(_target_tablet_id, target_filename);
    EXPECT_FALSE(fs::path_exist(target_path));
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
    ASSERT_OK(_tablet_mgr->get_txn_slog(_target_tablet_id, _transaction_id));

    TxnInfoPB txn_info;
    txn_info.set_txn_id(_transaction_id);
    txn_info.set_txn_type(TxnTypePB::TXN_REPLICATION);
    std::vector<TxnInfoPB> txns{txn_info};
    abort_txn(_tablet_mgr.get(), _target_tablet_id, txns);
    StorageEngine::instance()->wait_storage_cleanup_tasks();
    EXPECT_TRUE(_tablet_mgr->get_txn_slog(_target_tablet_id, _transaction_id).status().is_not_found());
}

TEST_F(LakeReplicationRemoteStorageTest, test_abort_fence_is_non_blocking_and_active_task_self_cleans) {
    const std::string physical_segment = "local-fence-stops-active-copy";
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(physical_segment);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    constexpr const char* kSegmentFilename = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000017.dat";
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(10);
    rowset->set_data_size(physical_segment.size());
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(kSegmentFilename);
    segment->set_size(physical_segment.size());
    src_meta_v2->set_next_rowset_id(2);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    CountDownLatch task_before_final_log(1);
    CountDownLatch release_task(1);
    CountDownLatch abort_fence_installed(1);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::before_final_txn_log", [&](void*) {
        task_before_final_log.count_down();
        release_task.wait();
    });
    SyncPoint::GetInstance()->SetCallBack("ReplicationTxnManager::abort_replication_txn:fenced",
                                          [&](void*) { abort_fence_installed.count_down(); });

    ReplicationTxnManager txn_manager(_tablet_mgr.get());
    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(next_id());
    Status replication_status;
    std::thread replication([&]() { replication_status = txn_manager.replicate_snapshot(request, nullptr); });
    if (!task_before_final_log.wait_for(std::chrono::seconds(5))) {
        release_task.count_down();
        replication.join();
        FAIL() << "replication task did not reach the final-log barrier";
    }

    CountDownLatch fence_returned(1);
    std::thread fence([&]() {
        txn_manager.abort_replication_txn(_transaction_id);
        fence_returned.count_down();
    });
    if (!abort_fence_installed.wait_for(std::chrono::seconds(5))) {
        release_task.count_down();
        replication.join();
        fence.join();
        FAIL() << "abort task did not install its local fence";
    }
    EXPECT_TRUE(fence_returned.wait_for(std::chrono::seconds(1)));
    release_task.count_down();
    replication.join();
    fence.join();
    SyncPoint::GetInstance()->ClearCallBack("LakeReplicationTxnManager::before_final_txn_log");
    SyncPoint::GetInstance()->ClearCallBack("ReplicationTxnManager::abort_replication_txn:fenced");
    (void)update_master_info(original_master_info);

    ASSERT_TRUE(replication_status.is_aborted()) << replication_status;
    const auto target_filename = gen_filename_from(_transaction_id, kSegmentFilename);
    EXPECT_FALSE(fs::path_exist(_tablet_mgr->segment_location(_target_tablet_id, target_filename)));
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
    ASSERT_OK(_tablet_mgr->get_txn_slog(_target_tablet_id, _transaction_id));

    TxnInfoPB txn_info;
    txn_info.set_txn_id(_transaction_id);
    txn_info.set_txn_type(TxnTypePB::TXN_REPLICATION);
    std::vector<TxnInfoPB> txns{txn_info};
    abort_txn(_tablet_mgr.get(), _target_tablet_id, txns);
    StorageEngine::instance()->wait_storage_cleanup_tasks();
    EXPECT_TRUE(_tablet_mgr->get_txn_slog(_target_tablet_id, _transaction_id).status().is_not_found());
}

TEST_F(LakeReplicationRemoteStorageTest, test_singleflight_failure_is_shared_and_retry_recovers) {
    const std::string physical_bundle = "complete-bundle-after-copy-retry";
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>(physical_bundle);
    mock_fs->set_fail_open_count(1);
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    constexpr const char* kBundleFilename = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000016.dat";
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_num_rows(10);
    rowset->set_data_size(physical_bundle.size() - 4);
    auto* segment = rowset->add_segment_metas();
    segment->set_filename(kBundleFilename);
    segment->set_size(physical_bundle.size() - 4);
    segment->set_bundle_file_offset(4);
    segment->set_shared(true);
    src_meta_v2->set_next_rowset_id(2);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    const int64_t second_target_tablet_id = _target_tablet_id + 1;
    use_aliased_local_location_provider({_target_tablet_id, second_target_tablet_id});
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_target_tablet_metadata));
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*generate_simple_tablet_metadata(second_target_tablet_id)));

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    CountDownLatch leader_started(1);
    CountDownLatch follower_joined(1);
    std::atomic<bool> leader_wait_timed_out{false};
    SyncPoint::GetInstance()->SetCallBack("singleflight::Group::Do:2", [&](void*) {
        leader_started.count_down();
        if (!follower_joined.wait_for(std::chrono::seconds(5))) {
            leader_wait_timed_out = true;
        }
    });
    SyncPoint::GetInstance()->SetCallBack("singleflight::Group::Do:1", [&](void*) { follower_joined.count_down(); });

    auto first_request = build_request(false /* with_full_path */);
    first_request.__set_virtual_tablet_id(next_id());
    auto second_request = first_request;
    second_request.__set_tablet_id(second_target_tablet_id);
    second_request.__set_src_tablet_id(_src_tablet_id + 1);
    Status first_status;
    Status second_status;
    std::thread first(
            [&]() { first_status = _replication_txn_manager->replicate_lake_remote_storage(first_request, nullptr); });
    bool observed_leader = leader_started.wait_for(std::chrono::seconds(5));
    std::thread second([&]() {
        second_status = _replication_txn_manager->replicate_lake_remote_storage(second_request, nullptr);
    });
    first.join();
    second.join();
    SyncPoint::GetInstance()->ClearCallBack("singleflight::Group::Do:1");
    SyncPoint::GetInstance()->ClearCallBack("singleflight::Group::Do:2");

    EXPECT_TRUE(observed_leader);
    EXPECT_FALSE(leader_wait_timed_out.load());
    ASSERT_FALSE(first_status.ok());
    ASSERT_FALSE(second_status.ok());
    EXPECT_EQ(first_status.to_string(), second_status.to_string());
    EXPECT_EQ(1, mock_fs->opened_paths().size());
    ASSIGN_OR_ABORT(auto first_intent, _tablet_mgr->get_txn_slog(_target_tablet_id, _transaction_id));
    ASSIGN_OR_ABORT(auto second_intent, _tablet_mgr->get_txn_slog(second_target_tablet_id, _transaction_id));
    EXPECT_EQ(ReplicationTxnStatePB::TXN_PREPARED, first_intent->op_replication().txn_meta().txn_state());
    EXPECT_EQ(ReplicationTxnStatePB::TXN_PREPARED, second_intent->op_replication().txn_meta().txn_state());

    const auto target_filename = gen_filename_from(_transaction_id, kBundleFilename);
    const auto target_path = _tablet_mgr->segment_location(_target_tablet_id, target_filename);
    EXPECT_FALSE(fs::path_exist(target_path));

    TxnInfoPB txn_info;
    txn_info.set_txn_id(_transaction_id);
    txn_info.set_txn_type(TxnTypePB::TXN_REPLICATION);
    std::vector<TxnInfoPB> txns{txn_info};
    abort_txn(_tablet_mgr.get(), _target_tablet_id, txns);
    abort_txn(_tablet_mgr.get(), second_target_tablet_id, txns);
    StorageEngine::instance()->wait_storage_cleanup_tasks();

    // Group::Do removes a failed flight, so retry after abort executes a fresh source read and copy.
    ASSERT_OK(_replication_txn_manager->replicate_lake_remote_storage(first_request, nullptr));
    EXPECT_EQ(2, mock_fs->opened_paths().size());
    ASSIGN_OR_ABORT(auto target_bundle_file, fs::new_random_access_file(target_path));
    ASSIGN_OR_ABORT(auto target_bundle_size, target_bundle_file->get_size());
    ASSERT_EQ(physical_bundle.size(), target_bundle_size);
    std::string target_bundle(target_bundle_size, '\0');
    ASSERT_OK(target_bundle_file->read_at_fully(0, target_bundle.data(), target_bundle.size()));
    EXPECT_EQ(physical_bundle, target_bundle);

    (void)update_master_info(original_master_info);
}

TEST_F(LakeReplicationRemoteStorageTest, test_reject_column_id_conversion_for_bundled_segment) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    src_meta_v2->mutable_schema()->mutable_column(0)->set_unique_id(101);
    auto* segment = src_meta_v2->add_rowsets()->add_segment_metas();
    segment->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000010.dat");
    segment->set_size(128);
    segment->set_bundle_file_offset(256);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(next_id());
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);
    EXPECT_TRUE(status.is_not_supported()) << status;
    EXPECT_NE(std::string::npos, status.message().find("column unique ids"));
    EXPECT_TRUE(mock_fs->opened_paths().empty());
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
    std::vector<std::string> target_files;
    ASSERT_OK(fs::get_children(lake::join_path(_test_dir, lake::kSegmentDirectoryName), &target_files));
    EXPECT_TRUE(target_files.empty());
}

TEST_F(LakeReplicationRemoteStorageTest, test_reject_encrypted_bundled_segment) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* segment = src_meta_v2->add_rowsets()->add_segment_metas();
    segment->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000011.dat");
    segment->set_size(128);
    segment->set_bundle_file_offset(256);
    segment->set_encryption_meta("source-slice-encryption-meta");
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(next_id());
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);
    EXPECT_TRUE(status.is_not_supported()) << status;
    EXPECT_NE(std::string::npos, status.message().find("encrypted bundled segments"));
    EXPECT_TRUE(mock_fs->opened_paths().empty());
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
    std::vector<std::string> target_files;
    ASSERT_OK(fs::get_children(lake::join_path(_test_dir, lake::kSegmentDirectoryName), &target_files));
    EXPECT_TRUE(target_files.empty());
}

TEST_F(LakeReplicationRemoteStorageTest, test_reject_encrypted_source_segment_without_target_encryption) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* segment = src_meta_v2->add_rowsets()->add_segment_metas();
    segment->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000015.dat");
    segment->set_size(128);
    segment->set_encryption_meta("source-encryption-meta");
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    BoolConfigGuard encryption_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;
    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(next_id());
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);
    EXPECT_TRUE(status.is_not_supported()) << status;
    EXPECT_NE(std::string::npos, status.message().find("encrypted source files"));
    EXPECT_TRUE(mock_fs->opened_paths().empty());
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
    std::vector<std::string> target_files;
    ASSERT_OK(fs::get_children(lake::join_path(_test_dir, lake::kSegmentDirectoryName), &target_files));
    EXPECT_TRUE(target_files.empty());
}

TEST_F(LakeReplicationRemoteStorageTest, test_reject_encrypted_shared_delvec_without_target_encryption) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto& delvec = (*src_meta_v2->mutable_delvec_meta()->mutable_version_to_file())[2];
    delvec.set_name("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000018.delvec");
    delvec.set_shared(true);
    delvec.set_encryption_meta("source-shared-delvec-encryption-meta");
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    BoolConfigGuard encryption_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = false;
    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(next_id());
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);
    EXPECT_TRUE(status.is_not_supported()) << status;
    EXPECT_NE(std::string::npos, status.message().find("encrypted source files"));
    EXPECT_TRUE(mock_fs->opened_paths().empty());
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
    std::vector<std::string> target_files;
    ASSERT_OK(fs::get_children(lake::join_path(_test_dir, lake::kSegmentDirectoryName), &target_files));
    EXPECT_TRUE(target_files.empty());
}

TEST_F(LakeReplicationRemoteStorageTest, test_reject_shared_file_with_target_encryption) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* segment = src_meta_v2->add_rowsets()->add_segment_metas();
    segment->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000013.dat");
    segment->set_size(128);
    segment->set_shared(true);
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });

    BoolConfigGuard encryption_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = true;
    auto request = build_request(false /* with_full_path */);
    request.__set_virtual_tablet_id(next_id());
    auto status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);
    EXPECT_TRUE(status.is_not_supported()) << status;
    EXPECT_NE(std::string::npos, status.message().find("shared files with transparent data encryption"));
    EXPECT_TRUE(mock_fs->opened_paths().empty());
    EXPECT_TRUE(_tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id).status().is_not_found());
}

// Test Case 9: Sequential copy with mocked file operations - covers task lambda body,
// segment download path, non-segment copy path, size tracking, encryption, slow log.
TEST_F(LakeReplicationRemoteStorageTest, test_sequential_copy_with_mocked_file_operations) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    mock_fs->set_stat_size(8192);
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
    auto* bundled_rowset = src_meta_v2->add_rowsets();
    bundled_rowset->set_id(2);
    bundled_rowset->set_overlapped(false);
    bundled_rowset->set_num_rows(10);
    bundled_rowset->set_data_size(1234);
    auto* bundled_segment = bundled_rowset->add_segment_metas();
    bundled_segment->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000004.dat");
    bundled_segment->set_size(1234); // logical segment size, not the physical bundle size
    bundled_segment->set_bundle_file_offset(4096);
    // Add a delvec for non-segment path
    auto* delvec_meta = src_meta_v2->mutable_delvec_meta();
    auto& delvec_entry = (*delvec_meta->mutable_version_to_file())[2];
    delvec_entry.set_name("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000003.delvec");
    src_meta_v2->set_next_rowset_id(3);

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
    // Use a unique virtual shard so this test cannot reuse a Starlet filesystem cached by an
    // earlier test that installed a different mock implementation.
    request.__set_virtual_tablet_id(next_id());
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);

    (void)update_master_info(original_master_info);

    ASSERT_OK(status);
    auto txn_log = _tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id);
    ASSERT_OK(txn_log);
    const auto& replicated_rowsets = txn_log.value()->op_replication().tablet_metadata().rowsets();
    ASSERT_EQ(2, replicated_rowsets.size());
    EXPECT_EQ(2048, replicated_rowsets.Get(0).segment_metas(0).size());
    const auto& replicated_bundled_segment = replicated_rowsets.Get(1).segment_metas(0);
    EXPECT_EQ(1234, replicated_bundled_segment.size());
    ASSERT_TRUE(replicated_bundled_segment.has_bundle_file_offset());
    EXPECT_EQ(4096, replicated_bundled_segment.bundle_file_offset());
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

    // Create thread pool and pass it to replication manager
    std::unique_ptr<ThreadPool> pool;
    ASSERT_OK(ThreadPoolBuilder("lake_repl_test")
                      .set_min_threads(2)
                      .set_max_threads(4)
                      .set_max_queue_size(16)
                      .build(&pool));

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, pool.get());

    (void)update_master_info(original_master_info);

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
    ASSERT_OK(ThreadPoolBuilder("lake_repl_err")
                      .set_min_threads(2)
                      .set_max_threads(4)
                      .set_max_queue_size(16)
                      .build(&pool));

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, pool.get());

    (void)update_master_info(original_master_info);

    // Parallel copy should fail because download_lake_file_with_converter fails with mock FS
    EXPECT_FALSE(status.ok());
    pool->shutdown();
}

// Regression: lake-to-lake replication must replicate the source's IDG (.idx) fast-path
// indexes and must NOT carry the target's pre-replication idg_meta.
// Before the fix, convert_and_build_new_tablet_meta cleared rowsets/dcg/sstable/delvec but
// left idg_meta untouched and never rebuilt it from the source, so the built (copied) tablet
// metadata carried the target's STALE idg_meta and never picked up the source's — silently
// losing the index on the replica and leaking the target's stale .idx files. This drives the
// full replicate path and inspects the produced replication txn log (whose tablet_metadata is
// exactly what the publish-time applier commits).
TEST_F(LakeReplicationRemoteStorageTest, test_idg_meta_replicated_and_stale_dropped) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    // Give the TARGET tablet (data_version 1) a pre-existing (stale) idg_meta entry keyed by an
    // rssid the replicated rowsets won't use, referencing a distinct .idx file. It must not survive.
    const std::string stale_idx = "00000000000000ff_ffffffff-ffff-ffff-ffff-0000000000ff.idx";
    {
        auto target_v1 = std::make_shared<TabletMetadata>(*_target_tablet_metadata);
        auto& stale_ver = (*target_v1->mutable_idg_meta()->mutable_idgs())[999];
        auto* stale_entry = stale_ver.add_entries();
        auto* sk = stale_entry->add_keys();
        sk->set_col_unique_id(42);
        sk->set_index_type(BITMAP);
        stale_entry->set_index_file(stale_idx);
        stale_entry->set_version(1);
        // A stale entry with an empty index_file exercises the empty-index_file skip in
        // build_existed_filename_uuids_map (the file is not registered for dedup); it must
        // still be dropped along with the rest of the target's stale idg_meta.
        auto* stale_empty = stale_ver.add_entries();
        stale_empty->add_keys()->set_col_unique_id(43);
        stale_empty->set_version(1); // index_file intentionally unset
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*target_v1));
    }

    // Source tablet metadata (version 2) with one rowset + segment and a source IDG entry
    // (rssid=1) referencing a source .idx file.
    const std::string src_idx = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000aa.idx";
    const std::string src_uuid = "aaaaaaaa-bbbb-cccc-dddd-0000000000aa";
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(4096);
    rowset->add_segment_metas()->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
    src_meta_v2->set_next_rowset_id(2);
    {
        auto& src_ver = (*src_meta_v2->mutable_idg_meta()->mutable_idgs())[1];
        auto* src_entry = src_ver.add_entries();
        auto* k = src_entry->add_keys();
        k->set_col_unique_id(1);
        k->set_index_type(NGRAMBF); // non-default (BITMAP==0) so carry-through is discriminating
        src_entry->set_index_file(src_idx);
        src_entry->set_version(2);
    }
    {
        // A source entry with no index_file exercises the empty-index_file skip in the rebuild
        // loop: it must be carried through verbatim (no filename rewrite, no copy registration).
        auto& src_ver2 = (*src_meta_v2->mutable_idg_meta()->mutable_idgs())[2];
        auto* e = src_ver2.add_entries();
        e->add_keys()->set_col_unique_id(7);
        e->set_version(2); // index_file intentionally unset
    }

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::download_segment",
                                          [&](void* arg) { *static_cast<size_t*>(arg) = 1024; });
    // The .idx file goes through the non-segment copy path (use_converter=false); mock it.
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::copy_non_segment",
                                          [&](void* arg) { *static_cast<size_t*>(arg) = 512; });

    Int32ConfigGuard min_file_guard(&config::lake_replication_parallel_copy_min_file_count);
    config::lake_replication_parallel_copy_min_file_count = 0; // sequential path

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);
    (void)update_master_info(original_master_info);
    ASSERT_OK(status);

    // Inspect the produced replication txn log; its tablet_metadata is what publish will apply.
    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id));
    ASSERT_TRUE(txn_log->has_op_replication());
    ASSERT_TRUE(txn_log->op_replication().has_tablet_metadata());
    const auto& built_meta = txn_log->op_replication().tablet_metadata();

    // The source's IDG entries (rssid=1 with a .idx, rssid=2 with none) survive; the target's
    // stale entries (rssid=999) are gone.
    const auto& idgs = built_meta.idg_meta().idgs();
    ASSERT_EQ(2u, idgs.size());
    ASSERT_TRUE(idgs.find(999) == idgs.end());
    auto it1 = idgs.find(1);
    ASSERT_TRUE(it1 != idgs.end());

    // The empty-index_file source entry (rssid=2) is carried through verbatim: still present,
    // still has no index_file (the rebuild loop skipped it without rewriting/registering a file).
    auto it2 = idgs.find(2);
    ASSERT_TRUE(it2 != idgs.end());
    ASSERT_EQ(1, it2->second.entries_size());
    EXPECT_FALSE(it2->second.entries(0).has_index_file());

    const auto& built_ver = it1->second;
    ASSERT_EQ(1, built_ver.entries_size());
    const auto& built_entry = built_ver.entries(0);
    ASSERT_TRUE(built_entry.has_index_file());
    // The .idx filename is rewritten to the target's txn-scoped name (source UUID preserved),
    // so it differs from both the raw source name and the target's stale name.
    EXPECT_NE(src_idx, built_entry.index_file());
    EXPECT_NE(stale_idx, built_entry.index_file());
    EXPECT_NE(std::string::npos, built_entry.index_file().find(".idx"));
    EXPECT_NE(std::string::npos, built_entry.index_file().find(src_uuid)); // UUID carried across rename
    EXPECT_EQ(extract_uuid_from(src_idx), extract_uuid_from(built_entry.index_file()));
    // The stale .idx filename must never appear in the built metadata.
    EXPECT_EQ(std::string::npos, built_entry.index_file().find("0000000000ff"));
    // Key metadata is carried through.
    ASSERT_EQ(1, built_entry.keys_size());
    EXPECT_EQ(1, built_entry.keys(0).col_unique_id());
    EXPECT_EQ(NGRAMBF, built_entry.keys(0).index_type());
    EXPECT_EQ(2, built_entry.version());
}

// Regression: when the source and target tablets assign DIFFERENT column unique ids to the same
// logical column (fast-schema-change divergence, calc_column_unique_id_map non-empty), replication
// must NOT copy the source idg_meta / .idx verbatim. The IDG entry keys and the col_unique_ids
// embedded in the .idx payload footer are keyed by the SOURCE id, while the scan probe and
// IndexFileReader::find() look up by the TARGET id, so a verbatim copy would be silently ignored or
// (on a unique-id collision) mis-applied to a different column and prune rows wrongly. Until the
// .idx-footer + IDG-key remap is implemented, the fast path is skipped and idg_meta stays empty on
// the replica (index absent, to be rebuilt on the target) -- never a mismappable index.
TEST_F(LakeReplicationRemoteStorageTest, test_idg_meta_skipped_on_divergent_column_ids) {
    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    // Source tablet metadata (version 2) with one rowset + segment and a source IDG entry.
    const std::string src_idx = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000aa.idx";
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    // Diverge the source's column unique-id space from the target's (target c1 uid == 2). This makes
    // calc_column_unique_id_map() non-empty and must trigger the IDG replication skip.
    src_meta_v2->mutable_schema()->mutable_column(1)->set_unique_id(9999);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(4096);
    rowset->add_segment_metas()->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
    src_meta_v2->set_next_rowset_id(2);
    {
        auto& src_ver = (*src_meta_v2->mutable_idg_meta()->mutable_idgs())[1];
        auto* src_entry = src_ver.add_entries();
        auto* k = src_entry->add_keys();
        k->set_col_unique_id(2); // source-space id for column c1
        k->set_index_type(NGRAMBF);
        src_entry->set_index_file(src_idx);
        src_entry->set_version(2);
    }

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::download_segment",
                                          [&](void* arg) { *static_cast<size_t*>(arg) = 1024; });
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::copy_non_segment",
                                          [&](void* arg) { *static_cast<size_t*>(arg) = 512; });

    Int32ConfigGuard min_file_guard(&config::lake_replication_parallel_copy_min_file_count);
    config::lake_replication_parallel_copy_min_file_count = 0; // sequential path

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);
    (void)update_master_info(original_master_info);
    ASSERT_OK(status);

    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id));
    ASSERT_TRUE(txn_log->has_op_replication());
    ASSERT_TRUE(txn_log->op_replication().has_tablet_metadata());
    const auto& built_meta = txn_log->op_replication().tablet_metadata();

    // The rowset/segment still replicates; only the IDG fast-path index is skipped.
    ASSERT_EQ(1, built_meta.rowsets_size());
    // idg_meta must be empty: neither the source's entry nor any target stale entry survives.
    EXPECT_TRUE(built_meta.idg_meta().idgs().empty());
}

// Regression companion: with transparent data encryption ON, the replicated source IDG entry must
// carry a freshly-derived (target-side) encryption_meta for its .idx file, mirroring the segment/
// sstable/dcg handling. Covers the enable_transparent_data_encryption branch of the idg rebuild.
TEST_F(LakeReplicationRemoteStorageTest, test_idg_meta_replicated_encrypted) {
    // Seed a master key so create_encryption_meta_pair_using_current_kek() succeeds.
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
    BoolConfigGuard enc_guard(&config::enable_transparent_data_encryption);
    config::enable_transparent_data_encryption = true;

    auto mock_fs = std::make_shared<MockStarletFileSystemForReplication>();
    SyncPoint::GetInstance()->SetCallBack("new_fs_starlet::get_shard_filesystem", [&](void* arg) {
        auto* fs_st = static_cast<absl::StatusOr<std::shared_ptr<staros::starlet::fslib::FileSystem>>*>(arg);
        *fs_st = mock_fs;
    });

    const std::string src_idx = "0000000000000001_aaaaaaaa-bbbb-cccc-dddd-0000000000aa.idx";
    auto src_meta_v2 = std::make_shared<TabletMetadata>(*_src_tablet_metadata);
    src_meta_v2->set_version(2);
    auto* rowset = src_meta_v2->add_rowsets();
    rowset->set_id(1);
    rowset->set_overlapped(false);
    rowset->set_num_rows(10);
    rowset->set_data_size(4096);
    rowset->add_segment_metas()->set_filename("0000000000000001_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat");
    src_meta_v2->set_next_rowset_id(2);
    {
        auto& src_ver = (*src_meta_v2->mutable_idg_meta()->mutable_idgs())[1];
        auto* src_entry = src_ver.add_entries();
        src_entry->add_keys()->set_col_unique_id(1);
        src_entry->set_index_file(src_idx);
        src_entry->set_version(2);
    }

    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                                          [&](void* arg) {
                                              auto* meta_ptr = static_cast<TabletMetadataPtr*>(arg);
                                              *meta_ptr = src_meta_v2;
                                          });
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::download_segment",
                                          [&](void* arg) { *static_cast<size_t*>(arg) = 1024; });
    SyncPoint::GetInstance()->SetCallBack("LakeReplicationTxnManager::replicate_task::copy_non_segment",
                                          [&](void* arg) { *static_cast<size_t*>(arg) = 512; });

    Int32ConfigGuard min_file_guard(&config::lake_replication_parallel_copy_min_file_count);
    config::lake_replication_parallel_copy_min_file_count = 0; // sequential path

    auto original_master_info = get_master_info();
    TMasterInfo info = original_master_info;
    info.__set_min_active_txn_id(0);
    ASSERT_TRUE(update_master_info(info));

    auto request = build_request(false /* with_full_path */);
    Status status = _replication_txn_manager->replicate_lake_remote_storage(request, nullptr);
    (void)update_master_info(original_master_info);
    ASSERT_OK(status);

    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(_target_tablet_id, _transaction_id));
    ASSERT_TRUE(txn_log->has_op_replication());
    ASSERT_TRUE(txn_log->op_replication().has_tablet_metadata());
    const auto& built_meta = txn_log->op_replication().tablet_metadata();

    const auto& idgs = built_meta.idg_meta().idgs();
    auto it1 = idgs.find(1);
    ASSERT_TRUE(it1 != idgs.end());
    ASSERT_EQ(1, it1->second.entries_size());
    const auto& built_entry = it1->second.entries(0);
    // The .idx entry carries a freshly-derived, non-empty target encryption_meta.
    EXPECT_TRUE(built_entry.has_encryption_meta());
    EXPECT_FALSE(built_entry.encryption_meta().empty());
}
#endif // USE_STAROS

INSTANTIATE_TEST_SUITE_P(SharedDataReplicationTxnManagerTest, SharedDataReplicationTxnManagerTest,
                         testing::Values(KeysType::DUP_KEYS, KeysType::AGG_KEYS, KeysType::PRIMARY_KEYS));

} // namespace starrocks::lake
