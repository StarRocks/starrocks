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

#include <random>

#include "testutil/sync_point.h"
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
<<<<<<< HEAD
#include "common/config.h"
=======
#include "common/config_lake_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/config_starlet_fwd.h"
#include "fs/fs_factory.h"
>>>>>>> 2fffac2c47 ([Enhancement] Add file integrity verification for shared-data cross-cluster replication with non-segment file copy (#70093))
        #include "fs/fs_starlet.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gutil/strings/join.h"
#include "runtime/exec_env.h"
#include "service/staros_worker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/meta_file.h"
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
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/failpoint/fail_point.h"

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
                ASSERT_OK(lake::publish_version(_tablet_mgr.get(), PublishTabletInfo(_src_tablet_id), version,
                                                version + 1, txn_info_span, false));
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

    TEST_P(SharedDataReplicationTxnManagerTest, test_replicate_normal) {
        // Skipped: this test relied on the removed local filesystem fallback code path (#else block).
        // The USE_STAROS code path is now covered by LakeReplicationRemoteStorageTest.
        GTEST_SKIP() << "Local filesystem fallback code path has been removed";
        // write data
        write_src_tablet_data();

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
        request.__set_src_visible_version(_src_version);
        request.__set_src_db_id(_src_db_id);
        request.__set_src_table_id(_src_table_id);
        request.__set_src_partition_id(_src_partition_id);

        // virtual tablet
        request.__set_virtual_tablet_id(_virtual_tablet_id);

        Status status = _replication_txn_manager->replicate_lake_remote_storage(request);
        EXPECT_TRUE(status.ok()) << status;

        auto txn_info = TxnInfoPB();
        txn_info.set_txn_id(_transaction_id);
        txn_info.set_combined_txn_log(false);
        txn_info.set_commit_time(0);
        auto txn_info_span = std::span<const TxnInfoPB>(&txn_info, 1);
        auto status_or = lake::publish_version(_tablet_mgr.get(), PublishTabletInfo(_target_tablet_id), _version,
                                               _src_version, txn_info_span, false);
        EXPECT_TRUE(status_or.ok()) << status_or.status();

        EXPECT_EQ(_src_version, status_or.value()->version());
        // Verify compaction_inputs: since target tablet was empty before replication,
        // old_rowsets is empty, so compaction_inputs should also be empty.
        EXPECT_EQ(0, status_or.value()->compaction_inputs_size());
    }

    TEST_P(SharedDataReplicationTxnManagerTest, test_replicate_continuous_same_rowset_id) {
        // Skipped: this test relied on the removed local filesystem fallback code path (#else block).
        // The USE_STAROS code path is now covered by LakeReplicationRemoteStorageTest.
        GTEST_SKIP() << "Local filesystem fallback code path has been removed";
        // This test verifies that when performing continuous lake replication,
        // rowsets with the same ID as new rowsets are NOT added to compaction_inputs.
        // This is critical because lake replication preserves rowset IDs from source,
        // and files referenced by both old and new rowsets should not be deleted by vacuum.

        // Step 1: Write data to source tablet (creates 3 rowsets with versions 2, 3, 4)
        write_src_tablet_data();
        // Now _src_version = 4, source tablet has 3 rowsets

        // Step 2: First replication from v1 to v4
        TReplicateSnapshotRequest request1;
        request1.__set_transaction_id(_transaction_id);
        request1.__set_table_id(_table_id);
        request1.__set_partition_id(_partition_id);
        request1.__set_tablet_id(_target_tablet_id);
        request1.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
        request1.__set_schema_hash(_schema_hash);
        request1.__set_visible_version(_version);
        request1.__set_data_version(_version);
        request1.__set_src_tablet_id(_src_tablet_id);
        request1.__set_src_tablet_type(TTabletType::TABLET_TYPE_LAKE);
        request1.__set_src_visible_version(_src_version);
        request1.__set_src_db_id(_src_db_id);
        request1.__set_src_table_id(_src_table_id);
        request1.__set_src_partition_id(_src_partition_id);
        request1.__set_virtual_tablet_id(_virtual_tablet_id);

        ASSERT_TRUE(_replication_txn_manager->replicate_lake_remote_storage(request1).ok());

        auto txn_info1 = TxnInfoPB();
        txn_info1.set_txn_id(_transaction_id);
        txn_info1.set_combined_txn_log(false);
        txn_info1.set_commit_time(0);
        auto txn_info_span1 = std::span<const TxnInfoPB>(&txn_info1, 1);
        auto status_or1 = lake::publish_version(_tablet_mgr.get(), _target_tablet_id, _version, _src_version,
                                                txn_info_span1, false);
        ASSERT_TRUE(status_or1.ok()) << status_or1.status();
        EXPECT_EQ(_src_version, status_or1.value()->version());
        EXPECT_EQ(3, status_or1.value()->rowsets_size());
        // First replication: target was empty, so compaction_inputs should be empty
        EXPECT_EQ(0, status_or1.value()->compaction_inputs_size());

        // Collect rowset IDs from first replication
        std::unordered_set<uint32_t> first_replication_rowset_ids;
        for (const auto& rowset : status_or1.value()->rowsets()) {
            first_replication_rowset_ids.insert(rowset.id());
        }

        // Step 3: Second replication with the same source version (simulates incremental sync
        // where new metadata still contains some of the same rowset IDs)
        // In this case, we replicate again from v4 to v4 (same snapshot)
        auto new_txn_id = _transaction_id + 1;
        TReplicateSnapshotRequest request2;
        request2.__set_transaction_id(new_txn_id);
        request2.__set_table_id(_table_id);
        request2.__set_partition_id(_partition_id);
        request2.__set_tablet_id(_target_tablet_id);
        request2.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
        request2.__set_schema_hash(_schema_hash);
        request2.__set_visible_version(_src_version); // target is now at v4
        request2.__set_data_version(_src_version);    // data version is v4
        request2.__set_src_tablet_id(_src_tablet_id);
        request2.__set_src_tablet_type(TTabletType::TABLET_TYPE_LAKE);
        request2.__set_src_visible_version(_src_version); // source is still at v4
        request2.__set_src_db_id(_src_db_id);
        request2.__set_src_table_id(_src_table_id);
        request2.__set_src_partition_id(_src_partition_id);
        request2.__set_virtual_tablet_id(_virtual_tablet_id);

        // This should fail because there are no missing versions (v4 -> v4)
        // So we need a different approach: write more data to source and then replicate
        // Let's write one more version to source tablet
        auto chunk0 = generate_data(kChunkSize, 10, 3);
        auto indexes = std::vector<uint32_t>(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) {
            indexes[i] = i;
        }
        auto write_txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, lake::DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_src_tablet_id)
                                                   .set_txn_id(write_txn_id)
                                                   .set_partition_id(_src_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_src_tablet_metadata->schema().id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        auto write_txn_info = TxnInfoPB();
        write_txn_info.set_txn_id(write_txn_id);
        write_txn_info.set_combined_txn_log(false);
        write_txn_info.set_commit_time(0);
        auto write_txn_info_span = std::span<const TxnInfoPB>(&write_txn_info, 1);
        ASSERT_OK(lake::publish_version(_tablet_mgr.get(), _src_tablet_id, _src_version, _src_version + 1,
                                        write_txn_info_span, false));
        auto new_src_version = _src_version + 1; // v5

        // Step 4: Second replication from v4 to v5
        request2.__set_visible_version(_src_version);        // target visible version is v4
        request2.__set_data_version(_src_version);           // data version is v4
        request2.__set_src_visible_version(new_src_version); // source is now at v5

        ASSERT_TRUE(_replication_txn_manager->replicate_lake_remote_storage(request2).ok());

        auto txn_info2 = TxnInfoPB();
        txn_info2.set_txn_id(new_txn_id);
        txn_info2.set_combined_txn_log(false);
        txn_info2.set_commit_time(0);
        auto txn_info_span2 = std::span<const TxnInfoPB>(&txn_info2, 1);
        auto status_or2 = lake::publish_version(_tablet_mgr.get(), _target_tablet_id, _src_version, new_src_version,
                                                txn_info_span2, false);
        ASSERT_TRUE(status_or2.ok()) << status_or2.status();
        EXPECT_EQ(new_src_version, status_or2.value()->version());
        EXPECT_EQ(4, status_or2.value()->rowsets_size());

        // Verify compaction_inputs: since all old rowset IDs (from v4) are still present in new rowsets (v5),
        // the compaction_inputs should be empty - no rowsets should be marked for garbage collection
        EXPECT_EQ(0, status_or2.value()->compaction_inputs_size())
                << "compaction_inputs should be empty because all old rowset IDs are still present in new metadata";

        // Also verify that the rowset IDs from first replication are still in the new metadata
        for (const auto& rowset : status_or2.value()->rowsets()) {
            if (first_replication_rowset_ids.count(rowset.id()) > 0) {
                first_replication_rowset_ids.erase(rowset.id());
            }
        }
        EXPECT_TRUE(first_replication_rowset_ids.empty())
                << "All rowset IDs from first replication should still be present in new metadata";
    }

    TEST_P(SharedDataReplicationTxnManagerTest, test_replicate_normal_encrypted) {
        // Skipped: this test relied on the removed local filesystem fallback code path (#else block).
        // The USE_STAROS code path is now covered by LakeReplicationRemoteStorageTest.
        GTEST_SKIP() << "Local filesystem fallback code path has been removed";
        EncryptionKeyPB pb;
        pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
        pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
        pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
        pb.set_plain_key("0000000000000000");
        std::unique_ptr<EncryptionKey> root_encryption_key = EncryptionKey::create_from_pb(pb).value();
        auto val_st = root_encryption_key->generate_key();
        EXPECT_TRUE(val_st.ok());
        std::unique_ptr<EncryptionKey> encryption_key = std::move(val_st.value());
        encryption_key->set_id(2);
        KeyCache::instance().add_key(root_encryption_key);
        KeyCache::instance().add_key(encryption_key);

        // write source tablet data (without encryption)
        write_src_tablet_data();

        // enable transparent data encryption
        config::enable_transparent_data_encryption = true;

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
        request.__set_src_visible_version(_src_version);
        request.__set_src_db_id(_src_db_id);
        request.__set_src_table_id(_src_table_id);
        request.__set_src_partition_id(_src_partition_id);

        // virtual tablet
        request.__set_virtual_tablet_id(_virtual_tablet_id);

        Status status = _replication_txn_manager->replicate_lake_remote_storage(request);
        EXPECT_TRUE(status.ok()) << status;

        auto txn_info = TxnInfoPB();
        txn_info.set_txn_id(_transaction_id);
        txn_info.set_combined_txn_log(false);
        txn_info.set_commit_time(0);
        auto txn_info_span = std::span<const TxnInfoPB>(&txn_info, 1);
        auto status_or = lake::publish_version(_tablet_mgr.get(), PublishTabletInfo(_target_tablet_id), _version,
                                               _src_version, txn_info_span, false);
        EXPECT_TRUE(status_or.ok()) << status_or.status();

        EXPECT_EQ(_src_version, status_or.value()->version());
        // Verify compaction_inputs: since target tablet was empty before replication,
        // old_rowsets is empty, so compaction_inputs should also be empty.
        EXPECT_EQ(0, status_or.value()->compaction_inputs_size());
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
        auto result = LakeReplicationTxnManager::copy_non_segment_file_with_retry(src_path, src_fs, dst_path, opts,
                                                                                  max_retry);
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
            return lake::join_path(_test_dir,
                                   fmt::format("db{}/{}/{}/meta", _src_db_id, _src_table_id, _src_partition_id));
        }
        std::string build_current_format_data_dir() {
            return lake::join_path(_test_dir,
                                   fmt::format("db{}/{}/{}/data", _src_db_id, _src_table_id, _src_partition_id));
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
    // Mock staros::starlet::fslib::FileSystem for SyncPoint injection
    class MockStarletFileSystemForReplication : public staros::starlet::fslib::FileSystem {
    public:
        MockStarletFileSystemForReplication() : staros::starlet::fslib::FileSystem() {}
        ~MockStarletFileSystemForReplication() override = default;

        std::string_view scheme() override { return "mock"; }

        absl::StatusOr<std::unique_ptr<staros::starlet::fslib::ReadOnlyFile>> open(
                std::string_view path, const staros::starlet::fslib::ReadOptions& opts) override {
            return absl::UnimplementedError("MockStarletFileSystemForReplication::open not implemented");
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
            _tablet_mgr = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 16384);
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
            auto c0 = schema->add_column();
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
            auto c1 = schema->add_column();
            c1->set_unique_id(next_id());
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
#endif // USE_STAROS

    INSTANTIATE_TEST_SUITE_P(SharedDataReplicationTxnManagerTest, SharedDataReplicationTxnManagerTest,
                             testing::Values(KeysType::DUP_KEYS, KeysType::AGG_KEYS, KeysType::PRIMARY_KEYS));

} // namespace starrocks::lake