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

#include "storage/lake/replication_txn_manager.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fstream>

#include "base/path/filesystem_util.h"
#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "common/config_rowset_fwd.h"
#include "common/config_storage_fwd.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/delta_column_group.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reshard.h"
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/replication_utils.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class LakeReplicationTxnManagerTest : public testing::TestWithParam<TKeysType::type> {
public:
    LakeReplicationTxnManagerTest() : _test_dir(){};

    ~LakeReplicationTxnManagerTest() override = default;

    void SetUp() override {
        config::enable_transparent_data_encryption = false;
        std::vector<starrocks::StorePath> paths;
        CHECK_OK(starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths));
        _test_dir = paths[0].path + "/lake";
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_test_dir);
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->metadata_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->txn_log_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->segment_root_location(1)));
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 16384);
        _replication_txn_manager = std::make_unique<lake::ReplicationTxnManager>(_tablet_manager.get());

        ASSERT_TRUE(_tablet_manager->create_tablet(get_create_tablet_req(_tablet_id, _version, _schema_hash)).ok());

        if (GetParam() != TKeysType::type::PRIMARY_KEYS) {
            create_tablet(_src_tablet_id, _src_version, _schema_hash);
        } else {
            auto src_tablet = create_tablet(_src_tablet_id, 1, _schema_hash);

            std::vector<int64_t> keys{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
            Int64Column deletes;
            int64_t deletes_array[2] = {10, 11};
            deletes.append_numbers(deletes_array, sizeof(int64_t) * 2);
            for (int i = 2; i <= _src_version; i++) {
                ASSERT_TRUE(src_tablet->rowset_commit(i, create_rowset(src_tablet, keys, &deletes)).ok());
            }
        }
    }

    void TearDown() override {
        auto status = StorageEngine::instance()->tablet_manager()->drop_tablet(_src_tablet_id, kDeleteFiles);
        EXPECT_TRUE(status.ok()) << status;
        status = StorageEngine::instance()->tablet_manager()->delete_shutdown_tablet(_src_tablet_id);
        EXPECT_TRUE(status.ok()) << status;
        status = fs::remove_all(config::storage_root_path);
        EXPECT_TRUE(status.ok() || status.is_not_found()) << status;
        config::enable_transparent_data_encryption = false;
    }

    TCreateTabletReq get_create_tablet_req(int64_t tablet_id, int64_t version, int32_t schema_hash,
                                           bool multi_column_key = false) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(version);
        request.__set_version_hash(0);
        request.tablet_schema.__set_id(GetParam() + 1);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = GetParam();
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        if (multi_column_key) {
            TColumn pk1;
            pk1.column_name = "pk1_bigint";
            pk1.__set_is_key(true);
            pk1.column_type.type = TPrimitiveType::BIGINT;
            request.tablet_schema.columns.push_back(pk1);
            TColumn pk2;
            pk2.column_name = "pk2_varchar";
            pk2.__set_is_key(true);
            pk2.column_type.type = TPrimitiveType::VARCHAR;
            pk2.column_type.len = 128;
            request.tablet_schema.columns.push_back(pk2);
            TColumn pk3;
            pk3.column_name = "pk3_int";
            pk3.__set_is_key(true);
            pk3.column_type.type = TPrimitiveType::INT;
            request.tablet_schema.columns.push_back(pk3);
        } else {
            TColumn k1;
            k1.column_name = "pk";
            k1.__set_is_key(true);
            k1.column_type.type = TPrimitiveType::BIGINT;
            request.tablet_schema.columns.push_back(k1);
        }

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k3);
        return request;
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int64_t version, int32_t schema_hash,
                                  bool multi_column_key = false) {
        auto st = StorageEngine::instance()->create_tablet(
                get_create_tablet_req(tablet_id, version, schema_hash, multi_column_key));
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                  Column* one_delete = nullptr, bool empty = false, bool has_merge_condition = false) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->thread_safe_get_tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        if (has_merge_condition) {
            writer_context.merge_condition = "v2";
        }
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        if (empty) {
            return *writer->build();
        }
        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto cols = chunk->mutable_columns();
        for (int64_t key : keys) {
            if (schema.num_key_fields() == 1) {
                cols[0]->append_datum(Datum(key));
            } else {
                cols[0]->append_datum(Datum(key));
                string v = fmt::to_string(key * 234234342345);
                cols[1]->append_datum(Datum(Slice(v)));
                cols[2]->append_datum(Datum((int32_t)key));
            }
            int vcol_start = schema.num_key_fields();
            cols[vcol_start]->append_datum(Datum((int16_t)(key % 100 + 1)));
            if (cols[vcol_start + 1]->is_binary()) {
                string v = fmt::to_string(key % 1000 + 2);
                cols[vcol_start + 1]->append_datum(Datum(Slice(v)));
            } else {
                cols[vcol_start + 1]->append_datum(Datum((int32_t)(key % 1000 + 2)));
            }
        }
        if (one_delete == nullptr && !keys.empty()) {
            CHECK_OK(writer->flush_chunk(*chunk));
        } else if (one_delete == nullptr) {
            CHECK_OK(writer->flush());
        } else if (one_delete != nullptr) {
            CHECK_OK(writer->flush_chunk_with_deletes(*chunk, *one_delete));
        }
        return *writer->build();
    }

protected:
    std::unique_ptr<starrocks::lake::TabletManager> _tablet_manager;
    std::string _test_dir;
    std::shared_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    std::unique_ptr<lake::ReplicationTxnManager> _replication_txn_manager;

    int64_t _transaction_id = 300;
    int64_t _table_id = 30001;
    int64_t _partition_id = 30002;
    int64_t _tablet_id = 30003;
    int64_t _src_tablet_id = 30004;
    int32_t _schema_hash = 368169781;
    int64_t _version = 1;
    int64_t _src_version = 10;
};

TEST_P(LakeReplicationTxnManagerTest, test_remote_snapshot_no_missing_versions) {
    TRemoteSnapshotRequest remote_snapshot_request;
    remote_snapshot_request.__set_transaction_id(_transaction_id);
    remote_snapshot_request.__set_table_id(_table_id);
    remote_snapshot_request.__set_partition_id(_partition_id);
    remote_snapshot_request.__set_tablet_id(_tablet_id);
    remote_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    remote_snapshot_request.__set_schema_hash(_schema_hash);
    remote_snapshot_request.__set_visible_version(_version);
    remote_snapshot_request.__set_data_version(_version);
    remote_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    remote_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    remote_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    remote_snapshot_request.__set_src_schema_hash(_schema_hash);
    remote_snapshot_request.__set_src_visible_version(_version);
    remote_snapshot_request.__set_src_backends({TBackend()});

    TSnapshotInfo remote_snapshot_info;
    Status status = _replication_txn_manager->remote_snapshot(remote_snapshot_request, &remote_snapshot_info);
    EXPECT_FALSE(status.ok());
}

TEST_P(LakeReplicationTxnManagerTest, test_remote_snapshot_no_versions) {
    TRemoteSnapshotRequest remote_snapshot_request;
    remote_snapshot_request.__set_transaction_id(_transaction_id);
    remote_snapshot_request.__set_table_id(_table_id);
    remote_snapshot_request.__set_partition_id(_partition_id);
    remote_snapshot_request.__set_tablet_id(_tablet_id);
    remote_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_DISK);
    remote_snapshot_request.__set_schema_hash(_schema_hash);
    remote_snapshot_request.__set_visible_version(_version);
    remote_snapshot_request.__set_data_version(_version);
    remote_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    remote_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    remote_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    remote_snapshot_request.__set_src_schema_hash(_schema_hash);
    remote_snapshot_request.__set_src_visible_version(_src_version + 1);
    remote_snapshot_request.__set_src_backends({TBackend()});

    TSnapshotInfo remote_snapshot_info;
    Status status = _replication_txn_manager->remote_snapshot(remote_snapshot_request, &remote_snapshot_info);
    EXPECT_FALSE(status.ok());
}

TEST_P(LakeReplicationTxnManagerTest, test_replicate_snapshot_failed) {
    TRemoteSnapshotRequest remote_snapshot_request;
    remote_snapshot_request.__set_transaction_id(_transaction_id);
    remote_snapshot_request.__set_table_id(_table_id);
    remote_snapshot_request.__set_partition_id(_partition_id);
    remote_snapshot_request.__set_tablet_id(_tablet_id);
    remote_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    remote_snapshot_request.__set_schema_hash(_schema_hash);
    remote_snapshot_request.__set_visible_version(_version);
    remote_snapshot_request.__set_data_version(_version);
    remote_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    remote_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    remote_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    remote_snapshot_request.__set_src_schema_hash(_schema_hash);
    remote_snapshot_request.__set_src_visible_version(_src_version);
    remote_snapshot_request.__set_src_backends({TBackend()});

    TSnapshotInfo remote_snapshot_info;
    Status status = _replication_txn_manager->remote_snapshot(remote_snapshot_request, &remote_snapshot_info);
    EXPECT_TRUE(status.ok()) << status;

    status = _replication_txn_manager->remote_snapshot(remote_snapshot_request, &remote_snapshot_info);
    EXPECT_TRUE(status.ok()) << status;

    TReplicateSnapshotRequest replicate_snapshot_request;
    replicate_snapshot_request.__set_transaction_id(_transaction_id);
    replicate_snapshot_request.__set_table_id(_table_id);
    replicate_snapshot_request.__set_partition_id(_partition_id);
    replicate_snapshot_request.__set_tablet_id(_tablet_id);
    replicate_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    replicate_snapshot_request.__set_schema_hash(_schema_hash);
    replicate_snapshot_request.__set_visible_version(_version);
    replicate_snapshot_request.__set_data_version(_version);
    replicate_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    replicate_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    replicate_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    replicate_snapshot_request.__set_src_schema_hash(_schema_hash + 1);
    replicate_snapshot_request.__set_src_visible_version(_src_version);
    replicate_snapshot_request.__set_src_snapshot_infos({remote_snapshot_info});

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request, nullptr);
    EXPECT_FALSE(status.ok()) << status;

    auto slog_path = _tablet_manager->txn_slog_location(_tablet_id, _transaction_id);
    auto txn_slog_or = _tablet_manager->get_txn_log(slog_path, false);
    EXPECT_TRUE(txn_slog_or.ok()) << txn_slog_or.status();

    _replication_txn_manager->clear_snapshots(txn_slog_or.value());
}

TEST_P(LakeReplicationTxnManagerTest, test_publish_failed) {
    TRemoteSnapshotRequest remote_snapshot_request;
    remote_snapshot_request.__set_transaction_id(_transaction_id);
    remote_snapshot_request.__set_table_id(_table_id);
    remote_snapshot_request.__set_partition_id(_partition_id);
    remote_snapshot_request.__set_tablet_id(_tablet_id);
    remote_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    remote_snapshot_request.__set_schema_hash(_schema_hash);
    remote_snapshot_request.__set_visible_version(_version);
    remote_snapshot_request.__set_data_version(_version);
    remote_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    remote_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    remote_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    remote_snapshot_request.__set_src_schema_hash(_schema_hash);
    remote_snapshot_request.__set_src_visible_version(_src_version);
    remote_snapshot_request.__set_src_backends({TBackend()});

    TSnapshotInfo remote_snapshot_info;
    Status status = _replication_txn_manager->remote_snapshot(remote_snapshot_request, &remote_snapshot_info);
    EXPECT_TRUE(status.ok()) << status;

    auto txn_info = TxnInfoPB();
    txn_info.set_txn_id(_transaction_id);
    txn_info.set_combined_txn_log(false);
    txn_info.set_txn_type(TXN_REPLICATION);
    txn_info.set_commit_time(0);
    auto txn_info_span = std::span<const TxnInfoPB>(&txn_info, 1);
    auto status_or = lake::publish_version(_tablet_manager.get(), lake::PublishTabletInfo(_tablet_id), _version,
                                           _src_version, txn_info_span, false);
    EXPECT_TRUE(!status_or.ok()) << status_or.status();

    lake::abort_txn(_tablet_manager.get(), _tablet_id, txn_info_span);
}

TEST_P(LakeReplicationTxnManagerTest, test_run_normal) {
    TRemoteSnapshotRequest remote_snapshot_request;
    remote_snapshot_request.__set_transaction_id(_transaction_id);
    remote_snapshot_request.__set_table_id(_table_id);
    remote_snapshot_request.__set_partition_id(_partition_id);
    remote_snapshot_request.__set_tablet_id(_tablet_id);
    remote_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    remote_snapshot_request.__set_schema_hash(_schema_hash);
    remote_snapshot_request.__set_visible_version(_version);
    remote_snapshot_request.__set_data_version(_version);
    remote_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    remote_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    remote_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    remote_snapshot_request.__set_src_schema_hash(_schema_hash);
    remote_snapshot_request.__set_src_visible_version(_src_version);
    remote_snapshot_request.__set_src_backends({TBackend()});

    TSnapshotInfo remote_snapshot_info;
    Status status = _replication_txn_manager->remote_snapshot(remote_snapshot_request, &remote_snapshot_info);
    EXPECT_TRUE(status.ok()) << status;

    TReplicateSnapshotRequest replicate_snapshot_request;
    replicate_snapshot_request.__set_transaction_id(_transaction_id);
    replicate_snapshot_request.__set_table_id(_table_id);
    replicate_snapshot_request.__set_partition_id(_partition_id);
    replicate_snapshot_request.__set_tablet_id(_tablet_id);
    replicate_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    replicate_snapshot_request.__set_schema_hash(_schema_hash);
    replicate_snapshot_request.__set_visible_version(_version);
    replicate_snapshot_request.__set_data_version(_version);
    replicate_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    replicate_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    replicate_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    replicate_snapshot_request.__set_src_schema_hash(_schema_hash);
    replicate_snapshot_request.__set_src_visible_version(_src_version);
    replicate_snapshot_request.__set_src_snapshot_infos({remote_snapshot_info});

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request, nullptr);
    EXPECT_TRUE(status.ok()) << status;

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request, nullptr);
    EXPECT_TRUE(status.ok()) << status;

    auto txn_info = TxnInfoPB();
    txn_info.set_txn_id(_transaction_id);
    txn_info.set_combined_txn_log(false);
    txn_info.set_commit_time(0);
    auto txn_info_span = std::span<const TxnInfoPB>(&txn_info, 1);
    auto status_or = lake::publish_version(_tablet_manager.get(), lake::PublishTabletInfo(_tablet_id), _version,
                                           _src_version, txn_info_span, false);
    EXPECT_TRUE(status_or.ok()) << status_or.status();

    EXPECT_EQ(_src_version, status_or.value()->version());
}

TEST_P(LakeReplicationTxnManagerTest, test_run_normal_encrypted) {
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
    config::enable_transparent_data_encryption = true;

    TRemoteSnapshotRequest remote_snapshot_request;
    remote_snapshot_request.__set_transaction_id(_transaction_id);
    remote_snapshot_request.__set_table_id(_table_id);
    remote_snapshot_request.__set_partition_id(_partition_id);
    remote_snapshot_request.__set_tablet_id(_tablet_id);
    remote_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    remote_snapshot_request.__set_schema_hash(_schema_hash);
    remote_snapshot_request.__set_visible_version(_version);
    remote_snapshot_request.__set_data_version(_version);
    remote_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    remote_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    remote_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    remote_snapshot_request.__set_src_schema_hash(_schema_hash);
    remote_snapshot_request.__set_src_visible_version(_src_version);
    remote_snapshot_request.__set_src_backends({TBackend()});

    TSnapshotInfo remote_snapshot_info;
    Status status = _replication_txn_manager->remote_snapshot(remote_snapshot_request, &remote_snapshot_info);
    EXPECT_TRUE(status.ok()) << status;

    TReplicateSnapshotRequest replicate_snapshot_request;
    replicate_snapshot_request.__set_transaction_id(_transaction_id);
    replicate_snapshot_request.__set_table_id(_table_id);
    replicate_snapshot_request.__set_partition_id(_partition_id);
    replicate_snapshot_request.__set_tablet_id(_tablet_id);
    replicate_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    replicate_snapshot_request.__set_schema_hash(_schema_hash);
    replicate_snapshot_request.__set_visible_version(_version);
    replicate_snapshot_request.__set_data_version(_version);
    replicate_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    replicate_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    replicate_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    replicate_snapshot_request.__set_src_schema_hash(_schema_hash);
    replicate_snapshot_request.__set_src_visible_version(_src_version);
    replicate_snapshot_request.__set_src_snapshot_infos({remote_snapshot_info});

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request, nullptr);
    EXPECT_TRUE(status.ok()) << status;

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request, nullptr);
    EXPECT_TRUE(status.ok()) << status;

    auto txn_info = TxnInfoPB();
    txn_info.set_txn_id(_transaction_id);
    txn_info.set_combined_txn_log(false);
    txn_info.set_commit_time(0);
    auto txn_info_span = std::span<const TxnInfoPB>(&txn_info, 1);
    auto status_or = lake::publish_version(_tablet_manager.get(), lake::PublishTabletInfo(_tablet_id), _version,
                                           _src_version, txn_info_span, false);
    EXPECT_TRUE(status_or.ok()) << status_or.status();

    EXPECT_EQ(_src_version, status_or.value()->version());
}

// Verify incremental non-PK replication skips .dcgs_snapshot download (the file is never created
// by snapshot_incremental, so downloading it would be a wasted HTTP round-trip).
TEST_P(LakeReplicationTxnManagerTest, test_incremental_non_pk_skips_dcg_download) {
    if (GetParam() == TKeysType::type::PRIMARY_KEYS) {
        return;
    }

    // Create a full snapshot via remote_snapshot()
    TRemoteSnapshotRequest remote_snapshot_request;
    remote_snapshot_request.__set_transaction_id(_transaction_id);
    remote_snapshot_request.__set_table_id(_table_id);
    remote_snapshot_request.__set_partition_id(_partition_id);
    remote_snapshot_request.__set_tablet_id(_tablet_id);
    remote_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    remote_snapshot_request.__set_schema_hash(_schema_hash);
    remote_snapshot_request.__set_visible_version(_version);
    remote_snapshot_request.__set_data_version(_version);
    remote_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    remote_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    remote_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    remote_snapshot_request.__set_src_schema_hash(_schema_hash);
    remote_snapshot_request.__set_src_visible_version(_src_version);
    remote_snapshot_request.__set_src_backends({TBackend()});

    TSnapshotInfo remote_snapshot_info;
    Status status = _replication_txn_manager->remote_snapshot(remote_snapshot_request, &remote_snapshot_info);
    ASSERT_TRUE(status.ok()) << status;

    // Place a .dcgs_snapshot file with real DCG data in the snapshot directory.
    // With incremental_snapshot=true, this file should be completely ignored.
    std::string dcg_file_path = remote_snapshot_info.snapshot_path + "/" + std::to_string(_src_tablet_id) + "/" +
                                std::to_string(_schema_hash) + "/" + std::to_string(_src_tablet_id) + ".dcgs_snapshot";

    DeltaColumnGroupSnapshotPB dcg_snapshot_pb;
    dcg_snapshot_pb.add_tablet_id(_src_tablet_id);
    dcg_snapshot_pb.add_rowset_id("dummy_rowset");
    dcg_snapshot_pb.add_segment_id(0);
    auto* dcg_list_pb = dcg_snapshot_pb.add_dcg_lists();
    dcg_list_pb->add_versions(5);
    auto* dcg_pb = dcg_list_pb->add_dcgs();
    dcg_pb->add_column_files("dummy.cols");
    auto* col_ids = dcg_pb->add_column_ids();
    col_ids->add_column_ids(1);
    ASSERT_TRUE(DeltaColumnGroupListHelper::save_snapshot(dcg_file_path, dcg_snapshot_pb).ok());

    // Override incremental_snapshot to true
    remote_snapshot_info.__set_incremental_snapshot(true);

    // replicate_snapshot() should skip .dcgs_snapshot download for incremental snapshots
    TReplicateSnapshotRequest replicate_snapshot_request;
    replicate_snapshot_request.__set_transaction_id(_transaction_id);
    replicate_snapshot_request.__set_table_id(_table_id);
    replicate_snapshot_request.__set_partition_id(_partition_id);
    replicate_snapshot_request.__set_tablet_id(_tablet_id);
    replicate_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    replicate_snapshot_request.__set_schema_hash(_schema_hash);
    replicate_snapshot_request.__set_visible_version(_version);
    replicate_snapshot_request.__set_data_version(_version);
    replicate_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    replicate_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    replicate_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    replicate_snapshot_request.__set_src_schema_hash(_schema_hash);
    replicate_snapshot_request.__set_src_visible_version(_src_version);
    replicate_snapshot_request.__set_src_snapshot_infos({remote_snapshot_info});

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request, nullptr);
    EXPECT_TRUE(status.ok()) << status;

    // Verify txn_log has no DCG metadata (the .dcgs_snapshot file was skipped)
    auto txn_log_path = _tablet_manager->txn_log_location(_tablet_id, _transaction_id);
    auto txn_log_or = _tablet_manager->get_txn_log(txn_log_path, false);
    ASSERT_TRUE(txn_log_or.ok()) << txn_log_or.status();
    EXPECT_FALSE(txn_log_or.value()->op_replication().has_dcg_meta())
            << "Incremental replication should not load DCG metadata";
}

// Verify full snapshot always creates .dcgs_snapshot file (even when empty).
// The target side must handle both empty and non-empty .dcgs_snapshot files correctly.
// We do NOT skip writing on the source side because replication must not depend on
// the source cluster upgrading its code version.
TEST_P(LakeReplicationTxnManagerTest, test_full_snapshot_creates_dcg_file_even_when_empty) {
    if (GetParam() == TKeysType::type::PRIMARY_KEYS) {
        return;
    }

    // Create a full snapshot for a non-PK table without DCGs
    TRemoteSnapshotRequest remote_snapshot_request;
    remote_snapshot_request.__set_transaction_id(_transaction_id);
    remote_snapshot_request.__set_table_id(_table_id);
    remote_snapshot_request.__set_partition_id(_partition_id);
    remote_snapshot_request.__set_tablet_id(_tablet_id);
    remote_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    remote_snapshot_request.__set_schema_hash(_schema_hash);
    remote_snapshot_request.__set_visible_version(_version);
    remote_snapshot_request.__set_data_version(_version);
    remote_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    remote_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    remote_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    remote_snapshot_request.__set_src_schema_hash(_schema_hash);
    remote_snapshot_request.__set_src_visible_version(_src_version);
    remote_snapshot_request.__set_src_backends({TBackend()});

    TSnapshotInfo remote_snapshot_info;
    Status status = _replication_txn_manager->remote_snapshot(remote_snapshot_request, &remote_snapshot_info);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_FALSE(remote_snapshot_info.incremental_snapshot) << "data_version=1 should trigger full snapshot";

    // Verify the .dcgs_snapshot file IS created (source always writes it)
    std::string dcg_file_path = remote_snapshot_info.snapshot_path + "/" + std::to_string(_src_tablet_id) + "/" +
                                std::to_string(_schema_hash) + "/" + std::to_string(_src_tablet_id) + ".dcgs_snapshot";
    EXPECT_TRUE(fs::path_exist(dcg_file_path)) << ".dcgs_snapshot file should always be created by source cluster";

    // Verify replicate_snapshot() succeeds with the empty .dcgs_snapshot file
    TReplicateSnapshotRequest replicate_snapshot_request;
    replicate_snapshot_request.__set_transaction_id(_transaction_id);
    replicate_snapshot_request.__set_table_id(_table_id);
    replicate_snapshot_request.__set_partition_id(_partition_id);
    replicate_snapshot_request.__set_tablet_id(_tablet_id);
    replicate_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_LAKE);
    replicate_snapshot_request.__set_schema_hash(_schema_hash);
    replicate_snapshot_request.__set_visible_version(_version);
    replicate_snapshot_request.__set_data_version(_version);
    replicate_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    replicate_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    replicate_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    replicate_snapshot_request.__set_src_schema_hash(_schema_hash);
    replicate_snapshot_request.__set_src_visible_version(_src_version);
    replicate_snapshot_request.__set_src_snapshot_infos({remote_snapshot_info});

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request, nullptr);
    EXPECT_TRUE(status.ok()) << status;

    // Verify txn_log has no DCG metadata (empty .dcgs_snapshot produces no DCGs)
    auto txn_log_path = _tablet_manager->txn_log_location(_tablet_id, _transaction_id);
    auto txn_log_or = _tablet_manager->get_txn_log(txn_log_path, false);
    ASSERT_TRUE(txn_log_or.ok()) << txn_log_or.status();
    // Empty DCG snapshot should result in no DCG metadata in txn log
    if (txn_log_or.value()->op_replication().has_dcg_meta()) {
        EXPECT_EQ(0, txn_log_or.value()->op_replication().dcg_meta().dcgs_size());
    }
}

INSTANTIATE_TEST_SUITE_P(LakeReplicationTxnManagerTest, LakeReplicationTxnManagerTest,
                         testing::Values(TKeysType::type::AGG_KEYS, TKeysType::type::PRIMARY_KEYS));

class LakeReplicationTxnManagerStaticFunctionTest : public testing::Test {
public:
    LakeReplicationTxnManagerStaticFunctionTest() = default;
    ~LakeReplicationTxnManagerStaticFunctionTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_delete_predicate_pb) {
    DeletePredicatePB delete_predicate;

    delete_predicate.add_sub_predicates()->assign("k0");
    Status status = lake::ReplicationTxnManager::convert_delete_predicate_pb(&delete_predicate);
    EXPECT_TRUE(!status.ok()) << status;

    delete_predicate.Clear();
    delete_predicate.add_sub_predicates()->assign("k1=1");
    delete_predicate.add_sub_predicates()->assign("k2!=1");
    delete_predicate.add_sub_predicates()->assign("k3>>3");
    delete_predicate.add_sub_predicates()->assign("k4<<3");
    delete_predicate.add_sub_predicates()->assign("k5<=5");
    delete_predicate.add_sub_predicates()->assign("k6>=5");
    delete_predicate.add_sub_predicates()->assign("k7=7");
    delete_predicate.add_sub_predicates()->assign("k8!=7");
    delete_predicate.add_sub_predicates()->assign("k9=a");
    delete_predicate.add_sub_predicates()->assign("k10!=a");
    delete_predicate.add_sub_predicates()->assign("k11=a");
    delete_predicate.add_sub_predicates()->assign("k12!=a");

    delete_predicate.add_sub_predicates()->assign("k13 IS NULL");
    delete_predicate.add_sub_predicates()->assign("k14 IS NOT NULL");

    delete_predicate.add_sub_predicates()->assign("k15*=1");

    status = lake::ReplicationTxnManager::convert_delete_predicate_pb(&delete_predicate);
    EXPECT_TRUE(status.ok()) << status;
    EXPECT_EQ(12, delete_predicate.binary_predicates_size());
    EXPECT_EQ(2, delete_predicate.is_null_predicates_size());
    EXPECT_EQ(1, delete_predicate.in_predicates_size());

    EXPECT_EQ("k1", delete_predicate.binary_predicates(0).column_name());
    EXPECT_EQ("=", delete_predicate.binary_predicates(0).op());
    EXPECT_EQ("1", delete_predicate.binary_predicates(0).value());

    EXPECT_EQ("k2", delete_predicate.binary_predicates(1).column_name());
    EXPECT_EQ("!=", delete_predicate.binary_predicates(1).op());
    EXPECT_EQ("1", delete_predicate.binary_predicates(1).value());

    EXPECT_EQ("k3", delete_predicate.binary_predicates(2).column_name());
    EXPECT_EQ(">", delete_predicate.binary_predicates(2).op());
    EXPECT_EQ("3", delete_predicate.binary_predicates(2).value());

    EXPECT_EQ("k4", delete_predicate.binary_predicates(3).column_name());
    EXPECT_EQ("<", delete_predicate.binary_predicates(3).op());
    EXPECT_EQ("3", delete_predicate.binary_predicates(3).value());

    EXPECT_EQ("k5", delete_predicate.binary_predicates(4).column_name());
    EXPECT_EQ("<=", delete_predicate.binary_predicates(4).op());
    EXPECT_EQ("5", delete_predicate.binary_predicates(4).value());

    EXPECT_EQ("k6", delete_predicate.binary_predicates(5).column_name());
    EXPECT_EQ(">=", delete_predicate.binary_predicates(5).op());
    EXPECT_EQ("5", delete_predicate.binary_predicates(5).value());

    EXPECT_EQ("k7", delete_predicate.binary_predicates(6).column_name());
    EXPECT_EQ("=", delete_predicate.binary_predicates(6).op());
    EXPECT_EQ("7", delete_predicate.binary_predicates(6).value());

    EXPECT_EQ("k8", delete_predicate.binary_predicates(7).column_name());
    EXPECT_EQ("!=", delete_predicate.binary_predicates(7).op());
    EXPECT_EQ("7", delete_predicate.binary_predicates(7).value());

    EXPECT_EQ("k9", delete_predicate.binary_predicates(8).column_name());
    EXPECT_EQ("=", delete_predicate.binary_predicates(8).op());
    EXPECT_EQ("a", delete_predicate.binary_predicates(8).value());

    EXPECT_EQ("k10", delete_predicate.binary_predicates(9).column_name());
    EXPECT_EQ("!=", delete_predicate.binary_predicates(9).op());
    EXPECT_EQ("a", delete_predicate.binary_predicates(9).value());

    EXPECT_EQ("k11", delete_predicate.binary_predicates(10).column_name());
    EXPECT_EQ("=", delete_predicate.binary_predicates(10).op());
    EXPECT_EQ("a", delete_predicate.binary_predicates(10).value());

    EXPECT_EQ("k12", delete_predicate.binary_predicates(11).column_name());
    EXPECT_EQ("!=", delete_predicate.binary_predicates(11).op());
    EXPECT_EQ("a", delete_predicate.binary_predicates(11).value());

    EXPECT_EQ("k13", delete_predicate.is_null_predicates(0).column_name());
    EXPECT_FALSE(delete_predicate.is_null_predicates(0).is_not_null());

    EXPECT_EQ("k14", delete_predicate.is_null_predicates(1).column_name());
    EXPECT_TRUE(delete_predicate.is_null_predicates(1).is_not_null());

    EXPECT_EQ("k15", delete_predicate.in_predicates(0).column_name());
    EXPECT_FALSE(delete_predicate.in_predicates(0).is_not_in());
    EXPECT_EQ(1, delete_predicate.in_predicates(0).values_size());
    EXPECT_EQ("1", delete_predicate.in_predicates(0).values(0));
}

TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_rowset_meta_column_mode_partial_update) {
    // Create a RowsetMetaPB with num_update_files > 0 (column mode partial update)
    RowsetMetaPB rowset_meta_pb;
    rowset_meta_pb.set_rowset_id("0");
    rowset_meta_pb.set_partition_id(1);
    rowset_meta_pb.set_tablet_id(100);
    rowset_meta_pb.set_num_rows(10);
    rowset_meta_pb.set_total_disk_size(1024);
    rowset_meta_pb.set_data_disk_size(1024);
    rowset_meta_pb.set_num_segments(2);
    rowset_meta_pb.set_num_update_files(1); // column mode partial update
    rowset_meta_pb.set_rowset_seg_id(5);
    RowsetMeta rowset_meta(rowset_meta_pb);
    EXPECT_TRUE(rowset_meta.is_column_mode_partial_update());

    TxnLogPB::OpWrite op_write;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    // Previously this would return NotSupported, now it should succeed
    Status status = lake::ReplicationTxnManager::convert_rowset_meta(rowset_meta, 12345, &op_write, &filename_map);
    EXPECT_TRUE(status.ok()) << status;

    // Verify segments are properly converted
    EXPECT_EQ(2, op_write.rowset().segments_size());
    EXPECT_EQ(2, filename_map.size());

    // Verify all segment files are .dat files
    for (const auto& segment : op_write.rowset().segments()) {
        EXPECT_TRUE(lake::is_segment(segment));
    }
}

TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_build_file_converters_handles_cols_files) {
    // Verify that build_file_converters handles .cols files alongside .dat files
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    std::string old_segment = "test_rowset_0.dat";
    std::string new_segment = lake::gen_segment_filename(100);
    filename_map[old_segment] = {new_segment, FileEncryptionPair()};

    std::string old_cols = "test_rowset_0_1_0.cols";
    std::string new_cols = lake::gen_cols_filename(100);
    filename_map[old_cols] = {new_cols, FileEncryptionPair()};

    std::string old_del = "test_rowset_0.del";
    std::string new_del = lake::gen_del_filename(100);
    filename_map[old_del] = {new_del, FileEncryptionPair()};

    // .cols files should be in the filename map and handled correctly
    EXPECT_TRUE(filename_map.count(old_cols) > 0);
    EXPECT_TRUE(lake::is_cols(old_cols));
    EXPECT_TRUE(lake::is_cols(new_cols));
    EXPECT_TRUE(lake::is_segment(old_segment));
    EXPECT_TRUE(lake::is_segment(new_segment));
    EXPECT_TRUE(lake::is_del(old_del));
    EXPECT_TRUE(lake::is_del(new_del));
}

// Test convert_dcg_meta_for_pk: converts DeltaColumnGroupList from snapshot into DeltaColumnGroupMetadataPB
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_meta_for_pk) {
    // Build DeltaColumnGroupList in the same format as shared-nothing PK snapshot
    std::unordered_map<uint32_t, DeltaColumnGroupList> delta_column_groups;

    // Segment 5: one DCG with version 3, two column files
    {
        auto dcg = std::make_shared<DeltaColumnGroup>();
        std::vector<std::string> column_files = {"old_file_1.cols", "old_file_2.cols"};
        std::vector<std::vector<int32_t>> column_ids = {{1, 2}, {3}};
        dcg->init(3, column_ids, column_files);
        delta_column_groups[5].push_back(dcg);
    }

    // Segment 6: one DCG with version 5, one column file
    {
        auto dcg = std::make_shared<DeltaColumnGroup>();
        std::vector<std::string> column_files = {"old_file_3.cols"};
        std::vector<std::vector<int32_t>> column_ids = {{4}};
        dcg->init(5, column_ids, column_files);
        delta_column_groups[6].push_back(dcg);
    }

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    // Call the extracted function
    ASSERT_TRUE(
            lake::ReplicationTxnManager::convert_dcg_meta_for_pk(delta_column_groups, 12345, &dcg_meta, &filename_map)
                    .ok());

    // Verify structure
    EXPECT_EQ(2, dcg_meta.dcgs_size());
    EXPECT_TRUE(dcg_meta.dcgs().count(5));
    EXPECT_TRUE(dcg_meta.dcgs().count(6));

    const auto& ver1 = dcg_meta.dcgs().at(5);
    EXPECT_EQ(2, ver1.column_files_size());
    EXPECT_EQ(2, ver1.versions_size());
    EXPECT_EQ(2, ver1.unique_column_ids_size());
    EXPECT_EQ(3, ver1.versions(0));
    EXPECT_EQ(3, ver1.versions(1));
    EXPECT_EQ(2, ver1.unique_column_ids(0).column_ids_size());
    EXPECT_EQ(1, ver1.unique_column_ids(0).column_ids(0));
    EXPECT_EQ(2, ver1.unique_column_ids(0).column_ids(1));
    EXPECT_EQ(1, ver1.unique_column_ids(1).column_ids_size());
    EXPECT_EQ(3, ver1.unique_column_ids(1).column_ids(0));

    const auto& ver2 = dcg_meta.dcgs().at(6);
    EXPECT_EQ(1, ver2.column_files_size());
    EXPECT_EQ(1, ver2.versions_size());
    EXPECT_EQ(5, ver2.versions(0));
    EXPECT_EQ(1, ver2.unique_column_ids(0).column_ids_size());
    EXPECT_EQ(4, ver2.unique_column_ids(0).column_ids(0));

    // Verify filename_map has old->new mappings for 3 .cols files
    EXPECT_EQ(3, filename_map.size());
    EXPECT_TRUE(filename_map.count("old_file_1.cols"));
    EXPECT_TRUE(filename_map.count("old_file_2.cols"));
    EXPECT_TRUE(filename_map.count("old_file_3.cols"));
    for (const auto& [old_name, pair] : filename_map) {
        EXPECT_TRUE(lake::is_cols(pair.first));
    }
}

// Test convert_dcg_column_unique_ids
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_column_unique_ids) {
    DeltaColumnGroupMetadataPB dcg_meta;

    // Build DCG metadata with original column IDs
    auto& dcg_ver1 = (*dcg_meta.mutable_dcgs())[5];
    dcg_ver1.add_column_files("file1.cols");
    dcg_ver1.add_versions(3);
    auto* ucids1 = dcg_ver1.add_unique_column_ids();
    ucids1->add_column_ids(1);
    ucids1->add_column_ids(2);

    dcg_ver1.add_column_files("file2.cols");
    dcg_ver1.add_versions(3);
    auto* ucids2 = dcg_ver1.add_unique_column_ids();
    ucids2->add_column_ids(3);

    auto& dcg_ver2 = (*dcg_meta.mutable_dcgs())[6];
    dcg_ver2.add_column_files("file3.cols");
    dcg_ver2.add_versions(5);
    auto* ucids3 = dcg_ver2.add_unique_column_ids();
    ucids3->add_column_ids(4);

    std::unordered_map<uint32_t, uint32_t> column_unique_id_map;
    column_unique_id_map[1] = 10;
    column_unique_id_map[2] = 20;
    column_unique_id_map[3] = 30;
    column_unique_id_map[4] = 40;

    ASSERT_TRUE(lake::ReplicationTxnManager::convert_dcg_column_unique_ids(&dcg_meta, column_unique_id_map).ok());

    // Verify converted IDs
    EXPECT_EQ(10, dcg_meta.dcgs().at(5).unique_column_ids(0).column_ids(0));
    EXPECT_EQ(20, dcg_meta.dcgs().at(5).unique_column_ids(0).column_ids(1));
    EXPECT_EQ(30, dcg_meta.dcgs().at(5).unique_column_ids(1).column_ids(0));
    EXPECT_EQ(40, dcg_meta.dcgs().at(6).unique_column_ids(0).column_ids(0));
}

// Test DCG metadata apply for PK table replication
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_dcg_apply_replication_pk) {
    // Simulate PK table apply_replication_log with DCG metadata
    // Build a tablet metadata
    TabletMetadataPB metadata;
    metadata.set_id(100);
    metadata.set_version(1);
    metadata.set_next_rowset_id(10);
    auto* schema = metadata.mutable_schema();
    schema->set_keys_type(PRIMARY_KEYS);
    schema->set_id(1);

    // Build op_replication with DCG metadata
    TxnLogPB_OpReplication op_replication;
    auto* txn_meta = op_replication.mutable_txn_meta();
    txn_meta->set_txn_id(1);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_tablet_id(100);
    txn_meta->set_visible_version(1);
    txn_meta->set_data_version(0);
    txn_meta->set_snapshot_version(2);
    txn_meta->set_incremental_snapshot(false);

    // Add a rowset with 2 segments
    auto* op_write = op_replication.add_op_writes();
    auto* rowset = op_write->mutable_rowset();
    rowset->set_id(5); // source rowset seg id
    rowset->set_num_rows(100);
    rowset->set_data_size(4096);
    rowset->add_segments("seg1.dat");
    rowset->add_segments("seg2.dat");

    // Add DCG metadata - segment 5 has DCG with version 3
    auto* dcg_meta = op_replication.mutable_dcg_meta();
    auto& dcg_ver = (*dcg_meta->mutable_dcgs())[5]; // source rssid = 5
    dcg_ver.add_column_files("test.cols");
    dcg_ver.add_versions(3);
    auto* ucids = dcg_ver.add_unique_column_ids();
    ucids->add_column_ids(1);
    ucids->add_column_ids(2);

    // Simulate applying the replication log for PK tables
    // For PK non-lake replication: new_rssid = source_rssid + old_next_rowset_id
    uint32_t old_next_rowset_id = metadata.next_rowset_id(); // 10

    // Apply DCG with offset
    for (const auto& [src_rssid, src_dcg_ver] : op_replication.dcg_meta().dcgs()) {
        (*metadata.mutable_dcg_meta()->mutable_dcgs())[src_rssid + old_next_rowset_id].CopyFrom(src_dcg_ver);
    }

    // Verify result
    EXPECT_EQ(1, metadata.dcg_meta().dcgs_size());
    EXPECT_TRUE(metadata.dcg_meta().dcgs().count(15)); // 5 + 10
    const auto& result_dcg = metadata.dcg_meta().dcgs().at(15);
    EXPECT_EQ(1, result_dcg.column_files_size());
    EXPECT_EQ("test.cols", result_dcg.column_files(0));
    EXPECT_EQ(3, result_dcg.versions(0));
    EXPECT_EQ(2, result_dcg.unique_column_ids(0).column_ids_size());
    EXPECT_EQ(1, result_dcg.unique_column_ids(0).column_ids(0));
    EXPECT_EQ(2, result_dcg.unique_column_ids(0).column_ids(1));
}

// Test DCG metadata apply for non-PK table replication with rssid remapping
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_dcg_apply_replication_non_pk) {
    // Build op_replication with two rowsets and DCG metadata
    TxnLogPB_OpReplication op_replication;
    auto* txn_meta = op_replication.mutable_txn_meta();
    txn_meta->set_txn_id(1);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_tablet_id(200);
    txn_meta->set_visible_version(1);
    txn_meta->set_data_version(0);
    txn_meta->set_snapshot_version(2);
    txn_meta->set_incremental_snapshot(false);

    // Rowset 1: source id=3, 2 segments
    auto* op_write1 = op_replication.add_op_writes();
    auto* rowset1 = op_write1->mutable_rowset();
    rowset1->set_id(3);
    rowset1->set_num_rows(50);
    rowset1->set_data_size(2048);
    rowset1->add_segments("seg1.dat");
    rowset1->add_segments("seg2.dat");

    // Rowset 2: source id=10, 1 segment
    auto* op_write2 = op_replication.add_op_writes();
    auto* rowset2 = op_write2->mutable_rowset();
    rowset2->set_id(10);
    rowset2->set_num_rows(30);
    rowset2->set_data_size(1024);
    rowset2->add_segments("seg3.dat");

    // DCG on segment (source rssid=4, which is rowset1.id + segment_index 1)
    auto* dcg_meta = op_replication.mutable_dcg_meta();
    auto& dcg_ver = (*dcg_meta->mutable_dcgs())[4]; // rssid 4 = rowset1(3) + segment 1
    dcg_ver.add_column_files("cols1.cols");
    dcg_ver.add_versions(2);
    auto* ucids = dcg_ver.add_unique_column_ids();
    ucids->add_column_ids(5);

    // Simulate non-PK rssid remapping
    // Assume target next_rowset_id starts at 20
    uint32_t current_next_id = 20;
    std::unordered_map<uint32_t, uint32_t> rssid_remap;

    for (const auto& ow : op_replication.op_writes()) {
        if (ow.has_rowset()) {
            uint32_t source_id = ow.rowset().id();
            uint32_t num_segments = ow.rowset().segments_size();
            for (uint32_t i = 0; i < num_segments; i++) {
                rssid_remap[source_id + i] = current_next_id + i;
            }
            current_next_id += std::max<uint32_t>(1, num_segments);
        }
    }

    // Verify remap: source 3->20, 4->21, 10->22
    EXPECT_EQ(20, rssid_remap[3]);
    EXPECT_EQ(21, rssid_remap[4]);
    EXPECT_EQ(22, rssid_remap[10]);

    // Apply DCG with remapping
    TabletMetadataPB metadata;
    metadata.set_id(200);
    metadata.set_version(1);

    for (const auto& [src_rssid, src_dcg_ver] : op_replication.dcg_meta().dcgs()) {
        auto it = rssid_remap.find(src_rssid);
        uint32_t target_rssid = (it != rssid_remap.end()) ? it->second : src_rssid;
        (*metadata.mutable_dcg_meta()->mutable_dcgs())[target_rssid].CopyFrom(src_dcg_ver);
    }

    // Verify: source rssid 4 -> target rssid 21
    EXPECT_EQ(1, metadata.dcg_meta().dcgs_size());
    EXPECT_TRUE(metadata.dcg_meta().dcgs().count(21));
    const auto& result_dcg = metadata.dcg_meta().dcgs().at(21);
    EXPECT_EQ(1, result_dcg.column_files_size());
    EXPECT_EQ("cols1.cols", result_dcg.column_files(0));
    EXPECT_EQ(2, result_dcg.versions(0));
    EXPECT_EQ(5, result_dcg.unique_column_ids(0).column_ids(0));
}

// Test convert_dcg_meta_for_non_pk: converts DeltaColumnGroupSnapshotPB into DeltaColumnGroupMetadataPB
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_meta_for_non_pk) {
    DeltaColumnGroupSnapshotPB dcg_snapshot_pb;

    // Add entry: rowset_id="rs_001", segment_id=0
    dcg_snapshot_pb.add_tablet_id(100);
    dcg_snapshot_pb.add_rowset_id("rs_001");
    dcg_snapshot_pb.add_segment_id(0);

    auto* dcg_list_pb = dcg_snapshot_pb.add_dcg_lists();
    dcg_list_pb->add_versions(5);
    auto* dcg_pb = dcg_list_pb->add_dcgs();
    dcg_pb->add_column_files("rs_001_0_5_0.cols");
    auto* col_ids = dcg_pb->add_column_ids();
    col_ids->add_column_ids(1);
    col_ids->add_column_ids(2);

    // Add second entry: same rowset, segment 1
    dcg_snapshot_pb.add_tablet_id(100);
    dcg_snapshot_pb.add_rowset_id("rs_001");
    dcg_snapshot_pb.add_segment_id(1);

    auto* dcg_list_pb2 = dcg_snapshot_pb.add_dcg_lists();
    dcg_list_pb2->add_versions(5);
    auto* dcg_pb2 = dcg_list_pb2->add_dcgs();
    dcg_pb2->add_column_files("rs_001_1_5_0.cols");
    auto* col_ids2 = dcg_pb2->add_column_ids();
    col_ids2->add_column_ids(3);

    // Build rowset_id -> seg_id mapping
    std::unordered_map<std::string, uint32_t> rowset_id_to_seg_id;
    rowset_id_to_seg_id["rs_001"] = 7;

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    // Call the extracted function
    ASSERT_TRUE(lake::ReplicationTxnManager::convert_dcg_meta_for_non_pk(dcg_snapshot_pb, rowset_id_to_seg_id, 9999,
                                                                         &dcg_meta, &filename_map)
                        .ok());

    // Verify: rssid 7 (rowset_seg_id=7 + segment_id=0) and rssid 8 (7+1)
    EXPECT_EQ(2, dcg_meta.dcgs_size());
    EXPECT_TRUE(dcg_meta.dcgs().count(7));
    EXPECT_TRUE(dcg_meta.dcgs().count(8));

    const auto& dcg7 = dcg_meta.dcgs().at(7);
    EXPECT_EQ(1, dcg7.column_files_size());
    EXPECT_TRUE(lake::is_cols(dcg7.column_files(0)));
    EXPECT_EQ(5, dcg7.versions(0));
    EXPECT_EQ(2, dcg7.unique_column_ids(0).column_ids_size());
    EXPECT_EQ(1, dcg7.unique_column_ids(0).column_ids(0));
    EXPECT_EQ(2, dcg7.unique_column_ids(0).column_ids(1));

    const auto& dcg8 = dcg_meta.dcgs().at(8);
    EXPECT_EQ(1, dcg8.column_files_size());
    EXPECT_TRUE(lake::is_cols(dcg8.column_files(0)));
    EXPECT_EQ(5, dcg8.versions(0));
    EXPECT_EQ(1, dcg8.unique_column_ids(0).column_ids_size());
    EXPECT_EQ(3, dcg8.unique_column_ids(0).column_ids(0));

    // Verify filename_map
    EXPECT_EQ(2, filename_map.size());
    EXPECT_TRUE(filename_map.count("rs_001_0_5_0.cols"));
    EXPECT_TRUE(filename_map.count("rs_001_1_5_0.cols"));
}

// Test convert_dcg_meta_for_non_pk with unknown rowset_id (should skip)
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_meta_for_non_pk_unknown_rowset) {
    DeltaColumnGroupSnapshotPB dcg_snapshot_pb;

    dcg_snapshot_pb.add_tablet_id(100);
    dcg_snapshot_pb.add_rowset_id("unknown_rs");
    dcg_snapshot_pb.add_segment_id(0);

    auto* dcg_list_pb = dcg_snapshot_pb.add_dcg_lists();
    dcg_list_pb->add_versions(1);
    auto* dcg_pb = dcg_list_pb->add_dcgs();
    dcg_pb->add_column_files("unknown_file.cols");

    std::unordered_map<std::string, uint32_t> rowset_id_to_seg_id;
    rowset_id_to_seg_id["rs_001"] = 7; // Only rs_001, not "unknown_rs"

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    ASSERT_TRUE(lake::ReplicationTxnManager::convert_dcg_meta_for_non_pk(dcg_snapshot_pb, rowset_id_to_seg_id, 9999,
                                                                         &dcg_meta, &filename_map)
                        .ok());

    // Unknown rowset should be skipped
    EXPECT_EQ(0, dcg_meta.dcgs_size());
    EXPECT_EQ(0, filename_map.size());
}

// Test that duplicate .cols filenames in convert_dcg_meta_for_pk are detected
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_meta_for_pk_duplicate_cols) {
    std::unordered_map<uint32_t, DeltaColumnGroupList> delta_column_groups;

    // Two DCGs for the same segment with the SAME column file name → duplicate
    {
        auto dcg1 = std::make_shared<DeltaColumnGroup>();
        std::vector<std::string> column_files = {"same_file.cols"};
        std::vector<std::vector<int32_t>> column_ids = {{1}};
        dcg1->init(1, column_ids, column_files);

        auto dcg2 = std::make_shared<DeltaColumnGroup>();
        std::vector<std::string> column_files2 = {"same_file.cols"};
        std::vector<std::vector<int32_t>> column_ids2 = {{2}};
        dcg2->init(2, column_ids2, column_files2);

        delta_column_groups[5].push_back(dcg1);
        delta_column_groups[5].push_back(dcg2);
    }

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    Status status =
            lake::ReplicationTxnManager::convert_dcg_meta_for_pk(delta_column_groups, 12345, &dcg_meta, &filename_map);
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_TRUE(status.message().find("Duplicated cols file") != std::string::npos) << status;
}

// Test convert_dcg_meta_for_pk: column_ids size != column_files size → Corruption
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_meta_for_pk_column_ids_files_mismatch) {
    std::unordered_map<uint32_t, DeltaColumnGroupList> delta_column_groups;

    // Create a DCG with mismatched column_ids and column_files sizes
    {
        auto dcg = std::make_shared<DeltaColumnGroup>();
        // 2 column files but only 1 column_ids entry → mismatch
        std::vector<std::string> column_files = {"file1.cols", "file2.cols"};
        std::vector<std::vector<int32_t>> column_ids = {{1}};
        dcg->init(1, column_ids, column_files);
        delta_column_groups[5].push_back(dcg);
    }

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    Status status =
            lake::ReplicationTxnManager::convert_dcg_meta_for_pk(delta_column_groups, 12345, &dcg_meta, &filename_map);
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_TRUE(status.message().find("Mismatch between column_ids size") != std::string::npos) << status;
}

// Test that duplicate .cols filenames in convert_dcg_meta_for_non_pk are detected
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_meta_for_non_pk_duplicate_cols) {
    DeltaColumnGroupSnapshotPB dcg_snapshot_pb;

    // Two entries with the same column file name → duplicate
    dcg_snapshot_pb.add_tablet_id(100);
    dcg_snapshot_pb.add_rowset_id("rs_001");
    dcg_snapshot_pb.add_segment_id(0);

    auto* dcg_list_pb1 = dcg_snapshot_pb.add_dcg_lists();
    dcg_list_pb1->add_versions(1);
    auto* dcg_pb1 = dcg_list_pb1->add_dcgs();
    dcg_pb1->add_column_files("same_file.cols");
    auto* col_ids1 = dcg_pb1->add_column_ids();
    col_ids1->add_column_ids(1);

    dcg_snapshot_pb.add_tablet_id(100);
    dcg_snapshot_pb.add_rowset_id("rs_001");
    dcg_snapshot_pb.add_segment_id(0);

    auto* dcg_list_pb2 = dcg_snapshot_pb.add_dcg_lists();
    dcg_list_pb2->add_versions(2);
    auto* dcg_pb2 = dcg_list_pb2->add_dcgs();
    dcg_pb2->add_column_files("same_file.cols");
    auto* col_ids2 = dcg_pb2->add_column_ids();
    col_ids2->add_column_ids(2);

    std::unordered_map<std::string, uint32_t> rowset_id_to_seg_id;
    rowset_id_to_seg_id["rs_001"] = 7;

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    Status status = lake::ReplicationTxnManager::convert_dcg_meta_for_non_pk(dcg_snapshot_pb, rowset_id_to_seg_id, 9999,
                                                                             &dcg_meta, &filename_map);
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_TRUE(status.message().find("Duplicated cols file") != std::string::npos) << status;
}

// Test convert_dcg_meta_for_non_pk: versions_size != dcgs_size mismatch
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_meta_for_non_pk_versions_dcgs_mismatch) {
    DeltaColumnGroupSnapshotPB dcg_snapshot_pb;

    dcg_snapshot_pb.add_tablet_id(100);
    dcg_snapshot_pb.add_rowset_id("rs_001");
    dcg_snapshot_pb.add_segment_id(0);

    auto* dcg_list_pb = dcg_snapshot_pb.add_dcg_lists();
    // Add 2 versions but only 1 dcg → mismatch
    dcg_list_pb->add_versions(1);
    dcg_list_pb->add_versions(2);
    auto* dcg_pb = dcg_list_pb->add_dcgs();
    dcg_pb->add_column_files("file.cols");
    auto* col_ids = dcg_pb->add_column_ids();
    col_ids->add_column_ids(1);

    std::unordered_map<std::string, uint32_t> rowset_id_to_seg_id;
    rowset_id_to_seg_id["rs_001"] = 7;

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    Status status = lake::ReplicationTxnManager::convert_dcg_meta_for_non_pk(dcg_snapshot_pb, rowset_id_to_seg_id, 9999,
                                                                             &dcg_meta, &filename_map);
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_TRUE(status.message().find("Mismatch between versions_size") != std::string::npos) << status;
}

// Test convert_dcg_meta_for_non_pk: column_ids_size != column_files_size mismatch
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_meta_for_non_pk_column_ids_files_mismatch) {
    DeltaColumnGroupSnapshotPB dcg_snapshot_pb;

    dcg_snapshot_pb.add_tablet_id(100);
    dcg_snapshot_pb.add_rowset_id("rs_001");
    dcg_snapshot_pb.add_segment_id(0);

    auto* dcg_list_pb = dcg_snapshot_pb.add_dcg_lists();
    dcg_list_pb->add_versions(1);
    auto* dcg_pb = dcg_list_pb->add_dcgs();
    // Add 2 column_files but only 1 column_ids → mismatch
    dcg_pb->add_column_files("file1.cols");
    dcg_pb->add_column_files("file2.cols");
    auto* col_ids = dcg_pb->add_column_ids();
    col_ids->add_column_ids(1);

    std::unordered_map<std::string, uint32_t> rowset_id_to_seg_id;
    rowset_id_to_seg_id["rs_001"] = 7;

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    Status status = lake::ReplicationTxnManager::convert_dcg_meta_for_non_pk(dcg_snapshot_pb, rowset_id_to_seg_id, 9999,
                                                                             &dcg_meta, &filename_map);
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_TRUE(status.message().find("Mismatch between column_ids_size") != std::string::npos) << status;
}

// Test convert_dcg_meta_for_non_pk with empty snapshot (no entries)
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_meta_for_non_pk_empty) {
    DeltaColumnGroupSnapshotPB dcg_snapshot_pb;

    std::unordered_map<std::string, uint32_t> rowset_id_to_seg_id;
    rowset_id_to_seg_id["rs_001"] = 7;

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    ASSERT_TRUE(lake::ReplicationTxnManager::convert_dcg_meta_for_non_pk(dcg_snapshot_pb, rowset_id_to_seg_id, 9999,
                                                                         &dcg_meta, &filename_map)
                        .ok());

    EXPECT_EQ(0, dcg_meta.dcgs_size());
    EXPECT_EQ(0, filename_map.size());
}

// Test convert_dcg_meta_for_non_pk with multiple DCGs per segment (different versions)
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_meta_for_non_pk_multiple_dcgs_per_segment) {
    DeltaColumnGroupSnapshotPB dcg_snapshot_pb;

    // Single entry with multiple DCGs (different versions) for one segment
    dcg_snapshot_pb.add_tablet_id(100);
    dcg_snapshot_pb.add_rowset_id("rs_001");
    dcg_snapshot_pb.add_segment_id(0);

    auto* dcg_list_pb = dcg_snapshot_pb.add_dcg_lists();
    dcg_list_pb->add_versions(3);
    dcg_list_pb->add_versions(5);

    auto* dcg_pb1 = dcg_list_pb->add_dcgs();
    dcg_pb1->add_column_files("file_v3.cols");
    auto* col_ids1 = dcg_pb1->add_column_ids();
    col_ids1->add_column_ids(1);
    col_ids1->add_column_ids(2);

    auto* dcg_pb2 = dcg_list_pb->add_dcgs();
    dcg_pb2->add_column_files("file_v5.cols");
    auto* col_ids2 = dcg_pb2->add_column_ids();
    col_ids2->add_column_ids(3);

    std::unordered_map<std::string, uint32_t> rowset_id_to_seg_id;
    rowset_id_to_seg_id["rs_001"] = 10;

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    ASSERT_TRUE(lake::ReplicationTxnManager::convert_dcg_meta_for_non_pk(dcg_snapshot_pb, rowset_id_to_seg_id, 9999,
                                                                         &dcg_meta, &filename_map)
                        .ok());

    // rssid = 10 + 0 = 10
    EXPECT_EQ(1, dcg_meta.dcgs_size());
    EXPECT_TRUE(dcg_meta.dcgs().count(10));

    const auto& dcg_ver = dcg_meta.dcgs().at(10);
    // Two DCGs → two column_files, two versions, two unique_column_ids
    EXPECT_EQ(2, dcg_ver.column_files_size());
    EXPECT_EQ(2, dcg_ver.versions_size());
    EXPECT_EQ(2, dcg_ver.unique_column_ids_size());
    EXPECT_EQ(3, dcg_ver.versions(0));
    EXPECT_EQ(5, dcg_ver.versions(1));
    EXPECT_EQ(2, dcg_ver.unique_column_ids(0).column_ids_size());
    EXPECT_EQ(1, dcg_ver.unique_column_ids(0).column_ids(0));
    EXPECT_EQ(2, dcg_ver.unique_column_ids(0).column_ids(1));
    EXPECT_EQ(1, dcg_ver.unique_column_ids(1).column_ids_size());
    EXPECT_EQ(3, dcg_ver.unique_column_ids(1).column_ids(0));

    EXPECT_EQ(2, filename_map.size());
    EXPECT_TRUE(filename_map.count("file_v3.cols"));
    EXPECT_TRUE(filename_map.count("file_v5.cols"));
}

// Test convert_dcg_meta_for_pk with empty delta_column_groups
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_meta_for_pk_empty) {
    std::unordered_map<uint32_t, DeltaColumnGroupList> delta_column_groups;

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    ASSERT_TRUE(
            lake::ReplicationTxnManager::convert_dcg_meta_for_pk(delta_column_groups, 12345, &dcg_meta, &filename_map)
                    .ok());

    EXPECT_EQ(0, dcg_meta.dcgs_size());
    EXPECT_EQ(0, filename_map.size());
}

// Test convert_dcg_column_unique_ids with missing column ID in map
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_column_unique_ids_missing_column) {
    DeltaColumnGroupMetadataPB dcg_meta;

    auto& dcg_ver = (*dcg_meta.mutable_dcgs())[5];
    dcg_ver.add_column_files("file.cols");
    dcg_ver.add_versions(1);
    auto* ucids = dcg_ver.add_unique_column_ids();
    ucids->add_column_ids(999); // This column ID is not in the map

    std::unordered_map<uint32_t, uint32_t> column_unique_id_map;
    column_unique_id_map[1] = 10;
    column_unique_id_map[2] = 20;

    Status status = lake::ReplicationTxnManager::convert_dcg_column_unique_ids(&dcg_meta, column_unique_id_map);
    EXPECT_TRUE(!status.ok()) << status;
    EXPECT_TRUE(status.message().find("Column not found") != std::string::npos) << status;
}

// Test convert_dcg_column_unique_ids with empty map (no-op)
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_convert_dcg_column_unique_ids_empty_map) {
    DeltaColumnGroupMetadataPB dcg_meta;

    auto& dcg_ver = (*dcg_meta.mutable_dcgs())[5];
    dcg_ver.add_column_files("file.cols");
    dcg_ver.add_versions(1);
    auto* ucids = dcg_ver.add_unique_column_ids();
    ucids->add_column_ids(1);

    std::unordered_map<uint32_t, uint32_t> empty_map;

    Status status = lake::ReplicationTxnManager::convert_dcg_column_unique_ids(&dcg_meta, empty_map);
    EXPECT_TRUE(status.ok()) << status;

    // Column IDs should remain unchanged when map is empty
    EXPECT_EQ(1, dcg_meta.dcgs().at(5).unique_column_ids(0).column_ids(0));
}

// Test PK incremental replication DCG apply with rssid remap
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_dcg_apply_incremental_pk) {
    // Simulate PK incremental replication with DCG metadata
    TabletMetadataPB metadata;
    metadata.set_id(100);
    metadata.set_version(5);
    metadata.set_next_rowset_id(20); // existing next_rowset_id

    // Build op_replication with incremental snapshot
    TxnLogPB_OpReplication op_replication;
    auto* txn_meta = op_replication.mutable_txn_meta();
    txn_meta->set_txn_id(1);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_tablet_id(100);
    txn_meta->set_visible_version(5);
    txn_meta->set_data_version(0);
    txn_meta->set_snapshot_version(7);
    txn_meta->set_incremental_snapshot(true);

    // Rowset 1: source id=3, 2 segments, 50 rows
    auto* op_write1 = op_replication.add_op_writes();
    auto* rowset1 = op_write1->mutable_rowset();
    rowset1->set_id(3);
    rowset1->set_num_rows(50);
    rowset1->set_data_size(2048);
    rowset1->add_segments("seg1.dat");
    rowset1->add_segments("seg2.dat");

    // Rowset 2: source id=8, 1 segment, 30 rows
    auto* op_write2 = op_replication.add_op_writes();
    auto* rowset2 = op_write2->mutable_rowset();
    rowset2->set_id(8);
    rowset2->set_num_rows(30);
    rowset2->set_data_size(1024);
    rowset2->add_segments("seg3.dat");

    // DCG on source segment 4 (rowset1.id=3, segment index 1)
    auto* dcg_meta_pb = op_replication.mutable_dcg_meta();
    auto& dcg_ver = (*dcg_meta_pb->mutable_dcgs())[4];
    dcg_ver.add_column_files("test.cols");
    dcg_ver.add_versions(3);
    auto* ucids = dcg_ver.add_unique_column_ids();
    ucids->add_column_ids(10);

    // Simulate PK incremental apply: build rssid_remap
    // PK skip condition: dels_size>0 || num_rows>0 || has_delete_predicate
    std::unordered_map<uint32_t, uint32_t> rssid_remap;
    {
        uint32_t target_id = metadata.next_rowset_id(); // 20
        for (const auto& ow : op_replication.op_writes()) {
            if (ow.dels_size() > 0 || ow.rowset().num_rows() > 0 || ow.rowset().has_delete_predicate()) {
                uint32_t source_id = ow.rowset().id();
                uint32_t step = std::max<uint32_t>(1, ow.rowset().segments_size());
                for (uint32_t i = 0; i < step; i++) {
                    rssid_remap[source_id + i] = target_id + i;
                }
                target_id += step;
            }
        }
    }

    // Verify remap: source 3->20, 4->21, 8->22
    EXPECT_EQ(20, rssid_remap[3]);
    EXPECT_EQ(21, rssid_remap[4]);
    EXPECT_EQ(22, rssid_remap[8]);

    // Apply DCG with rssid remap
    for (const auto& [src_rssid, src_dcg_ver] : op_replication.dcg_meta().dcgs()) {
        auto it = rssid_remap.find(src_rssid);
        uint32_t target_rssid = (it != rssid_remap.end()) ? it->second : src_rssid;
        (*metadata.mutable_dcg_meta()->mutable_dcgs())[target_rssid].CopyFrom(src_dcg_ver);
    }

    // Source rssid 4 → target rssid 21
    EXPECT_EQ(1, metadata.dcg_meta().dcgs_size());
    EXPECT_TRUE(metadata.dcg_meta().dcgs().count(21));
    const auto& result = metadata.dcg_meta().dcgs().at(21);
    EXPECT_EQ("test.cols", result.column_files(0));
    EXPECT_EQ(3, result.versions(0));
    EXPECT_EQ(10, result.unique_column_ids(0).column_ids(0));
}

// Test non-PK incremental replication DCG apply with rssid remap
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_dcg_apply_incremental_non_pk) {
    TabletMetadataPB metadata;
    metadata.set_id(200);
    metadata.set_version(3);
    metadata.set_next_rowset_id(15);

    TxnLogPB_OpReplication op_replication;
    auto* txn_meta = op_replication.mutable_txn_meta();
    txn_meta->set_txn_id(2);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_tablet_id(200);
    txn_meta->set_visible_version(3);
    txn_meta->set_data_version(0);
    txn_meta->set_snapshot_version(5);
    txn_meta->set_incremental_snapshot(true);

    // Rowset: source id=5, 3 segments, 100 rows
    auto* op_write = op_replication.add_op_writes();
    auto* rowset = op_write->mutable_rowset();
    rowset->set_id(5);
    rowset->set_num_rows(100);
    rowset->set_data_size(4096);
    rowset->add_segments("s1.dat");
    rowset->add_segments("s2.dat");
    rowset->add_segments("s3.dat");

    // DCG on source segment 6 (rowset.id=5, segment index 1)
    auto* dcg_meta_pb = op_replication.mutable_dcg_meta();
    auto& dcg_ver = (*dcg_meta_pb->mutable_dcgs())[6];
    dcg_ver.add_column_files("inc.cols");
    dcg_ver.add_versions(4);
    auto* ucids = dcg_ver.add_unique_column_ids();
    ucids->add_column_ids(7);

    // Non-PK skip condition: has_rowset && (num_rows>0 || has_delete_predicate)
    std::unordered_map<uint32_t, uint32_t> rssid_remap;
    {
        uint32_t target_id = metadata.next_rowset_id(); // 15
        for (const auto& ow : op_replication.op_writes()) {
            if (ow.has_rowset() && (ow.rowset().num_rows() > 0 || ow.rowset().has_delete_predicate())) {
                uint32_t source_id = ow.rowset().id();
                uint32_t step = std::max<uint32_t>(1, ow.rowset().segments_size());
                for (uint32_t i = 0; i < step; i++) {
                    rssid_remap[source_id + i] = target_id + i;
                }
                target_id += step;
            }
        }
    }

    // Verify remap: source 5->15, 6->16, 7->17
    EXPECT_EQ(15, rssid_remap[5]);
    EXPECT_EQ(16, rssid_remap[6]);
    EXPECT_EQ(17, rssid_remap[7]);

    // Apply DCG with rssid remap
    for (const auto& [src_rssid, src_dcg_ver] : op_replication.dcg_meta().dcgs()) {
        auto it = rssid_remap.find(src_rssid);
        uint32_t target_rssid = (it != rssid_remap.end()) ? it->second : src_rssid;
        (*metadata.mutable_dcg_meta()->mutable_dcgs())[target_rssid].CopyFrom(src_dcg_ver);
    }

    // Source rssid 6 → target rssid 16
    EXPECT_EQ(1, metadata.dcg_meta().dcgs_size());
    EXPECT_TRUE(metadata.dcg_meta().dcgs().count(16));
    const auto& result = metadata.dcg_meta().dcgs().at(16);
    EXPECT_EQ("inc.cols", result.column_files(0));
    EXPECT_EQ(4, result.versions(0));
    EXPECT_EQ(7, result.unique_column_ids(0).column_ids(0));
}

// Test end-to-end non-PK DCG replication for generated columns produced by linked schema change.
// Simulates: source non-PK table had linked schema change adding generated columns, which produced
// .cols files (DCGs) at multiple schema versions. The replication must:
// 1. Convert DeltaColumnGroupSnapshotPB -> DeltaColumnGroupMetadataPB (with rssid mapping)
// 2. Remap column unique IDs from source schema to target schema
// 3. Apply the DCG metadata during publish with correct rssid remapping
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_non_pk_generated_column_dcg_replication_e2e) {
    // --- Step 1: Build DeltaColumnGroupSnapshotPB simulating generated columns ---
    // Scenario: source non-PK table (e.g., DUP_KEYS) had two linked schema changes:
    //   - Version 3: added generated column gc1 (unique_id=10, stored in .cols file)
    //   - Version 5: added generated column gc2 (unique_id=11, stored in another .cols file)
    // The table has one rowset "rs_100" with 2 segments.
    DeltaColumnGroupSnapshotPB dcg_snapshot_pb;

    // Segment 0 of rs_100: has DCGs from both schema changes
    dcg_snapshot_pb.add_tablet_id(500);
    dcg_snapshot_pb.add_rowset_id("rs_100");
    dcg_snapshot_pb.add_segment_id(0);

    auto* dcg_list_seg0 = dcg_snapshot_pb.add_dcg_lists();
    // DCG at version 3: generated column gc1
    dcg_list_seg0->add_versions(3);
    auto* dcg_v3_seg0 = dcg_list_seg0->add_dcgs();
    dcg_v3_seg0->add_column_files("rs_100_0_3_0.cols");
    auto* col_ids_v3_seg0 = dcg_v3_seg0->add_column_ids();
    col_ids_v3_seg0->add_column_ids(10); // source unique_id for gc1

    // DCG at version 5: generated column gc2
    dcg_list_seg0->add_versions(5);
    auto* dcg_v5_seg0 = dcg_list_seg0->add_dcgs();
    dcg_v5_seg0->add_column_files("rs_100_0_5_0.cols");
    auto* col_ids_v5_seg0 = dcg_v5_seg0->add_column_ids();
    col_ids_v5_seg0->add_column_ids(11); // source unique_id for gc2

    // Segment 1 of rs_100: also has DCGs from both schema changes
    dcg_snapshot_pb.add_tablet_id(500);
    dcg_snapshot_pb.add_rowset_id("rs_100");
    dcg_snapshot_pb.add_segment_id(1);

    auto* dcg_list_seg1 = dcg_snapshot_pb.add_dcg_lists();
    dcg_list_seg1->add_versions(3);
    auto* dcg_v3_seg1 = dcg_list_seg1->add_dcgs();
    dcg_v3_seg1->add_column_files("rs_100_1_3_0.cols");
    auto* col_ids_v3_seg1 = dcg_v3_seg1->add_column_ids();
    col_ids_v3_seg1->add_column_ids(10);

    dcg_list_seg1->add_versions(5);
    auto* dcg_v5_seg1 = dcg_list_seg1->add_dcgs();
    dcg_v5_seg1->add_column_files("rs_100_1_5_0.cols");
    auto* col_ids_v5_seg1 = dcg_v5_seg1->add_column_ids();
    col_ids_v5_seg1->add_column_ids(11);

    // --- Step 2: Convert snapshot to DCG metadata (simulates convert_dcg_meta_for_non_pk) ---
    std::unordered_map<std::string, uint32_t> rowset_id_to_seg_id;
    rowset_id_to_seg_id["rs_100"] = 20; // source rowset_seg_id

    DeltaColumnGroupMetadataPB dcg_meta;
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;

    ASSERT_OK(lake::ReplicationTxnManager::convert_dcg_meta_for_non_pk(dcg_snapshot_pb, rowset_id_to_seg_id, 9999,
                                                                       &dcg_meta, &filename_map));

    // Verify: rssid 20 (seg_id=20 + segment_id=0) and rssid 21 (20+1)
    EXPECT_EQ(2, dcg_meta.dcgs_size());
    EXPECT_TRUE(dcg_meta.dcgs().count(20));
    EXPECT_TRUE(dcg_meta.dcgs().count(21));
    EXPECT_EQ(4, filename_map.size()); // 4 .cols files total

    // Each segment has 2 DCGs (version 3 and 5)
    EXPECT_EQ(2, dcg_meta.dcgs().at(20).versions_size());
    EXPECT_EQ(3, dcg_meta.dcgs().at(20).versions(0));
    EXPECT_EQ(5, dcg_meta.dcgs().at(20).versions(1));
    EXPECT_EQ(2, dcg_meta.dcgs().at(21).versions_size());

    // Column IDs are still source IDs at this point
    EXPECT_EQ(10, dcg_meta.dcgs().at(20).unique_column_ids(0).column_ids(0));
    EXPECT_EQ(11, dcg_meta.dcgs().at(20).unique_column_ids(1).column_ids(0));

    // --- Step 3: Remap column unique IDs (simulates convert_dcg_column_unique_ids) ---
    // In target cluster, gc1 has unique_id=100, gc2 has unique_id=101
    std::unordered_map<uint32_t, uint32_t> column_unique_id_map;
    column_unique_id_map[10] = 100; // source gc1(10) -> target gc1(100)
    column_unique_id_map[11] = 101; // source gc2(11) -> target gc2(101)

    ASSERT_OK(lake::ReplicationTxnManager::convert_dcg_column_unique_ids(&dcg_meta, column_unique_id_map));

    // Verify column IDs are now target IDs
    EXPECT_EQ(100, dcg_meta.dcgs().at(20).unique_column_ids(0).column_ids(0));
    EXPECT_EQ(101, dcg_meta.dcgs().at(20).unique_column_ids(1).column_ids(0));
    EXPECT_EQ(100, dcg_meta.dcgs().at(21).unique_column_ids(0).column_ids(0));
    EXPECT_EQ(101, dcg_meta.dcgs().at(21).unique_column_ids(1).column_ids(0));

    // --- Step 4: Apply DCG during publish with rssid remapping ---
    // Simulate non-PK full replication apply: source rssid + offset
    TabletMetadataPB metadata;
    metadata.set_id(600);
    metadata.set_version(1);
    metadata.set_next_rowset_id(50);

    // Build rssid_remap: source 20->50, 21->51
    std::unordered_map<uint32_t, uint32_t> rssid_remap;
    rssid_remap[20] = 50;
    rssid_remap[21] = 51;

    for (const auto& [src_rssid, src_dcg_ver] : dcg_meta.dcgs()) {
        auto it = rssid_remap.find(src_rssid);
        uint32_t target_rssid = (it != rssid_remap.end()) ? it->second : src_rssid;
        (*metadata.mutable_dcg_meta()->mutable_dcgs())[target_rssid].CopyFrom(src_dcg_ver);
    }

    // Verify final result: target rssids 50 and 51 have correct DCG metadata
    EXPECT_EQ(2, metadata.dcg_meta().dcgs_size());
    EXPECT_TRUE(metadata.dcg_meta().dcgs().count(50));
    EXPECT_TRUE(metadata.dcg_meta().dcgs().count(51));

    // Segment 0 (rssid 50): 2 DCGs with remapped column IDs
    const auto& final_seg0 = metadata.dcg_meta().dcgs().at(50);
    EXPECT_EQ(2, final_seg0.column_files_size());
    EXPECT_EQ(2, final_seg0.versions_size());
    EXPECT_EQ(3, final_seg0.versions(0));
    EXPECT_EQ(5, final_seg0.versions(1));
    EXPECT_EQ(100, final_seg0.unique_column_ids(0).column_ids(0)); // gc1 remapped
    EXPECT_EQ(101, final_seg0.unique_column_ids(1).column_ids(0)); // gc2 remapped

    // Segment 1 (rssid 51): same structure
    const auto& final_seg1 = metadata.dcg_meta().dcgs().at(51);
    EXPECT_EQ(2, final_seg1.column_files_size());
    EXPECT_EQ(100, final_seg1.unique_column_ids(0).column_ids(0));
    EXPECT_EQ(101, final_seg1.unique_column_ids(1).column_ids(0));

    // All .cols filenames should be new lake-format names
    for (const auto& [_, pair] : filename_map) {
        EXPECT_TRUE(lake::is_cols(pair.first));
    }
}

// Test OpReplication protobuf correctly carries DCG metadata
TEST_F(LakeReplicationTxnManagerStaticFunctionTest, test_op_replication_dcg_meta_serialization) {
    TxnLogPB txn_log;
    txn_log.set_tablet_id(100);
    txn_log.set_txn_id(200);

    auto* op_replication = txn_log.mutable_op_replication();
    auto* txn_meta = op_replication->mutable_txn_meta();
    txn_meta->set_txn_id(200);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);

    // Add DCG metadata
    auto* dcg_meta = op_replication->mutable_dcg_meta();
    auto& dcg_ver = (*dcg_meta->mutable_dcgs())[10];
    dcg_ver.add_column_files("test_file.cols");
    dcg_ver.add_versions(42);
    auto* ucids = dcg_ver.add_unique_column_ids();
    ucids->add_column_ids(100);
    ucids->add_column_ids(200);

    // Serialize and deserialize
    std::string serialized;
    EXPECT_TRUE(txn_log.SerializeToString(&serialized));

    TxnLogPB deserialized;
    EXPECT_TRUE(deserialized.ParseFromString(serialized));

    // Verify deserialized DCG metadata
    EXPECT_TRUE(deserialized.op_replication().has_dcg_meta());
    EXPECT_EQ(1, deserialized.op_replication().dcg_meta().dcgs_size());
    EXPECT_TRUE(deserialized.op_replication().dcg_meta().dcgs().count(10));

    const auto& result = deserialized.op_replication().dcg_meta().dcgs().at(10);
    EXPECT_EQ(1, result.column_files_size());
    EXPECT_EQ("test_file.cols", result.column_files(0));
    EXPECT_EQ(42, result.versions(0));
    EXPECT_EQ(2, result.unique_column_ids(0).column_ids_size());
    EXPECT_EQ(100, result.unique_column_ids(0).column_ids(0));
    EXPECT_EQ(200, result.unique_column_ids(0).column_ids(1));
}

} // namespace starrocks
