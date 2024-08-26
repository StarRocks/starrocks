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

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/filesystem_util.h"

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
        _location_provider = std::make_unique<lake::FixedLocationProvider>(_test_dir);
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->metadata_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->txn_log_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->segment_root_location(1)));
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider.get(), _mem_tracker.get());
        _tablet_manager = std::make_unique<lake::TabletManager>(_location_provider.get(), _update_manager.get(), 16384);
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
        auto& cols = chunk->columns();
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
    std::unique_ptr<lake::LocationProvider> _location_provider;
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
    replicate_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    replicate_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    replicate_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    replicate_snapshot_request.__set_src_schema_hash(_schema_hash + 1);
    replicate_snapshot_request.__set_src_visible_version(_src_version);
    replicate_snapshot_request.__set_src_snapshot_infos({remote_snapshot_info});

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request);
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
    txn_info.set_commit_time(0);
    txn_info.set_force_publish(false);
    const int64_t txn_ids[] = {_transaction_id};
    auto txn_id_span = std::span<const int64_t>(txn_ids, 1);
    auto status_or = lake::publish_version(_tablet_manager.get(), _tablet_id, _version, _src_version, {txn_info});
    EXPECT_TRUE(!status_or.ok()) << status_or.status();

    const int32_t txn_types[] = {TxnTypePB::TXN_REPLICATION};
    auto txn_type_span = std::span<const int32_t>(txn_types, 1);
    lake::abort_txn(_tablet_manager.get(), _tablet_id, txn_id_span, txn_type_span);
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
    replicate_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    replicate_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    replicate_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    replicate_snapshot_request.__set_src_schema_hash(_schema_hash);
    replicate_snapshot_request.__set_src_visible_version(_src_version);
    replicate_snapshot_request.__set_src_snapshot_infos({remote_snapshot_info});

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request);
    EXPECT_TRUE(status.ok()) << status;

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request);
    EXPECT_TRUE(status.ok()) << status;

    auto txn_info = TxnInfoPB();
    txn_info.set_txn_id(_transaction_id);
    txn_info.set_combined_txn_log(false);
    txn_info.set_commit_time(0);
    txn_info.set_force_publish(false);
    auto status_or = lake::publish_version(_tablet_manager.get(), _tablet_id, _version, _src_version, {txn_info});
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
    replicate_snapshot_request.__set_src_token(ExecEnv::GetInstance()->token());
    replicate_snapshot_request.__set_src_tablet_id(_src_tablet_id);
    replicate_snapshot_request.__set_src_tablet_type(TTabletType::TABLET_TYPE_DISK);
    replicate_snapshot_request.__set_src_schema_hash(_schema_hash);
    replicate_snapshot_request.__set_src_visible_version(_src_version);
    replicate_snapshot_request.__set_src_snapshot_infos({remote_snapshot_info});

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request);
    EXPECT_TRUE(status.ok()) << status;

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request);
    EXPECT_TRUE(status.ok()) << status;

    auto txn_info = TxnInfoPB();
    txn_info.set_txn_id(_transaction_id);
    txn_info.set_combined_txn_log(false);
    txn_info.set_commit_time(0);
    txn_info.set_force_publish(false);
    auto status_or = lake::publish_version(_tablet_manager.get(), _tablet_id, _version, _src_version, {txn_info});
    EXPECT_TRUE(status_or.ok()) << status_or.status();

    EXPECT_EQ(_src_version, status_or.value()->version());
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

} // namespace starrocks
