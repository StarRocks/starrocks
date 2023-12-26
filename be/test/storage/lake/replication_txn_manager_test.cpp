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
#include "runtime/exec_env.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/filesystem_util.h"

namespace starrocks {

class LakeReplicationTxnManagerTest : public testing::Test {
public:
    LakeReplicationTxnManagerTest() : _test_dir(){};

    ~LakeReplicationTxnManagerTest() override = default;

    void SetUp() override {
        std::vector<starrocks::StorePath> paths;
        CHECK_OK(starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths));
        _test_dir = paths[0].path + "/lake";
        _location_provider = std::make_unique<lake::FixedLocationProvider>(_test_dir);
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->metadata_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->txn_log_root_location(1)));
        CHECK_OK(FileSystem::Default()->create_dir_recursive(_location_provider->segment_root_location(1)));
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider.get());
        _tablet_manager = std::make_unique<lake::TabletManager>(_location_provider.get(), _update_manager.get(), 16384);
        _replication_txn_manager = std::make_unique<lake::ReplicationTxnManager>(_tablet_manager.get());

        TCreateTabletReq create_tablet_req = get_create_tablet_request(_tablet_id, _schema_hash, _version);
        Status create_st = _tablet_manager->create_tablet(create_tablet_req);
        ASSERT_TRUE(create_st.ok());

        TCreateTabletReq src_create_tablet_req = get_create_tablet_request(_src_tablet_id, _schema_hash, _src_version);
        Status src_create_st = StorageEngine::instance()->tablet_manager()->create_tablet(
                src_create_tablet_req, StorageEngine::instance()->get_stores());
        ASSERT_TRUE(src_create_st.ok());
    }

    void TearDown() override { (void)FileSystem::Default()->delete_dir_recursive(_test_dir); }

    TCreateTabletReq get_create_tablet_request(int64_t tablet_id, int schema_hash, int64_t version) {
        TColumnType col_type;
        col_type.__set_type(TPrimitiveType::SMALLINT);
        TColumn col1;
        col1.__set_column_name("col1");
        col1.__set_column_type(col_type);
        col1.__set_is_key(true);
        std::vector<TColumn> cols;
        cols.push_back(col1);
        TTabletSchema tablet_schema;
        tablet_schema.__set_id(1);
        tablet_schema.__set_short_key_column_count(1);
        tablet_schema.__set_schema_hash(schema_hash);
        tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
        tablet_schema.__set_storage_type(TStorageType::COLUMN);
        tablet_schema.__set_columns(cols);
        TCreateTabletReq create_tablet_req;
        create_tablet_req.__set_tablet_schema(tablet_schema);
        create_tablet_req.__set_tablet_id(tablet_id);
        create_tablet_req.__set_version(version);
        create_tablet_req.__set_version_hash(0);
        return create_tablet_req;
    }

protected:
    std::unique_ptr<starrocks::lake::TabletManager> _tablet_manager;
    std::string _test_dir;
    std::unique_ptr<lake::LocationProvider> _location_provider;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    std::unique_ptr<lake::ReplicationTxnManager> _replication_txn_manager;

    int64_t _transaction_id = 100;
    int64_t _table_id = 10001;
    int64_t _partition_id = 10002;
    int64_t _tablet_id = 10003;
    int64_t _src_tablet_id = 10004;
    int32_t _schema_hash = 368169781;
    int64_t _version = 1;
    int64_t _src_version = 10;
};

TEST_F(LakeReplicationTxnManagerTest, test_remote_snapshot_no_missing_versions) {
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

    std::string snapshot_path;
    bool incremental_snapshot = false;
    Status status =
            _replication_txn_manager->remote_snapshot(remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_FALSE(status.ok());
}

TEST_F(LakeReplicationTxnManagerTest, test_remote_snapshot_no_versions) {
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

    std::string snapshot_path;
    bool incremental_snapshot = false;
    Status status =
            _replication_txn_manager->remote_snapshot(remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_FALSE(status.ok());
}

TEST_F(LakeReplicationTxnManagerTest, test_replicate_snapshot_failed) {
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

    std::string snapshot_path;
    bool incremental_snapshot = false;
    Status status =
            _replication_txn_manager->remote_snapshot(remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_TRUE(status.ok());

    status = _replication_txn_manager->remote_snapshot(remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_TRUE(status.ok());

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
    TRemoteSnapshotInfo remote_snapshot_info;
    remote_snapshot_info.__set_backend(TBackend());
    remote_snapshot_info.__set_snapshot_path(snapshot_path);
    remote_snapshot_info.__set_incremental_snapshot(incremental_snapshot);
    replicate_snapshot_request.__set_src_snapshot_infos({remote_snapshot_info});

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request);
    EXPECT_FALSE(status.ok());

    auto slog_path = _tablet_manager->txn_slog_location(_tablet_id, _transaction_id);
    auto txn_slog_or = _tablet_manager->get_txn_log(slog_path, false);
    EXPECT_TRUE(txn_slog_or.ok());

    _replication_txn_manager->clear_snapshots(txn_slog_or.value());
}

TEST_F(LakeReplicationTxnManagerTest, test_publish_failed) {
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

    std::string snapshot_path;
    bool incremental_snapshot = false;
    Status status =
            _replication_txn_manager->remote_snapshot(remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_TRUE(status.ok());

    const int64_t txn_ids[] = {_transaction_id};
    auto txn_id_span = std::span<const int64_t>(txn_ids, 1);
    auto status_or = lake::publish_version(_tablet_manager.get(), _tablet_id, _version, _src_version, txn_id_span, 0);
    EXPECT_TRUE(!status_or.ok());

    const int32_t txn_types[] = {TxnTypePB::TXN_REPLICATION};
    auto txn_type_span = std::span<const int32_t>(txn_types, 1);
    lake::abort_txn(_tablet_manager.get(), _tablet_id, txn_id_span, txn_type_span);
}

TEST_F(LakeReplicationTxnManagerTest, test_run_normal) {
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

    std::string snapshot_path;
    bool incremental_snapshot = false;
    Status status =
            _replication_txn_manager->remote_snapshot(remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_TRUE(status.ok());

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
    TRemoteSnapshotInfo remote_snapshot_info;
    remote_snapshot_info.__set_backend(TBackend());
    remote_snapshot_info.__set_snapshot_path(snapshot_path);
    remote_snapshot_info.__set_incremental_snapshot(incremental_snapshot);
    replicate_snapshot_request.__set_src_snapshot_infos({remote_snapshot_info});

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request);
    EXPECT_TRUE(status.ok());

    status = _replication_txn_manager->replicate_snapshot(replicate_snapshot_request);
    EXPECT_TRUE(status.ok());

    const int64_t txn_ids[] = {_transaction_id};
    auto txn_id_span = std::span<const int64_t>(txn_ids, 1);
    auto status_or = lake::publish_version(_tablet_manager.get(), _tablet_id, _version, _src_version, txn_id_span, 0);
    EXPECT_TRUE(status_or.ok()) << status_or.status();

    EXPECT_EQ(_src_version, status_or.value()->version());
}

} // namespace starrocks
