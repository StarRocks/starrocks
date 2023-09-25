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

#include "storage/replication_txn_manager.h"

#include <gtest/gtest.h>

#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gen_cpp/AgentService_types.h"
#include "runtime/exec_env.h"
#include "storage/olap_define.h"
#include "storage/tablet_manager.h"
#include "testutil/assert.h"
#include "util/uuid_generator.h"

namespace starrocks {

class ReplicationTxnManagerTest : public testing::Test {
public:
    ReplicationTxnManagerTest() = default;
    ~ReplicationTxnManagerTest() override = default;
    void SetUp() override {
        TCreateTabletReq create_tablet_req = get_create_tablet_request(_tablet_id, _schema_hash, _version);
        Status create_st = StorageEngine::instance()->tablet_manager()->create_tablet(
                create_tablet_req, StorageEngine::instance()->get_stores());
        ASSERT_TRUE(create_st.ok());

        TCreateTabletReq src_create_tablet_req = get_create_tablet_request(_src_tablet_id, _schema_hash, _src_version);
        Status src_create_st = StorageEngine::instance()->tablet_manager()->create_tablet(
                src_create_tablet_req, StorageEngine::instance()->get_stores());
        ASSERT_TRUE(src_create_st.ok());
    }

    void TearDown() override {}

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
    int64_t _transaction_id = 100;
    int64_t _table_id = 10001;
    int64_t _partition_id = 10002;
    int64_t _tablet_id = 10003;
    int64_t _src_tablet_id = 10004;
    int32_t _schema_hash = 368169781;
    int64_t _version = 2;
    int64_t _src_version = 10;
};

TEST_F(ReplicationTxnManagerTest, test_remote_snapshot_no_missing_versions) {
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
    remote_snapshot_request.__set_src_visible_version(_version);
    remote_snapshot_request.__set_src_backends({TBackend()});

    std::string snapshot_path;
    bool incremental_snapshot = false;
    Status status = StorageEngine::instance()->replication_txn_manager()->remote_snapshot(
            remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_FALSE(status.ok());

    StorageEngine::instance()->replication_txn_manager()->clear_txn(_transaction_id);

    StorageEngine::instance()->replication_txn_manager()->clear_expired_snapshots();
}

TEST_F(ReplicationTxnManagerTest, test_remote_snapshot_no_versions) {
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
    Status status = StorageEngine::instance()->replication_txn_manager()->remote_snapshot(
            remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_FALSE(status.ok());

    StorageEngine::instance()->replication_txn_manager()->clear_txn(_transaction_id);

    StorageEngine::instance()->replication_txn_manager()->clear_expired_snapshots();
}

TEST_F(ReplicationTxnManagerTest, test_replicate_snapshot_failed) {
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
    remote_snapshot_request.__set_src_visible_version(_src_version);
    remote_snapshot_request.__set_src_backends({TBackend()});

    std::string snapshot_path;
    bool incremental_snapshot = false;
    Status status = StorageEngine::instance()->replication_txn_manager()->remote_snapshot(
            remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_TRUE(status.ok());

    status = StorageEngine::instance()->replication_txn_manager()->remote_snapshot(
            remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_TRUE(status.ok());

    TReplicateSnapshotRequest replicate_snapshot_request;
    replicate_snapshot_request.__set_transaction_id(_transaction_id);
    replicate_snapshot_request.__set_table_id(_table_id);
    replicate_snapshot_request.__set_partition_id(_partition_id);
    replicate_snapshot_request.__set_tablet_id(_tablet_id);
    replicate_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_DISK);
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

    status = StorageEngine::instance()->replication_txn_manager()->replicate_snapshot(replicate_snapshot_request);
    EXPECT_FALSE(status.ok());

    StorageEngine::instance()->replication_txn_manager()->clear_txn(_transaction_id);

    StorageEngine::instance()->replication_txn_manager()->clear_expired_snapshots();
}

TEST_F(ReplicationTxnManagerTest, test_publish_failed) {
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
    remote_snapshot_request.__set_src_visible_version(_src_version);
    remote_snapshot_request.__set_src_backends({TBackend()});

    std::string snapshot_path;
    bool incremental_snapshot = false;
    Status status = StorageEngine::instance()->replication_txn_manager()->remote_snapshot(
            remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_TRUE(status.ok());

    TabletSharedPtr tablet_ptr = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id);
    EXPECT_TRUE(tablet_ptr != nullptr);

    status = StorageEngine::instance()->replication_txn_manager()->publish_txn(_transaction_id, _partition_id,
                                                                               tablet_ptr, _src_version);
    EXPECT_TRUE(!status.ok());

    StorageEngine::instance()->replication_txn_manager()->clear_txn(_transaction_id);

    StorageEngine::instance()->replication_txn_manager()->clear_expired_snapshots();
}

TEST_F(ReplicationTxnManagerTest, test_run_normal) {
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
    remote_snapshot_request.__set_src_visible_version(_src_version);
    remote_snapshot_request.__set_src_backends({TBackend()});

    std::string snapshot_path;
    bool incremental_snapshot = false;
    Status status = StorageEngine::instance()->replication_txn_manager()->remote_snapshot(
            remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_TRUE(status.ok());

    TReplicateSnapshotRequest replicate_snapshot_request;
    replicate_snapshot_request.__set_transaction_id(_transaction_id);
    replicate_snapshot_request.__set_table_id(_table_id);
    replicate_snapshot_request.__set_partition_id(_partition_id);
    replicate_snapshot_request.__set_tablet_id(_tablet_id);
    replicate_snapshot_request.__set_tablet_type(TTabletType::TABLET_TYPE_DISK);
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

    status = StorageEngine::instance()->replication_txn_manager()->replicate_snapshot(replicate_snapshot_request);
    EXPECT_TRUE(status.ok());

    status = StorageEngine::instance()->replication_txn_manager()->replicate_snapshot(replicate_snapshot_request);
    EXPECT_TRUE(status.ok());

    TabletSharedPtr tablet_ptr = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id);
    EXPECT_TRUE(tablet_ptr != nullptr);

    status = StorageEngine::instance()->replication_txn_manager()->publish_txn(_transaction_id, _partition_id,
                                                                               tablet_ptr, _src_version);
    EXPECT_TRUE(status.ok());

    status = StorageEngine::instance()->replication_txn_manager()->publish_txn(_transaction_id, _partition_id,
                                                                               tablet_ptr, _src_version);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(_src_version, tablet_ptr->max_version().second);

    StorageEngine::instance()->replication_txn_manager()->clear_txn(_transaction_id);

    StorageEngine::instance()->replication_txn_manager()->clear_expired_snapshots();
}

} // namespace starrocks
