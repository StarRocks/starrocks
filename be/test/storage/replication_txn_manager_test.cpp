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
#include "storage/chunk_helper.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_manager.h"
#include "testutil/assert.h"
#include "util/uuid_generator.h"

namespace starrocks {

class ReplicationTxnManagerTest : public testing::TestWithParam<TKeysType::type> {
public:
    ReplicationTxnManagerTest() = default;
    ~ReplicationTxnManagerTest() override = default;
    void SetUp() override {
        if (GetParam() != TKeysType::type::PRIMARY_KEYS) {
            create_tablet(_tablet_id, _version, _schema_hash);
            create_tablet(_src_tablet_id, _src_version, _schema_hash);
        } else {
            auto tablet = create_tablet(_tablet_id, 1, _schema_hash);
            auto src_tablet = create_tablet(_src_tablet_id, 1, _schema_hash);

            std::vector<int64_t> keys{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
            for (int i = 2; i <= _version; i++) {
                ASSERT_TRUE(tablet->rowset_commit(i, create_rowset(tablet, keys)).ok());
            }
            for (int i = 2; i <= _src_version; i++) {
                ASSERT_TRUE(src_tablet->rowset_commit(i, create_rowset(src_tablet, keys)).ok());
            }
        }
    }

    void TearDown() override {
        auto status = StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet_id, kDeleteFiles);
        EXPECT_TRUE(status.ok()) << status;
        status = StorageEngine::instance()->tablet_manager()->drop_tablet(_src_tablet_id, kDeleteFiles);
        EXPECT_TRUE(status.ok()) << status;
        status = StorageEngine::instance()->tablet_manager()->delete_shutdown_tablet(_tablet_id);
        EXPECT_TRUE(status.ok()) << status;
        status = StorageEngine::instance()->tablet_manager()->delete_shutdown_tablet(_src_tablet_id);
        EXPECT_TRUE(status.ok()) << status;
        status = fs::remove_all(config::storage_root_path);
        EXPECT_TRUE(status.ok() || status.is_not_found()) << status;
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int64_t version, int32_t schema_hash,
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
        auto st = StorageEngine::instance()->create_tablet(request);
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
    int64_t _transaction_id = 100;
    int64_t _table_id = 10001;
    int64_t _partition_id = 10002;
    int64_t _tablet_id = 10003;
    int64_t _src_tablet_id = 10004;
    int32_t _schema_hash = 368169781;
    int64_t _version = 2;
    int64_t _src_version = 10;
};

TEST_P(ReplicationTxnManagerTest, test_remote_snapshot_no_missing_versions) {
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
    EXPECT_FALSE(status.ok()) << status;

    StorageEngine::instance()->replication_txn_manager()->clear_txn(_transaction_id);

    StorageEngine::instance()->replication_txn_manager()->clear_expired_snapshots();
}

TEST_P(ReplicationTxnManagerTest, test_remote_snapshot_no_versions) {
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
    EXPECT_FALSE(status.ok()) << status;

    StorageEngine::instance()->replication_txn_manager()->clear_txn(_transaction_id);

    StorageEngine::instance()->replication_txn_manager()->clear_expired_snapshots();
}

TEST_P(ReplicationTxnManagerTest, test_replicate_snapshot_failed) {
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
    EXPECT_TRUE(status.ok()) << status;

    status = StorageEngine::instance()->replication_txn_manager()->remote_snapshot(
            remote_snapshot_request, &snapshot_path, &incremental_snapshot);
    EXPECT_TRUE(status.ok()) << status;

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
    EXPECT_FALSE(status.ok()) << status;

    StorageEngine::instance()->replication_txn_manager()->clear_txn(_transaction_id);

    StorageEngine::instance()->replication_txn_manager()->clear_expired_snapshots();
}

TEST_P(ReplicationTxnManagerTest, test_publish_failed) {
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
    EXPECT_TRUE(status.ok()) << status;

    TabletSharedPtr tablet_ptr = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id);
    EXPECT_TRUE(tablet_ptr != nullptr);

    status = StorageEngine::instance()->replication_txn_manager()->publish_txn(_transaction_id, _partition_id,
                                                                               tablet_ptr, _src_version);
    EXPECT_TRUE(!status.ok()) << status;

    StorageEngine::instance()->replication_txn_manager()->clear_txn(_transaction_id);

    StorageEngine::instance()->replication_txn_manager()->clear_expired_snapshots();
}

TEST_P(ReplicationTxnManagerTest, test_run_normal) {
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
    EXPECT_TRUE(status.ok()) << status;

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
    EXPECT_TRUE(status.ok()) << status;

    status = StorageEngine::instance()->replication_txn_manager()->replicate_snapshot(replicate_snapshot_request);
    EXPECT_TRUE(status.ok()) << status;

    TabletSharedPtr tablet_ptr = StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_id);
    EXPECT_TRUE(tablet_ptr != nullptr);

    status = StorageEngine::instance()->replication_txn_manager()->publish_txn(_transaction_id, _partition_id,
                                                                               tablet_ptr, _src_version);
    EXPECT_TRUE(status.ok()) << status;

    status = StorageEngine::instance()->replication_txn_manager()->publish_txn(_transaction_id, _partition_id,
                                                                               tablet_ptr, _src_version);
    EXPECT_TRUE(status.ok()) << status;

    EXPECT_EQ(_src_version, tablet_ptr->max_version().second);

    StorageEngine::instance()->replication_txn_manager()->clear_txn(_transaction_id);

    StorageEngine::instance()->replication_txn_manager()->clear_expired_snapshots();
}

INSTANTIATE_TEST_SUITE_P(ReplicationTxnManagerTest, ReplicationTxnManagerTest,
                         testing::Values(TKeysType::type::AGG_KEYS, TKeysType::type::PRIMARY_KEYS));

} // namespace starrocks
