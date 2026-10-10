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

#include "storage/snapshot_manager.h"

#include <gtest/gtest.h>

#include <ctime>
#include <filesystem>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "common/config_storage_fwd.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/options.h"
#include "storage/replication_txn_manager.h"
#include "storage/rowset/rowset.h"
#include "storage/snapshot_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class SnapshotManagerTest : public testing::Test {
protected:
    void SetUp() override {
        _default_storage_root_path = config::storage_root_path;
        config::storage_root_path = std::filesystem::current_path().string() + "/snapshot_manager_test";

        ASSERT_OK(fs::remove_all(config::storage_root_path));
        ASSERT_TRUE(fs::create_directories(config::storage_root_path).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path);
        EngineOptions options;
        options.store_paths = paths;
        ASSERT_OK(StorageEngine::open(options, &_engine));

        _clone_dir = config::storage_root_path + "/clone";
        ASSERT_TRUE(fs::create_directories(_clone_dir).ok());
    }

    void TearDown() override {
        if (_engine != nullptr) {
            _engine->stop();
            delete _engine;
            _engine = nullptr;
        }
        if (fs::path_exist(config::storage_root_path)) {
            ASSERT_TRUE(fs::remove_all(config::storage_root_path).ok());
        }
        config::storage_root_path = _default_storage_root_path;
    }

    static std::shared_ptr<TabletSchema> create_gin_tablet_schema(const std::string& imp_lib) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(1024);
        schema_pb.set_next_column_unique_id(3);

        ColumnPB* k1 = schema_pb.add_column();
        k1->set_unique_id(1);
        k1->set_name("k1");
        k1->set_type("INT");
        k1->set_is_key(true);
        k1->set_length(4);
        k1->set_index_length(4);
        k1->set_is_nullable(false);

        ColumnPB* v1 = schema_pb.add_column();
        v1->set_unique_id(2);
        v1->set_name("v1");
        v1->set_type("VARCHAR");
        v1->set_is_key(false);
        v1->set_length(64);
        v1->set_is_nullable(false);

        TabletIndexPB* index_pb = schema_pb.add_table_indices();
        index_pb->set_index_id(100);
        index_pb->set_index_name("gin_v1");
        index_pb->set_index_type(GIN);
        index_pb->add_col_unique_id(2);
        index_pb->set_index_properties(R"({"common_properties":{")" + INVERTED_IMP_KEY + R"(":")" + imp_lib + R"("}})");

        return std::make_shared<TabletSchema>(schema_pb);
    }

    void build_single_segment_snapshot(SnapshotMeta* snapshot_meta, RowsetId* rowset_id) {
        *rowset_id = StorageEngine::instance()->next_rowset_id();

        ASSIGN_OR_ABORT(auto wfile,
                        FileSystem::Default()->new_writable_file(Rowset::segment_file_path(_clone_dir, *rowset_id, 0)));
        ASSERT_OK(wfile->append("dummy segment"));
        ASSERT_OK(wfile->close());

        snapshot_meta->set_snapshot_type(SNAPSHOT_TYPE_INCREMENTAL);
        snapshot_meta->set_snapshot_format(4);
        snapshot_meta->rowset_metas().resize(1);
        RowsetMetaPB& rowset_meta_pb = snapshot_meta->rowset_metas()[0];
        rowset_meta_pb.set_deprecated_rowset_id(0);
        rowset_meta_pb.set_rowset_id(rowset_id->to_string());
        rowset_meta_pb.set_tablet_id(12345);
        rowset_meta_pb.set_partition_id(1);
        rowset_meta_pb.set_rowset_state(VISIBLE);
        rowset_meta_pb.set_start_version(2);
        rowset_meta_pb.set_end_version(2);
        rowset_meta_pb.set_num_rows(1);
        rowset_meta_pb.set_num_segments(1);
        rowset_meta_pb.set_num_delete_files(0);
        rowset_meta_pb.set_data_disk_size(1);
        rowset_meta_pb.set_empty(false);
        rowset_meta_pb.set_creation_time(time(nullptr));
    }

    StorageEngine* _engine = nullptr;
    std::string _default_storage_root_path;
    std::string _clone_dir;
};

// Builtin GIN lives inside the segment file, so there is no standalone .ivt directory to relocate.
TEST_F(SnapshotManagerTest, assign_new_rowset_id_skips_builtin_gin_index) {
    auto schema = create_gin_tablet_schema(TYPE_BUILTIN);

    SnapshotMeta snapshot_meta;
    RowsetId old_rowset_id;
    build_single_segment_snapshot(&snapshot_meta, &old_rowset_id);

    ASSERT_OK(SnapshotManager::instance()->assign_new_rowset_id(&snapshot_meta, _clone_dir, schema));

    const std::string& new_rowset_id = snapshot_meta.rowset_metas()[0].rowset_id();
    ASSERT_NE(old_rowset_id.to_string(), new_rowset_id);

    RowsetId parsed;
    parsed.init(new_rowset_id);
    ASSERT_TRUE(fs::path_exist(Rowset::segment_file_path(_clone_dir, parsed, 0)));
}

// A new primary-key replica restores the transport layout before assigning rowset IDs.
#ifndef __APPLE__
TEST_F(SnapshotManagerTest, restore_flattened_clucene_files_before_assigning_rowset_ids) {
    auto schema = create_gin_tablet_schema(TYPE_CLUCENE);
    SnapshotMeta snapshot_meta;
    RowsetId old_rowset_id;
    build_single_segment_snapshot(&snapshot_meta, &old_rowset_id);

    // Snapshot transport only downloads top-level files. The index file name itself may contain underscores.
    const auto flat_file = _clone_dir + "/" + old_rowset_id.to_string() + "_0_100__0.tis";
    ASSIGN_OR_ABORT(auto file, FileSystem::Default()->new_writable_file(flat_file));
    ASSERT_OK(file->append("clucene index contents"));
    ASSERT_OK(file->close());

    ASSIGN_OR_ABORT(auto original_md5, fs::md5sum(flat_file));
    ASSERT_OK(SnapshotManager::restore_clucene_index_files(_clone_dir));
    ASSERT_FALSE(fs::path_exist(flat_file));
    std::set<std::string> before;
    ASSERT_OK(SnapshotManager::list_snapshot_files(_clone_dir, &before));
    ASSERT_OK(SnapshotManager::restore_clucene_index_files(_clone_dir));
    std::set<std::string> after;
    ASSERT_OK(SnapshotManager::list_snapshot_files(_clone_dir, &after));
    ASSERT_EQ(before, after);
    ASSERT_OK(SnapshotManager::instance()->assign_new_rowset_id(&snapshot_meta, _clone_dir, schema));
    const auto index_file = _clone_dir + "/" + snapshot_meta.rowset_metas()[0].rowset_id() + "_0_100.ivt/_0.tis";
    ASSERT_TRUE(fs::path_exist(index_file));
    ASSIGN_OR_ABORT(auto actual, fs::md5sum(index_file));
    const auto restored_file = _clone_dir + "/" + old_rowset_id.to_string() + "_0_100.ivt/_0.tis";
    ASSIGN_OR_ABORT(auto expected, fs::md5sum(restored_file));
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(original_md5, actual);
}

TEST_F(SnapshotManagerTest, restore_clucene_rejects_malformed_names) {
    for (const std::string name :
         {"rogue.tis", "rowset_0.tis", "rowset_0_100.tis", "_0_100_file.tis", "rowset__100_file.tis",
          "rowset_0__file.tis", "rowset_x_100_file.tis", "rowset_0_x_file.tis", "rowset_1x_100_file.tis",
          "rowset_0_1x_file.tis", "rowset_-1_100_file.tis", "rowset_0_-1_file.tis", "rowset_2147483648_100_file.tis",
          "rowset_0_9223372036854775808_file.tis"}) {
        SCOPED_TRACE(name);
        const auto path = _clone_dir + "/" + name;
        ASSIGN_OR_ABORT(auto file, FileSystem::Default()->new_writable_file(path));
        ASSERT_OK(file->close());
        auto status = SnapshotManager::restore_clucene_index_files(_clone_dir);
        ASSERT_TRUE(status.is_invalid_argument()) << status;
        ASSERT_TRUE(fs::path_exist(path));
        ASSERT_OK(fs::delete_file(path));
    }
}

TEST_F(SnapshotManagerTest, restore_clucene_accepts_int64_index_id) {
    const auto flat = _clone_dir + "/rowset_0_9223372036854775807__0.tis";
    ASSIGN_OR_ABORT(auto file, FileSystem::Default()->new_writable_file(flat));
    ASSERT_OK(file->append("index contents"));
    ASSERT_OK(file->close());
    ASSIGN_OR_ABORT(auto expected, fs::md5sum(flat));
    ASSERT_OK(SnapshotManager::restore_clucene_index_files(_clone_dir));
    ASSIGN_OR_ABORT(auto actual, fs::md5sum(_clone_dir + "/rowset_0_9223372036854775807.ivt/_0.tis"));
    ASSERT_EQ(expected, actual);
}

TEST_F(SnapshotManagerTest, replication_converts_index_column_unique_ids) {
    auto schema = create_gin_tablet_schema(TYPE_CLUCENE);
    SnapshotMeta meta;
    RowsetId old_id;
    build_single_segment_snapshot(&meta, &old_id);
    schema->to_schema_pb(meta.rowset_metas()[0].mutable_tablet_schema());
    ASSERT_OK(meta.serialize_to_file(_clone_dir + "/meta"));
    auto index_dir = _clone_dir + "/" + old_id.to_string() + "_0_100.ivt";
    ASSERT_OK(fs::create_directories(index_dir));
    ASSIGN_OR_ABORT(auto file, FileSystem::Default()->new_writable_file(index_dir + "/_0.tis"));
    ASSERT_OK(file->append("index contents"));
    ASSERT_OK(file->close());
    std::unordered_map<uint32_t, uint32_t> uid_map{{1, 11}, {2, 22}};
    TReplicateSnapshotRequest request;
    request.__set_tablet_id(12345);
    request.__set_partition_id(1);
    ReplicationTxnManager replication;
    ASSERT_OK(replication.convert_snapshot_for_primary(_clone_dir + "/", &uid_map, request, schema));
    ASSIGN_OR_ABORT(auto converted, SnapshotManager::instance()->parse_snapshot_meta(_clone_dir + "/meta"));
    const auto& rowset = converted.rowset_metas()[0];
    ASSERT_EQ(11, rowset.tablet_schema().column(0).unique_id());
    ASSERT_EQ(22, rowset.tablet_schema().column(1).unique_id());
    ASSERT_EQ(22, rowset.tablet_schema().table_indices(0).col_unique_id(0));
    // Readers look up the index with the destination column UID, but filenames retain index ID 100.
    auto converted_schema = TabletSchema::create(rowset.tablet_schema());
    std::shared_ptr<TabletIndex> index;
    ASSERT_OK(converted_schema->get_indexes_for_column(22, GIN, index));
    ASSERT_NE(nullptr, index);
    ASSERT_EQ(100, index->index_id());
    ASSERT_NE(old_id.to_string(), rowset.rowset_id());
    ASSIGN_OR_ABORT(auto expected, fs::md5sum(index_dir + "/_0.tis"));
    ASSIGN_OR_ABORT(auto actual, fs::md5sum(_clone_dir + "/" + rowset.rowset_id() + "_0_100.ivt/_0.tis"));
    ASSERT_EQ(expected, actual);
}

TEST_F(SnapshotManagerTest, replication_rejects_unmapped_index_column_without_rewriting_snapshot) {
    auto schema = create_gin_tablet_schema(TYPE_BUILTIN);
    SnapshotMeta meta;
    RowsetId old_id;
    build_single_segment_snapshot(&meta, &old_id);
    schema->to_schema_pb(meta.rowset_metas()[0].mutable_tablet_schema());
    meta.rowset_metas()[0].mutable_tablet_schema()->mutable_table_indices(0)->set_col_unique_id(0, 999);
    const auto path = _clone_dir + "/meta";
    ASSERT_OK(meta.serialize_to_file(path));
    ASSIGN_OR_ABORT(auto before, fs::md5sum(path));
    std::unordered_map<uint32_t, uint32_t> uid_map{{1, 11}, {2, 22}};
    TReplicateSnapshotRequest request;
    ReplicationTxnManager replication;
    auto status = replication.convert_snapshot_for_primary(_clone_dir + "/", &uid_map, request, schema);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos, status.to_string().find("Column not found"));
    ASSIGN_OR_ABORT(auto after, fs::md5sum(path));
    ASSERT_EQ(before, after);
    ASSERT_TRUE(fs::path_exist(Rowset::segment_file_path(_clone_dir, old_id, 0)));
}

TEST_F(SnapshotManagerTest, replication_rejects_unmapped_dropped_index_column_without_rewriting_snapshot) {
    auto schema = create_gin_tablet_schema(TYPE_BUILTIN);
    SnapshotMeta meta;
    RowsetId old_id;
    build_single_segment_snapshot(&meta, &old_id);
    schema->to_schema_pb(meta.rowset_metas()[0].mutable_tablet_schema());
    auto* rowset_schema = meta.rowset_metas()[0].mutable_tablet_schema();
    auto* dropped = rowset_schema->add_dropped_table_indices();
    dropped->CopyFrom(rowset_schema->table_indices(0));
    dropped->set_index_id(101);
    dropped->set_col_unique_id(0, 999);
    const auto path = _clone_dir + "/meta";
    ASSERT_OK(meta.serialize_to_file(path));
    ASSIGN_OR_ABORT(auto before, fs::md5sum(path));
    std::unordered_map<uint32_t, uint32_t> uid_map{{1, 11}, {2, 22}};
    TReplicateSnapshotRequest request;
    ReplicationTxnManager replication;
    auto status = replication.convert_snapshot_for_primary(_clone_dir + "/", &uid_map, request, schema);
    ASSERT_FALSE(status.ok());
    ASSERT_NE(std::string::npos, status.to_string().find("Column not found"));
    ASSIGN_OR_ABORT(auto after, fs::md5sum(path));
    ASSERT_EQ(before, after);
    ASSERT_TRUE(fs::path_exist(Rowset::segment_file_path(_clone_dir, old_id, 0)));
}

TEST_F(SnapshotManagerTest, replication_maps_each_rowset_once_with_overlapping_uids) {
    TabletSchemaPB schema_pb;
    create_gin_tablet_schema(TYPE_BUILTIN)->to_schema_pb(&schema_pb);
    auto* dropped = schema_pb.add_dropped_table_indices();
    dropped->CopyFrom(schema_pb.table_indices(0));
    dropped->set_index_id(101);
    dropped->set_index_name("dropped_gin_v1");
    auto schema = TabletSchema::create(schema_pb);
    SnapshotMeta meta;
    meta.set_snapshot_type(SNAPSHOT_TYPE_INCREMENTAL);
    meta.set_snapshot_format(4);
    for (int i = 0; i < 2; ++i) {
        SnapshotMeta rowset_snapshot;
        RowsetId id;
        build_single_segment_snapshot(&rowset_snapshot, &id);
        rowset_snapshot.rowset_metas()[0].mutable_tablet_schema()->CopyFrom(schema_pb);
        meta.rowset_metas().push_back(rowset_snapshot.rowset_metas()[0]);
    }
    meta.tablet_meta().mutable_schema()->CopyFrom(schema_pb);
    ASSERT_OK(meta.serialize_to_file(_clone_dir + "/meta"));
    // Swapping UIDs catches accidental conversion of a destination UID a second time.
    std::unordered_map<uint32_t, uint32_t> uid_map{{1, 2}, {2, 1}};
    const auto original_map = uid_map;
    TReplicateSnapshotRequest request;
    ReplicationTxnManager replication;
    ASSERT_OK(replication.convert_snapshot_for_primary(_clone_dir + "/", &uid_map, request, schema));
    ASSIGN_OR_ABORT(auto converted, SnapshotManager::instance()->parse_snapshot_meta(_clone_dir + "/meta"));
    ASSERT_EQ(2, converted.rowset_metas().size());
    for (const auto& rowset : converted.rowset_metas()) {
        ASSERT_EQ(2, rowset.tablet_schema().column(0).unique_id());
        ASSERT_EQ(1, rowset.tablet_schema().column(1).unique_id());
        ASSERT_EQ(1, rowset.tablet_schema().table_indices(0).col_unique_id(0));
        ASSERT_EQ(1, rowset.tablet_schema().dropped_table_indices_size());
        ASSERT_EQ(1, rowset.tablet_schema().dropped_table_indices(0).col_unique_id(0));
        ASSERT_EQ(101, rowset.tablet_schema().dropped_table_indices(0).index_id());
        auto converted_schema = TabletSchema::create(rowset.tablet_schema());
        ASSERT_TRUE(converted_schema->has_dropped_index(1, GIN));
        ASSERT_FALSE(converted_schema->has_dropped_index(2, GIN));
    }
    ASSERT_EQ(original_map, uid_map);
    // The top-level source schema must remain available for later incremental replication.
    ASSERT_EQ(1, converted.tablet_meta().schema().column(0).unique_id());
    ASSERT_EQ(2, converted.tablet_meta().schema().table_indices(0).col_unique_id(0));
    ASSERT_EQ(1, converted.tablet_meta().schema().dropped_table_indices_size());
    ASSERT_EQ(2, converted.tablet_meta().schema().dropped_table_indices(0).col_unique_id(0));
}

#endif

// CLucene keeps a standalone directory, so a missing one must still be reported.
TEST_F(SnapshotManagerTest, assign_new_rowset_id_reports_missing_clucene_gin_index) {
    auto schema = create_gin_tablet_schema(TYPE_CLUCENE);

    SnapshotMeta snapshot_meta;
    RowsetId old_rowset_id;
    build_single_segment_snapshot(&snapshot_meta, &old_rowset_id);

    auto st = SnapshotManager::instance()->assign_new_rowset_id(&snapshot_meta, _clone_dir, schema);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_not_found()) << st.to_string();
}

// These tests exercise standalone file relocation, without requiring an ANN build.
TEST_F(SnapshotManagerTest, assign_new_rowset_id_preserves_vector_index_files) {
    TabletSchemaPB schema_pb;
    create_gin_tablet_schema(TYPE_BUILTIN)->to_schema_pb(&schema_pb);
    auto* vector_column = schema_pb.mutable_column(1);
    vector_column->set_type("ARRAY");
    vector_column->set_length(24);
    auto* element = vector_column->add_children_columns();
    element->set_unique_id(3);
    element->set_name("element");
    element->set_type("FLOAT");
    element->set_length(4);
    element->set_is_nullable(true);
    schema_pb.set_next_column_unique_id(4);
    auto* index = schema_pb.mutable_table_indices(0);
    index->set_index_type(VECTOR);
    index->set_index_properties(R"({"common_properties":{"index_type":"hnsw","dim":"3","metric_type":"l2_distance"}})");
    auto schema = TabletSchema::create(schema_pb);
    for (bool has_index : {false, true}) {
        SnapshotMeta meta;
        RowsetId old_id;
        build_single_segment_snapshot(&meta, &old_id);
        auto old_file = IndexDescriptor::vector_index_file_path(_clone_dir, old_id.to_string(), 0, 100);
        if (has_index) {
            ASSIGN_OR_ABORT(auto file, FileSystem::Default()->new_writable_file(old_file));
            ASSERT_OK(file->append("standalone vector index payload"));
            ASSERT_OK(file->close());
        }
        ASSERT_OK(SnapshotManager::restore_clucene_index_files(_clone_dir));
        ASSERT_OK(SnapshotManager::instance()->assign_new_rowset_id(&meta, _clone_dir, schema));
        auto new_id = meta.rowset_metas()[0].rowset_id();
        ASSERT_NE(old_id.to_string(), new_id);
        auto new_file = IndexDescriptor::vector_index_file_path(_clone_dir, new_id, 0, 100);
        ASSERT_EQ(has_index, fs::path_exist(new_file));
        if (has_index) {
            ASSIGN_OR_ABORT(auto expected, fs::md5sum(old_file));
            ASSIGN_OR_ABORT(auto actual, fs::md5sum(new_file));
            ASSERT_EQ(expected, actual);
        }
    }
}

} // namespace starrocks
