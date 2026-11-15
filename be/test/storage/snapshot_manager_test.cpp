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
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/options.h"
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

        snapshot_meta->rowset_metas().resize(1);
        RowsetMetaPB& rowset_meta_pb = snapshot_meta->rowset_metas()[0];
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

} // namespace starrocks
