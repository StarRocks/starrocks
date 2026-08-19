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

#include "data_workflows/clone/engine_clone_task.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "data_workflows/snapshot/snapshot_loader.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/inverted/clucene/clucene_plugin.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"

namespace starrocks {

#ifndef __APPLE__

class EngineCloneTaskTest : public testing::Test {
protected:
    static TCreateTabletReq create_tablet_request(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.__set_id(1);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::DUP_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn key;
        key.column_name = "k1";
        key.__set_is_key(true);
        key.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.emplace_back(std::move(key));

        TColumn value;
        value.column_name = "v1";
        value.__set_is_key(false);
        value.column_type.type = TPrimitiveType::VARCHAR;
        value.column_type.len = 65535;
        request.tablet_schema.columns.emplace_back(std::move(value));

        request.tablet_schema.__isset.indexes = true;
        request.tablet_schema.indexes.emplace_back();
        auto& gin_index = request.tablet_schema.indexes.back();
        gin_index.__set_index_id(1);
        gin_index.__set_index_name("v1_gin");
        gin_index.__set_index_type(TIndexType::GIN);
        gin_index.__set_columns({"v1"});
        gin_index.__set_common_properties({{"imp_lib", "clucene"}});
        return request;
    }

    static TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        auto request = create_tablet_request(tablet_id, schema_hash);
        CHECK_OK(StorageEngine::instance()->create_tablet(request));
        auto tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
        CHECK(tablet != nullptr);
        return tablet;
    }

    static RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, int64_t version) {
        RowsetWriterContext context;
        context.rowset_id = StorageEngine::instance()->next_rowset_id();
        context.tablet_id = tablet->tablet_id();
        context.tablet_schema_hash = tablet->schema_hash();
        context.partition_id = 1;
        context.rowset_path_prefix = tablet->schema_hash_path();
        context.rowset_state = COMMITTED;
        context.tablet_schema = tablet->tablet_schema();
        context.version = Version(0, 0);
        context.segments_overlap = NONOVERLAPPING;

        std::unique_ptr<RowsetWriter> writer;
        CHECK_OK(RowsetFactory::create_rowset_writer(context, &writer));

        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        auto chunk = ChunkFactory::new_chunk(schema, 3);
        auto columns = chunk->columns();
        for (int i = 0; i < 3; ++i) {
            int64_t key = version * 10 + i;
            columns[0]->as_mutable_ptr()->append_datum(Datum(key));
            std::string value = fmt::format("version-{}-row-{}", version, i);
            columns[1]->as_mutable_ptr()->append_datum(Datum(Slice(value)));
        }
        CHECK_OK(writer->flush_chunk(*chunk));
        auto rowset = writer->build();
        CHECK_OK(rowset.status());
        return *rowset;
    }

    static void assert_flattened_indexes(const std::string& snapshot_dir) {
        std::set<std::string> directories;
        std::set<std::string> files;
        ASSERT_OK(fs::list_dirs_files(snapshot_dir, &directories, &files));
        ASSERT_TRUE(std::none_of(directories.begin(), directories.end(),
                                 [](const std::string& dir) { return dir.ends_with(".ivt"); }));
        ASSERT_TRUE(std::any_of(files.begin(), files.end(),
                                [](const std::string& file) { return CLucenePlugin::is_index_files(file); }));
    }

    static void assert_tablet_indexes(const TabletSharedPtr& tablet) {
        std::set<std::string> tablet_files;
        ASSERT_OK(fs::list_dirs_files(tablet->schema_hash_path(), nullptr, &tablet_files));
        ASSERT_TRUE(std::none_of(tablet_files.begin(), tablet_files.end(),
                                 [](const std::string& file) { return CLucenePlugin::is_index_files(file); }));

        size_t index_directory_count = 0;
        for (const auto& rowset_meta : tablet->tablet_meta()->all_rs_metas()) {
            for (int segment_id = 0; segment_id < rowset_meta->num_segments(); ++segment_id) {
                const auto index_dir = IndexDescriptor::inverted_index_file_path(
                        tablet->schema_hash_path(), rowset_meta->rowset_id().to_string(), segment_id, 1);
                auto is_directory = fs::is_directory(index_dir);
                ASSERT_OK(is_directory.status());
                ASSERT_TRUE(is_directory.value()) << index_dir;

                std::set<std::string> index_files;
                ASSERT_OK(fs::list_dirs_files(index_dir, nullptr, &index_files));
                ASSERT_FALSE(index_files.empty()) << index_dir;
                ++index_directory_count;
            }
        }
        ASSERT_GE(index_directory_count, 2);
    }

    static void run_clone(bool incremental_clone, int64_t tablet_id) {
        constexpr int32_t kSchemaHash = 987654321;
        constexpr int64_t kCommittedVersion = 3;
        auto* tablet_manager = StorageEngine::instance()->tablet_manager();

        (void)tablet_manager->drop_tablet(tablet_id, kDeleteFiles);
        (void)tablet_manager->delete_shutdown_tablet(tablet_id);

        TabletSharedPtr source_tablet;
        TabletSharedPtr destination_tablet;
        std::string snapshot_root;
        DeferOp cleanup([&]() {
            source_tablet.reset();
            destination_tablet.reset();
            (void)tablet_manager->drop_tablet(tablet_id, kDeleteFiles);
            (void)tablet_manager->delete_shutdown_tablet(tablet_id);
            if (!snapshot_root.empty()) {
                (void)fs::remove_all(snapshot_root);
            }
        });

        source_tablet = create_tablet(tablet_id, kSchemaHash);
        ASSERT_OK(source_tablet->add_inc_rowset(create_rowset(source_tablet, 2), 2));
        ASSERT_OK(source_tablet->add_inc_rowset(create_rowset(source_tablet, 3), 3));

        StatusOr<std::string> snapshot =
                incremental_clone ? SnapshotManager::instance()->snapshot_incremental(source_tablet, {2, 3}, 3600)
                                  : SnapshotManager::instance()->snapshot_full(source_tablet, kCommittedVersion, 3600);
        ASSERT_OK(snapshot.status());
        snapshot_root = *snapshot;
        const std::string snapshot_dir =
                SnapshotManager::instance()->get_schema_hash_full_path(source_tablet, snapshot_root);
        ASSERT_NO_FATAL_FAILURE(assert_flattened_indexes(snapshot_dir));

        ASSERT_OK(SnapshotManager::instance()->convert_rowset_ids(snapshot_dir, tablet_id, kSchemaHash));

        source_tablet.reset();
        ASSERT_OK(tablet_manager->drop_tablet(tablet_id, kDeleteFiles));
        ASSERT_OK(tablet_manager->delete_shutdown_tablet(tablet_id));
        destination_tablet = create_tablet(tablet_id, kSchemaHash);

        TCloneReq clone_req;
        clone_req.tablet_id = tablet_id;
        clone_req.schema_hash = kSchemaHash;
        clone_req.__set_committed_version(kCommittedVersion);
        std::vector<std::string> error_messages;
        std::vector<TTabletInfo> tablet_infos;
        Status clone_status;
        EngineCloneTask clone_task(nullptr, clone_req, 0, &error_messages, &tablet_infos, &clone_status);
        ASSERT_OK(
                clone_task._finish_clone(destination_tablet.get(), snapshot_dir, kCommittedVersion, incremental_clone));

        ASSERT_EQ(kCommittedVersion, destination_tablet->max_version().second);
        ASSERT_NO_FATAL_FAILURE(assert_tablet_indexes(destination_tablet));
    }
};

TEST_F(EngineCloneTaskTest, incremental_clone_preserves_gin_indexes) {
    run_clone(true, 910000000001);
}

TEST_F(EngineCloneTaskTest, full_clone_preserves_gin_indexes) {
    run_clone(false, 910000000002);
}

TEST_F(EngineCloneTaskTest, backup_restore_preserves_gin_indexes) {
    constexpr int64_t kTabletId = 910000000003;
    constexpr int32_t kSchemaHash = 987654321;
    constexpr int64_t kCommittedVersion = 3;
    auto* tablet_manager = StorageEngine::instance()->tablet_manager();

    (void)tablet_manager->drop_tablet(kTabletId, kDeleteFiles);
    (void)tablet_manager->delete_shutdown_tablet(kTabletId);

    TabletSharedPtr tablet;
    TabletSharedPtr restored_tablet;
    std::string snapshot_root;
    DeferOp cleanup([&]() {
        tablet.reset();
        restored_tablet.reset();
        (void)tablet_manager->drop_tablet(kTabletId, kDeleteFiles);
        (void)tablet_manager->delete_shutdown_tablet(kTabletId);
        if (!snapshot_root.empty()) {
            (void)fs::remove_all(snapshot_root);
        }
    });

    tablet = create_tablet(kTabletId, kSchemaHash);
    ASSERT_OK(tablet->add_inc_rowset(create_rowset(tablet, 2), 2));
    ASSERT_OK(tablet->add_inc_rowset(create_rowset(tablet, 3), 3));

    auto snapshot = SnapshotManager::instance()->snapshot_full(tablet, kCommittedVersion, 3600);
    ASSERT_OK(snapshot.status());
    snapshot_root = *snapshot;
    const std::string snapshot_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet, snapshot_root);
    ASSERT_NO_FATAL_FAILURE(assert_flattened_indexes(snapshot_dir));

    SnapshotLoader loader(nullptr, 1, 1);
    ASSERT_OK(loader.move(snapshot_dir, tablet, true));

    restored_tablet = tablet_manager->get_tablet(kTabletId, false);
    ASSERT_NE(nullptr, restored_tablet);
    ASSERT_EQ(kCommittedVersion, restored_tablet->max_version().second);
    ASSERT_NO_FATAL_FAILURE(assert_tablet_indexes(restored_tablet));

    const auto& restored_rowsets = restored_tablet->tablet_meta()->all_rs_metas();
    const auto rowset_meta = std::find_if(restored_rowsets.begin(), restored_rowsets.end(),
                                          [](const RowsetMetaSharedPtr& meta) { return meta->num_segments() > 0; });
    ASSERT_NE(restored_rowsets.end(), rowset_meta);
    const auto source_index_dir =
            IndexDescriptor::inverted_index_file_path(snapshot_dir, (*rowset_meta)->rowset_id().to_string(), 0, 1);
    const auto destination_index_dir = IndexDescriptor::inverted_index_file_path(
            restored_tablet->schema_hash_path(), (*rowset_meta)->rowset_id().to_string(), 0, 1);
    std::set<std::string> source_index_files;
    ASSERT_OK(fs::list_dirs_files(source_index_dir, nullptr, &source_index_files));
    ASSERT_FALSE(source_index_files.empty());
    const auto& index_file = *source_index_files.begin();
    ASSERT_TRUE(
            std::filesystem::equivalent(source_index_dir + "/" + index_file, destination_index_dir + "/" + index_file));
}

#endif

} // namespace starrocks
