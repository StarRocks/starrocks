// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/tablet_updates.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "column/datum_tuple.h"
#include "column/vectorized_fwd.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/empty_iterator.h"
#include "storage/kv_store.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment.h"
#include "storage/schema_change.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_reader.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "util/defer_op.h"
#include "util/path_util.h"

namespace starrocks {

enum PartialUpdateCloneCase {
    CASE1, // rowset status is committed in meta, rowset file is partial rowset
    CASE2, // rowset status is committed in meta, rowset file is partial rowset, but rowset is apply success after link file
    CASE3, // rowset status is committed in meta, rowset file is full rowset
    CASE4  // rowset status is applied in meta, rowset file is full rowset
};

class TabletUpdatesTest : public testing::Test {
public:
    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                  vectorized::Column* one_delete = nullptr, bool empty = false,
                                  bool has_merge_condition = false) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &tablet->tablet_schema();
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
        auto schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (int64_t key : keys) {
            if (schema.num_key_fields() == 1) {
                cols[0]->append_datum(vectorized::Datum(key));
            } else {
                cols[0]->append_datum(vectorized::Datum(key));
                string v = fmt::to_string(key * 234234342345);
                cols[1]->append_datum(vectorized::Datum(Slice(v)));
                cols[2]->append_datum(vectorized::Datum((int32_t)key));
            }
            int vcol_start = schema.num_key_fields();
            cols[vcol_start]->append_datum(vectorized::Datum((int16_t)(key % 100 + 1)));
            if (cols[vcol_start + 1]->is_binary()) {
                string v = fmt::to_string(key % 1000 + 2);
                cols[vcol_start + 1]->append_datum(vectorized::Datum(Slice(v)));
            } else {
                cols[vcol_start + 1]->append_datum(vectorized::Datum((int32_t)(key % 1000 + 2)));
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

    RowsetSharedPtr create_rowset_with_mutiple_segments(const TabletSharedPtr& tablet,
                                                        const vector<vector<int64_t>>& keys_by_segment,
                                                        vectorized::Column* one_delete = nullptr, bool empty = false,
                                                        bool has_merge_condition = false) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = OVERLAP_UNKNOWN;
        if (has_merge_condition) {
            writer_context.merge_condition = "v2";
        }
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        if (empty) {
            return *writer->build();
        }
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        for (int i = 0; i < keys_by_segment.size(); i++) {
            auto chunk = ChunkHelper::new_chunk(schema, keys_by_segment[i].size());
            auto& cols = chunk->columns();
            for (int64_t key : keys_by_segment[i]) {
                if (schema.num_key_fields() == 1) {
                    cols[0]->append_datum(vectorized::Datum(key));
                } else {
                    cols[0]->append_datum(vectorized::Datum(key));
                    string v = fmt::to_string(key * 234234342345);
                    cols[1]->append_datum(vectorized::Datum(Slice(v)));
                    cols[2]->append_datum(vectorized::Datum((int32_t)key));
                }
                int vcol_start = schema.num_key_fields();
                cols[vcol_start]->append_datum(vectorized::Datum((int16_t)(key % 100 + 1)));
                if (cols[vcol_start + 1]->is_binary()) {
                    string v = fmt::to_string(key % 1000 + 2);
                    cols[vcol_start + 1]->append_datum(vectorized::Datum(Slice(v)));
                } else {
                    cols[vcol_start + 1]->append_datum(vectorized::Datum((int32_t)(key % 1000 + 2)));
                }
            }
            if (one_delete == nullptr && !keys_by_segment[i].empty()) {
                CHECK_OK(writer->flush_chunk(*chunk));
            } else if (one_delete == nullptr) {
                CHECK_OK(writer->flush());
            } else if (one_delete != nullptr) {
                CHECK_OK(writer->flush_chunk_with_deletes(*chunk, *one_delete));
            }
        }
        return *writer->build();
    }

    RowsetSharedPtr create_partial_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                          std::vector<int32_t>& column_indexes,
                                          const std::shared_ptr<TabletSchema>& partial_schema) {
        // create partial rowset
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.partial_update_tablet_schema = partial_schema;
        writer_context.referenced_column_ids = column_indexes;
        writer_context.tablet_schema = partial_schema.get();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema_to_format_v2(*partial_schema.get());

        if (keys.size() > 0) {
            auto chunk = ChunkHelper::new_chunk(schema, keys.size());
            EXPECT_TRUE(2 == chunk->num_columns());
            auto& cols = chunk->columns();
            for (long key : keys) {
                cols[0]->append_datum(vectorized::Datum(key));
                cols[1]->append_datum(vectorized::Datum((int16_t)(key % 100 + 3)));
            }
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        RowsetSharedPtr partial_rowset = *writer->build();

        return partial_rowset;
    }

    RowsetSharedPtr create_rowsets(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                   std::size_t max_rows_per_segment) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
        for (std::size_t written_rows = 0; written_rows < keys.size(); written_rows += max_rows_per_segment) {
            auto chunk = ChunkHelper::new_chunk(schema, max_rows_per_segment);
            auto& cols = chunk->columns();
            for (size_t i = 0; i < max_rows_per_segment; i++) {
                cols[0]->append_datum(vectorized::Datum(keys[written_rows + i]));
                cols[1]->append_datum(vectorized::Datum((int16_t)(keys[written_rows + i] % 100 + 1)));
                cols[2]->append_datum(vectorized::Datum((int32_t)(keys[written_rows + i] % 1000 + 2)));
            }
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        return *writer->build();
    }

    RowsetSharedPtr create_rowset_schema_change_sort_key(const TabletSharedPtr& tablet, const vector<int64_t>& keys) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
        const auto nkeys = keys.size();
        auto chunk = ChunkHelper::new_chunk(schema, nkeys);
        auto& cols = chunk->columns();
        for (int64_t key : keys) {
            cols[0]->append_datum(vectorized::Datum(key));
            cols[1]->append_datum(vectorized::Datum((int16_t)(nkeys - 1 - key)));
            cols[2]->append_datum(vectorized::Datum((int32_t)(key)));
        }
        CHECK_OK(writer->flush_chunk(*chunk));
        return *writer->build();
    }

    RowsetSharedPtr create_rowset_sort_key_error_encode_case(const TabletSharedPtr& tablet,
                                                             const vector<int64_t>& keys) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
        const auto nkeys = keys.size();
        auto chunk = ChunkHelper::new_chunk(schema, nkeys);
        auto& cols = chunk->columns();
        for (auto i = 0; i < nkeys; ++i) {
            cols[0]->append_datum(vectorized::Datum(keys[i]));
            cols[1]->append_datum(vectorized::Datum((int16_t)1));
            cols[2]->append_datum(vectorized::Datum((int32_t)(keys[nkeys - 1 - i])));
        }
        CHECK_OK(writer->flush_chunk(*chunk));
        return *writer->build();
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash, bool multi_column_pk = false) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        if (multi_column_pk) {
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

    TabletSharedPtr create_tablet_with_sort_key(int64_t tablet_id, int32_t schema_hash,
                                                std::vector<int32_t> sort_key_idxes) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;
        request.tablet_schema.sort_key_idxes = sort_key_idxes;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

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

    TabletSharedPtr create_tablet2(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

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

        TColumn k4;
        k4.column_name = "v3";
        k4.__set_is_key(false);
        k4.column_type.type = TPrimitiveType::INT;
        k4.__set_default_value("1");
        request.tablet_schema.columns.push_back(k4);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    TabletSharedPtr create_tablet_to_schema_change(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::VARCHAR;
        k3.column_type.len = 128;
        request.tablet_schema.columns.push_back(k3);

        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    void SetUp() override { _compaction_mem_tracker = std::make_unique<MemTracker>(-1); }

    void TearDown() override {
        if (_tablet2) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet2->tablet_id());
            _tablet2.reset();
        }
        if (_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
    }

    static Status full_clone(const TabletSharedPtr& source_tablet, int clone_version,
                             const TabletSharedPtr& dest_tablet) {
        auto snapshot_dir = SnapshotManager::instance()->snapshot_full(source_tablet, clone_version, 3600);
        CHECK(snapshot_dir.ok()) << snapshot_dir.status();

        DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

        auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(source_tablet, *snapshot_dir);
        auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
        CHECK(snapshot_meta.ok()) << snapshot_meta.status();

        RETURN_IF_ERROR(SnapshotManager::instance()->assign_new_rowset_id(&(*snapshot_meta), meta_dir));

        std::set<std::string> files;
        auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
        CHECK(st.ok()) << st;
        files.erase("meta");

        for (const auto& f : files) {
            std::string src = meta_dir + "/" + f;
            std::string dst = dest_tablet->schema_hash_path() + "/" + f;
            st = FileSystem::Default()->link_file(src, dst);
            if (st.ok()) {
                LOG(INFO) << "Linked " << src << " to " << dst;
            } else if (st.is_already_exist()) {
                LOG(INFO) << dst << " already exist";
            } else {
                return st;
            }
        }
        // Pretend that source_tablet is a peer replica of dest_tablet
        snapshot_meta->tablet_meta().set_tablet_id(dest_tablet->tablet_id());
        snapshot_meta->tablet_meta().set_schema_hash(dest_tablet->schema_hash());
        for (auto& rm : snapshot_meta->rowset_metas()) {
            rm.set_tablet_id(dest_tablet->tablet_id());
        }

        st = dest_tablet->updates()->load_snapshot(*snapshot_meta);
        dest_tablet->updates()->remove_expired_versions(time(nullptr));
        return st;
    }

    static StatusOr<TabletSharedPtr> clone_a_new_replica(const TabletSharedPtr& source_tablet, int64_t new_tablet_id) {
        auto clone_version = source_tablet->max_version().second;
        auto snapshot_dir = SnapshotManager::instance()->snapshot_full(source_tablet, clone_version, 3600);
        CHECK(snapshot_dir.ok()) << snapshot_dir.status();

        DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

        auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(source_tablet, *snapshot_dir);
        auto meta_file = meta_dir + "/meta";
        auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_file);
        CHECK(snapshot_meta.ok()) << snapshot_meta.status();

        // Assign a new tablet_id and overwrite the meta file.
        snapshot_meta->tablet_meta().set_tablet_id(new_tablet_id);
        CHECK(snapshot_meta->serialize_to_file(meta_file).ok());

        RETURN_IF_ERROR(SnapshotManager::instance()->assign_new_rowset_id(&(*snapshot_meta), meta_dir));

        auto store = source_tablet->data_dir();
        auto new_schema_hash = source_tablet->schema_hash();
        std::string new_tablet_path = store->path() + DATA_PREFIX;
        new_tablet_path = path_util::join_path_segments(new_tablet_path, std::to_string(source_tablet->shard_id()));
        new_tablet_path = path_util::join_path_segments(new_tablet_path, std::to_string(new_tablet_id));
        new_tablet_path = path_util::join_path_segments(new_tablet_path, std::to_string(new_schema_hash));
        CHECK(std::filesystem::create_directories(new_tablet_path));

        std::set<std::string> files;
        CHECK(fs::list_dirs_files(meta_dir, nullptr, &files).ok());
        for (const auto& f : files) {
            std::string src = meta_dir + "/" + f;
            std::string dst = new_tablet_path + "/" + f;
            Status st = FileSystem::Default()->link_file(src, dst);
            if (st.ok()) {
                LOG(INFO) << "Linked " << src << " to " << dst;
            } else if (st.is_already_exist()) {
                LOG(INFO) << dst << " already exist";
            } else {
                return st;
            }
        }

        auto tablet_manager = StorageEngine::instance()->tablet_manager();
        auto st = tablet_manager->create_tablet_from_meta_snapshot(store, new_tablet_id, new_schema_hash,
                                                                   new_tablet_path);
        CHECK(st.ok()) << st;
        return tablet_manager->get_tablet(new_tablet_id, false);
    }

    void test_writeread(bool enable_persistent_index);
    void test_writeread_with_delete(bool enable_persistent_index);
    void test_noncontinous_commit(bool enable_persistent_index);
    void test_noncontinous_meta_save_load(bool enable_persistent_index);
    void test_save_meta(bool enable_persistent_index);
    void test_load_from_pb(bool enable_persistent_index);
    void test_remove_expired_versions(bool enable_persistent_index);
    void test_apply(bool enable_persistent_index, bool has_merge_condition);
    void test_concurrent_write_read_and_gc(bool enable_persistent_index);
    void test_compaction_score_not_enough(bool enable_persistent_index);
    void test_compaction_score_enough_duplicate(bool enable_persistent_index);
    void test_compaction_score_enough_normal(bool enable_persistent_index);
    void test_horizontal_compaction(bool enable_persistent_index);
    void test_vertical_compaction(bool enable_persistent_index);
    void test_compaction_with_empty_rowset(bool enable_persistent_index, bool vertical, bool multi_column_pk);
    void test_link_from(bool enable_persistent_index);
    void test_convert_from(bool enable_persistent_index);
    void test_convert_from_with_pending(bool enable_persistent_index);
    void test_convert_from_with_mutiple_segment(bool enable_persistent_index);
    void test_reorder_from(bool enable_persistent_index);
    void test_load_snapshot_incremental(bool enable_persistent_index);
    void test_load_snapshot_incremental_ignore_already_committed_version(bool enable_persistent_index);
    void test_load_snapshot_incremental_mismatched_tablet_id(bool enable_persistent_index);
    void test_load_snapshot_incremental_data_file_not_exist(bool enable_persistent_index);
    void test_load_snapshot_incremental_incorrect_version(bool enable_persistent_index);
    void test_load_snapshot_incremental_with_partial_rowset_old(bool enable_persistent_index);
    void test_load_snapshot_incremental_with_partial_rowset_new(bool enable_persistent_index,
                                                                PartialUpdateCloneCase update_case);
    void test_load_snapshot_primary(int64_t num_version, const std::vector<uint64_t>& holes);
    void test_load_snapshot_primary(int64_t max_version, const std::vector<uint64_t>& holes,
                                    bool enable_persistent_index);
    void test_load_snapshot_full(bool enable_persistent_index);
    void test_load_snapshot_full_file_not_exist(bool enable_persistent_index);
    void test_load_snapshot_full_mismatched_tablet_id(bool enable_persistent_index);
    void test_issue_4193(bool enable_persistent_index);
    void test_issue_4181(bool enable_persistent_index);
    void test_snapshot_with_empty_rowset(bool enable_persistent_index);
    void test_get_column_values(bool enable_persistent_index);
    void test_get_missing_version_ranges(const std::vector<int64_t>& versions,
                                         const std::vector<int64_t>& expected_missing_ranges);
    void test_get_rowsets_for_incremental_snapshot(const std::vector<int64_t>& versions,
                                                   const std::vector<int64_t>& missing_ranges,
                                                   const std::vector<int64_t>& expect_rowset_versions, bool gc,
                                                   bool expect_error);

    void tablets_prepare(const TabletSharedPtr& tablet0, const TabletSharedPtr& tablet1,
                         std::vector<int32_t>& column_indexes, const std::shared_ptr<TabletSchema>& partial_schema);
    void snapshot_prepare(const TabletSharedPtr& tablet, const std::vector<int64_t>& delta_versions,
                          std::string* snapshot_id_path, std::string* snapshot_dir,
                          std::vector<RowsetSharedPtr>* snapshot_rowsets,
                          std::vector<RowsetMetaSharedPtr>* snapshot_rowset_metas,
                          const TabletMetaSharedPtr& snapshot_tablet_meta);
    void load_snapshot(const std::string& meta_dir, const TabletSharedPtr& tablet, SegmentFooterPB* footer);

protected:
    TabletSharedPtr _tablet;
    TabletSharedPtr _tablet2;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
};

static TabletSharedPtr load_same_tablet_from_store(const TabletSharedPtr& tablet) {
    auto data_dir = tablet->data_dir();
    auto tablet_id = tablet->tablet_id();
    auto schema_hash = tablet->schema_hash();

    std::string enc_key = strings::Substitute("tabletmeta_$0_$1", tablet_id, schema_hash);
    std::string serialized_meta;
    auto meta = tablet->data_dir()->get_meta();
    auto st = meta->get(META_COLUMN_FAMILY_INDEX, enc_key, &serialized_meta);
    CHECK(st.ok()) << st;

    // Parse tablet meta.
    auto tablet_meta = std::make_shared<TabletMeta>();
    CHECK(tablet_meta->deserialize(serialized_meta).ok());

    // Create a new tablet instance from the latest snapshot.
    auto tablet1 = Tablet::create_tablet_from_meta(tablet_meta, data_dir);
    CHECK(tablet1 != nullptr);
    CHECK(tablet1->init().ok());
    CHECK(tablet1->init_succeeded());
    return tablet1;
}

static vectorized::ChunkIteratorPtr create_tablet_iterator(vectorized::TabletReader& reader,
                                                           vectorized::Schema& schema) {
    vectorized::TabletReaderParams params;
    if (!reader.prepare().ok()) {
        LOG(ERROR) << "reader prepare failed";
        return nullptr;
    }
    std::vector<ChunkIteratorPtr> seg_iters;
    if (!reader.get_segment_iterators(params, &seg_iters).ok()) {
        LOG(ERROR) << "reader get segment iterators fail";
        return nullptr;
    }
    if (seg_iters.empty()) {
        return vectorized::new_empty_iterator(schema, DEFAULT_CHUNK_SIZE);
    }
    return vectorized::new_union_iterator(seg_iters);
}

static ssize_t read_and_compare(const vectorized::ChunkIteratorPtr& iter, const vector<int64_t>& keys) {
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    auto full_chunk = ChunkHelper::new_chunk(iter->schema(), keys.size());
    auto& cols = full_chunk->columns();
    for (long key : keys) {
        cols[0]->append_datum(vectorized::Datum(key));
        cols[1]->append_datum(vectorized::Datum((int16_t)(key % 100 + 1)));
        cols[2]->append_datum(vectorized::Datum((int32_t)(key % 1000 + 2)));
    }
    size_t count = 0;
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            for (auto i = 0; i < chunk->num_rows(); i++) {
                EXPECT_EQ(full_chunk->get(count + i).compare(iter->schema(), chunk->get(i)), 0);
            }
            count += chunk->num_rows();
            chunk->reset();
        } else {
            return -1;
        }
    }
    return count;
}

static ssize_t read_until_eof(const vectorized::ChunkIteratorPtr& iter) {
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    size_t count = 0;
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            count += chunk->num_rows();
            chunk->reset();
        } else {
            LOG(WARNING) << "read error: " << st.to_string();
            return -1;
        }
    }
    return count;
}

static Status read_with_cancel(const TabletSharedPtr& tablet, int64_t version) {
    vectorized::Schema schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
    vectorized::TabletReader reader(tablet, Version(0, version), schema);
    vectorized::TabletReaderParams params;
    RuntimeState state;
    params.runtime_state = &state;
    RETURN_IF_ERROR(reader.prepare());
    std::vector<ChunkIteratorPtr> seg_iters;
    RETURN_IF_ERROR(reader.get_segment_iterators(params, &seg_iters));
    if (seg_iters.empty()) {
        return Status::OK();
    }
    state.set_is_cancelled(true);
    auto iter = vectorized::new_union_iterator(seg_iters);
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            chunk->reset();
        } else {
            LOG(WARNING) << "read error: " << st.to_string();
            return st;
        }
    }
    return Status::OK();
}

static ssize_t read_tablet(const TabletSharedPtr& tablet, int64_t version) {
    vectorized::Schema schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
    vectorized::TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    return read_until_eof(iter);
}

static ssize_t read_tablet_and_compare(const TabletSharedPtr& tablet, int64_t version, const vector<int64_t>& keys) {
    vectorized::Schema schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
    vectorized::TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    return read_and_compare(iter, keys);
}

static ssize_t read_tablet_and_compare_schema_changed(const TabletSharedPtr& tablet, int64_t version,
                                                      const vector<int64_t>& keys) {
    vectorized::Schema schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
    vectorized::TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    auto full_chunk = ChunkHelper::new_chunk(iter->schema(), keys.size());
    auto& cols = full_chunk->columns();
    for (long key : keys) {
        cols[0]->append_datum(vectorized::Datum((int64_t)key));
        cols[1]->append_datum(vectorized::Datum((int16_t)(key % 100 + 1)));
        auto v = std::to_string((int64_t)(key % 1000 + 2));
        cols[2]->append_datum(vectorized::Datum(Slice{v}));
    }
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    size_t count = 0;
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            for (auto i = 0; i < chunk->num_rows(); i++) {
                EXPECT_EQ(full_chunk->get(count + i).compare(iter->schema(), chunk->get(i)), 0);
            }
            count += chunk->num_rows();
            chunk->reset();
        } else {
            return -1;
        }
    }
    return count;
}

static ssize_t read_tablet_and_compare_schema_changed_sort_key1(const TabletSharedPtr& tablet, int64_t version,
                                                                const vector<int64_t>& keys) {
    vectorized::Schema schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
    vectorized::TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    const auto nkeys = keys.size();
    auto full_chunk = ChunkHelper::new_chunk(iter->schema(), nkeys);
    auto& cols = full_chunk->columns();
    for (long key : keys) {
        cols[0]->append_datum(vectorized::Datum((int64_t)(nkeys - 1 - key)));
        cols[1]->append_datum(vectorized::Datum((int16_t)key));
        cols[2]->append_datum(vectorized::Datum((int32_t)(nkeys - 1 - key)));
    }
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    size_t count = 0;
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            for (auto i = 0; i < chunk->num_rows(); i++) {
                EXPECT_EQ(full_chunk->get(count + i).compare(iter->schema(), chunk->get(i)), 0);
            }
            count += chunk->num_rows();
            chunk->reset();
        } else {
            return -1;
        }
    }
    return count;
}

static ssize_t read_tablet_and_compare_schema_changed_sort_key2(const TabletSharedPtr& tablet, int64_t version,
                                                                const vector<int64_t>& keys) {
    vectorized::Schema schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
    vectorized::TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    const auto nkeys = keys.size();
    auto full_chunk = ChunkHelper::new_chunk(iter->schema(), nkeys);
    auto& cols = full_chunk->columns();
    for (long key : keys) {
        cols[0]->append_datum(vectorized::Datum((int64_t)key));
        cols[1]->append_datum(vectorized::Datum((int16_t)(nkeys - 1 - key)));
        cols[2]->append_datum(vectorized::Datum((int32_t)key));
    }
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    size_t count = 0;
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            for (auto i = 0; i < chunk->num_rows(); i++) {
                EXPECT_EQ(full_chunk->get(count + i).compare(iter->schema(), chunk->get(i)), 0);
            }
            count += chunk->num_rows();
            chunk->reset();
        } else {
            return -1;
        }
    }
    return count;
}

static ssize_t read_tablet_and_compare_sort_key_error_encode_case(const TabletSharedPtr& tablet, int64_t version,
                                                                  const vector<int64_t>& keys) {
    vectorized::Schema schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
    vectorized::TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    auto full_chunk = ChunkHelper::new_chunk(iter->schema(), keys.size());
    auto& cols = full_chunk->columns();
    for (auto i = 0; i < keys.size(); ++i) {
        cols[0]->append_datum(vectorized::Datum((int64_t)keys[i]));
        cols[1]->append_datum(vectorized::Datum((int16_t)1));
        cols[2]->append_datum(vectorized::Datum((int32_t)i));
    }
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    size_t count = 0;
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            for (auto i = 0; i < chunk->num_rows(); i++) {
                EXPECT_EQ(full_chunk->get(count + i).compare(iter->schema(), chunk->get(i)), 0);
            }
            count += chunk->num_rows();
            chunk->reset();
        } else {
            return -1;
        }
    }
    return count;
}

void TabletUpdatesTest::test_writeread(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    // write
    const int N = 8000;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    auto rs0 = create_rowset(_tablet, keys);
    ASSERT_TRUE(_tablet->rowset_commit(2, rs0).ok());
    ASSERT_EQ(2, _tablet->updates()->max_version());
    auto rs1 = create_rowset(_tablet, keys);
    ASSERT_TRUE(_tablet->rowset_commit(3, rs1).ok());
    ASSERT_EQ(3, _tablet->updates()->max_version());
    auto rs2 = create_rowset(_tablet, keys, nullptr, true);
    ASSERT_TRUE(_tablet->rowset_commit(4, rs2).ok());
    ASSERT_EQ(4, _tablet->updates()->max_version());

    // read
    ASSERT_EQ(N, read_tablet(_tablet, 4));
    ASSERT_EQ(N, read_tablet(_tablet, 3));
    ASSERT_EQ(N, read_tablet(_tablet, 2));
    ASSERT_TRUE(read_with_cancel(_tablet, 4).is_cancelled());
}

TEST_F(TabletUpdatesTest, writeread) {
    test_writeread(false);
}

TEST_F(TabletUpdatesTest, writeread_with_persistent_index) {
    test_writeread(true);
}

TEST_F(TabletUpdatesTest, writeread_with_sort_key) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet_with_sort_key(rand(), rand(), {1});
    // write
    const int N = 8000;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    auto rs0 = create_rowset(_tablet, keys);
    ASSERT_TRUE(_tablet->rowset_commit(2, rs0).ok());
    ASSERT_EQ(2, _tablet->updates()->max_version());
    auto rs1 = create_rowset(_tablet, keys);
    ASSERT_TRUE(_tablet->rowset_commit(3, rs1).ok());
    ASSERT_EQ(3, _tablet->updates()->max_version());
    auto rs2 = create_rowset(_tablet, keys, nullptr, true);
    ASSERT_TRUE(_tablet->rowset_commit(4, rs2).ok());
    ASSERT_EQ(4, _tablet->updates()->max_version());

    // read
    ASSERT_EQ(N, read_tablet(_tablet, 4));
    ASSERT_EQ(N, read_tablet(_tablet, 3));
    ASSERT_EQ(N, read_tablet(_tablet, 2));
}

void TabletUpdatesTest::test_writeread_with_delete(bool enable_persistent_index) {
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    // write
    const int N = 8000;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    // Insert [0, 1, 2 ... N)
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    ASSERT_EQ(2, _tablet->updates()->max_version());

    // Delete [0, 1, 2 ... N/2)
    vectorized::Int64Column deletes;
    deletes.append_numbers(keys.data(), sizeof(int64_t) * keys.size() / 2);
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, {}, &deletes)).ok());
    ASSERT_EQ(3, _tablet->updates()->max_version());
    ASSERT_EQ(N / 2, read_tablet(_tablet, 3));

    // Delete [0, 1, 2 ... N) and insert [N, N+1, N+2 ... 2*N)
    deletes.resize(0);
    deletes.append_numbers(keys.data(), sizeof(int64_t) * keys.size());
    for (int i = 0; i < N; i++) {
        keys[i] = N + i;
    }
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys, &deletes)).ok());
    ASSERT_EQ(4, _tablet->updates()->max_version());
    ASSERT_EQ(N, read_tablet(_tablet, 4));
}

TEST_F(TabletUpdatesTest, writeread_with_delete) {
    test_writeread_with_delete(false);
}

TEST_F(TabletUpdatesTest, writeread_with_delete_with_persistent_index) {
    test_writeread_with_delete(true);
}

TEST_F(TabletUpdatesTest, writeread_with_delete_with_sort_key) {
    _tablet = create_tablet_with_sort_key(rand(), rand(), {1});
    // write
    const int N = 8000;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    // Insert [0, 1, 2 ... N)
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    ASSERT_EQ(2, _tablet->updates()->max_version());

    // Delete [0, 1, 2 ... N/2)
    vectorized::Int64Column deletes;
    deletes.append_numbers(keys.data(), sizeof(int64_t) * keys.size() / 2);
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, {}, &deletes)).ok());
    ASSERT_EQ(3, _tablet->updates()->max_version());
    ASSERT_EQ(N / 2, read_tablet(_tablet, 3));

    // Delete [0, 1, 2 ... N) and insert [N, N+1, N+2 ... 2*N)
    deletes.resize(0);
    deletes.append_numbers(keys.data(), sizeof(int64_t) * keys.size());
    for (int i = 0; i < N; i++) {
        keys[i] = N + i;
    }
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys, &deletes)).ok());
    ASSERT_EQ(4, _tablet->updates()->max_version());
    ASSERT_EQ(N, read_tablet(_tablet, 4));
}

TEST_F(TabletUpdatesTest, writeread_with_overlapping_deletes_only_batches) {
    _tablet = create_tablet(rand(), rand());

    std::vector<int64_t> keys;

    const int N = 8000;

    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    // Insert [0, 1, 2 ... N)
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());

    for (int i = 0; i < N; i++) {
        keys[i] = N + i;
    }
    // Insert [N, N + 1, N + 2 ... 2N)
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());

    for (int i = 0; i < N; i++) {
        keys[i] = 2 * N + i;
    }
    // Insert [2N, 2N + 1, 2N + 2 ... 3N)
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());

    vectorized::Int64Column deletes;
    for (int i = N / 2; i < N + N / 2; i++) {
        deletes.append(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(5, create_rowset(_tablet, {}, &deletes)).ok());

    deletes.resize(0);
    for (int i = N; i < 2 * N; i++) {
        deletes.append(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(6, create_rowset(_tablet, {}, &deletes)).ok());

    deletes.resize(0);
    for (int i = N + N / 2; i < 2 * N + N / 2; i++) {
        deletes.append(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(7, create_rowset(_tablet, {}, &deletes)).ok());

    ASSERT_EQ(N, read_tablet(_tablet, 7));
}

void TabletUpdatesTest::test_noncontinous_commit(bool enable_persistent_index) {
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    const int N = 100;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    ASSERT_EQ(2, _tablet->updates()->max_version());

    ASSERT_TRUE(_tablet->rowset_commit(5, create_rowset(_tablet, keys)).ok());
    ASSERT_EQ(2, _tablet->updates()->max_version());

    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());
    ASSERT_EQ(2, _tablet->updates()->max_version());

    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());
    ASSERT_EQ(5, _tablet->updates()->max_version());
}

TEST_F(TabletUpdatesTest, noncontinous_commit) {
    test_noncontinous_commit(false);
}

TEST_F(TabletUpdatesTest, noncontinous_commit_with_persistent_index) {
    test_noncontinous_commit(true);
}

void TabletUpdatesTest::test_noncontinous_meta_save_load(bool enable_persistent_index) {
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    const int N = 100;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    ASSERT_EQ(2, _tablet->updates()->max_version());

    ASSERT_TRUE(_tablet->rowset_commit(5, create_rowset(_tablet, keys)).ok());
    ASSERT_EQ(2, _tablet->updates()->max_version());

    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());
    ASSERT_EQ(2, _tablet->updates()->max_version());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    _tablet->save_meta();

    auto tablet1 = load_same_tablet_from_store(_tablet);

    ASSERT_EQ(2, tablet1->updates()->num_pending());
    ASSERT_EQ(2, tablet1->updates()->max_version());
}

TEST_F(TabletUpdatesTest, noncontinous_meta_save_load) {
    test_noncontinous_meta_save_load(false);
}

TEST_F(TabletUpdatesTest, noncontinous_meta_save_load_with_persistent_index) {
    test_noncontinous_commit(true);
}

void TabletUpdatesTest::test_save_meta(bool enable_persistent_index) {
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);

    // Prepare records for test.
    const int N = 10;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    for (int i = 0; i < 30; i++) {
        std::cout << "rowset " << i << std::endl;
        ASSERT_TRUE(_tablet->rowset_commit(i + 2, create_rowset(_tablet, keys)).ok());
    }
    ASSERT_EQ(31, _tablet->updates()->version_history_count());
    ASSERT_EQ(31, _tablet->updates()->max_version());

    // Read from the latest version, this can ensure that all versions are applied.
    ASSERT_EQ(N, read_tablet(_tablet, 31));
    ASSERT_EQ(N, read_tablet(_tablet, 16));
    ASSERT_EQ(N, read_tablet(_tablet, 2));

    _tablet->save_meta();

    auto tablet1 = load_same_tablet_from_store(_tablet);
    ASSERT_EQ(31, tablet1->updates()->version_history_count());
    ASSERT_EQ(31, tablet1->updates()->max_version());

    // Ensure that all meta logs have been erased.
    size_t log_count = 0;
    auto apply_log_func = [&](uint64_t logid, const TabletMetaLogPB& log) -> bool {
        log_count++;
        std::cout << log.DebugString() << std::endl;
        return true;
    };
    auto status = TabletMetaManager::traverse_meta_logs(_tablet->data_dir(), _tablet->tablet_id(), apply_log_func);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(0, log_count);

    // Ensure we can read all records from the latest version.
    ASSERT_EQ(N, read_tablet(tablet1, 30));
    ASSERT_EQ(N, read_tablet(tablet1, 10));
    ASSERT_EQ(N, read_tablet(tablet1, 2));
}

TEST_F(TabletUpdatesTest, save_meta) {
    test_save_meta(false);
}

TEST_F(TabletUpdatesTest, save_meta_with_persistent_index) {
    test_save_meta(true);
}

void TabletUpdatesTest::test_load_from_pb(bool enable_persistent_index) {
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);

    // Prepare records for test
    const int N = 30;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());

    {
        const int N = 10;
        std::vector<int64_t> keys;
        for (int64_t i = 0; i < N; i++) {
            keys.emplace_back(i);
        }
        vectorized::Int64Column deletes_1;
        deletes_1.append_numbers(keys.data(), sizeof(int64_t) * 5);
        ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys, &deletes_1)).ok());

        keys.clear();
        for (int64_t i = 0; i < N; i++) {
            keys.emplace_back(i + 10);
        }
        vectorized::Int64Column deletes_2;
        deletes_2.append_numbers(keys.data(), sizeof(int64_t) * 5);
        ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys, &deletes_2)).ok());

        ASSERT_EQ(4, _tablet->updates()->version_history_count());
        ASSERT_EQ(4, _tablet->updates()->max_version());

        ASSERT_EQ(30, read_tablet(_tablet, 2));
        ASSERT_EQ(25, read_tablet(_tablet, 3));
        ASSERT_EQ(20, read_tablet(_tablet, 4));

        _tablet->save_meta();
    }

    {
        auto tablet1 = load_same_tablet_from_store(_tablet);
        ASSERT_EQ(4, tablet1->updates()->version_history_count());
        ASSERT_EQ(4, tablet1->updates()->max_version());

        ASSERT_EQ(30, read_tablet(tablet1, 2));
        ASSERT_EQ(25, read_tablet(tablet1, 3));
        ASSERT_EQ(20, read_tablet(tablet1, 4));
    }
}

TEST_F(TabletUpdatesTest, load_from_pb) {
    test_load_from_pb(false);
}

TEST_F(TabletUpdatesTest, load_from_pb_with_persistent_index) {
    test_load_from_pb(true);
}

void TabletUpdatesTest::test_remove_expired_versions(bool enable_persistent_index) {
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);

    // Prepare records for test.
    const int N = 100;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());
    ASSERT_EQ(4, _tablet->updates()->version_history_count());
    ASSERT_EQ(4, _tablet->updates()->max_version());

    // Read from the latest version, this can ensure that all versions are applied.
    ASSERT_EQ(N, read_tablet(_tablet, 4));
    ASSERT_EQ(N, read_tablet(_tablet, 3));
    ASSERT_EQ(N, read_tablet(_tablet, 2));
    ASSERT_EQ(0, read_tablet(_tablet, 1));

    // Create iterators before remove expired version, but read them after removal.
    vectorized::Schema schema = ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema());
    vectorized::TabletReader reader1(_tablet, Version(0, 1), schema);
    vectorized::TabletReader reader2(_tablet, Version(0, 2), schema);
    vectorized::TabletReader reader3(_tablet, Version(0, 3), schema);
    vectorized::TabletReader reader4(_tablet, Version(0, 4), schema);
    auto iter_v1 = create_tablet_iterator(reader1, schema);
    auto iter_v2 = create_tablet_iterator(reader2, schema);
    auto iter_v3 = create_tablet_iterator(reader3, schema);
    auto iter_v4 = create_tablet_iterator(reader4, schema);

    // Remove all but the last version.
    _tablet->updates()->remove_expired_versions(time(nullptr));
    ASSERT_EQ(1, _tablet->updates()->version_history_count());
    ASSERT_EQ(4, _tablet->updates()->max_version());

    EXPECT_EQ(N, read_tablet(_tablet, 4));
    EXPECT_EQ(N, read_until_eof(iter_v4));
    EXPECT_EQ(0, read_until_eof(iter_v1)); // iter_v1 is empty iterator

    // read already opened iterator/reader should succeed
    EXPECT_EQ(N, read_until_eof(iter_v3));
    EXPECT_EQ(N, read_until_eof(iter_v2));
    // Read expired versions should fail.
    EXPECT_EQ(-1, read_tablet(_tablet, 3));
    EXPECT_EQ(-1, read_tablet(_tablet, 2));
    EXPECT_EQ(-1, read_tablet(_tablet, 1));

    auto tablet1 = load_same_tablet_from_store(_tablet);
    EXPECT_EQ(1, tablet1->updates()->version_history_count());
    EXPECT_EQ(4, tablet1->updates()->max_version());
    EXPECT_EQ(N, read_tablet(tablet1, 4));
    EXPECT_EQ(-1, read_tablet(tablet1, 3));
    EXPECT_EQ(-1, read_tablet(tablet1, 2));
    EXPECT_EQ(-1, read_tablet(tablet1, 1));
}

TEST_F(TabletUpdatesTest, remove_expired_versions) {
    test_remove_expired_versions(false);
}

TEST_F(TabletUpdatesTest, remove_expired_versions_with_persistent_index) {
    test_remove_expired_versions(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_apply(bool enable_persistent_index, bool has_merge_condition = false) {
    const int N = 10;
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    ASSERT_EQ(1, _tablet->updates()->version_history_count());

    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(64);
    for (int i = 0; i < 64; i++) {
        rowsets.emplace_back(create_rowset(_tablet, keys, nullptr, false, has_merge_condition));
    }
    auto pool = StorageEngine::instance()->update_manager()->apply_thread_pool();
    for (int i = 0; i < rowsets.size(); i++) {
        auto version = i + 2;
        auto st = _tablet->rowset_commit(version, rowsets[i]);
        ASSERT_TRUE(st.ok()) << st.to_string();
        // Ensure that there is at most one thread doing the version apply job.
        ASSERT_LE(pool->num_threads(), 1);
        ASSERT_EQ(version, _tablet->updates()->max_version());
        ASSERT_EQ(version, _tablet->updates()->version_history_count());
    }
    ASSERT_EQ(N, read_tablet(_tablet, rowsets.size() + 1));

    // Ensure the persistent meta is correct.
    auto max_version = rowsets.size() + 1;
    auto tablet1 = load_same_tablet_from_store(_tablet);
    // `enable_persistent_index` is not persistent in this case
    // so we reset the `enable_persistent_index` after load
    tablet1->set_enable_persistent_index(enable_persistent_index);
    EXPECT_EQ(max_version, tablet1->updates()->max_version());
    EXPECT_EQ(max_version, tablet1->updates()->version_history_count());
    for (int i = 2; i <= max_version; i++) {
        ASSERT_EQ(N, read_tablet(_tablet, i));
    }
}

TEST_F(TabletUpdatesTest, apply) {
    test_apply(false);
}

TEST_F(TabletUpdatesTest, apply_with_persistent_index) {
    test_apply(true);
}

TEST_F(TabletUpdatesTest, apply_with_merge_condition) {
    test_apply(false, true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_concurrent_write_read_and_gc(bool enable_persistent_index) {
    const int N = 2000;
    std::atomic<bool> started{false};
    std::atomic<bool> stopped{false};
    std::atomic<int64_t> version{1};
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);

    auto wait_start = [&]() {
        while (!started) {
            sched_yield();
        }
    };

    auto rowset_commit_thread = [&]() {
        std::vector<int64_t> keys(N);
        for (int i = 0; i < N; i++) {
            keys[i] = i;
        }
        wait_start();
        while (!stopped) {
            ASSERT_TRUE(_tablet->rowset_commit(1 + version.load(), create_rowset(_tablet, keys)).ok());
            version.fetch_add(1);
        }
    };

    auto version_gc_thread = [&]() {
        wait_start();
        while (!stopped) {
            _tablet->updates()->remove_expired_versions(time(nullptr));
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    };

    auto read_thread = [&]() {
        wait_start();
        while (!stopped) {
            ssize_t ret = read_tablet(_tablet, std::max<int64_t>(2, version.load()));
            ASSERT_TRUE(ret == -1 || ret == N) << ret;
        }
    };

    std::vector<std::thread> threads;
    threads.emplace_back(rowset_commit_thread);
    for (int i = 0; i < 10; i++) {
        threads.emplace_back(read_thread);
    }
    threads.emplace_back(version_gc_thread);
    started.store(true);
    std::this_thread::sleep_for(std::chrono::seconds(5));
    while (version.load() < 100) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
    stopped.store(true);
    for (auto& t : threads) {
        t.join();
    }
    std::cout << "version count=" << version.load() << std::endl;
    EXPECT_EQ(N, read_tablet(_tablet, version.load()));
    _tablet->updates()->remove_expired_versions(time(nullptr));
    EXPECT_EQ(1, _tablet->updates()->version_history_count());
    EXPECT_EQ(version.load(), _tablet->updates()->max_version());

    // Ensure the persistent meta is correct.
    auto tablet1 = load_same_tablet_from_store(_tablet);
    EXPECT_EQ(1, tablet1->updates()->version_history_count());
    EXPECT_EQ(version.load(), tablet1->updates()->max_version());
    EXPECT_EQ(N, read_tablet(tablet1, version.load()));
}

TEST_F(TabletUpdatesTest, concurrent_write_read_and_gc) {
    test_concurrent_write_read_and_gc(false);
}

TEST_F(TabletUpdatesTest, concurrent_write_read_and_gc_with_persistent_index) {
    test_concurrent_write_read_and_gc(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_compaction_score_not_enough(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    std::vector<int64_t> keys;
    for (int i = 0; i < 100; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const auto& best_tablet =
            StorageEngine::instance()->tablet_manager()->find_best_tablet_to_do_update_compaction(_tablet->data_dir());
    EXPECT_EQ(best_tablet, nullptr);
    // the compaction score is not enough due to the enough rows and lacking deletion.
    EXPECT_LT(_tablet->updates()->get_compaction_score(), 0);
}

TEST_F(TabletUpdatesTest, compaction_score_not_enough) {
    test_compaction_score_not_enough(false);
}

TEST_F(TabletUpdatesTest, compaction_score_not_enough_with_persistent_index) {
    test_compaction_score_not_enough(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_compaction_score_enough_duplicate(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    std::vector<int64_t> keys;
    for (int i = 0; i < 100; i++) {
        keys.push_back(i);
    }
    // Delete [0, 1, 2 ... 86)
    vectorized::Int64Column deletes;
    deletes.append_numbers(keys.data(), sizeof(int64_t) * 86);
    // This (keys and deletes has duplicate keys) is illegal and won't happen in real world
    // but currently underlying implementation still support this, so we test this case anyway
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys, &deletes)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    const auto& best_tablet =
            StorageEngine::instance()->tablet_manager()->find_best_tablet_to_do_update_compaction(_tablet->data_dir());
    EXPECT_NE(best_tablet, nullptr);
    // the compaction score is enough due to the enough deletion.
    EXPECT_GT(_tablet->updates()->get_compaction_score(), 0);
}

TEST_F(TabletUpdatesTest, compaction_score_enough_duplicate) {
    test_compaction_score_enough_duplicate(false);
}

TEST_F(TabletUpdatesTest, compaction_score_enough_duplicate_with_persistent_index) {
    test_compaction_score_enough_duplicate(true);
}

void TabletUpdatesTest::test_compaction_score_enough_normal(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    std::vector<int64_t> keys;
    for (int i = 0; i < 100; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    // Delete [0, 1, 2 ... 86)
    vectorized::Int64Column deletes;
    deletes.append_numbers(keys.data(), sizeof(int64_t) * 86);
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, {}, &deletes)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    const auto& best_tablet =
            StorageEngine::instance()->tablet_manager()->find_best_tablet_to_do_update_compaction(_tablet->data_dir());
    EXPECT_NE(best_tablet, nullptr);
    // the compaction score is enough due to the enough deletion.
    EXPECT_GT(_tablet->updates()->get_compaction_score(), 0);
}

TEST_F(TabletUpdatesTest, compaction_score_enough_normal) {
    test_compaction_score_enough_normal(false);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_horizontal_compaction(bool enable_persistent_index) {
    auto orig = config::vertical_compaction_max_columns_per_group;
    config::vertical_compaction_max_columns_per_group = 5;
    DeferOp unset_config([&] { config::vertical_compaction_max_columns_per_group = orig; });

    int N = 100;
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_EQ(_tablet->updates()->version_history_count(), 4);
    ASSERT_EQ(N, read_tablet(_tablet, 4));
    const auto& best_tablet =
            StorageEngine::instance()->tablet_manager()->find_best_tablet_to_do_update_compaction(_tablet->data_dir());
    EXPECT_EQ(best_tablet->tablet_id(), _tablet->tablet_id());
    EXPECT_GT(best_tablet->updates()->get_compaction_score(), 0);
    ASSERT_TRUE(best_tablet->updates()->compaction(_compaction_mem_tracker.get()).ok());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(100, read_tablet_and_compare(best_tablet, 3, keys));
    ASSERT_EQ(best_tablet->updates()->num_rowsets(), 1);
    ASSERT_EQ(best_tablet->updates()->version_history_count(), 5);
    // the time interval is not enough after last compaction
    EXPECT_EQ(best_tablet->updates()->get_compaction_score(), -1);
}

TEST_F(TabletUpdatesTest, horizontal_compaction) {
    test_horizontal_compaction(false);
}

TEST_F(TabletUpdatesTest, horizontal_compaction_with_persistent_index) {
    test_horizontal_compaction(true);
}

TEST_F(TabletUpdatesTest, horizontal_compaction_with_sort_key) {
    auto orig = config::vertical_compaction_max_columns_per_group;
    config::vertical_compaction_max_columns_per_group = 5;
    DeferOp unset_config([&] { config::vertical_compaction_max_columns_per_group = orig; });

    int N = 100;
    int loop = 4;
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet_with_sort_key(rand(), rand(), {1, 2});

    std::vector<int64_t> sorted_keys;
    for (int i = 0; i < 100; i++) {
        for (int j = 0; j < loop; j++) {
            sorted_keys.emplace_back(100 * j + i);
        }
    }
    for (int i = 0; i < loop; i++) {
        std::vector<int64_t> keys;
        for (int j = 0; j < N; j++) {
            keys.push_back(i * 100 + j);
        }
        ASSERT_TRUE(_tablet->rowset_commit(2 + i, create_rowset(_tablet, keys)).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    ASSERT_EQ(N * loop, read_tablet(_tablet, loop + 1));
    const auto& best_tablet =
            StorageEngine::instance()->tablet_manager()->find_best_tablet_to_do_update_compaction(_tablet->data_dir());
    EXPECT_EQ(best_tablet->tablet_id(), _tablet->tablet_id());
    EXPECT_GT(best_tablet->updates()->get_compaction_score(), 0);
    ASSERT_TRUE(best_tablet->updates()->compaction(_compaction_mem_tracker.get()).ok());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(N * loop, read_tablet_and_compare(best_tablet, loop + 1, sorted_keys));
    ASSERT_EQ(best_tablet->updates()->num_rowsets(), 1);
    ASSERT_EQ(best_tablet->updates()->version_history_count(), loop + 2);
    // the time interval is not enough after last compaction
    EXPECT_EQ(best_tablet->updates()->get_compaction_score(), -1);

    auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    auto sk_chunk = ChunkHelper::new_chunk(schema, loop);
    auto& cols = sk_chunk->columns();
    for (int i = 0; i < loop; i++) {
        int64_t key = sorted_keys[i * 100];
        cols[0]->append_datum(vectorized::Datum(key));
        cols[1]->append_datum(vectorized::Datum((int16_t)(key % 100 + 1)));
        cols[2]->append_datum(vectorized::Datum((int32_t)(key % 1000 + 2)));
    }
    std::vector<RowsetSharedPtr> rowsets;
    ASSERT_TRUE(_tablet->updates()->get_applied_rowsets(loop + 1, &rowsets).ok());
    std::vector<std::string> sk_index_values;
    for (auto& rowset : rowsets) {
        ASSERT_TRUE(rowset->get_segment_sk_index(&sk_index_values).ok());
    }
    ASSERT_EQ(sk_index_values.size(), loop);
    size_t keys = _tablet->tablet_schema().num_short_key_columns();
    for (size_t i = 0; i < loop; i++) {
        vectorized::SeekTuple tuple(schema, sk_chunk->get(i).datums());
        std::string encoded_key = tuple.short_key_encode(keys, {1, 2}, 0);
        ASSERT_EQ(encoded_key, sk_index_values[i]);
    }
}

TEST_F(TabletUpdatesTest, horizontal_compaction_with_sort_key_error_encode_case) {
    auto orig = config::vertical_compaction_max_columns_per_group;
    config::vertical_compaction_max_columns_per_group = 5;
    DeferOp unset_config([&] { config::vertical_compaction_max_columns_per_group = orig; });

    srand(GetCurrentTimeMicros());
    _tablet = create_tablet_with_sort_key(rand(), rand(), {1, 2});
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset_sort_key_error_encode_case(_tablet, {4, 3, 2, 1, 0})).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset_sort_key_error_encode_case(_tablet, {9, 8, 7, 6, 5})).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_EQ(_tablet->updates()->version_history_count(), 3);
    ASSERT_EQ(10, read_tablet(_tablet, 3));
    const auto& best_tablet =
            StorageEngine::instance()->tablet_manager()->find_best_tablet_to_do_update_compaction(_tablet->data_dir());
    EXPECT_EQ(best_tablet->tablet_id(), _tablet->tablet_id());
    EXPECT_GT(best_tablet->updates()->get_compaction_score(), 0);
    ASSERT_TRUE(best_tablet->updates()->compaction(_compaction_mem_tracker.get()).ok());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(10, read_tablet_and_compare_sort_key_error_encode_case(best_tablet, 3, {4, 3, 2, 1, 0, 9, 8, 7, 6, 5}));
    ASSERT_EQ(best_tablet->updates()->num_rowsets(), 1);
    ASSERT_EQ(best_tablet->updates()->version_history_count(), 4);
    // the time interval is not enough after last compaction
    EXPECT_EQ(best_tablet->updates()->get_compaction_score(), -1);
}

void TabletUpdatesTest::test_vertical_compaction(bool enable_persistent_index) {
    auto orig = config::vertical_compaction_max_columns_per_group;
    config::vertical_compaction_max_columns_per_group = 1;
    DeferOp unset_config([&] { config::vertical_compaction_max_columns_per_group = orig; });

    int N = 100;
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_EQ(_tablet->updates()->version_history_count(), 4);
    ASSERT_EQ(N, read_tablet(_tablet, 4));
    const auto& best_tablet =
            StorageEngine::instance()->tablet_manager()->find_best_tablet_to_do_update_compaction(_tablet->data_dir());
    EXPECT_EQ(best_tablet->tablet_id(), _tablet->tablet_id());
    EXPECT_GT(best_tablet->updates()->get_compaction_score(), 0);
    ASSERT_TRUE(best_tablet->updates()->compaction(_compaction_mem_tracker.get()).ok());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(100, read_tablet_and_compare(best_tablet, 3, keys));
    ASSERT_EQ(best_tablet->updates()->num_rowsets(), 1);
    ASSERT_EQ(best_tablet->updates()->version_history_count(), 5);
    // the time interval is not enough after last compaction
    EXPECT_EQ(best_tablet->updates()->get_compaction_score(), -1);
}

TEST_F(TabletUpdatesTest, vertical_compaction) {
    test_vertical_compaction(false);
}

TEST_F(TabletUpdatesTest, vertical_compaction_with_persistent_index) {
    test_vertical_compaction(true);
}

TEST_F(TabletUpdatesTest, vertical_compaction_with_sort_key) {
    auto orig = config::vertical_compaction_max_columns_per_group;
    config::vertical_compaction_max_columns_per_group = 1;
    DeferOp unset_config([&] { config::vertical_compaction_max_columns_per_group = orig; });

    int N = 100;
    int loop = 4;
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet_with_sort_key(rand(), rand(), {1, 2});
    std::vector<int64_t> sorted_keys;
    for (int i = 0; i < 100; i++) {
        for (int j = 0; j < loop; j++) {
            sorted_keys.emplace_back(100 * j + i);
        }
    }

    for (int i = 0; i < loop; i++) {
        std::vector<int64_t> keys;
        for (int j = 0; j < N; j++) {
            keys.push_back(i * 100 + j);
        }
        ASSERT_TRUE(_tablet->rowset_commit(2 + i, create_rowset(_tablet, keys)).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    ASSERT_EQ(N * loop, read_tablet(_tablet, loop + 1));
    const auto& best_tablet =
            StorageEngine::instance()->tablet_manager()->find_best_tablet_to_do_update_compaction(_tablet->data_dir());
    EXPECT_EQ(best_tablet->tablet_id(), _tablet->tablet_id());
    EXPECT_GT(best_tablet->updates()->get_compaction_score(), 0);
    ASSERT_TRUE(best_tablet->updates()->compaction(_compaction_mem_tracker.get()).ok());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(N * loop, read_tablet_and_compare(best_tablet, loop + 1, sorted_keys));
    ASSERT_EQ(best_tablet->updates()->num_rowsets(), 1);
    ASSERT_EQ(best_tablet->updates()->version_history_count(), loop + 2);
    // the time interval is not enough after last compaction
    EXPECT_EQ(best_tablet->updates()->get_compaction_score(), -1);

    auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    auto sk_chunk = ChunkHelper::new_chunk(schema, loop);
    auto& cols = sk_chunk->columns();
    for (int i = 0; i < loop; i++) {
        int64_t key = sorted_keys[i * 100];
        cols[0]->append_datum(vectorized::Datum(key));
        cols[1]->append_datum(vectorized::Datum((int16_t)(key % 100 + 1)));
        cols[2]->append_datum(vectorized::Datum((int32_t)(key % 1000 + 2)));
    }
    std::vector<RowsetSharedPtr> rowsets;
    ASSERT_TRUE(_tablet->updates()->get_applied_rowsets(loop + 1, &rowsets).ok());
    std::vector<std::string> sk_index_values;
    for (auto& rowset : rowsets) {
        ASSERT_TRUE(rowset->get_segment_sk_index(&sk_index_values).ok());
    }
    ASSERT_EQ(sk_index_values.size(), loop);
    size_t keys = _tablet->tablet_schema().num_short_key_columns();
    for (size_t i = 0; i < loop; i++) {
        vectorized::SeekTuple tuple(schema, sk_chunk->get(i).datums());
        std::string encoded_key = tuple.short_key_encode(keys, {1, 2}, 0);
        ASSERT_EQ(encoded_key, sk_index_values[i]);
    }
}

void TabletUpdatesTest::test_compaction_with_empty_rowset(bool enable_persistent_index, bool vertical,
                                                          bool multi_column_pk) {
    auto orig = config::vertical_compaction_max_columns_per_group;
    config::vertical_compaction_max_columns_per_group = vertical ? 1 : 20;
    DeferOp unset_config([&] { config::vertical_compaction_max_columns_per_group = orig; });
    int N = 10000;
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand(), multi_column_pk);
    _tablet->set_enable_persistent_index(enable_persistent_index);
    std::vector<int64_t> keys2;
    for (int i = 0; i < N; i++) {
        keys2.push_back(i * 3);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys2)).ok());
    std::vector<int64_t> keys3;
    for (int i = 0; i < N; i++) {
        keys3.push_back(i * 3 + 2);
    }
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys3)).ok());

    std::vector<int64_t> keys4;
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys4)).ok());

    std::vector<int64_t> keys5;
    for (int i = 0; i < N; i++) {
        keys5.push_back(i * 3 + 2);
    }
    ASSERT_TRUE(_tablet->rowset_commit(5, create_rowset(_tablet, keys5)).ok());
    {
        // used for wait merely
        std::vector<RowsetSharedPtr> dummy_rowsets;
        ASSERT_TRUE(_tablet->updates()->get_applied_rowsets(5, &dummy_rowsets).ok());
    }
    ASSERT_TRUE(_tablet->updates()->compaction(_compaction_mem_tracker.get()).ok());
    // Wait until compaction applied.
    while (true) {
        std::vector<RowsetSharedPtr> dummy_rowsets;
        EditVersion full_version;
        ASSERT_TRUE(_tablet->updates()->get_applied_rowsets(5, &dummy_rowsets, &full_version).ok());
        if (full_version.minor() == 1) {
            break;
        }
        std::cerr << "waiting for compaction applied\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

TEST_F(TabletUpdatesTest, compaction_with_empty_rowset) {
    test_compaction_with_empty_rowset(false, true, false);
    test_compaction_with_empty_rowset(false, true, true);
    test_compaction_with_empty_rowset(false, false, false);
    test_compaction_with_empty_rowset(false, false, true);
    test_compaction_with_empty_rowset(true, true, false);
    test_compaction_with_empty_rowset(true, true, true);
    test_compaction_with_empty_rowset(true, false, false);
    test_compaction_with_empty_rowset(true, false, true);
}

void TabletUpdatesTest::test_link_from(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet2 = create_tablet2(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    _tablet2->set_enable_persistent_index(enable_persistent_index);
    std::vector<int64_t> keys;
    int N = 100;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    _tablet2->set_tablet_state(TABLET_NOTREADY);
    ASSERT_TRUE(_tablet2->updates()->link_from(_tablet.get(), 4).ok());

    ASSERT_EQ(N, read_tablet(_tablet2, 4));
}

TEST_F(TabletUpdatesTest, link_from) {
    test_link_from(false);
}

TEST_F(TabletUpdatesTest, link_from_with_persistent_index) {
    test_link_from(true);
}

void TabletUpdatesTest::test_convert_from(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    const auto& tablet_to_schema_change = create_tablet_to_schema_change(rand(), rand());
    std::vector<int64_t> keys;
    int N = 100;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());

    tablet_to_schema_change->set_tablet_state(TABLET_NOTREADY);
    auto chunk_changer = std::make_unique<vectorized::ChunkChanger>(tablet_to_schema_change->tablet_schema());
    for (int i = 0; i < tablet_to_schema_change->tablet_schema().num_columns(); ++i) {
        const auto& new_column = tablet_to_schema_change->tablet_schema().column(i);
        int32_t column_index = _tablet->field_index(std::string{new_column.name()});
        auto column_mapping = chunk_changer->get_mutable_column_mapping(i);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
        }
    }
    ASSERT_TRUE(tablet_to_schema_change->updates()->convert_from(_tablet, 4, chunk_changer.get()).ok());

    ASSERT_EQ(N, read_tablet_and_compare_schema_changed(tablet_to_schema_change, 4, keys));
}

void TabletUpdatesTest::test_convert_from_with_pending(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    const auto& tablet_to_schema_change = create_tablet_to_schema_change(rand(), rand());
    int N = 100;
    std::vector<int64_t> keys2;   // [0, 100)
    std::vector<int64_t> keys3;   // [50, 150)
    std::vector<int64_t> keys4;   // [100, 200)
    std::vector<int64_t> allkeys; // [0, 200)
    for (int i = 0; i < N; i++) {
        keys2.push_back(i);
        keys3.push_back(N / 2 + i);
        keys4.push_back(N + i);
        allkeys.push_back(i * 2);
        allkeys.push_back(i * 2 + 1);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys2)).ok());

    tablet_to_schema_change->set_tablet_state(TABLET_NOTREADY);
    auto chunk_changer = std::make_unique<vectorized::ChunkChanger>(tablet_to_schema_change->tablet_schema());
    for (int i = 0; i < tablet_to_schema_change->tablet_schema().num_columns(); ++i) {
        const auto& new_column = tablet_to_schema_change->tablet_schema().column(i);
        int32_t column_index = _tablet->field_index(std::string{new_column.name()});
        auto column_mapping = chunk_changer->get_mutable_column_mapping(i);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
        }
    }
    ASSERT_TRUE(tablet_to_schema_change->rowset_commit(3, create_rowset(tablet_to_schema_change, keys3)).ok());
    ASSERT_TRUE(tablet_to_schema_change->rowset_commit(4, create_rowset(tablet_to_schema_change, keys4)).ok());

    ASSERT_TRUE(tablet_to_schema_change->updates()->convert_from(_tablet, 2, chunk_changer.get()).ok());

    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys3)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys4)).ok());

    ASSERT_EQ(2 * N, read_tablet_and_compare_schema_changed(tablet_to_schema_change, 4, allkeys));
}

void TabletUpdatesTest::test_convert_from_with_mutiple_segment(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    const auto& tablet_to_schema_change = create_tablet_to_schema_change(rand(), rand());
    std::vector<std::vector<int64_t>> keys_by_segment;
    keys_by_segment.resize(2);
    for (int i = 100; i < 200; i++) {
        keys_by_segment[0].emplace_back(i);
    }
    for (int i = 0; i < 100; i++) {
        keys_by_segment[1].emplace_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset_with_mutiple_segments(_tablet, keys_by_segment)).ok());

    tablet_to_schema_change->set_tablet_state(TABLET_NOTREADY);
    auto chunk_changer = std::make_unique<vectorized::ChunkChanger>(tablet_to_schema_change->tablet_schema());
    for (int i = 0; i < tablet_to_schema_change->tablet_schema().num_columns(); ++i) {
        const auto& new_column = tablet_to_schema_change->tablet_schema().column(i);
        int32_t column_index = _tablet->field_index(std::string{new_column.name()});
        auto column_mapping = chunk_changer->get_mutable_column_mapping(i);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
        }
    }
    std::vector<int64_t> ori_keys;
    for (int i = 100; i < 200; i++) {
        ori_keys.emplace_back(i);
    }
    for (int i = 0; i < 100; i++) {
        ori_keys.emplace_back(i);
    }
    ASSERT_EQ(200, read_tablet_and_compare(_tablet, 2, ori_keys));

    ASSERT_TRUE(tablet_to_schema_change->updates()->convert_from(_tablet, 2, chunk_changer.get()).ok());

    std::vector<int64_t> keys;
    for (int i = 0; i < 200; i++) {
        keys.emplace_back(i);
    }
    ASSERT_EQ(200, read_tablet_and_compare_schema_changed(tablet_to_schema_change, 2, keys));
}

TEST_F(TabletUpdatesTest, convert_from) {
    test_convert_from(false);
}

TEST_F(TabletUpdatesTest, convert_from_with_persistent_index) {
    test_convert_from(true);
}

TEST_F(TabletUpdatesTest, convert_from_with_pending) {
    test_convert_from_with_pending(false);
}

TEST_F(TabletUpdatesTest, convert_from_with_pending_and_persistent_index) {
    test_convert_from_with_pending(true);
}

TEST_F(TabletUpdatesTest, convert_from_with_mutiple_segment) {
    test_convert_from_with_mutiple_segment(false);
}

TEST_F(TabletUpdatesTest, convert_from_with_mutiple_segment_with_persistent_index) {
    test_convert_from_with_mutiple_segment(true);
}

void TabletUpdatesTest::test_reorder_from(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet->set_enable_persistent_index(enable_persistent_index);
    const auto& tablet_with_sort_key1 = create_tablet_with_sort_key(rand(), rand(), {1});
    std::vector<int64_t> keys;
    int N = 100;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset_schema_change_sort_key(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset_schema_change_sort_key(_tablet, keys)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset_schema_change_sort_key(_tablet, keys)).ok());

    tablet_with_sort_key1->set_tablet_state(TABLET_NOTREADY);
    ASSERT_TRUE(tablet_with_sort_key1->updates()->reorder_from(_tablet, 4).ok());

    ASSERT_EQ(N, read_tablet_and_compare_schema_changed_sort_key1(tablet_with_sort_key1, 4, keys));

    const auto& tablet_with_sort_key2 = create_tablet_with_sort_key(rand(), rand(), {2});
    tablet_with_sort_key2->set_tablet_state(TABLET_NOTREADY);
    ASSERT_TRUE(tablet_with_sort_key2->updates()->reorder_from(tablet_with_sort_key1, 4).ok());
    ASSERT_EQ(N, read_tablet_and_compare_schema_changed_sort_key2(tablet_with_sort_key2, 4, keys));
}

TEST_F(TabletUpdatesTest, reorder_from) {
    test_reorder_from(false);
}

TEST_F(TabletUpdatesTest, reorder_from_with_persistent_index) {
    test_reorder_from(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_load_snapshot_incremental(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int64_t> keys0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2 /*version*/, create_rowset(tablet0, keys0)).ok());
    }

    std::vector<int64_t> keys1{0, 1, 2, 3};
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tablet1->rowset_commit(i + 2 /*version*/, create_rowset(tablet1, keys1)).ok());
    }

    auto snapshot_dir = SnapshotManager::instance()->snapshot_incremental(tablet0, {4, 5, 6}, 3600);
    ASSERT_TRUE(snapshot_dir.ok()) << snapshot_dir.status();

    DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet1->schema_hash_path() + "/" + f;
        st = FileSystem::Default()->link_file(src, dst);
        ASSERT_TRUE(st.ok()) << st;
        LOG(INFO) << "Linked " << src << " to " << dst;
    }
    // Pretend that tablet0 is a peer replica of tablet1
    snapshot_meta->tablet_meta().set_tablet_id(tablet1->tablet_id());
    snapshot_meta->tablet_meta().set_schema_hash(tablet1->schema_hash());
    for (auto& rm : snapshot_meta->rowset_metas()) {
        rm.set_tablet_id(tablet1->tablet_id());
    }

    st = tablet1->updates()->load_snapshot(*snapshot_meta);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(6, tablet1->updates()->max_version());
    ASSERT_EQ(6, tablet1->updates()->version_history_count());
    EXPECT_EQ(10, read_tablet(tablet1, 6));

    auto tablet2 = load_same_tablet_from_store(tablet1);
    ASSERT_EQ(6, tablet2->updates()->max_version());
    ASSERT_EQ(6, tablet2->updates()->version_history_count());
    EXPECT_EQ(10, read_tablet(tablet2, 6));
}

TEST_F(TabletUpdatesTest, load_snapshot_incremental) {
    test_load_snapshot_incremental(false);
}

TEST_F(TabletUpdatesTest, load_snapshot_incremental_with_persistent_index) {
    test_load_snapshot_incremental(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_load_snapshot_incremental_ignore_already_committed_version(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int64_t> keys0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2 /*version*/, create_rowset(tablet0, keys0)).ok());
    }

    std::vector<int64_t> keys1{0, 1, 2, 3};
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tablet1->rowset_commit(i + 2 /*version*/, create_rowset(tablet1, keys1)).ok());
    }

    auto snapshot_dir = SnapshotManager::instance()->snapshot_incremental(tablet0, {2, 3, 4, 5, 6}, 3600);
    ASSERT_TRUE(snapshot_dir.ok()) << snapshot_dir.status();

    DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet1->schema_hash_path() + "/" + f;
        st = FileSystem::Default()->link_file(src, dst);
        ASSERT_TRUE(st.ok()) << st;
        LOG(INFO) << "Linked " << src << " to " << dst;
    }
    // Pretend that tablet0 is a peer replica of tablet1
    snapshot_meta->tablet_meta().set_tablet_id(tablet1->tablet_id());
    snapshot_meta->tablet_meta().set_schema_hash(tablet1->schema_hash());
    for (auto& rm : snapshot_meta->rowset_metas()) {
        rm.set_tablet_id(tablet1->tablet_id());
    }

    st = tablet1->updates()->load_snapshot(*snapshot_meta);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(6, tablet1->updates()->max_version());
    ASSERT_EQ(6, tablet1->updates()->version_history_count());
    EXPECT_EQ(10, read_tablet(tablet1, 6));

    auto tablet2 = load_same_tablet_from_store(tablet1);
    ASSERT_EQ(6, tablet2->updates()->max_version());
    ASSERT_EQ(6, tablet2->updates()->version_history_count());
    EXPECT_EQ(10, read_tablet(tablet2, 6));
}

TEST_F(TabletUpdatesTest, load_snapshot_incremental_ignore_already_committed_version) {
    test_load_snapshot_incremental_ignore_already_committed_version(false);
}

TEST_F(TabletUpdatesTest, load_snapshot_incremental_ignore_already_committed_version_with_persistent_index) {
    test_load_snapshot_incremental_ignore_already_committed_version(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_load_snapshot_incremental_mismatched_tablet_id(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int64_t> keys0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2 /*version*/, create_rowset(tablet0, keys0)).ok());
    }

    std::vector<int64_t> keys1{0, 1, 2, 3};
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tablet1->rowset_commit(i + 2 /*version*/, create_rowset(tablet1, keys1)).ok());
    }

    auto snapshot_dir = SnapshotManager::instance()->snapshot_incremental(tablet0, {4, 5, 6}, 3600);
    ASSERT_TRUE(snapshot_dir.ok()) << snapshot_dir.status();

    DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet1->schema_hash_path() + "/" + f;
        st = FileSystem::Default()->link_file(src, dst);
        ASSERT_TRUE(st.ok()) << st;
        LOG(INFO) << "Linked " << src << " to " << dst;
    }

    st = tablet1->updates()->load_snapshot(*snapshot_meta);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("mismatched tablet id") != std::string::npos);
}

TEST_F(TabletUpdatesTest, load_snapshot_incremental_mismatched_tablet_id) {
    test_load_snapshot_incremental_mismatched_tablet_id(false);
}

TEST_F(TabletUpdatesTest, load_snapshot_incremental_mismatched_tablet_id_with_persistent_index) {
    test_load_snapshot_incremental_mismatched_tablet_id(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_load_snapshot_incremental_data_file_not_exist(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int64_t> keys0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2 /*version*/, create_rowset(tablet0, keys0)).ok());
    }

    std::vector<int64_t> keys1{0, 1, 2, 3};
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tablet1->rowset_commit(i + 2 /*version*/, create_rowset(tablet1, keys1)).ok());
    }

    auto snapshot_dir = SnapshotManager::instance()->snapshot_incremental(tablet0, {4, 5, 6}, 3600);
    ASSERT_TRUE(snapshot_dir.ok()) << snapshot_dir.status();

    DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    // Pretend that tablet0 is a peer replica of tablet1
    snapshot_meta->tablet_meta().set_tablet_id(tablet1->tablet_id());
    snapshot_meta->tablet_meta().set_schema_hash(tablet1->schema_hash());
    for (auto& rm : snapshot_meta->rowset_metas()) {
        rm.set_tablet_id(tablet1->tablet_id());
    }

    st = tablet1->updates()->load_snapshot(*snapshot_meta);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("segment file does not exist") != std::string::npos);
    ASSERT_EQ(3, tablet1->updates()->max_version());
    ASSERT_EQ(3, tablet1->updates()->version_history_count());
    EXPECT_EQ(4, read_tablet(tablet1, tablet1->updates()->max_version()));
}

TEST_F(TabletUpdatesTest, load_snapshot_incremental_data_file_not_exist) {
    test_load_snapshot_incremental_data_file_not_exist(false);
}

TEST_F(TabletUpdatesTest, load_snapshot_incremental_data_file_not_exist_with_persistent_index) {
    test_load_snapshot_incremental_data_file_not_exist(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_load_snapshot_incremental_incorrect_version(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int64_t> keys0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2 /*version*/, create_rowset(tablet0, keys0)).ok());
    }

    std::vector<int64_t> keys1{0, 1, 2, 3};
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tablet1->rowset_commit(i + 2 /*version*/, create_rowset(tablet1, keys1)).ok());
    }

    auto snapshot_dir = SnapshotManager::instance()->snapshot_incremental(tablet0, {5, 6}, 3600);
    ASSERT_TRUE(snapshot_dir.ok()) << snapshot_dir.status();

    DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet1->schema_hash_path() + "/" + f;
        st = FileSystem::Default()->link_file(src, dst);
        ASSERT_TRUE(st.ok()) << st;
        LOG(INFO) << "Linked " << src << " to " << dst;
    }
    // Pretend that tablet0 is a peer replica of tablet1
    snapshot_meta->tablet_meta().set_tablet_id(tablet1->tablet_id());
    snapshot_meta->tablet_meta().set_schema_hash(tablet1->schema_hash());
    for (auto& rm : snapshot_meta->rowset_metas()) {
        rm.set_tablet_id(tablet1->tablet_id());
    }

    st = tablet1->updates()->load_snapshot(*snapshot_meta);
    ASSERT_TRUE(st.ok()) << st;
}

TEST_F(TabletUpdatesTest, load_snapshot_incremental_incorrect_version) {
    test_load_snapshot_incremental_incorrect_version(false);
}

TEST_F(TabletUpdatesTest, load_snapshot_incremental_incorrect_version_with_persistent_index) {
    test_load_snapshot_incremental_incorrect_version(true);
}

void TabletUpdatesTest::tablets_prepare(const TabletSharedPtr& tablet0, const TabletSharedPtr& tablet1,
                                        std::vector<int32_t>& column_indexes,
                                        const std::shared_ptr<TabletSchema>& partial_schema) {
    std::vector<int64_t> keys0 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 4; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2 /*version*/, create_rowset(tablet0, keys0)).ok());
    }

    {
        EditVersion version;
        std::vector<RowsetSharedPtr> applied_rowsets;
        ASSERT_TRUE(tablet0->updates()->get_applied_rowsets(5, &applied_rowsets, &version).ok());
    }

    // create a partial rowset, commit but not apply
    tablet0->updates()->stop_apply(true);
    RowsetSharedPtr partial_rowset = create_partial_rowset(tablet0, keys0, column_indexes, partial_schema);
    ASSERT_TRUE(tablet0->rowset_commit(6, partial_rowset).ok());
    ASSERT_EQ(tablet0->updates()->max_version(), 6);
    EditVersion latest_applied_verison;
    tablet0->updates()->get_latest_applied_version(&latest_applied_verison);
    ASSERT_EQ(latest_applied_verison.major(), 5);
    LOG(INFO) << "commit partial rowset success";

    // create rowsets for tablet1
    std::vector<int64_t> keys1 = {0, 1, 2, 3};
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tablet1->rowset_commit(i + 2 /*version*/, create_rowset(tablet1, keys1)).ok());
    }
}

void TabletUpdatesTest::snapshot_prepare(const TabletSharedPtr& tablet, const std::vector<int64_t>& delta_versions,
                                         std::string* snapshot_id_path, std::string* snapshot_dir,
                                         std::vector<RowsetSharedPtr>* snapshot_rowsets,
                                         std::vector<RowsetMetaSharedPtr>* snapshot_rowset_metas,
                                         const TabletMetaSharedPtr& snapshot_tablet_meta) {
    std::shared_lock rdlock(tablet->get_header_lock());
    for (int64_t v : delta_versions) {
        auto rowset = tablet->get_inc_rowset_by_version(Version{v, v});
        if (rowset == nullptr && tablet->max_continuous_version() >= v) {
            LOG(WARNING) << "version " << v << " has been merged";
            ASSERT_TRUE(false);
        } else if (rowset == nullptr) {
            LOG(WARNING) << "no incremental rowset " << v;
            ASSERT_TRUE(false);
        }
        snapshot_rowsets->emplace_back(std::move(rowset));
    }

    tablet->generate_tablet_meta_copy_unlocked(snapshot_tablet_meta);
    rdlock.unlock();

    *snapshot_id_path = SnapshotManager::instance()->calc_snapshot_id_path(tablet, 3600);
    ASSERT_TRUE(!snapshot_id_path->empty());
    *snapshot_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet, *snapshot_id_path);
    (void)fs::remove_all(*snapshot_dir);
    ASSERT_TRUE(fs::create_directories(*snapshot_dir).ok());

    snapshot_rowset_metas->reserve(snapshot_rowsets->size());
    for (const auto& rowset : *snapshot_rowsets) {
        snapshot_rowset_metas->emplace_back(rowset->rowset_meta());
    }
}

void TabletUpdatesTest::load_snapshot(const std::string& meta_dir, const TabletSharedPtr& tablet,
                                      SegmentFooterPB* footer) {
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    ASSERT_TRUE(fs::list_dirs_files(meta_dir, nullptr, &files).ok());
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet->schema_hash_path() + "/" + f;
        ASSERT_TRUE(FileSystem::Default()->link_file(src, dst).ok());
        LOG(INFO) << "Linked " << src << " to " << dst;
    }

    // Pretend that tablet0 is a peer replica of tablet1
    snapshot_meta->tablet_meta().set_tablet_id(tablet->tablet_id());
    snapshot_meta->tablet_meta().set_schema_hash(tablet->schema_hash());
    for (auto& rm : snapshot_meta->rowset_metas()) {
        rm.set_tablet_id(tablet->tablet_id());
    }
    LOG(INFO) << "tablet1 start load snapshot";

    ASSERT_TRUE(tablet->updates()->load_snapshot(*snapshot_meta).ok());
    ASSERT_EQ(6, tablet->updates()->max_version());
    ASSERT_EQ(6, tablet->updates()->version_history_count());

    EditVersion full_edit_version;
    std::vector<RowsetSharedPtr> applied_rowsets;
    ASSERT_TRUE(tablet->updates()->get_applied_rowsets(6, &applied_rowsets, &full_edit_version).ok());
    ASSERT_EQ(5, applied_rowsets.size());

    RowsetSharedPtr last_rowset = applied_rowsets.back();
    int64_t num_segments = last_rowset->num_segments();
    ASSERT_EQ(1, num_segments);
    std::string rowset_path = last_rowset->rowset_path();
    std::string segment_path =
            strings::Substitute("$0/$1_$2.dat", rowset_path, last_rowset->rowset_id().to_string(), 0);
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString("posix://"));
    ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(segment_path));

    ASSERT_TRUE(Segment::parse_segment_footer(read_file.get(), footer, nullptr, nullptr).ok());
    LOG(INFO) << "parse segment footer success";
}

void TabletUpdatesTest::test_load_snapshot_incremental_with_partial_rowset_old(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int32_t> column_indexes = {0, 1};
    std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(tablet1->tablet_schema(), column_indexes);
    tablets_prepare(tablet0, tablet1, column_indexes, partial_schema);

    // try to do snapshot
    std::vector<int64_t> delta_versions = {4, 5, 6};
    TabletMetaSharedPtr snapshot_tablet_meta = std::make_shared<TabletMeta>();
    std::vector<RowsetSharedPtr> snapshot_rowsets;
    std::vector<RowsetMetaSharedPtr> snapshot_rowset_metas;
    std::string snapshot_id_path;
    std::string snapshot_dir;
    snapshot_prepare(tablet0, delta_versions, &snapshot_id_path, &snapshot_dir, &snapshot_rowsets,
                     &snapshot_rowset_metas, snapshot_tablet_meta);

    // link files first and then build snapshot meta file
    for (const auto& rowset : snapshot_rowsets) {
        ASSERT_TRUE(rowset->link_files_to(snapshot_dir, rowset->rowset_id()).ok());
    }

    // apply rowset
    tablet0->updates()->stop_apply(false);
    tablet0->updates()->check_for_apply();

    {
        EditVersion version;
        std::vector<RowsetSharedPtr> applied_rowsets;
        Status status = tablet0->updates()->get_applied_rowsets(6, &applied_rowsets, &version);
        EditVersion latest_applied_verison;
        tablet0->updates()->get_latest_applied_version(&latest_applied_verison);
        ASSERT_EQ(latest_applied_verison.major(), 6);
    }

    ASSERT_TRUE(SnapshotManager::instance()
                        ->make_snapshot_on_tablet_meta(SNAPSHOT_TYPE_INCREMENTAL, snapshot_dir, tablet0,
                                                       snapshot_rowset_metas, 0, 4 /*TSNAPSHOT_REQ_VERSION2*/)
                        .ok());

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, snapshot_id_path);
    SegmentFooterPB footer;
    load_snapshot(meta_dir, tablet1, &footer);
    ASSERT_EQ(footer.columns_size(), 2);
}

TEST_F(TabletUpdatesTest, load_snapshot_incremental_with_partial_rowset_old) {
    test_load_snapshot_incremental_with_partial_rowset_old(false);
}

void TabletUpdatesTest::test_load_snapshot_incremental_with_partial_rowset_new(bool enable_persistent_index,
                                                                               PartialUpdateCloneCase update_case) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int32_t> column_indexes = {0, 1};
    std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(tablet1->tablet_schema(), column_indexes);
    tablets_prepare(tablet0, tablet1, column_indexes, partial_schema);
    if (update_case == CASE4) {
        tablet0->updates()->stop_apply(false);
        tablet0->updates()->check_for_apply();
        {
            EditVersion version;
            std::vector<RowsetSharedPtr> applied_rowsets;
            Status status = tablet0->updates()->get_applied_rowsets(6, &applied_rowsets, &version);
            EditVersion latest_applied_verison;
            tablet0->updates()->get_latest_applied_version(&latest_applied_verison);
            ASSERT_EQ(latest_applied_verison.major(), 6);
        }
    }

    // try to do snapshot
    std::vector<int64_t> delta_versions = {4, 5, 6};
    TabletMetaSharedPtr snapshot_tablet_meta = std::make_shared<TabletMeta>();
    std::vector<RowsetSharedPtr> snapshot_rowsets;
    std::vector<RowsetMetaSharedPtr> snapshot_rowset_metas;
    std::string snapshot_id_path;
    std::string snapshot_dir;
    DeferOp remove([&]() {
        (void)fs::remove_all(snapshot_dir);
        (void)fs::remove_all(snapshot_id_path);
    });

    snapshot_prepare(tablet0, delta_versions, &snapshot_id_path, &snapshot_dir, &snapshot_rowsets,
                     &snapshot_rowset_metas, snapshot_tablet_meta);

    ASSERT_TRUE(SnapshotManager::instance()
                        ->make_snapshot_on_tablet_meta(SNAPSHOT_TYPE_INCREMENTAL, snapshot_dir, tablet0,
                                                       snapshot_rowset_metas, 0, 4 /*TSNAPSHOT_REQ_VERSION2*/)
                        .ok());
    switch (update_case) {
    case CASE1: {
        // rowset status is committed in meta, rowset file is partial rowset
        // link files directly
        for (const auto& rowset : snapshot_rowsets) {
            ASSERT_TRUE(rowset->link_files_to(snapshot_dir, rowset->rowset_id()).ok());
        }
        break;
    }
    case CASE2: {
        // rowset status is committed in meta, rowset file is partial rowset, but rowset is apply success after link file
        // link files first and do apply
        for (const auto& rowset : snapshot_rowsets) {
            ASSERT_TRUE(rowset->link_files_to(snapshot_dir, rowset->rowset_id()).ok());
        }

        tablet0->updates()->stop_apply(false);
        tablet0->updates()->check_for_apply();
        {
            EditVersion version;
            std::vector<RowsetSharedPtr> applied_rowsets;
            Status status = tablet0->updates()->get_applied_rowsets(6, &applied_rowsets, &version);
            EditVersion latest_applied_verison;
            tablet0->updates()->get_latest_applied_version(&latest_applied_verison);
            ASSERT_EQ(latest_applied_verison.major(), 6);
        }
        break;
    }
    case CASE3: {
        // rowset status is committed in meta, rowset file is full rowset
        // apply first and then do link files
        tablet0->updates()->stop_apply(false);
        tablet0->updates()->check_for_apply();

        {
            EditVersion version;
            std::vector<RowsetSharedPtr> applied_rowsets;
            Status status = tablet0->updates()->get_applied_rowsets(6, &applied_rowsets, &version);
            EditVersion latest_applied_verison;
            tablet0->updates()->get_latest_applied_version(&latest_applied_verison);
            ASSERT_EQ(latest_applied_verison.major(), 6);
        }

        for (const auto& rowset : snapshot_rowsets) {
            ASSERT_TRUE(rowset->link_files_to(snapshot_dir, rowset->rowset_id()).ok());
        }
        break;
    }
    case CASE4: {
        // rowset status is applied in meta, rowset file is full rowset
        // rowsets applied success, link files directly
        for (const auto& rowset : snapshot_rowsets) {
            ASSERT_TRUE(rowset->link_files_to(snapshot_dir, rowset->rowset_id()).ok());
        }
        break;
    }
    default:
        return;
    }

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, snapshot_id_path);
    SegmentFooterPB footer;
    load_snapshot(meta_dir, tablet1, &footer);
    ASSERT_EQ(footer.columns_size(), 3);
}

TEST_F(TabletUpdatesTest, test_load_snapshot_incremental_with_partial_rowset_new) {
    test_load_snapshot_incremental_with_partial_rowset_new(false, CASE1);
    test_load_snapshot_incremental_with_partial_rowset_new(false, CASE2);
    test_load_snapshot_incremental_with_partial_rowset_new(false, CASE3);
    test_load_snapshot_incremental_with_partial_rowset_new(false, CASE4);
}
// NOLINTNEXTLINE
void TabletUpdatesTest::test_load_snapshot_full(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int64_t> keys0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2, create_rowset(tablet0, keys0)).ok());
    }

    std::vector<int64_t> keys1{0, 1, 2, 3};
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tablet1->rowset_commit(i + 2, create_rowset(tablet1, keys1)).ok());
    }

    auto st = full_clone(tablet0, 11, tablet1);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(11, tablet1->updates()->max_version());
    ASSERT_EQ(1, tablet1->updates()->version_history_count());
    EXPECT_EQ(keys0.size(), read_tablet(tablet1, tablet1->updates()->max_version()));

    // Ensure that the tablet state is valid after process restarted.
    auto tablet2 = load_same_tablet_from_store(tablet1);
    ASSERT_EQ(11, tablet2->updates()->max_version());
    ASSERT_EQ(1, tablet2->updates()->version_history_count());
    EXPECT_EQ(keys0.size(), read_tablet(tablet2, tablet2->updates()->max_version()));
}

TEST_F(TabletUpdatesTest, load_snapshot_full) {
    test_load_snapshot_full(false);
}

TEST_F(TabletUpdatesTest, load_snapshot_full_with_persistent_index) {
    test_load_snapshot_full(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_load_snapshot_full_file_not_exist(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int64_t> keys0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2, create_rowset(tablet0, keys0)).ok());
    }

    std::vector<int64_t> keys1{0, 1, 2, 3};
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tablet1->rowset_commit(i + 2, create_rowset(tablet1, keys1)).ok());
    }

    auto snapshot_dir = SnapshotManager::instance()->snapshot_full(tablet0, 11, 3600);
    ASSERT_TRUE(snapshot_dir.ok()) << snapshot_dir.status();

    DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    // Pretend that tablet0 is a peer replica of tablet1
    snapshot_meta->tablet_meta().set_tablet_id(tablet1->tablet_id());
    snapshot_meta->tablet_meta().set_schema_hash(tablet1->schema_hash());
    for (auto& rm : snapshot_meta->rowset_metas()) {
        rm.set_tablet_id(tablet1->tablet_id());
    }

    // Segment files does not link to the directory of tablet1.
    st = tablet1->updates()->load_snapshot(*snapshot_meta);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("segment file does not exist") != std::string::npos);
    ASSERT_EQ(3, tablet1->updates()->max_version());
    ASSERT_EQ(3, tablet1->updates()->version_history_count());
    EXPECT_EQ(keys1.size(), read_tablet(tablet1, tablet1->updates()->max_version()));

    // Ensure that the persistent meta is still valid.
    auto tablet2 = load_same_tablet_from_store(tablet1);
    ASSERT_EQ(3, tablet2->updates()->max_version());
    ASSERT_EQ(3, tablet2->updates()->version_history_count());
    EXPECT_EQ(keys1.size(), read_tablet(tablet2, tablet2->updates()->max_version()));
}

TEST_F(TabletUpdatesTest, load_snapshot_full_file_not_exist) {
    test_load_snapshot_full_file_not_exist(false);
}

TEST_F(TabletUpdatesTest, load_snapshot_full_file_not_exist_with_persistent_index) {
    test_load_snapshot_full_file_not_exist(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_load_snapshot_full_mismatched_tablet_id(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int64_t> keys0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2, create_rowset(tablet0, keys0)).ok());
    }

    std::vector<int64_t> keys1{0, 1, 2, 3};
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tablet1->rowset_commit(i + 2, create_rowset(tablet1, keys1)).ok());
    }

    auto snapshot_dir = SnapshotManager::instance()->snapshot_full(tablet0, 11, 3600);
    ASSERT_TRUE(snapshot_dir.ok()) << snapshot_dir.status();

    DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet1->schema_hash_path() + "/" + f;
        st = FileSystem::Default()->link_file(src, dst);
        ASSERT_TRUE(st.ok()) << st;
        LOG(INFO) << "Linked " << src << " to " << dst;
    }

    // tablet_id and schema_hash does not match.
    st = tablet1->updates()->load_snapshot(*snapshot_meta);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("mismatched tablet id") != std::string::npos);
}

TEST_F(TabletUpdatesTest, load_snapshot_full_mismatched_tablet_id) {
    test_load_snapshot_full_mismatched_tablet_id(false);
}

TEST_F(TabletUpdatesTest, load_snapshot_full_mismatched_tablet_id_with_persistent_index) {
    test_load_snapshot_full_mismatched_tablet_id(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_issue_4193(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int64_t> keys0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    // commit tablet0 to version 11
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2, create_rowset(tablet0, keys0)).ok());
    }

    std::vector<int64_t> keys1{0, 1, 2, 3};
    // commit tablet1 to version 3
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tablet1->rowset_commit(i + 2, create_rowset(tablet1, keys1)).ok());
    }
    keys1 = {10, 11, 12};
    // commit tablet1 extra two pending rowsets
    ASSERT_TRUE(tablet1->rowset_commit(12, create_rowset(tablet1, keys1)).ok());
    ASSERT_TRUE(tablet1->rowset_commit(13, create_rowset(tablet1, keys1)).ok());

    auto st = full_clone(tablet0, 11, tablet1);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(13, tablet1->updates()->max_version());
    EXPECT_EQ(keys0.size() + keys1.size(), read_tablet(tablet1, tablet1->updates()->max_version()));

    // Ensure that the tablet state is valid after process restarted.
    auto tablet2 = load_same_tablet_from_store(tablet1);
    ASSERT_EQ(13, tablet2->updates()->max_version());
    EXPECT_EQ(keys0.size() + keys1.size(), read_tablet(tablet2, tablet2->updates()->max_version()));
}

TEST_F(TabletUpdatesTest, test_issue_4193) {
    test_issue_4193(false);
}

TEST_F(TabletUpdatesTest, test_issue_4193_with_persistent_index) {
    test_issue_4193(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_issue_4181(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);
    tablet1->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    std::vector<int64_t> keys0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    // commit tablet0 to version 11
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2, create_rowset(tablet0, keys0)).ok());
    }

    std::vector<int64_t> keys1{0, 1, 2, 3};
    // commit tablet1 to version 3
    for (int i = 0; i < 2; i++) {
        ASSERT_TRUE(tablet1->rowset_commit(i + 2, create_rowset(tablet1, keys1)).ok());
    }

    auto st = full_clone(tablet0, 9, tablet1);
    ASSERT_TRUE(st.ok()) << st;

    st = full_clone(tablet0, 10, tablet1);
    ASSERT_TRUE(st.ok()) << st;

    st = full_clone(tablet0, 11, tablet1);

    ASSERT_EQ(11, tablet1->updates()->max_version());
    EXPECT_EQ(keys0.size(), read_tablet(tablet1, tablet1->updates()->max_version()));

    // Ensure that the tablet state is valid after process restarted.
    auto tablet2 = load_same_tablet_from_store(tablet1);
    ASSERT_EQ(11, tablet2->updates()->max_version());
    EXPECT_EQ(keys0.size(), read_tablet(tablet2, tablet2->updates()->max_version()));
}

TEST_F(TabletUpdatesTest, test_issue_4181) {
    test_issue_4181(false);
}

TEST_F(TabletUpdatesTest, test_issue_4181_with_persistent_index) {
    test_issue_4181(true);
}

// NOLINTNEXTLINE
void TabletUpdatesTest::test_snapshot_with_empty_rowset(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    tablet0->set_enable_persistent_index(enable_persistent_index);

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)fs::remove_all(tablet0->schema_hash_path());
    });

    std::vector<int64_t> keys0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(tablet0->rowset_commit(i + 2, create_rowset(tablet0, keys0)).ok());
    }
    // Empty rowset.
    ASSERT_TRUE(tablet0->rowset_commit(12, create_rowset(tablet0, std::vector<int64_t>{})).ok());

    auto res = clone_a_new_replica(tablet0, rand());
    ASSERT_TRUE(res.ok()) << res.status();
    ASSERT_TRUE(*res != nullptr);
    auto tablet1 = std::move(res).value();

    DeferOp defer2([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)fs::remove_all(tablet1->schema_hash_path());
    });

    ASSERT_EQ(12, tablet1->updates()->max_version());
    ASSERT_EQ(1, tablet1->updates()->version_history_count());

    MemTracker tracker;
    Status st = tablet1->updates()->compaction(&tracker);
    ASSERT_TRUE(st.ok()) << st;

    // Wait until compaction applied.
    while (true) {
        std::vector<RowsetSharedPtr> rowsets;
        EditVersion full_version;
        ASSERT_TRUE(tablet1->updates()->get_applied_rowsets(12, &rowsets, &full_version).ok());
        if (full_version.minor() == 1) {
            break;
        }
        std::cerr << "waiting for compaction applied\n";
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    ASSERT_EQ(12, tablet1->updates()->max_version());
    EXPECT_EQ(keys0.size(), read_tablet(tablet1, tablet1->updates()->max_version()));
}

TEST_F(TabletUpdatesTest, snapshot_with_empty_rowset) {
    test_snapshot_with_empty_rowset(false);
}

TEST_F(TabletUpdatesTest, snapshot_with_empty_rowset_with_persistent_index) {
    test_snapshot_with_empty_rowset(true);
}

void TabletUpdatesTest::test_get_column_values(bool enable_persistent_index) {
    srand(GetCurrentTimeMicros());
    auto tablet = create_tablet(rand(), rand());
    DeferOp del_tablet([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet->tablet_id());
        (void)fs::remove_all(tablet->schema_hash_path());
    });
    tablet->set_enable_persistent_index(enable_persistent_index);
    const int N = 8000;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    std::size_t max_rows_per_segment = 1000;
    ASSERT_TRUE(tablet->rowset_commit(2, create_rowsets(tablet, keys, max_rows_per_segment)).ok());
    ASSERT_TRUE(tablet->rowset_commit(3, create_rowsets(tablet, keys, max_rows_per_segment)).ok());
    std::vector<uint32_t> read_column_ids = {1, 2};
    std::vector<std::unique_ptr<vectorized::Column>> read_columns(read_column_ids.size());
    const auto& tablet_schema = tablet->tablet_schema();
    for (auto i = 0; i < read_column_ids.size(); i++) {
        const auto read_column_id = read_column_ids[i];
        auto tablet_column = tablet_schema.column(read_column_id);
        auto column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        read_columns[i] = column->clone_empty();
    }
    std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
    int num_segments = N / max_rows_per_segment;
    for (auto i = 0; i < num_segments; i++) {
        const int num_rowids = rand() % max_rows_per_segment;
        std::vector<uint32_t> rowids;
        for (auto i = 0; i < num_rowids; i++) {
            rowids.push_back(rand() % max_rows_per_segment);
        }
        std::sort(rowids.begin(), rowids.end());
        rowids_by_rssid.emplace(i, rowids);
    }
    tablet->updates()->get_column_values(read_column_ids, false, rowids_by_rssid, &read_columns);
    auto values_str_generator = [&rowids_by_rssid](const int modulus, const int base) {
        std::stringstream ss;
        ss << "[";
        for (const auto& [rssid, rowids] : rowids_by_rssid) {
            for (const auto rowid : rowids) {
                ss << rowid % modulus + base << ", ";
            }
        }
        std::string values_str = ss.str();
        values_str.pop_back();
        values_str.pop_back();
        values_str.append("]");
        return values_str;
    };
    ASSERT_EQ(values_str_generator(100, 1), read_columns[0]->debug_string());
    ASSERT_EQ(values_str_generator(1000, 2), read_columns[1]->debug_string());
    for (const auto& read_column : read_columns) {
        read_column->reset_column();
    }
    tablet->updates()->get_column_values(read_column_ids, true, rowids_by_rssid, &read_columns);
    ASSERT_EQ(std::string("[0, ") + values_str_generator(100, 1).substr(1), read_columns[0]->debug_string());
    ASSERT_EQ(std::string("[0, ") + values_str_generator(1000, 2).substr(1), read_columns[1]->debug_string());
}

TEST_F(TabletUpdatesTest, get_column_values) {
    test_get_column_values(false);
}

TEST_F(TabletUpdatesTest, get_column_values_with_persistent_index) {
    test_get_column_values(true);
}

void TabletUpdatesTest::test_get_missing_version_ranges(const std::vector<int64_t>& versions,
                                                        const std::vector<int64_t>& expected_missing_ranges) {
    auto tablet = create_tablet(rand(), rand());
    DeferOp del_tablet([&]() {
        (void)StorageEngine::instance()->tablet_manager()->drop_tablet(tablet->tablet_id());
        (void)fs::remove_all(tablet->schema_hash_path());
    });
    const int num_keys_per_rowset = 1000;
    auto add_version = [&](int64_t v) {
        std::vector<int64_t> keys;
        for (int i = 0; i < num_keys_per_rowset; i++) {
            keys.push_back(num_keys_per_rowset * i + i);
        }
        auto rs = create_rowset(tablet, keys);
        ASSERT_TRUE(tablet->rowset_commit(v, rs).ok());
    };
    for (auto v : versions) {
        add_version(v);
    }
    vector<int64_t> missing_version_ranges;
    ASSERT_TRUE(tablet->updates()->get_missing_version_ranges(missing_version_ranges).ok());
    ASSERT_EQ(missing_version_ranges, expected_missing_ranges);
}

TEST_F(TabletUpdatesTest, get_missing_version_ranges) {
    srand(GetCurrentTimeMicros());
    test_get_missing_version_ranges({2, 3, 4, 6, 8, 10, 14, 15, 20, 23},
                                    {5, 5, 7, 7, 9, 9, 11, 13, 16, 19, 21, 22, 24});
    test_get_missing_version_ranges({}, {2});
    test_get_missing_version_ranges({2, 3, 4, 5}, {6});
    test_get_missing_version_ranges({3, 4, 5}, {2, 2, 6});
}

void TabletUpdatesTest::test_get_rowsets_for_incremental_snapshot(const std::vector<int64_t>& versions,
                                                                  const std::vector<int64_t>& missing_ranges,
                                                                  const std::vector<int64_t>& expect_rowset_versions,
                                                                  bool gc, bool expect_error) {
    auto tablet = create_tablet(rand(), rand());
    DeferOp del_tablet([&]() {
        (void)StorageEngine::instance()->tablet_manager()->drop_tablet(tablet->tablet_id());
        (void)fs::remove_all(tablet->schema_hash_path());
    });
    const int num_keys_per_rowset = 1000;
    auto add_version = [&](int64_t v) {
        std::vector<int64_t> keys;
        for (int i = 0; i < num_keys_per_rowset; i++) {
            keys.push_back(num_keys_per_rowset * i + i);
        }
        auto rs = create_rowset(tablet, keys);
        ASSERT_TRUE(tablet->rowset_commit(v, rs).ok());
    };
    for (auto v : versions) {
        add_version(v);
    }
    if (gc) {
        while (true) {
            EditVersion ev;
            tablet->updates()->get_latest_applied_version(&ev);
            if (ev.major() == versions.back()) {
                break;
            }
            SleepForMs(50);
        }
        // only keep last version
        tablet->updates()->remove_expired_versions(INT64_MAX);
    }
    std::vector<RowsetSharedPtr> rowsets;
    auto st = tablet->updates()->get_rowsets_for_incremental_snapshot(missing_ranges, rowsets);
    if (expect_error) {
        EXPECT_TRUE(st.is_not_found());
    } else {
        EXPECT_TRUE(st.ok());
    }
    std::vector<int64_t> rowset_versions;
    for (auto& rowset : rowsets) {
        rowset_versions.push_back(rowset->version().first);
    }
    EXPECT_EQ(expect_rowset_versions, rowset_versions);
}

TEST_F(TabletUpdatesTest, get_rowsets_for_incremental_snapshot) {
    srand(GetCurrentTimeMicros());
    test_get_rowsets_for_incremental_snapshot({2}, {3, 4, 6}, {}, false, true);
    test_get_rowsets_for_incremental_snapshot({2, 3, 4, 5}, {3, 3, 6}, {3}, false, false);
    test_get_rowsets_for_incremental_snapshot({2, 3, 4, 5, 6}, {3, 3, 5, 5, 7}, {3, 5}, false, false);
    test_get_rowsets_for_incremental_snapshot({2, 3, 4, 5, 6, 7}, {3, 3, 5, 5, 7}, {3, 5, 7}, false, false);
    // after gc, there will only be version:7 left, should get empty rowset list
    test_get_rowsets_for_incremental_snapshot({2, 3, 4, 5, 6, 7}, {3, 3, 5, 5, 7}, {}, true, false);
}

void TabletUpdatesTest::test_load_snapshot_primary(int64_t num_version, const std::vector<uint64_t>& holes) {
    test_load_snapshot_primary(num_version, holes, false);
    test_load_snapshot_primary(num_version, holes, true);
}

void TabletUpdatesTest::test_load_snapshot_primary(int64_t max_version, const std::vector<uint64_t>& holes,
                                                   bool enable_persistent_index) {
    auto src_tablet = create_tablet(rand(), rand());
    auto dest_tablet = create_tablet(rand(), rand());
    src_tablet->set_enable_persistent_index(enable_persistent_index);
    dest_tablet->set_enable_persistent_index(enable_persistent_index);
    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(src_tablet->tablet_id());
        (void)tablet_mgr->drop_tablet(dest_tablet->tablet_id());
        (void)fs::remove_all(src_tablet->schema_hash_path());
        (void)fs::remove_all(dest_tablet->schema_hash_path());
    });
    const int num_keys_per_rowset = 1000;
    auto add_version = [&](TabletSharedPtr& tablet, int64_t v) {
        std::vector<int64_t> keys;
        for (int i = 0; i < num_keys_per_rowset; i++) {
            keys.push_back(num_keys_per_rowset * i + i);
        }
        auto rs = create_rowset(tablet, keys);
        ASSERT_TRUE(tablet->rowset_commit(v, rs).ok());
    };
    size_t holes_index = 0;
    for (int64_t v = 2; v <= max_version; v++) {
        add_version(src_tablet, v);
        if (holes_index < holes.size() && v == holes[holes_index]) {
            holes_index++;
        } else {
            add_version(dest_tablet, v);
        }
    }
    std::vector<int64_t> missing_version_ranges;
    ASSERT_TRUE(dest_tablet->updates()->get_missing_version_ranges(missing_version_ranges).ok());

    auto snapshot_dir = SnapshotManager::instance()->snapshot_primary(src_tablet, missing_version_ranges, 3600);
    ASSERT_TRUE(snapshot_dir.ok()) << snapshot_dir.status();
    DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(src_tablet, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = dest_tablet->schema_hash_path() + "/" + f;
        st = FileSystem::Default()->link_file(src, dst);
        ASSERT_TRUE(st.ok()) << st;
        LOG(INFO) << "Linked " << src << " to " << dst;
    }
    // Pretend that tablet0 is a peer replica of tablet1
    snapshot_meta->tablet_meta().set_tablet_id(dest_tablet->tablet_id());
    snapshot_meta->tablet_meta().set_schema_hash(dest_tablet->schema_hash());
    for (auto& rm : snapshot_meta->rowset_metas()) {
        rm.set_tablet_id(dest_tablet->tablet_id());
    }

    st = dest_tablet->updates()->load_snapshot(*snapshot_meta);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(max_version, dest_tablet->updates()->max_version());
}

TEST_F(TabletUpdatesTest, load_snapshot_primary) {
    srand(GetCurrentTimeMicros());
    test_load_snapshot_primary(7, {3, 4, 5});
    test_load_snapshot_primary(7, {3, 5, 7});
}

TEST_F(TabletUpdatesTest, multiple_delete_and_upsert) {
    _tablet = create_tablet(rand(), rand());

    RowsetWriterContext writer_context;
    RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
    writer_context.rowset_id = rowset_id;
    writer_context.tablet_id = _tablet->tablet_id();
    writer_context.tablet_schema_hash = _tablet->schema_hash();
    writer_context.partition_id = 0;
    writer_context.rowset_path_prefix = _tablet->schema_hash_path();
    writer_context.rowset_state = COMMITTED;
    writer_context.tablet_schema = &_tablet->tablet_schema();
    writer_context.version.first = 0;
    writer_context.version.second = 0;
    writer_context.segments_overlap = NONOVERLAPPING;
    std::unique_ptr<RowsetWriter> writer;
    EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());

    // 1. upsert [0, 1, 2 ... 100)
    {
        std::vector<int64_t> keys;
        for (int i = 0; i < 100; i++) {
            keys.emplace_back(i);
        }
        auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (int64_t key : keys) {
            cols[0]->append_datum(vectorized::Datum(key));
            cols[1]->append_datum(vectorized::Datum((int16_t)(key % 100 + 1)));
            cols[2]->append_datum(vectorized::Datum((int32_t)(key % 100 + 2)));
        }
        CHECK_OK(writer->flush_chunk(*chunk));
    }
    // 2. delete [0, 1, 2 ... 50)
    {
        vectorized::Int64Column deletes;
        for (int64_t i = 0; i < 50; i++) {
            deletes.append_datum(vectorized::Datum(i));
        }
        auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, 0);
        CHECK_OK(writer->flush_chunk_with_deletes(*chunk, deletes));
    }
    // 3. upsert [0, 1, 2 ... 50)
    {
        std::vector<int64_t> keys;
        for (int i = 0; i < 50; i++) {
            keys.emplace_back(i);
        }
        auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (int64_t key : keys) {
            cols[0]->append_datum(vectorized::Datum(key));
            cols[1]->append_datum(vectorized::Datum((int16_t)(key % 100 + 2)));
            cols[2]->append_datum(vectorized::Datum((int32_t)(key % 100 + 3)));
        }
        CHECK_OK(writer->flush_chunk(*chunk));
    }

    // 4. upsert [100, 102, 103 ... 200) and delete [50, 51, 52 ... 100)
    {
        std::vector<int64_t> keys;
        for (int i = 100; i < 200; i++) {
            keys.emplace_back(i);
        }
        vectorized::Int64Column deletes;
        for (int64_t i = 50; i < 100; i++) {
            deletes.append_datum(vectorized::Datum(i));
        }

        auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (int64_t key : keys) {
            cols[0]->append_datum(vectorized::Datum(key));
            cols[1]->append_datum(vectorized::Datum((int16_t)(key % 100 + 1)));
            cols[2]->append_datum(vectorized::Datum((int32_t)(key % 100 + 2)));
        }
        CHECK_OK(writer->flush_chunk_with_deletes(*chunk, deletes));
    }
    // 5. delete [150, 151, 152 ... 200)
    {
        vectorized::Int64Column deletes;
        for (int64_t i = 150; i < 200; i++) {
            deletes.append_datum(vectorized::Datum(i));
        }
        auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, 0);
        CHECK_OK(writer->flush_chunk_with_deletes(*chunk, deletes));
    }
    RowsetSharedPtr rowset = *writer->build();
    ASSERT_TRUE(_tablet->rowset_commit(2, rowset).ok());

    auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    vectorized::TabletReader reader(_tablet, Version(0, 2), schema);
    auto iter = create_tablet_iterator(reader, schema);
    ASSERT_TRUE(iter != nullptr);
    std::vector<int64_t> keys;
    for (int i = 0; i < 50; i++) {
        keys.emplace_back(i);
    }
    for (int i = 100; i < 150; i++) {
        keys.emplace_back(i);
    }
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    auto full_chunk = ChunkHelper::new_chunk(iter->schema(), keys.size());
    auto& cols = full_chunk->columns();
    for (int i = 0; i < 50; i++) {
        cols[0]->append_datum(vectorized::Datum(keys[i]));
        cols[1]->append_datum(vectorized::Datum((int16_t)(keys[i] % 100 + 2)));
        cols[2]->append_datum(vectorized::Datum((int32_t)(keys[i] % 100 + 3)));
    }

    for (int i = 50; i < 100; i++) {
        cols[0]->append_datum(vectorized::Datum(keys[i]));
        cols[1]->append_datum(vectorized::Datum((int16_t)(keys[i] % 100 + 1)));
        cols[2]->append_datum(vectorized::Datum((int32_t)(keys[i] % 100 + 2)));
    }

    size_t count = 0;
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            for (auto i = 0; i < chunk->num_rows(); i++) {
                EXPECT_EQ(full_chunk->get(count + i).compare(iter->schema(), chunk->get(i)), 0);
            }
            count += chunk->num_rows();
            chunk->reset();
        } else {
            ASSERT_TRUE(false);
        }
    }
    ASSERT_TRUE(count == keys.size());
}

} // namespace starrocks
