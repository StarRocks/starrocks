// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/tablet_updates.h"

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

#include "column/datum_tuple.h"
#include "column/vectorized_fwd.h"
#include "gutil/strings/substitute.h"
#include "storage/fs/file_block_manager.h"
#include "storage/kv_store.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/vectorized/rowset_options.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "storage/update_manager.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/empty_iterator.h"
#include "storage/vectorized/schema_change.h"
#include "storage/vectorized/tablet_reader.h"
#include "storage/vectorized/union_iterator.h"
#include "storage/wrapper_field.h"
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
                                  vectorized::Column* one_delete = nullptr) {
        RowsetWriterContext writer_context(kDataFormatV2, config::storage_format_version);
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_type = BETA_ROWSET;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
        auto chunk = vectorized::ChunkHelper::new_chunk(schema, keys.size());
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

    RowsetSharedPtr create_partial_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                          std::vector<int32_t>& column_indexes,
                                          std::shared_ptr<TabletSchema> partial_schema) {
        // create partial rowset
        RowsetWriterContext writer_context(kDataFormatV2, config::storage_format_version);
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_type = BETA_ROWSET;
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
        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*partial_schema.get());

        if (keys.size() > 0) {
            auto chunk = vectorized::ChunkHelper::new_chunk(schema, keys.size());
            EXPECT_TRUE(2 == chunk->num_columns());
            auto& cols = chunk->columns();
            for (size_t i = 0; i < keys.size(); i++) {
                cols[0]->append_datum(vectorized::Datum(keys[i]));
                cols[1]->append_datum(vectorized::Datum((int16_t)(keys[i] % 100 + 3)));
            }
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        RowsetSharedPtr partial_rowset = *writer->build();

        return partial_rowset;
    }

    RowsetSharedPtr create_rowsets(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                   std::size_t max_rows_per_segment) {
        RowsetWriterContext writer_context(kDataFormatV2, config::storage_format_version);
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_type = BETA_ROWSET;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
        for (std::size_t written_rows = 0; written_rows < keys.size(); written_rows += max_rows_per_segment) {
            auto chunk = vectorized::ChunkHelper::new_chunk(schema, max_rows_per_segment);
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

    TabletSharedPtr create_tablet2(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 6;
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
        request.tablet_schema.short_key_column_count = 6;
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

    void SetUp() override {
        _compaction_mem_tracker.reset(new MemTracker(-1));
        _tablet_meta_mem_tracker = std::make_unique<MemTracker>();
    }

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

        DeferOp defer1([&]() { (void)FileUtils::remove_all(*snapshot_dir); });

        auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(source_tablet, *snapshot_dir);
        auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
        CHECK(snapshot_meta.ok()) << snapshot_meta.status();

        RETURN_IF_ERROR(SnapshotManager::instance()->assign_new_rowset_id(&(*snapshot_meta), meta_dir));

        std::set<std::string> files;
        auto st = FileUtils::list_dirs_files(meta_dir, NULL, &files, Env::Default());
        CHECK(st.ok()) << st;
        files.erase("meta");

        for (const auto& f : files) {
            std::string src = meta_dir + "/" + f;
            std::string dst = dest_tablet->schema_hash_path() + "/" + f;
            st = Env::Default()->link_file(src, dst);
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
        dest_tablet->updates()->remove_expired_versions(time(NULL));
        return st;
    }

    static StatusOr<TabletSharedPtr> clone_a_new_replica(const TabletSharedPtr& source_tablet, int64_t new_tablet_id) {
        auto clone_version = source_tablet->max_version().second;
        auto snapshot_dir = SnapshotManager::instance()->snapshot_full(source_tablet, clone_version, 3600);
        CHECK(snapshot_dir.ok()) << snapshot_dir.status();

        DeferOp defer1([&]() { (void)FileUtils::remove_all(*snapshot_dir); });

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
        CHECK(FileUtils::list_dirs_files(meta_dir, NULL, &files, Env::Default()).ok());
        for (const auto& f : files) {
            std::string src = meta_dir + "/" + f;
            std::string dst = new_tablet_path + "/" + f;
            Status st = Env::Default()->link_file(src, dst);
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

    void test_load_snapshot_incremental_with_partial_rowset_old();
    void test_load_snapshot_incremental_with_partial_rowset_new(PartialUpdateCloneCase update_case);

    void tablets_prepare(TabletSharedPtr tablet0, TabletSharedPtr tablet1, std::vector<int32_t>& column_indexes,
                         const std::shared_ptr<TabletSchema>& partial_schema);
    void snapshot_prepare(const TabletSharedPtr& tablet, const std::vector<int64_t>& delta_versions,
                          std::string* snapshot_id_path, std::string* snapshot_dir,
                          std::vector<RowsetSharedPtr>* snapshot_rowsets,
                          std::vector<RowsetMetaSharedPtr>* snapshot_rowset_metas,
                          TabletMetaSharedPtr snapshot_tablet_meta);
    void load_snapshot(const std::string& meta_dir, const TabletSharedPtr& tablet, SegmentFooterPB* footer);

protected:
    TabletSharedPtr _tablet;
    TabletSharedPtr _tablet2;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
    std::unique_ptr<MemTracker> _tablet_meta_mem_tracker;
};

static TabletSharedPtr load_same_tablet_from_store(MemTracker* mem_tracker, const TabletSharedPtr& tablet) {
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
    auto tablet1 = Tablet::create_tablet_from_meta(mem_tracker, tablet_meta, data_dir);
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
    auto chunk = vectorized::ChunkHelper::new_chunk(iter->schema(), 100);
    auto full_chunk = vectorized::ChunkHelper::new_chunk(iter->schema(), keys.size());
    auto& cols = full_chunk->columns();
    for (size_t i = 0; i < keys.size(); i++) {
        cols[0]->append_datum(vectorized::Datum(keys[i]));
        cols[1]->append_datum(vectorized::Datum((int16_t)(keys[i] % 100 + 1)));
        cols[2]->append_datum(vectorized::Datum((int32_t)(keys[i] % 1000 + 2)));
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
    auto chunk = vectorized::ChunkHelper::new_chunk(iter->schema(), 100);
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

static ssize_t read_tablet(const TabletSharedPtr& tablet, int64_t version) {
    vectorized::Schema schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
    vectorized::TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    return read_until_eof(iter);
}

static ssize_t read_tablet_and_compare(const TabletSharedPtr& tablet, int64_t version, const vector<int64_t>& keys) {
    vectorized::Schema schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
    vectorized::TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    return read_and_compare(iter, keys);
}

static ssize_t read_tablet_and_compare_schema_changed(const TabletSharedPtr& tablet, int64_t version,
                                                      const vector<int64_t>& keys) {
    vectorized::Schema schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
    vectorized::TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    auto full_chunk = vectorized::ChunkHelper::new_chunk(iter->schema(), keys.size());
    auto& cols = full_chunk->columns();
    for (size_t i = 0; i < keys.size(); i++) {
        cols[0]->append_datum(vectorized::Datum((int64_t)keys[i]));
        cols[1]->append_datum(vectorized::Datum((int16_t)(keys[i] % 100 + 1)));
        auto v = std::to_string((int64_t)(keys[i] % 1000 + 2));
        cols[2]->append_datum(vectorized::Datum(Slice{v}));
    }
    auto chunk = vectorized::ChunkHelper::new_chunk(iter->schema(), 100);
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

TEST_F(TabletUpdatesTest, writeread) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
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
    // read
    ASSERT_EQ(N, read_tablet(_tablet, 3));
    ASSERT_EQ(N, read_tablet(_tablet, 2));
}

TEST_F(TabletUpdatesTest, writeread_with_delete) {
    _tablet = create_tablet(rand(), rand());
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

TEST_F(TabletUpdatesTest, noncontinous_commit) {
    _tablet = create_tablet(rand(), rand());
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

TEST_F(TabletUpdatesTest, noncontinous_meta_save_load) {
    _tablet = create_tablet(rand(), rand());
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

    auto tablet1 = load_same_tablet_from_store(_tablet_meta_mem_tracker.get(), _tablet);

    ASSERT_EQ(2, tablet1->updates()->num_pending());
    ASSERT_EQ(2, tablet1->updates()->max_version());
}

TEST_F(TabletUpdatesTest, save_meta) {
    _tablet = create_tablet(rand(), rand());

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

    auto tablet1 = load_same_tablet_from_store(_tablet_meta_mem_tracker.get(), _tablet);
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

TEST_F(TabletUpdatesTest, remove_expired_versions) {
    _tablet = create_tablet(rand(), rand());

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
    vectorized::Schema schema = vectorized::ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema());
    vectorized::TabletReader reader1(_tablet, Version(0, 1), schema);
    vectorized::TabletReader reader2(_tablet, Version(0, 2), schema);
    vectorized::TabletReader reader3(_tablet, Version(0, 3), schema);
    vectorized::TabletReader reader4(_tablet, Version(0, 4), schema);
    auto iter_v0 = create_tablet_iterator(reader1, schema);
    auto iter_v1 = create_tablet_iterator(reader2, schema);
    auto iter_v2 = create_tablet_iterator(reader3, schema);
    auto iter_v3 = create_tablet_iterator(reader4, schema);

    // Remove all but the last version.
    _tablet->updates()->remove_expired_versions(time(NULL));
    ASSERT_EQ(1, _tablet->updates()->version_history_count());
    ASSERT_EQ(4, _tablet->updates()->max_version());

    EXPECT_EQ(N, read_tablet(_tablet, 4));
    EXPECT_EQ(N, read_until_eof(iter_v3));
    EXPECT_EQ(N, read_until_eof(iter_v2)); // delete vector v2 still valid.
    EXPECT_EQ(0, read_until_eof(iter_v0)); // iter_v0 is empty iterator

    // Read expired versions should fail.
    EXPECT_EQ(-1, read_until_eof(iter_v1));
    EXPECT_EQ(-1, read_tablet(_tablet, 3));
    EXPECT_EQ(-1, read_tablet(_tablet, 2));
    EXPECT_EQ(-1, read_tablet(_tablet, 1));

    auto tablet1 = load_same_tablet_from_store(_tablet_meta_mem_tracker.get(), _tablet);
    EXPECT_EQ(1, tablet1->updates()->version_history_count());
    EXPECT_EQ(4, tablet1->updates()->max_version());
    EXPECT_EQ(N, read_tablet(tablet1, 4));
    EXPECT_EQ(-1, read_tablet(tablet1, 3));
    EXPECT_EQ(-1, read_tablet(tablet1, 2));
    EXPECT_EQ(-1, read_tablet(tablet1, 1));
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, apply) {
    const int N = 10;
    _tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, _tablet->updates()->version_history_count());

    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(64);
    for (int i = 0; i < 64; i++) {
        rowsets.emplace_back(create_rowset(_tablet, keys));
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
    ASSERT_EQ(N, read_tablet(_tablet, rowsets.size()));

    // Ensure the persistent meta is correct.
    auto max_version = rowsets.size() + 1;
    auto tablet1 = load_same_tablet_from_store(_tablet_meta_mem_tracker.get(), _tablet);
    EXPECT_EQ(max_version, tablet1->updates()->max_version());
    EXPECT_EQ(max_version, tablet1->updates()->version_history_count());
    for (int i = 2; i <= max_version; i++) {
        ASSERT_EQ(N, read_tablet(_tablet, i));
    }
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, concurrent_write_read_and_gc) {
    const int N = 2000;
    std::atomic<bool> started{false};
    std::atomic<bool> stopped{false};
    std::atomic<int64_t> version{1};
    _tablet = create_tablet(rand(), rand());

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
            _tablet->updates()->remove_expired_versions(time(NULL));
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
    _tablet->updates()->remove_expired_versions(time(NULL));
    EXPECT_EQ(1, _tablet->updates()->version_history_count());
    EXPECT_EQ(version.load(), _tablet->updates()->max_version());

    // Ensure the persistent meta is correct.
    auto tablet1 = load_same_tablet_from_store(_tablet_meta_mem_tracker.get(), _tablet);
    EXPECT_EQ(1, tablet1->updates()->version_history_count());
    EXPECT_EQ(version.load(), tablet1->updates()->max_version());
    EXPECT_EQ(N, read_tablet(tablet1, version.load()));
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, compaction_score_not_enough) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
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

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, compaction_score_enough_duplicate) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
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

TEST_F(TabletUpdatesTest, compaction_score_enough_normal) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
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

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, horizontal_compaction) {
    auto orig = config::vertical_compaction_max_columns_per_group;
    config::vertical_compaction_max_columns_per_group = 5;
    DeferOp unset_config([&] { config::vertical_compaction_max_columns_per_group = orig; });

    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    std::vector<int64_t> keys;
    for (int i = 0; i < 100; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_EQ(_tablet->updates()->version_history_count(), 4);
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
    auto orig = config::vertical_compaction_max_columns_per_group;
    config::vertical_compaction_max_columns_per_group = 1;
    DeferOp unset_config([&] { config::vertical_compaction_max_columns_per_group = orig; });

    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    std::vector<int64_t> keys;
    for (int i = 0; i < 100; i++) {
        keys.push_back(i);
    }
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys)).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_EQ(_tablet->updates()->version_history_count(), 4);
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

TEST_F(TabletUpdatesTest, link_from) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    _tablet2 = create_tablet2(rand(), rand());
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

TEST_F(TabletUpdatesTest, convert_from) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
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
        } else {
            column_mapping->default_value = WrapperField::create(new_column);

            ASSERT_FALSE(column_mapping->default_value == nullptr) << "init column mapping failed: malloc error";

            if (new_column.is_nullable() && new_column.default_value().length() == 0) {
                column_mapping->default_value->set_null();
            } else {
                column_mapping->default_value->from_string(new_column.default_value());
            }
        }
    }
    ASSERT_TRUE(tablet_to_schema_change->updates()->convert_from(_tablet, 4, chunk_changer.get()).ok());

    ASSERT_EQ(N, read_tablet_and_compare_schema_changed(tablet_to_schema_change, 4, keys));
}

TEST_F(TabletUpdatesTest, convert_from_with_pending) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
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
        } else {
            column_mapping->default_value = WrapperField::create(new_column);

            ASSERT_FALSE(column_mapping->default_value == nullptr) << "init column mapping failed: malloc error";

            if (new_column.is_nullable() && new_column.default_value().length() == 0) {
                column_mapping->default_value->set_null();
            } else {
                column_mapping->default_value->from_string(new_column.default_value());
            }
        }
    }
    ASSERT_TRUE(tablet_to_schema_change->rowset_commit(3, create_rowset(tablet_to_schema_change, keys3)).ok());
    ASSERT_TRUE(tablet_to_schema_change->rowset_commit(4, create_rowset(tablet_to_schema_change, keys4)).ok());

    ASSERT_TRUE(tablet_to_schema_change->updates()->convert_from(_tablet, 2, chunk_changer.get()).ok());

    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowset(_tablet, keys3)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(4, create_rowset(_tablet, keys4)).ok());

    ASSERT_EQ(2 * N, read_tablet_and_compare_schema_changed(tablet_to_schema_change, 4, allkeys));
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, load_snapshot_incremental) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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

    DeferOp defer1([&]() { (void)FileUtils::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = FileUtils::list_dirs_files(meta_dir, NULL, &files, Env::Default());
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet1->schema_hash_path() + "/" + f;
        st = Env::Default()->link_file(src, dst);
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

    auto tablet2 = load_same_tablet_from_store(_tablet_meta_mem_tracker.get(), tablet1);
    ASSERT_EQ(6, tablet2->updates()->max_version());
    ASSERT_EQ(6, tablet2->updates()->version_history_count());
    EXPECT_EQ(10, read_tablet(tablet2, 6));
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, load_snapshot_incremental_ignore_already_committed_version) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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

    DeferOp defer1([&]() { (void)FileUtils::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = FileUtils::list_dirs_files(meta_dir, NULL, &files, Env::Default());
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet1->schema_hash_path() + "/" + f;
        st = Env::Default()->link_file(src, dst);
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

    auto tablet2 = load_same_tablet_from_store(_tablet_meta_mem_tracker.get(), tablet1);
    ASSERT_EQ(6, tablet2->updates()->max_version());
    ASSERT_EQ(6, tablet2->updates()->version_history_count());
    EXPECT_EQ(10, read_tablet(tablet2, 6));
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, load_snapshot_incremental_mismatched_tablet_id) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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

    DeferOp defer1([&]() { (void)FileUtils::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = FileUtils::list_dirs_files(meta_dir, NULL, &files, Env::Default());
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet1->schema_hash_path() + "/" + f;
        st = Env::Default()->link_file(src, dst);
        ASSERT_TRUE(st.ok()) << st;
        LOG(INFO) << "Linked " << src << " to " << dst;
    }

    st = tablet1->updates()->load_snapshot(*snapshot_meta);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("mismatched tablet id") != std::string::npos);
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, load_snapshot_incremental_data_file_not_exist) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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

    DeferOp defer1([&]() { (void)FileUtils::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = FileUtils::list_dirs_files(meta_dir, NULL, &files, Env::Default());
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

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, load_snapshot_incremental_incorrect_version) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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

    DeferOp defer1([&]() { (void)FileUtils::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = FileUtils::list_dirs_files(meta_dir, NULL, &files, Env::Default());
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet1->schema_hash_path() + "/" + f;
        st = Env::Default()->link_file(src, dst);
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

void TabletUpdatesTest::tablets_prepare(TabletSharedPtr tablet0, TabletSharedPtr tablet1,
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
                                         TabletMetaSharedPtr snapshot_tablet_meta) {
    std::shared_lock rdlock(tablet->get_header_lock());
    for (int64_t v : delta_versions) {
        auto rowset = tablet->get_inc_rowset_by_version(Version{v, v});
        if (rowset == nullptr && tablet->max_continuous_version_from_beginning().second >= v) {
            LOG(WARNING) << "version " << v << " has been merged";
            ASSERT_TRUE(false);
        } else if (rowset == nullptr) {
            LOG(WARNING) << "no incremental rowset " << v;
            ASSERT_TRUE(false);
        }
        snapshot_rowsets->emplace_back(std::move(rowset));
    }

    tablet->generate_tablet_meta_copy_unlocked(snapshot_tablet_meta);
    snapshot_tablet_meta->delete_alter_task();
    rdlock.unlock();

    *snapshot_id_path = SnapshotManager::instance()->calc_snapshot_id_path(tablet, 3600);
    ASSERT_TRUE(!snapshot_id_path->empty());
    *snapshot_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet, *snapshot_id_path);
    (void)FileUtils::remove_all(*snapshot_dir);
    ASSERT_TRUE(FileUtils::create_dir(*snapshot_dir).ok());

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
    ASSERT_TRUE(FileUtils::list_dirs_files(meta_dir, NULL, &files, Env::Default()).ok());
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet->schema_hash_path() + "/" + f;
        ASSERT_TRUE(Env::Default()->link_file(src, dst).ok());
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
    std::unique_ptr<fs::ReadableBlock> rblock;
    //std::shared_ptr<fs::BlockManager> block_mgr;
    //ASSIGN_OR_ABORT(block_mgr, fs::fs_util::block_manager("posix://"));
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    ASSERT_TRUE(block_mgr->open_block(segment_path, &rblock).ok());

    ASSERT_TRUE(Segment::parse_segment_footer(rblock.get(), footer, nullptr, nullptr).ok());
    rblock->close();
}

void TabletUpdatesTest::test_load_snapshot_incremental_with_partial_rowset_old() {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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
    test_load_snapshot_incremental_with_partial_rowset_old();
}

void TabletUpdatesTest::test_load_snapshot_incremental_with_partial_rowset_new(PartialUpdateCloneCase update_case) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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
        (void)FileUtils::remove_all(snapshot_dir);
        (void)FileUtils::remove_all(snapshot_id_path);
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
    test_load_snapshot_incremental_with_partial_rowset_new(CASE1);
    test_load_snapshot_incremental_with_partial_rowset_new(CASE2);
    test_load_snapshot_incremental_with_partial_rowset_new(CASE3);
    test_load_snapshot_incremental_with_partial_rowset_new(CASE4);
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, load_snapshot_full) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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
    auto tablet2 = load_same_tablet_from_store(_tablet_meta_mem_tracker.get(), tablet1);
    ASSERT_EQ(11, tablet2->updates()->max_version());
    ASSERT_EQ(1, tablet2->updates()->version_history_count());
    EXPECT_EQ(keys0.size(), read_tablet(tablet2, tablet2->updates()->max_version()));
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, load_snapshot_full_file_not_exist) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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

    DeferOp defer1([&]() { (void)FileUtils::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = FileUtils::list_dirs_files(meta_dir, NULL, &files, Env::Default());
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
    auto tablet2 = load_same_tablet_from_store(_tablet_meta_mem_tracker.get(), tablet1);
    ASSERT_EQ(3, tablet2->updates()->max_version());
    ASSERT_EQ(3, tablet2->updates()->version_history_count());
    EXPECT_EQ(keys1.size(), read_tablet(tablet2, tablet2->updates()->max_version()));
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, load_snapshot_full_mismatched_tablet_id) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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

    DeferOp defer1([&]() { (void)FileUtils::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(tablet0, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    ASSERT_TRUE(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = FileUtils::list_dirs_files(meta_dir, NULL, &files, Env::Default());
    ASSERT_TRUE(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = tablet1->schema_hash_path() + "/" + f;
        st = Env::Default()->link_file(src, dst);
        ASSERT_TRUE(st.ok()) << st;
        LOG(INFO) << "Linked " << src << " to " << dst;
    }

    // tablet_id and schema_hash does not match.
    st = tablet1->updates()->load_snapshot(*snapshot_meta);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("mismatched tablet id") != std::string::npos);
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, test_issue_4193) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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
    auto tablet2 = load_same_tablet_from_store(_tablet_meta_mem_tracker.get(), tablet1);
    ASSERT_EQ(13, tablet2->updates()->max_version());
    EXPECT_EQ(keys0.size() + keys1.size(), read_tablet(tablet2, tablet2->updates()->max_version()));
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, test_issue_4181) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());
    auto tablet1 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)tablet_mgr->drop_tablet(tablet1->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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
    auto tablet2 = load_same_tablet_from_store(_tablet_meta_mem_tracker.get(), tablet1);
    ASSERT_EQ(11, tablet2->updates()->max_version());
    EXPECT_EQ(keys0.size(), read_tablet(tablet2, tablet2->updates()->max_version()));
}

// NOLINTNEXTLINE
TEST_F(TabletUpdatesTest, snapshot_with_empty_rowset) {
    srand(GetCurrentTimeMicros());
    auto tablet0 = create_tablet(rand(), rand());

    DeferOp defer([&]() {
        auto tablet_mgr = StorageEngine::instance()->tablet_manager();
        (void)tablet_mgr->drop_tablet(tablet0->tablet_id());
        (void)FileUtils::remove_all(tablet0->schema_hash_path());
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
        (void)FileUtils::remove_all(tablet1->schema_hash_path());
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

TEST_F(TabletUpdatesTest, get_column_values) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    const int N = 8000;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    std::size_t max_rows_per_segment = 1000;
    ASSERT_TRUE(_tablet->rowset_commit(2, create_rowsets(_tablet, keys, max_rows_per_segment)).ok());
    ASSERT_TRUE(_tablet->rowset_commit(3, create_rowsets(_tablet, keys, max_rows_per_segment)).ok());
    std::vector<uint32_t> read_column_ids = {1, 2};
    std::vector<std::unique_ptr<vectorized::Column>> read_columns(read_column_ids.size());
    const auto& tablet_schema = _tablet->tablet_schema();
    for (auto i = 0; i < read_column_ids.size(); i++) {
        const auto read_column_id = read_column_ids[i];
        auto tablet_column = tablet_schema.column(read_column_id);
        auto column =
                vectorized::ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
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
    _tablet->updates()->get_column_values(read_column_ids, false, rowids_by_rssid, &read_columns);
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
    _tablet->updates()->get_column_values(read_column_ids, true, rowids_by_rssid, &read_columns);
    ASSERT_EQ(std::string("[0, ") + values_str_generator(100, 1).substr(1), read_columns[0]->debug_string());
    ASSERT_EQ(std::string("[0, ") + values_str_generator(1000, 2).substr(1), read_columns[1]->debug_string());
}

} // namespace starrocks