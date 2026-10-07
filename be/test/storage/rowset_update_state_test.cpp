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

#include "storage/rowset_update_state.h"

#include <gtest/gtest.h>

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "common/config_compaction_fwd.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs_memory.h"
#include "platform/key_cache.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/primary_key_compaction_conflict_resolver.h"
#include "storage/rows_mapper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"
#include "storage/tablet_schema.h"
#include "storage/update_compaction_state.h"
#include "storage/update_manager.h"
#include "storage_primitive/empty_iterator.h"
#include "storage_primitive/union_iterator.h"

namespace starrocks {

class RowsetUpdateStateTest : public ::testing::Test {
public:
    void SetUp() override {
        config::enable_transparent_data_encryption = true;
        // add encryption keys
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
        _compaction_mem_tracker = std::make_unique<MemTracker>(-1);
        _metadata_mem_tracker = std::make_unique<MemTracker>();
    }

    void TearDown() override {
        config::enable_transparent_data_encryption = false;
        if (_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
    }

    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                  Column* one_delete = nullptr) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        auto chunk = ChunkFactory::new_chunk(schema, keys.size());
        auto col0 = chunk->get_column_raw_ptr_by_index(0);
        auto col1 = chunk->get_column_raw_ptr_by_index(1);
        auto col2 = chunk->get_column_raw_ptr_by_index(2);
        for (int64_t key : keys) {
            col0->append_datum(Datum(key));
            col1->append_datum(Datum((int16_t)(key % 100 + 1)));
            col2->append_datum(Datum((int32_t)(key % 1000 + 2)));
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

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
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
        request.tablet_schema.columns.emplace_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.emplace_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.emplace_back(k3);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
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

        writer_context.tablet_schema = partial_schema;
        writer_context.referenced_column_ids = column_indexes;
        writer_context.full_tablet_schema = tablet->tablet_schema();
        writer_context.is_partial_update = true;
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(partial_schema);

        auto chunk = ChunkFactory::new_chunk(schema, keys.size());
        EXPECT_TRUE(2 == chunk->num_columns());
        auto cols = chunk->columns();
        for (int64_t key : keys) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(key));
            cols[1]->as_mutable_ptr()->append_datum(Datum((int16_t)(key % 100 + 3)));
        }
        CHECK_OK(writer->flush_chunk(*chunk));
        RowsetSharedPtr partial_rowset = *writer->build();

        return partial_rowset;
    }

    TabletSharedPtr create_varchar_pk_tablet(int64_t tablet_id, int32_t schema_hash) {
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
        TColumnType ctype;
        ctype.__set_type(TPrimitiveType::VARCHAR);
        ctype.__set_len(64);
        k1.__set_column_type(ctype);
        request.tablet_schema.columns.emplace_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.emplace_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.emplace_back(k3);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    // Write one segment per element of `segment_keys`, so the rowset has segment_keys.size() segments.
    RowsetSharedPtr create_multi_segment_varchar_rowset(const TabletSharedPtr& tablet,
                                                        const std::vector<std::vector<std::string>>& segment_keys,
                                                        int16_t v1) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        for (const auto& keys : segment_keys) {
            auto chunk = ChunkFactory::new_chunk(schema, keys.size());
            auto col0 = chunk->get_column_raw_ptr_by_index(0);
            auto col1 = chunk->get_column_raw_ptr_by_index(1);
            auto col2 = chunk->get_column_raw_ptr_by_index(2);
            for (const auto& key : keys) {
                col0->append_datum(Datum(Slice(key)));
                col1->append_datum(Datum(v1));
                col2->append_datum(Datum((int32_t)key.size()));
            }
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        return *writer->build();
    }

protected:
    TabletSharedPtr _tablet;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
    std::unique_ptr<MemTracker> _metadata_mem_tracker;
};

static ChunkIteratorPtr create_tablet_iterator(TabletReader& reader, Schema& schema) {
    TabletReaderParams params;
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
        return new_empty_iterator(schema, DEFAULT_CHUNK_SIZE);
    }
    return new_union_iterator(seg_iters);
}

static ssize_t read_until_eof(const ChunkIteratorPtr& iter) {
    auto chunk = ChunkFactory::new_chunk(iter->schema(), 100);
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
    Schema schema = ChunkHelper::convert_schema(tablet->tablet_schema());
    TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    return read_until_eof(iter);
}

TEST_F(RowsetUpdateStateTest, with_deletes) {
    const int N = 100;
    _tablet = create_tablet(rand(), rand());
    // create full rowsets first
    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    std::vector<int64_t> delete_keys;
    for (int i = 0; i < N / 2; i++) {
        delete_keys.emplace_back(N + i);
    }
    Int64Column deletes;
    deletes.append_numbers(delete_keys.data(), sizeof(int64_t) * delete_keys.size());
    RowsetSharedPtr rowset = create_rowset(_tablet, keys, &deletes);
    auto st = _tablet->rowset_commit(2, rowset, 2000);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(2, _tablet->updates()->max_version());
}

TEST_F(RowsetUpdateStateTest, prepare_partial_update_states) {
    const int N = 100;
    _tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, _tablet->updates()->version_history_count());

    // create full rowsets first
    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(10);
    for (int i = 0; i < 10; i++) {
        rowsets.emplace_back(create_rowset(_tablet, keys));
    }
    auto pool = StorageEngine::instance()->update_manager()->apply_thread_pool();
    for (int i = 0; i < rowsets.size(); i++) {
        auto version = i + 2;
        auto st = _tablet->rowset_commit(version, rowsets[i], 0);
        ASSERT_TRUE(st.ok()) << st.to_string();
        // Ensure that there is at most one thread doing the version apply job.
        ASSERT_LE(pool->num_threads(), 1);
        ASSERT_EQ(version, _tablet->updates()->max_version());
        ASSERT_EQ(version, _tablet->updates()->version_history_count());
    }
    ASSERT_EQ(N, read_tablet(_tablet, rowsets.size()));

    std::vector<int32_t> column_indexes = {0, 1};
    std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(_tablet->tablet_schema(), column_indexes);
    RowsetSharedPtr partial_rowset = create_partial_rowset(_tablet, keys, column_indexes, partial_schema);
    // check data of write column
    RowsetUpdateState state;
    state.load(_tablet.get(), partial_rowset.get());
    const std::vector<PartialUpdateState>& parital_update_states = state.parital_update_states();
    ASSERT_EQ(parital_update_states.size(), 1);
    ASSERT_EQ(parital_update_states[0].src_rss_rowids.size(), N);
    ASSERT_EQ(parital_update_states[0].write_columns.size(), 1);
    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ((int32_t)(keys[i] % 1000 + 2), parital_update_states[0].write_columns[0]->get(i).get_int32());
    }
}

TEST_F(RowsetUpdateStateTest, check_conflict) {
    // create full rowset first
    const int N = 100;
    _tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, _tablet->updates()->version_history_count());
    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    RowsetSharedPtr rowset = create_rowset(_tablet, keys);
    auto pool = StorageEngine::instance()->update_manager()->apply_thread_pool();
    auto version = 2;
    auto st = _tablet->rowset_commit(version, rowset, 0);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_LE(pool->num_threads(), 1);
    ASSERT_EQ(version, _tablet->updates()->max_version());
    ASSERT_EQ(version, _tablet->updates()->version_history_count());
    ASSERT_EQ(N, read_tablet(_tablet, 2));

    // create partial_rowset
    std::vector<int32_t> column_indexes = {0, 1};
    std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(_tablet->tablet_schema(), column_indexes);
    RowsetSharedPtr partial_rowset = create_partial_rowset(_tablet, keys, column_indexes, partial_schema);
    RowsetUpdateState state;
    state.load(_tablet.get(), partial_rowset.get());
    const std::vector<PartialUpdateState>& parital_update_states = state.parital_update_states();
    ASSERT_EQ(parital_update_states.size(), 1);
    ASSERT_EQ(parital_update_states[0].src_rss_rowids.size(), N);
    ASSERT_EQ(parital_update_states[0].write_columns.size(), 1);
    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ((int32_t)(keys[i] % 1000 + 2), parital_update_states[0].write_columns[0]->get(i).get_int32());
    }

    // create new rowset to make conflict
    RowsetWriterContext writer_context;
    RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
    writer_context.rowset_id = rowset_id;
    writer_context.tablet_id = _tablet->tablet_id();
    writer_context.tablet_schema_hash = _tablet->schema_hash();
    writer_context.partition_id = 0;
    writer_context.rowset_path_prefix = _tablet->schema_hash_path();
    writer_context.rowset_state = COMMITTED;
    writer_context.tablet_schema = _tablet->tablet_schema();
    writer_context.version.first = 0;
    writer_context.version.second = 0;
    writer_context.segments_overlap = NONOVERLAPPING;
    std::unique_ptr<RowsetWriter> writer;
    EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
    auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    auto chunk = ChunkFactory::new_chunk(schema, N);
    auto cols = chunk->columns();
    for (uint64_t i = 0; i < N; i++) {
        cols[0]->as_mutable_ptr()->append_datum(Datum(i));
        cols[1]->as_mutable_ptr()->append_datum(Datum((int16_t)(i % 100 + 1)));
        cols[2]->as_mutable_ptr()->append_datum(Datum((int32_t)(i % 1000 + 3)));
    }
    CHECK_OK(writer->flush_chunk(*chunk));
    RowsetSharedPtr new_rowset = *writer->build();
    version = 3;
    st = _tablet->rowset_commit(version, new_rowset, 0);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_LE(pool->num_threads(), 1);
    ASSERT_EQ(version, _tablet->updates()->max_version());
    ASSERT_EQ(version, _tablet->updates()->version_history_count());
    ASSERT_EQ(N, read_tablet(_tablet, 3));

    // check and resolve conflict
    EditVersion latest_applied_version(3, 0);
    auto manager = StorageEngine::instance()->update_manager();
    auto index_entry = manager->index_cache().get_or_create(_tablet->tablet_id());
    auto& index = index_entry->value();
    st = index.load(_tablet.get());
    std::vector<uint32_t> read_column_ids = {2};
    state.test_check_conflict(_tablet.get(), partial_rowset.get(), partial_rowset->rowset_meta()->get_rowset_seg_id(),
                              0, latest_applied_version, read_column_ids, index);

    // check data of write column
    const std::vector<PartialUpdateState>& new_parital_update_states = state.parital_update_states();
    ASSERT_EQ(new_parital_update_states.size(), 1);
    ASSERT_EQ(new_parital_update_states[0].src_rss_rowids.size(), N);
    ASSERT_EQ(new_parital_update_states[0].write_columns.size(), 1);
    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ((int32_t)(keys[i] % 1000 + 3), new_parital_update_states[0].write_columns[0]->get(i).get_int32());
    }

    manager->index_cache().release(index_entry);
}

// Primary keys must be written in sorted order, within a segment and across the segments of a rowset.
// Zero-pad the number so that the lexicographic order of the keys matches their numeric order.
static std::string make_pk(size_t k) {
    std::string digits = std::to_string(k);
    return "key_" + std::string(digits.size() < 6 ? 6 - digits.size() : 0, '0') + digits;
}

static std::vector<std::vector<std::string>> make_segment_keys(size_t num_segments, size_t rows_per_segment,
                                                               size_t key_begin) {
    std::vector<std::vector<std::string>> segment_keys(num_segments);
    size_t key = key_begin;
    for (auto& keys : segment_keys) {
        for (size_t i = 0; i < rows_per_segment; i++) {
            keys.emplace_back(make_pk(key++));
        }
    }
    return segment_keys;
}

// Read every row of a VARCHAR primary key tablet created by create_varchar_pk_tablet() as pk -> v1.
static Status read_varchar_pk_rows(const TabletSharedPtr& tablet, int64_t version,
                                   std::map<std::string, int16_t>* rows) {
    Schema schema = ChunkHelper::convert_schema(tablet->tablet_schema());
    TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return Status::InternalError("failed to create tablet iterator");
    }
    auto chunk = ChunkFactory::new_chunk(iter->schema(), 100);
    while (true) {
        chunk->reset();
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(st);
        for (size_t i = 0; i < chunk->num_rows(); i++) {
            auto key = chunk->get_column_by_index(0)->get(i).get_slice().to_string();
            if (!rows->emplace(key, chunk->get_column_by_index(1)->get(i).get_int16()).second) {
                return Status::InternalError("duplicate key " + key);
            }
        }
    }
}

// load_upserts() used to create the encoded primary keys of segments loaded during apply as a
// LargeBinaryColumn, while _do_load() preloaded segment 0 as a BinaryColumn. BinaryColumn grows its
// offsets to 64 bits on demand, so every segment must now be a BinaryColumn holding the segment's keys.
TEST_F(RowsetUpdateStateTest, load_upserts_uses_binary_column_for_every_segment) {
    const size_t kSegments = 3;
    const size_t kRowsPerSegment = 100;
    _tablet = create_varchar_pk_tablet(rand(), rand());
    auto segment_keys = make_segment_keys(kSegments, kRowsPerSegment, 0);
    RowsetSharedPtr rowset = create_multi_segment_varchar_rowset(_tablet, segment_keys, 1);
    ASSERT_EQ(static_cast<int64_t>(kSegments), rowset->num_segments());

    RowsetUpdateState state;
    ASSERT_OK(state.load(_tablet.get(), rowset.get()));
    for (uint32_t i = 0; i < kSegments; i++) {
        ASSERT_OK(state.load_upserts(i));
        const auto& pks = state.upserts()[i];
        ASSERT_NE(nullptr, pks) << "segment " << i;
        EXPECT_TRUE(pks->is_binary()) << "segment " << i << " is " << pks->get_name();
        EXPECT_FALSE(pks->is_large_binary()) << "segment " << i;
        ASSERT_EQ(kRowsPerSegment, pks->size()) << "segment " << i;
        for (size_t j = 0; j < kRowsPerSegment; j++) {
            ASSERT_EQ(segment_keys[i][j], pks->get(j).get_slice().to_string()) << "segment " << i << " row " << j;
        }
    }
}

// Apply two multi-segment rowsets with overlapping VARCHAR primary keys: every segment after the first
// goes through load_upserts() during apply, and the second rowset must replace the overlapping rows.
TEST_F(RowsetUpdateStateTest, apply_multi_segment_varchar_upserts) {
    const size_t kSegments = 3;
    const size_t kRowsPerSegment = 100;
    const size_t kTotalRows = kSegments * kRowsPerSegment;
    _tablet = create_varchar_pk_tablet(rand(), rand());

    // keys [0, 300) with v1 = 1
    RowsetSharedPtr rowset1 =
            create_multi_segment_varchar_rowset(_tablet, make_segment_keys(kSegments, kRowsPerSegment, 0), 1);
    ASSERT_EQ(static_cast<int64_t>(kSegments), rowset1->num_segments());
    auto st = _tablet->rowset_commit(2, rowset1, 0);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(2, _tablet->updates()->max_version());
    ASSERT_EQ(static_cast<ssize_t>(kTotalRows), read_tablet(_tablet, 2));

    // keys [150, 450) with v1 = 2: overwrites [150, 300) and adds [300, 450)
    const size_t kOverlapBegin = kTotalRows / 2;
    RowsetSharedPtr rowset2 = create_multi_segment_varchar_rowset(
            _tablet, make_segment_keys(kSegments, kRowsPerSegment, kOverlapBegin), 2);
    ASSERT_EQ(static_cast<int64_t>(kSegments), rowset2->num_segments());
    st = _tablet->rowset_commit(3, rowset2, 0);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(3, _tablet->updates()->max_version());

    std::map<std::string, int16_t> rows;
    ASSERT_OK(read_varchar_pk_rows(_tablet, 3, &rows));
    ASSERT_EQ(kOverlapBegin + kTotalRows, rows.size());
    for (size_t k = 0; k < kOverlapBegin + kTotalRows; k++) {
        auto it = rows.find(make_pk(k));
        ASSERT_TRUE(it != rows.end()) << "missing " << make_pk(k);
        ASSERT_EQ(k < kOverlapBegin ? 1 : 2, it->second) << make_pk(k);
    }
}

// CompactionState::_load_segments() used to create every segment's encoded primary keys as a
// LargeBinaryColumn. Load a three-segment rowset the way _apply_compaction_commit does (load() preloads
// segment 0, load_segments() loads the rest) and check each segment is a BinaryColumn with its keys.
TEST_F(RowsetUpdateStateTest, compaction_state_load_segments_uses_binary_column) {
    const size_t kSegments = 3;
    const size_t kRowsPerSegment = 100;
    _tablet = create_varchar_pk_tablet(rand(), rand());
    auto segment_keys = make_segment_keys(kSegments, kRowsPerSegment, 0);
    RowsetSharedPtr rowset = create_multi_segment_varchar_rowset(_tablet, segment_keys, 1);
    ASSERT_EQ(static_cast<int64_t>(kSegments), rowset->num_segments());

    CompactionState state;
    ASSERT_OK(state.load(rowset.get()));
    ASSERT_EQ(kSegments, state.pk_cols.size());
    for (uint32_t i = 0; i < kSegments; i++) {
        ASSERT_OK(state.load_segments(rowset.get(), i));
        const auto& pks = state.pk_cols[i];
        ASSERT_NE(nullptr, pks) << "segment " << i;
        EXPECT_TRUE(pks->is_binary()) << "segment " << i << " is " << pks->get_name();
        EXPECT_FALSE(pks->is_large_binary()) << "segment " << i;
        ASSERT_EQ(kRowsPerSegment, pks->size()) << "segment " << i;
        for (size_t j = 0; j < kRowsPerSegment; j++) {
            ASSERT_EQ(segment_keys[i][j], pks->get(j).get_slice().to_string()) << "segment " << i << " row " << j;
        }
        state.release_segment(i);
        ASSERT_EQ(nullptr, state.pk_cols[i]) << "segment " << i;
    }
}

// With light PK compaction publish disabled, compaction apply goes through CompactionState. Compact three
// multi-segment rowsets with overlapping VARCHAR primary keys and check every key keeps its newest value.
TEST_F(RowsetUpdateStateTest, compaction_without_light_publish_varchar_pk) {
    const bool old_light_publish = config::enable_light_pk_compaction_publish;
    config::enable_light_pk_compaction_publish = false;
    DeferOp restore_config([&]() { config::enable_light_pk_compaction_publish = old_light_publish; });

    const size_t kSegments = 3;
    const size_t kRowsPerSegment = 100;
    _tablet = create_varchar_pk_tablet(rand(), rand());

    // version 2: keys [0, 300) with v1 = 1
    // version 3: keys [150, 450) with v1 = 2
    // version 4: keys [0, 100) with v1 = 3
    ASSERT_OK(_tablet->rowset_commit(
            2, create_multi_segment_varchar_rowset(_tablet, make_segment_keys(kSegments, kRowsPerSegment, 0), 1), 0));
    ASSERT_OK(_tablet->rowset_commit(
            3, create_multi_segment_varchar_rowset(_tablet, make_segment_keys(kSegments, kRowsPerSegment, 150), 2), 0));
    ASSERT_OK(_tablet->rowset_commit(4, create_multi_segment_varchar_rowset(_tablet, make_segment_keys(1, 100, 0), 3),
                                     0));
    ASSERT_EQ(4, _tablet->updates()->max_version());
    // read_tablet() waits until version 4 is applied, so compaction sees all three rowsets.
    ASSERT_EQ(450, read_tablet(_tablet, 4));
    ASSERT_EQ(3u, _tablet->updates()->num_rowsets());

    // compaction() can return OK after its apply wait times out, so wait for the compaction apply itself;
    // otherwise the checks below could read the pre-compaction rowsets and never exercise CompactionState.
    ASSERT_OK(_tablet->updates()->compaction(_compaction_mem_tracker.get()));
    _tablet->updates()->wait_apply_done();
    EditVersion applied_version;
    ASSERT_OK(_tablet->updates()->get_latest_applied_version(&applied_version));
    ASSERT_EQ(EditVersion(4, 1), applied_version);
    ASSERT_EQ(1u, _tablet->updates()->num_rowsets());
    ASSERT_OK(_tablet->verify());

    std::map<std::string, int16_t> rows;
    ASSERT_OK(read_varchar_pk_rows(_tablet, 4, &rows));
    ASSERT_EQ(450u, rows.size());
    for (size_t k = 0; k < 450; k++) {
        auto it = rows.find(make_pk(k));
        ASSERT_TRUE(it != rows.end()) << "missing " << make_pk(k);
        const int16_t expected = k < 100 ? 3 : (k < 150 ? 1 : 2);
        ASSERT_EQ(expected, it->second) << make_pk(k);
    }
}

// Resolver driving PrimaryKeyCompactionConflictResolver::execute() over a real rowset and rows mapper file, but
// recording the primary key columns handed to replace_rows() instead of updating a primary index.
class RecordingCompactionConflictResolver : public PrimaryKeyCompactionConflictResolver {
public:
    struct ReplaceCall {
        uint32_t rssid;
        uint32_t rowid_start;
        std::vector<uint32_t> replace_indexes;
        bool is_binary;
        bool is_large_binary;
        std::string column_name;
        std::vector<std::string> keys;
    };

    RecordingCompactionConflictResolver(Rowset* rowset, std::string mapper_path, DelvecLoader* delvec_loader)
            : _rowset(rowset), _mapper_path(std::move(mapper_path)), _delvec_loader(delvec_loader) {}

    StatusOr<FileInfo> filename() const override { return FileInfo{.path = _mapper_path}; }

    StatusOr<PrimaryKeyEncodingType> primary_key_encoding_type() const override {
        return PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1;
    }

    Schema generate_pkey_schema() override {
        const auto& schema = _rowset->schema();
        std::vector<uint32_t> pk_columns;
        for (size_t i = 0; i < schema->num_key_columns(); i++) {
            pk_columns.push_back(static_cast<uint32_t>(i));
        }
        return ChunkHelper::convert_schema(schema, pk_columns);
    }

    Status segment_iterator(
            const std::function<Status(const CompactConflictResolveParams&, const std::vector<ChunkIteratorPtr>&,
                                       const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>& handler)
            override {
        OlapReaderStatistics stats;
        auto pkey_schema = generate_pkey_schema();
        ASSIGN_OR_RETURN(auto segment_iters, _rowset->get_segment_iterators2(pkey_schema, _rowset->schema(),
                                                                             MetaLoadMode::NONE, 0, &stats));
        CompactConflictResolveParams params;
        params.tablet_id = _rowset->rowset_meta()->tablet_id();
        params.rowset_id = _rowset->rowset_meta()->get_rowset_seg_id();
        params.base_version = 2;
        params.new_version = 3;
        params.delvec_loader = _delvec_loader;
        params.replace_rows = [this](uint32_t rssid, uint32_t rowid_start, const std::vector<uint32_t>& replace_indexes,
                                     const Column& pks) {
            ReplaceCall call{rssid,          rowid_start, replace_indexes, pks.is_binary(), pks.is_large_binary(),
                             pks.get_name(), {}};
            for (uint32_t idx : replace_indexes) {
                call.keys.emplace_back(pks.get(idx).get_slice().to_string());
            }
            replace_calls.emplace_back(std::move(call));
            return Status::OK();
        };
        return handler(params, segment_iters, [this](uint32_t rssid, const DelVectorPtr& dv, uint32_t num_dels) {
            num_dels_by_rssid[rssid] = num_dels;
        });
    }

    Status segment_iterator(
            const std::function<
                    Status(const CompactConflictResolveParams&, const std::vector<std::shared_ptr<Segment>>&,
                           const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>&)>& handler) override {
        return Status::NotSupported("not used by execute()");
    }

    std::vector<ReplaceCall> replace_calls;
    std::map<uint32_t, uint32_t> num_dels_by_rssid;

private:
    Rowset* _rowset;
    std::string _mapper_path;
    DelvecLoader* _delvec_loader;
};

// Marks the given rowids of one input rssid as deleted; every other input rssid has no deletes.
class FixedDelvecLoader : public DelvecLoader {
public:
    FixedDelvecLoader(uint32_t rssid, std::vector<uint32_t> deleted_rowids)
            : _rssid(rssid), _deleted_rowids(std::move(deleted_rowids)) {}

    Status load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) override {
        auto dv = std::make_shared<DelVector>();
        if (tsid.segment_id == _rssid) {
            dv->init(version, _deleted_rowids.data(), _deleted_rowids.size());
        } else {
            dv->init(version, nullptr, 0);
        }
        *pdelvec = std::move(dv);
        return Status::OK();
    }

private:
    uint32_t _rssid;
    std::vector<uint32_t> _deleted_rowids;
};

// PrimaryKeyCompactionConflictResolver::execute() (the light PK compaction publish path, used by default for
// both local and shared-data tables) used to encode the output rowset's primary keys into a LargeBinaryColumn.
// Resolve a three-segment VARCHAR primary key rowset whose input rows are partly deleted, and check that every
// batch passed to replace_rows() is a BinaryColumn with exactly the surviving keys, and that deleted rows go to
// the output delvec.
TEST_F(RowsetUpdateStateTest, compaction_conflict_resolver_uses_binary_column) {
    const size_t kSegments = 3;
    const size_t kRowsPerSegment = 100;
    const uint32_t kInputRssid = 1000;
    _tablet = create_varchar_pk_tablet(rand(), rand());
    auto segment_keys = make_segment_keys(kSegments, kRowsPerSegment, 0);
    RowsetSharedPtr rowset = create_multi_segment_varchar_rowset(_tablet, segment_keys, 1);
    ASSERT_EQ(static_cast<int64_t>(kSegments), rowset->num_segments());

    // Output row r comes from input (kInputRssid, r); every 10th input row was deleted after compaction started.
    const std::string mapper_path = _tablet->schema_hash_path() + "/compaction_conflict_resolver_test.crm";
    DeferOp remove_mapper([&]() { (void)FileSystem::Default()->delete_file(mapper_path); });
    std::vector<uint64_t> rssid_rowids;
    std::vector<uint32_t> deleted_rowids;
    for (uint32_t r = 0; r < kSegments * kRowsPerSegment; r++) {
        rssid_rowids.push_back((static_cast<uint64_t>(kInputRssid) << 32) | r);
        if (r % 10 == 0) {
            deleted_rowids.push_back(r);
        }
    }
    RowsMapperBuilder mapper_builder(mapper_path);
    ASSERT_OK(mapper_builder.append(rssid_rowids));
    ASSERT_OK(mapper_builder.finalize());

    FixedDelvecLoader delvec_loader(kInputRssid, deleted_rowids);
    RecordingCompactionConflictResolver resolver(rowset.get(), mapper_path, &delvec_loader);
    ASSERT_OK(resolver.execute());

    // Each segment fits in one batch (primary_key_compaction_replace_batch_rows is far above 100 rows).
    ASSERT_EQ(kSegments, resolver.replace_calls.size());
    const uint32_t base_rssid = rowset->rowset_meta()->get_rowset_seg_id();
    for (uint32_t i = 0; i < kSegments; i++) {
        const auto& call = resolver.replace_calls[i];
        EXPECT_EQ(base_rssid + i, call.rssid);
        EXPECT_EQ(0u, call.rowid_start);
        EXPECT_TRUE(call.is_binary) << "segment " << i << " is " << call.column_name;
        EXPECT_FALSE(call.is_large_binary) << "segment " << i;
        std::vector<std::string> expected_keys;
        std::vector<uint32_t> expected_indexes;
        for (uint32_t j = 0; j < kRowsPerSegment; j++) {
            if ((i * kRowsPerSegment + j) % 10 != 0) {
                expected_indexes.push_back(j);
                expected_keys.push_back(segment_keys[i][j]);
            }
        }
        EXPECT_EQ(expected_indexes, call.replace_indexes) << "segment " << i;
        EXPECT_EQ(expected_keys, call.keys) << "segment " << i;
        EXPECT_EQ(kRowsPerSegment / 10, resolver.num_dels_by_rssid[base_rssid + i]) << "segment " << i;
    }
}

} // namespace starrocks
