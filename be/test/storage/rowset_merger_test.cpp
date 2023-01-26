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

#include "storage/rowset_merger.h"

#include <gtest/gtest.h>

#include "gutil/strings/substitute.h"
#include "runtime/global_dict/types.h"
#include "storage/chunk_helper.h"
#include "storage/empty_iterator.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"

namespace starrocks {

class TestRowsetWriter : public RowsetWriter {
public:
    ~TestRowsetWriter() override = default;

    Status init() override { return Status::OK(); }

    Status add_chunk(const Chunk& chunk) override {
        all_pks->append(*chunk.get_column_by_index(0), 0, chunk.num_rows());
        return Status::OK();
    }

    Status flush_chunk(const Chunk& chunk, SegmentPB* seg_info = nullptr) override { return Status::NotSupported(""); }

    Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes, SegmentPB*) override {
        return Status::NotSupported("");
    }

    Status add_rowset(RowsetSharedPtr rowset) override { return Status::NotSupported(""); }

    Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset, const SchemaMapping& schema_mapping) override {
        return Status::NotSupported("");
    }

    StatusOr<RowsetSharedPtr> build() override { return RowsetSharedPtr(); }

    Version version() override { return {}; }

    int64_t num_rows() override { return all_pks->size(); }

    int64_t total_data_size() override { return 0; }

    RowsetId rowset_id() override { return {}; }

    Status flush() override { return Status::OK(); }
    Status flush_columns() override { return Status::OK(); }
    Status final_flush() override { return Status::OK(); }

    Status add_columns(const Chunk& chunk, const std::vector<uint32_t>& column_indexes, bool is_key) override {
        if (is_key) {
            all_pks->append(*chunk.get_column_by_index(0), 0, chunk.num_rows());
        } else {
            for (size_t i = 0; i < column_indexes.size(); ++i) {
                auto column_index = column_indexes[i];
                DCHECK_LT(column_index - 1, non_key_columns.size());
                non_key_columns[column_index - 1]->append(*chunk.get_column_by_index(i), 0, chunk.num_rows());
            }
        }
        return Status::OK();
    }

    const DictColumnsValidMap& global_dict_columns_valid_info() const override {
        return _global_dict_columns_valid_info;
    }

    std::unique_ptr<Column> all_pks;
    vector<uint32_t> all_rssids;

    vector<std::unique_ptr<Column>> non_key_columns;
    DictColumnsValidMap _global_dict_columns_valid_info;
};

class RowsetMergerTest : public testing::Test {
public:
    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                  Column* one_delete = nullptr) {
        // TODO(cbl): test multi-segment rowsets
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
        auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (long key : keys) {
            cols[0]->append_datum(Datum(key));
            cols[1]->append_datum(Datum((int16_t)(key % 100 + 1)));
            cols[2]->append_datum(Datum((int32_t)(key % 1000 + 2)));
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

    void create_tablet(int64_t tablet_id, int32_t schema_hash) {
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
        auto st = StorageEngine::instance()->create_tablet(request);
        ASSERT_TRUE(st.ok()) << st.to_string();
        _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        ASSERT_TRUE(_tablet);
    }

    void TearDown() override {
        if (_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
    }

protected:
    TabletSharedPtr _tablet;
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

TEST_F(RowsetMergerTest, horizontal_merge) {
    config::vertical_compaction_max_columns_per_group = 5;

    srand(GetCurrentTimeMicros());
    create_tablet(rand(), rand());
    const int max_segments = 8;
    const int num_segment = 1 + rand() % max_segments;
    const int N = 500000 + rand() % 1000000;
    MergeConfig cfg;
    cfg.chunk_size = 1000 + rand() % 2000;
    LOG(INFO) << "merge test #rowset:" << num_segment << " #row:" << N << " chunk_size:" << cfg.chunk_size;
    vector<uint32_t> rssids(N);
    vector<vector<int64_t>> segments(num_segment);
    for (int i = 0; i < N; i++) {
        rssids[i] = rand() % num_segment;
        segments[rssids[i]].push_back(i);
    }
    vector<RowsetSharedPtr> rowsets(num_segment * 2);
    for (int i = 0; i < num_segment; i++) {
        auto rs = create_rowset(_tablet, segments[i]);
        ASSERT_TRUE(_tablet->rowset_commit(i + 2, rs).ok());
        rowsets[i] = rs;
    }

    std::vector<int64_t> pks;
    for (int i = 0; i < num_segment; i++) {
        Int64Column deletes;
        deletes.append_numbers(segments[i].data(), sizeof(int64_t) * segments[i].size() / 2);
        auto rs = create_rowset(_tablet, {}, &deletes);
        ASSERT_TRUE(_tablet->rowset_commit(i + 2 + num_segment, rs).ok());
        rowsets[i + num_segment] = rs;
        pks.insert(pks.end(), segments[i].begin() + segments[i].size() / 2, segments[i].end());
    }
    std::sort(pks.begin(), pks.end());

    int64_t version = num_segment * 2 + 1;
    EXPECT_EQ(pks.size(), read_tablet(_tablet, version));
    TestRowsetWriter writer;
    Schema schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(schema, &writer.all_pks).ok());
    ASSERT_TRUE(compaction_merge_rowsets(*_tablet, version, rowsets, &writer, cfg).ok());
    ASSERT_EQ(pks.size(), writer.all_pks->size());
    const auto* raw_pk_array = reinterpret_cast<const int64_t*>(writer.all_pks->raw_data());

    for (int64_t i = 0; i < pks.size(); i++) {
        ASSERT_EQ(pks[i], raw_pk_array[i]);
    }
}

TEST_F(RowsetMergerTest, vertical_merge) {
    config::vertical_compaction_max_columns_per_group = 1;

    srand(GetCurrentTimeMicros());
    create_tablet(rand(), rand());
    const int max_segments = 8;
    const int num_segment = 2 + rand() % max_segments;
    const int N = 500000 + rand() % 1000000;
    MergeConfig cfg;
    cfg.chunk_size = 1000 + rand() % 2000;
    cfg.algorithm = VERTICAL_COMPACTION;
    vector<uint32_t> rssids(N);
    vector<vector<int64_t>> segments(num_segment);
    for (int i = 0; i < N; i++) {
        rssids[i] = rand() % num_segment;
        segments[rssids[i]].push_back(i);
    }
    vector<RowsetSharedPtr> rowsets(num_segment * 2);
    for (int i = 0; i < num_segment; i++) {
        auto rs = create_rowset(_tablet, segments[i]);
        ASSERT_TRUE(_tablet->rowset_commit(i + 2, rs).ok());
        rowsets[i] = rs;
    }

    std::vector<int64_t> pks;
    for (int i = 0; i < num_segment; i++) {
        Int64Column deletes;
        deletes.append_numbers(segments[i].data(), sizeof(int64_t) * segments[i].size() / 2);
        auto rs = create_rowset(_tablet, {}, &deletes);
        ASSERT_TRUE(_tablet->rowset_commit(i + 2 + num_segment, rs).ok());
        rowsets[i + num_segment] = rs;
        pks.insert(pks.end(), segments[i].begin() + segments[i].size() / 2, segments[i].end());
    }
    std::sort(pks.begin(), pks.end());

    int64_t version = num_segment * 2 + 1;
    EXPECT_EQ(pks.size(), read_tablet(_tablet, version));
    TestRowsetWriter writer;
    Schema schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(schema, &writer.all_pks).ok());
    writer.non_key_columns.emplace_back(std::move(Int16Column::create_mutable()));
    writer.non_key_columns.emplace_back(std::move(Int32Column::create_mutable()));
    ASSERT_TRUE(compaction_merge_rowsets(*_tablet, version, rowsets, &writer, cfg).ok());

    ASSERT_EQ(pks.size(), writer.all_pks->size());
    ASSERT_EQ(2, writer.non_key_columns.size());
    ASSERT_EQ(pks.size(), writer.non_key_columns[0]->size());
    ASSERT_EQ(pks.size(), writer.non_key_columns[1]->size());
    const auto* raw_pk_array = reinterpret_cast<const int64_t*>(writer.all_pks->raw_data());
    const auto* raw_k2_array = reinterpret_cast<const int16_t*>(writer.non_key_columns[0]->raw_data());
    const auto* raw_k3_array = reinterpret_cast<const int32_t*>(writer.non_key_columns[1]->raw_data());
    for (int64_t i = 0; i < pks.size(); i++) {
        ASSERT_EQ(pks[i], raw_pk_array[i]);
        ASSERT_EQ(pks[i] % 100 + 1, raw_k2_array[i]);
        ASSERT_EQ(pks[i] % 1000 + 2, raw_k3_array[i]);
    }
}

TEST_F(RowsetMergerTest, horizontal_merge_seq) {
    config::vertical_compaction_max_columns_per_group = 5;

    srand(GetCurrentTimeMicros());
    create_tablet(rand(), rand());
    const int max_segments = 8;
    const int num_segment = 1 + rand() % max_segments;
    const int N = 500000 + rand() % 1000000;
    MergeConfig cfg;
    cfg.chunk_size = 100 + rand() % 2000;
    // small size test
    //    const int num_segment = 3;
    //    const int N = 30;
    //    MergeConfig cfg;
    //    cfg.chunk_size = 20;
    LOG(INFO) << "seq merge test #rowset:" << num_segment << " #row:" << N << " chunk_size:" << cfg.chunk_size;
    vector<uint32_t> rssids(N);
    vector<vector<int64_t>> segments(num_segment);
    for (int i = 0; i < N; i++) {
        rssids[i] = num_segment * i / N;
        segments[rssids[i]].push_back(i);
    }
    vector<RowsetSharedPtr> rowsets(num_segment * 2);
    for (int i = 0; i < num_segment; i++) {
        auto rs = create_rowset(_tablet, segments[i]);
        ASSERT_TRUE(_tablet->rowset_commit(i + 2, rs).ok());
        rowsets[i] = rs;
    }

    std::vector<int64_t> pks;
    for (int i = 0; i < num_segment; i++) {
        Int64Column deletes;
        deletes.append_numbers(segments[i].data(), sizeof(int64_t) * segments[i].size() / 2);
        auto rs = create_rowset(_tablet, {}, &deletes);
        ASSERT_TRUE(_tablet->rowset_commit(i + 2 + num_segment, rs).ok());
        rowsets[i + num_segment] = rs;
        pks.insert(pks.end(), segments[i].begin() + segments[i].size() / 2, segments[i].end());
    }
    std::sort(pks.begin(), pks.end());

    int64_t version = num_segment * 2 + 1;
    EXPECT_EQ(pks.size(), read_tablet(_tablet, version));
    TestRowsetWriter writer;
    Schema schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(schema, &writer.all_pks).ok());
    ASSERT_TRUE(compaction_merge_rowsets(*_tablet, version, rowsets, &writer, cfg).ok());
    ASSERT_EQ(pks.size(), writer.all_pks->size());
    const auto* raw_pk_array = reinterpret_cast<const int64_t*>(writer.all_pks->raw_data());
    for (int64_t i = 0; i < pks.size(); i++) {
        ASSERT_EQ(pks[i], raw_pk_array[i]);
    }
}

TEST_F(RowsetMergerTest, vertical_merge_seq) {
    config::vertical_compaction_max_columns_per_group = 1;

    srand(GetCurrentTimeMicros());
    create_tablet(rand(), rand());
    const int max_segments = 8;
    const int num_segment = 2 + rand() % max_segments;
    const int N = 500000 + rand() % 1000000;
    MergeConfig cfg;
    cfg.chunk_size = 100 + rand() % 2000;
    cfg.algorithm = VERTICAL_COMPACTION;
    vector<uint32_t> rssids(N);
    vector<vector<int64_t>> segments(num_segment);
    for (int i = 0; i < N; i++) {
        rssids[i] = num_segment * i / N;
        segments[rssids[i]].push_back(i);
    }
    vector<RowsetSharedPtr> rowsets(num_segment * 2);
    for (int i = 0; i < num_segment; i++) {
        auto rs = create_rowset(_tablet, segments[i]);
        ASSERT_TRUE(_tablet->rowset_commit(i + 2, rs).ok());
        rowsets[i] = rs;
    }

    std::vector<int64_t> pks;
    for (int i = 0; i < num_segment; i++) {
        Int64Column deletes;
        deletes.append_numbers(segments[i].data(), sizeof(int64_t) * segments[i].size() / 2);
        auto rs = create_rowset(_tablet, {}, &deletes);
        ASSERT_TRUE(_tablet->rowset_commit(i + 2 + num_segment, rs).ok());
        rowsets[i + num_segment] = rs;
        pks.insert(pks.end(), segments[i].begin() + segments[i].size() / 2, segments[i].end());
    }
    std::sort(pks.begin(), pks.end());

    int64_t version = num_segment * 2 + 1;
    EXPECT_EQ(pks.size(), read_tablet(_tablet, version));
    TestRowsetWriter writer;
    Schema schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
    ASSERT_TRUE(PrimaryKeyEncoder::create_column(schema, &writer.all_pks).ok());
    writer.non_key_columns.emplace_back(std::move(Int16Column::create_mutable()));
    writer.non_key_columns.emplace_back(std::move(Int32Column::create_mutable()));
    ASSERT_TRUE(compaction_merge_rowsets(*_tablet, version, rowsets, &writer, cfg).ok());

    ASSERT_EQ(pks.size(), writer.all_pks->size());
    ASSERT_EQ(2, writer.non_key_columns.size());
    ASSERT_EQ(pks.size(), writer.non_key_columns[0]->size());
    ASSERT_EQ(pks.size(), writer.non_key_columns[1]->size());
    const auto* raw_pk_array = reinterpret_cast<const int64_t*>(writer.all_pks->raw_data());
    const auto* raw_k2_array = reinterpret_cast<const int16_t*>(writer.non_key_columns[0]->raw_data());
    const auto* raw_k3_array = reinterpret_cast<const int32_t*>(writer.non_key_columns[1]->raw_data());
    for (int64_t i = 0; i < pks.size(); i++) {
        ASSERT_EQ(pks[i], raw_pk_array[i]);
        ASSERT_EQ(pks[i] % 100 + 1, raw_k2_array[i]);
        ASSERT_EQ(pks[i] % 1000 + 2, raw_k3_array[i]);
    }
}

} // namespace starrocks
