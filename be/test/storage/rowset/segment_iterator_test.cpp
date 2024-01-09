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

#include "storage/rowset/segment_iterator.h"

#include <memory>
#include <string>
#include <unordered_map>

#include "common/object_pool.h"
#include "fs/fs_memory.h"
#include "gtest/gtest.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"

namespace starrocks {

class SegmentIteratorTest : public ::testing::Test {
public:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kSegmentDir).ok());
        _page_cache_mem_tracker = std::make_unique<MemTracker>();
        StoragePageCache::create_global_cache(_page_cache_mem_tracker.get(), 1000000000);
    }

    void TearDown() override { StoragePageCache::release_global_cache(); }

    const std::string kSegmentDir = "/segment_test";
    std::shared_ptr<MemoryFileSystem> _fs = nullptr;
    std::unique_ptr<MemTracker> _page_cache_mem_tracker = nullptr;
};

// NOLINTNEXTLINE
TEST_F(SegmentIteratorTest, TestGlobalDictNotSuperSet) {
    const int slice_num = 64;
    std::string prefix = "lowcard-";
    std::vector<std::string> values;
    for (int i = 0; i < slice_num; ++i) {
        values.push_back(prefix + std::to_string(i));
    }

    std::sort(values.begin(), values.end());

    std::vector<Slice> data_strs;
    for (const auto& data : values) {
        data_strs.emplace_back(data);
    }

    ColumnPB c1 = create_int_key_pb(1);
    ColumnPB c2 = create_with_default_value_pb("VARCHAR", "");
    c2.set_length(128);

    std::unique_ptr<TabletSchema> tablet_schema = TabletSchemaHelper::create_tablet_schema({c1, c2});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;

    std::string file_name = kSegmentDir + "/low_card_cols";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    SegmentWriter writer(std::move(wfile), 0, tablet_schema.get(), opts);

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = 10000;
    uint64_t file_size = 0;
    uint64_t index_size = 0;

    {
        // col0
        std::vector<uint32_t> column_indexes = {0};
        ASSERT_OK(writer.init(column_indexes, true));
        auto schema = ChunkHelper::convert_schema(*tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
            }
            ASSERT_OK(writer.append_chunk(*chunk));
        }
        ASSERT_OK(writer.finalize_columns(&index_size));
    }
    {
        // col1
        std::vector<uint32_t> column_indexes{1};
        ASSERT_OK(writer.init(column_indexes, false));
        auto schema = ChunkHelper::convert_schema(*tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(data_strs[j % slice_num]));
            }
            ASSERT_OK(writer.append_chunk(*chunk));
        }
        ASSERT_OK(writer.finalize_columns(&index_size));
    }
    ASSERT_OK(writer.finalize_footer(&file_size));

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema.get());
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;

    Schema vec_schema;
    auto f0 = std::make_shared<Field>(0, "c1", TYPE_INT, -1, -1, false);
    f0->set_uid(0);
    auto f1 = std::make_shared<Field>(1, "c2", TYPE_VARCHAR, -1, -1, false);
    f1->set_uid(1);
    vec_schema.append(f0);
    vec_schema.append(f1);

    ObjectPool pool;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;

    auto* con = pool.add(new ConjunctivePredicates());
    auto type_varchar = get_type_info(TYPE_VARCHAR);
    con->add(pool.add(new_column_ge_predicate(type_varchar, 1, Slice(values[8]))));
    seg_opts.delete_predicates.add(*con);

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < 8; ++i) {
        g_dict[Slice(values[i])] = i;
    }
    dict_map[1] = &g_dict;

    seg_opts.global_dictmaps = &dict_map;

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    chunk_iter->init_encoded_schema(dict_map);
    chunk_iter->init_output_schema(std::unordered_set<uint32_t>());

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);

    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
}

// NOLINTNEXTLINE
TEST_F(SegmentIteratorTest, TestGlobalDictNoLocalDict) {
    const int slice_num = 2;
    std::vector<std::string> values;
    const int overflow_sz = 1024 * 1024 + 10; // 1M
    for (int i = 0; i < slice_num; ++i) {
        std::string bigstr;
        bigstr.reserve(overflow_sz);
        for (int j = 0; j < overflow_sz; ++j) {
            bigstr.push_back(j);
        }
        bigstr.push_back(i);
        values.emplace_back(std::move(bigstr));
    }

    std::sort(values.begin(), values.end());

    std::vector<Slice> data_strs;
    for (const auto& data : values) {
        data_strs.emplace_back(data);
    }

    ColumnPB c1 = create_int_key_pb(1);
    ColumnPB c2 = create_with_default_value_pb("VARCHAR", "");
    c2.set_length(overflow_sz + 10);

    std::unique_ptr<TabletSchema> tablet_schema = TabletSchemaHelper::create_tablet_schema({c1, c2});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 1024;

    std::string file_name = kSegmentDir + "/no_dict";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    SegmentWriter writer(std::move(wfile), 0, tablet_schema.get(), opts);

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = slice_num;
    uint64_t file_size = 0;
    uint64_t index_size = 0;

    {
        // col0
        std::vector<uint32_t> column_indexes = {0};
        ASSERT_OK(writer.init(column_indexes, true));
        auto schema = ChunkHelper::convert_schema(*tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
            }
            ASSERT_OK(writer.append_chunk(*chunk));
        }
        ASSERT_OK(writer.finalize_columns(&index_size));
    }
    {
        // col1
        std::vector<uint32_t> column_indexes{1};
        ASSERT_OK(writer.init(column_indexes, false));
        auto schema = ChunkHelper::convert_schema(*tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(data_strs[j % slice_num]));
            }
            ASSERT_OK(writer.append_chunk(*chunk));
        }
        ASSERT_OK(writer.finalize_columns(&index_size));
    }
    ASSERT_OK(writer.finalize_footer(&file_size));

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema.get());
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;

    ColumnIteratorOptions iter_opts;
    ASSIGN_OR_ABORT(auto read_file, _fs->new_random_access_file(segment->file_name()));
    iter_opts.stats = &stats;
    iter_opts.use_page_cache = false;
    iter_opts.read_file = read_file.get();
    iter_opts.check_dict_encoding = true;
    iter_opts.reader_type = READER_QUERY;
    ASSIGN_OR_ABORT(auto scalar_iter, segment->new_column_iterator(1));
    ASSERT_OK(scalar_iter->init(iter_opts));
    ASSERT_FALSE(scalar_iter->all_page_dict_encoded());

    Schema vec_schema;
    auto f0 = std::make_shared<Field>(0, "c1", TYPE_INT, -1, -1, false);
    f0->set_uid(0);
    auto f1 = std::make_shared<Field>(1, "c2", TYPE_VARCHAR, -1, -1, false);
    f1->set_uid(1);
    vec_schema.append(f0);
    vec_schema.append(f1);

    ObjectPool pool;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < slice_num; ++i) {
        g_dict[Slice(values[i])] = i;
    }
    dict_map[1] = &g_dict;

    seg_opts.global_dictmaps = &dict_map;

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    chunk_iter->init_encoded_schema(dict_map);
    chunk_iter->init_output_schema(std::unordered_set<uint32_t>());

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);

    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
}

} // namespace starrocks
