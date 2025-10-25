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

#include <fmt/core.h>

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/object_pool.h"
#include "fs/fs_memory.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "gtest/gtest.h"
#include "runtime/global_dict/types.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/olap_common.h"
#include "storage/record_predicate/record_predicate_helper.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"
#include "types/logical_type.h"

namespace starrocks {

class SegmentIteratorTest : public ::testing::Test {
public:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kSegmentDir).ok());
    }

    void TearDown() override {}

    const std::string kSegmentDir = "/segment_test";
    std::shared_ptr<MemoryFileSystem> _fs = nullptr;
};

namespace test {
struct TabletSchemaBuilder {
private:
    std::vector<ColumnPB> _column_pbs;
    ColumnPB _create_pb(int32_t id, std::string name, bool nullable, LogicalType type, bool key) {
        ColumnPB col;

        col.set_unique_id(id);
        col.set_name(name);
        col.set_is_key(key);
        col.set_is_nullable(nullable);

        if (type == TYPE_INT) {
            col.set_type("INT");
            col.set_length(4);
            col.set_index_length(4);
        } else if (type == TYPE_VARCHAR) {
            col.set_type("VARCHAR");
            col.set_length(128);
            col.set_index_length(16);
        } else if (type == TYPE_CHAR) {
            col.set_type("CHAR");
            col.set_length(20);
            col.set_index_length(20);
        }

        col.set_default_value("0");
        col.set_aggregation("NONE");
        col.set_is_bf_column(false);
        col.set_has_bitmap_index(false);
        return col;
    }

public:
    TabletSchemaBuilder& create(int32_t id, bool nullable, LogicalType type, bool key = false) {
        if (type == TYPE_INT) {
            _column_pbs.emplace_back(_create_pb(id, std::to_string(id), nullable, type, key));
        } else if (type == TYPE_VARCHAR) {
            _column_pbs.emplace_back(_create_pb(id, std::to_string(id), nullable, type, key));
        } else if (type == TYPE_CHAR) {
            _column_pbs.emplace_back(_create_pb(id, std::to_string(id), nullable, type, key));
        } else {
            __builtin_unreachable();
        }
        return *this;
    }
    TabletSchemaBuilder& set_length(size_t length) {
        _column_pbs.back().set_length(length);
        return *this;
    }

    std::unique_ptr<TabletSchema> build() { return TabletSchemaHelper::create_tablet_schema(_column_pbs); }
};

struct TabletDataBuilder {
    TabletDataBuilder(SegmentWriter& writer_, std::shared_ptr<TabletSchema> schema, size_t chunk_size_,
                      size_t num_rows_)
            : writer(writer_), _schema(schema), chunk_size(chunk_size_), num_rows(num_rows_) {}

    template <class Provider>
    Status append(int32_t idx, Provider&& provider) {
        std::vector<uint32_t> column_indexes = {static_cast<unsigned int>(idx)};

        RETURN_IF_ERROR(writer.init(column_indexes, true));

        auto schema = ChunkHelper::convert_schema(_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(provider(static_cast<int32_t>(i * chunk_size + j)));
            }
            RETURN_IF_ERROR(writer.append_chunk(*chunk));
        }

        RETURN_IF_ERROR(writer.finalize_columns(&index_size));
        return Status::OK();
    }

    Status finalize_footer() { return writer.finalize_footer(&file_size); }

private:
    SegmentWriter& writer;
    std::shared_ptr<TabletSchema> _schema;
    const size_t chunk_size;
    const size_t num_rows;

    uint64_t file_size = 0;
    uint64_t index_size = 0;
};

struct VecSchemaBuilder {
    VecSchemaBuilder& add(int32_t id, std::string name, LogicalType type, bool nullable = false) {
        auto f = std::make_shared<Field>(id, name, type, -1, -1, nullable);
        f->set_uid(id);
        vec_schema.append(f);
        return *this;
    }
    Schema build() { return std::move(vec_schema); }

private:
    Schema vec_schema;
};

} // namespace test

// This case is only triggered by dictionary inconsistencies.
// NOLINTNEXTLINE
TEST_F(SegmentIteratorTest, TestGlobalDictNotSuperSetWithUnusedColumn) {
    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/low_card_cols_unused_column";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_INT, true)
                                                          .create(2, false, TYPE_VARCHAR)
                                                          .create(3, false, TYPE_INT)
                                                          .create(4, false, TYPE_INT)
                                                          .create(5, false, TYPE_VARCHAR)
                                                          .build();
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    const size_t num_rows = 10000;

    auto i32_provider = [](int32_t i) { return i; };
    std::vector<std::string> values(64);
    for (int i = 0; i < values.size(); ++i) {
        values[i] = fmt::format("prefix-{}", i);
    }
    auto slice_provider = [&values](int32_t i) { return Slice(values[i % values.size()]); };

    // tablet data builder
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, i32_provider));
    ASSERT_OK(segment_data_builder.append(1, slice_provider));
    ASSERT_OK(segment_data_builder.append(2, i32_provider));
    ASSERT_OK(segment_data_builder.append(3, i32_provider));
    ASSERT_OK(segment_data_builder.append(4, slice_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    //
    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;
    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT)
            .add(1, "c1", TYPE_VARCHAR)
            .add(2, "c2", TYPE_INT)
            .add(3, "c3", TYPE_INT)
            .add(4, "c4", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();
    ObjectPool pool;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;

    //
    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict1;
    GlobalDictMap g_dict2;
    for (int i = 0; i < values.size() - 1; ++i) {
        g_dict1[Slice(values[i])] = i;
        g_dict2[Slice(values[i])] = i;
    }
    g_dict2[Slice(values[values.size() - 1])] = values.size() - 1;
    dict_map[1] = &g_dict1;
    dict_map[4] = &g_dict2;
    seg_opts.global_dictmaps = &dict_map;
    seg_opts.tablet_schema = tablet_schema;

    std::unique_ptr<ColumnPredicate> predicate;
    predicate.reset(new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 1, "prefix"));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate.get()});
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(dict_map));
    std::unordered_set<uint32_t> set;
    set.insert(1);
    ASSERT_OK(chunk_iter->init_output_schema(set));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);

    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
}

// NOLINTNEXTLINE
TEST_F(SegmentIteratorTest, TestGlobalDictNoLocalDictWithUnusedColumn) {
    // prepare dict data
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

    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/no_dict_unused_column";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_INT, true)
                                                          .create(2, false, TYPE_VARCHAR)
                                                          .set_length(overflow_sz + 10)
                                                          .build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 1024;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = slice_num;

    auto i32_provider = [](int32_t i) { return i; };
    auto slice_provider = [&data_strs](int32_t i) { return data_strs[i % data_strs.size()]; };

    // tablet data builder
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, i32_provider));
    ASSERT_OK(segment_data_builder.append(1, slice_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;
    seg_options.tablet_schema = tablet_schema;

    ColumnIteratorOptions iter_opts;
    ASSIGN_OR_ABORT(auto read_file, _fs->new_random_access_file(segment->file_name()));
    iter_opts.stats = &stats;
    iter_opts.use_page_cache = false;
    iter_opts.read_file = read_file.get();
    iter_opts.check_dict_encoding = true;
    iter_opts.reader_type = READER_QUERY;

    ASSIGN_OR_ABORT(auto scalar_iter, segment->new_column_iterator(tablet_schema->column(1), nullptr));
    ASSERT_OK(scalar_iter->init(iter_opts));
    ASSERT_FALSE(scalar_iter->all_page_dict_encoded());

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    ObjectPool pool;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < slice_num; ++i) {
        g_dict[Slice(values[i])] = i;
    }
    dict_map[1] = &g_dict;

    seg_opts.global_dictmaps = &dict_map;
    std::unique_ptr<ColumnPredicate> predicate;
    predicate.reset(new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 1, values[0].c_str()));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate.get()});
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(dict_map));
    std::unordered_set<uint32_t> set;
    set.insert(1);
    ASSERT_OK(chunk_iter->init_output_schema(set));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);

    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
}

// NOLINTNEXTLINE
TEST_F(SegmentIteratorTest, TestGlobalDictNotSuperSet) {
    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/low_card_cols";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema =
            builder.create(1, false, TYPE_INT, true).create(2, false, TYPE_VARCHAR).build();
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    const size_t num_rows = 10000;

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

    auto i32_provider = [](int32_t i) { return i; };
    auto slice_provider = [&data_strs](int32_t i) { return data_strs[i % data_strs.size()]; };

    // tablet data builder
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, i32_provider));
    ASSERT_OK(segment_data_builder.append(1, slice_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

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
    ASSERT_OK(chunk_iter->init_encoded_schema(dict_map));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

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
    // prepare dict data
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

    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/no_dict";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_INT, true)
                                                          .create(2, false, TYPE_VARCHAR)
                                                          .set_length(overflow_sz + 10)
                                                          .build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 1024;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = slice_num;

    auto i32_provider = [](int32_t i) { return i; };
    auto slice_provider = [&data_strs](int32_t i) { return data_strs[i % data_strs.size()]; };

    // tablet data builder
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, i32_provider));
    ASSERT_OK(segment_data_builder.append(1, slice_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
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
    ASSIGN_OR_ABORT(auto scalar_iter, segment->new_column_iterator(tablet_schema->column(1), nullptr));
    ASSERT_OK(scalar_iter->init(iter_opts));
    ASSERT_FALSE(scalar_iter->all_page_dict_encoded());

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

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
    ASSERT_TRUE(chunk_iter->init_encoded_schema(dict_map).ok());
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);

    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    res_chunk->reset();
}

TEST_F(SegmentIteratorTest, testBasicColumnHashIsCongruentFilter) {
    const int slice_num = 6;
    std::vector<std::string> values;
    const int overflow_sz = 32;
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

    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/basic_column_hash_is_congruent_filter";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_VARCHAR, true)
                                                          .set_length(2048)
                                                          .create(2, false, TYPE_VARCHAR, false)
                                                          .set_length(2048)
                                                          .build();
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 1024;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = slice_num;

    auto slice_provider = [&data_strs](int32_t i) { return data_strs[i % data_strs.size()]; };

    // tablet data builder
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, slice_provider));
    ASSERT_OK(segment_data_builder.append(1, slice_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < slice_num; ++i) {
        g_dict[Slice(values[i])] = i;
    }
    dict_map[1] = &g_dict;
    seg_options.global_dictmaps = &dict_map;

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_VARCHAR).add(1, "c1", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_options);
    int num_columns = 2;
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    ASSERT_TRUE(res_chunk->num_rows() == 6);
    ASSERT_TRUE(res_chunk->num_columns() == num_columns);

    std::vector<uint32_t> hashes(num_rows, 0);
    res_chunk->get_column_by_id(0)->crc32_hash(&(hashes)[0], 0, num_rows);
    std::vector<uint32_t> mod_values;
    std::vector<uint32_t> index_mod_0;
    std::vector<uint32_t> index_mod_1;
    for (int i = 0; i < hashes.size(); ++i) {
        int cur_mod = hashes[i] % 2;
        if (cur_mod == 0) {
            index_mod_0.push_back(i);
        } else {
            index_mod_1.push_back(i);
        }
    }

    {
        // test remainder = 0
        RecordPredicatePB record_predicate_pb;
        record_predicate_pb.set_type(RecordPredicatePB::COLUMN_HASH_IS_CONGRUENT);
        auto column_hash_is_congruent_pb = record_predicate_pb.mutable_column_hash_is_congruent();
        column_hash_is_congruent_pb->set_modulus(2);
        column_hash_is_congruent_pb->set_remainder(0);
        column_hash_is_congruent_pb->add_column_names("c0");
        seg_options.record_predicate = std::move(RecordPredicateHelper::create(record_predicate_pb).value());
        auto chunk_iter_mod_0 = new_segment_iterator(segment, vec_schema, seg_options);
        auto res_chunk_mod_0 = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);
        ASSERT_OK(chunk_iter_mod_0->get_next(res_chunk_mod_0.get()));
        ASSERT_TRUE(res_chunk_mod_0->num_rows() == index_mod_0.size());
        ASSERT_TRUE(res_chunk_mod_0->num_columns() == num_columns);
        auto binary_0 = down_cast<BinaryColumn*>(res_chunk_mod_0->get_column_by_index(0).get());
        for (int i = 0; i < index_mod_0.size(); ++i) {
            ASSERT_EQ(binary_0->get_slice(i), Slice(values[index_mod_0[i]]));
        }
    }

    {
        // test remainder = 1
        RecordPredicatePB record_predicate_pb;
        record_predicate_pb.set_type(RecordPredicatePB::COLUMN_HASH_IS_CONGRUENT);
        auto column_hash_is_congruent_pb = record_predicate_pb.mutable_column_hash_is_congruent();
        column_hash_is_congruent_pb->set_modulus(2);
        column_hash_is_congruent_pb->set_remainder(1);
        column_hash_is_congruent_pb->add_column_names("c0");
        seg_options.record_predicate = std::move(RecordPredicateHelper::create(record_predicate_pb).value());
        auto chunk_iter_mod_1 = new_segment_iterator(segment, vec_schema, seg_options);
        auto res_chunk_mod_1 = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);
        ASSERT_OK(chunk_iter_mod_1->get_next(res_chunk_mod_1.get()));
        ASSERT_TRUE(res_chunk_mod_1->num_rows() == index_mod_1.size());
        ASSERT_TRUE(res_chunk_mod_1->num_columns() == num_columns);
        auto binary_1 = down_cast<BinaryColumn*>(res_chunk_mod_1->get_column_by_index(0).get());
        for (int i = 0; i < index_mod_1.size(); ++i) {
            ASSERT_EQ(binary_1->get_slice(i), Slice(values[index_mod_1[i]]));
        }
    }
}

// Test CHAR column storage with VARCHAR predicate zone map filtering after fast schema evolution
TEST_F(SegmentIteratorTest, testCharToVarcharZoneMapFilter) {
    // Create tablet schema with CHAR column
    std::shared_ptr<TabletSchema> tablet_schema = test::TabletSchemaBuilder()
                                                          .create(0, false, TYPE_INT, true)  // Primary key column
                                                          .create(1, false, TYPE_CHAR, true) // CHAR column
                                                          .build();

    // Create test data with CHAR values
    std::vector<std::string> char_values = {"abc", "def", "ghi", "jkl"};

    // Build segment using TabletDataBuilder pattern
    std::string file_name = kSegmentDir + "/char_to_varchar_zone_map_test";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 2; // Expect two data blocks
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    // Write all key columns together first
    std::vector<uint32_t> key_column_indexes{0, 1};
    ASSERT_OK(writer.init(key_column_indexes, true));

    auto schema = ChunkHelper::convert_schema(tablet_schema, key_column_indexes);
    auto chunk = ChunkHelper::new_chunk(schema, 1024);

    // Add data rows - create providers for each column
    auto int_provider = [](int32_t i) { return Datum(i); };
    auto char_provider = [&char_values](int32_t i) { return Datum(Slice(char_values[i])); };

    // Fill the chunk with data
    chunk->reset();
    auto& cols = chunk->columns();
    for (int i = 0; i < 4; ++i) {
        cols[0]->append_datum(int_provider(i));
        cols[1]->append_datum(char_provider(i));
    }
    ASSERT_OK(writer.append_chunk(*chunk));

    uint64_t index_size = 0;
    ASSERT_OK(writer.finalize_columns(&index_size));

    uint64_t file_size = 0;
    ASSERT_OK(writer.finalize_footer(&file_size));

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), 4);

    // Create VARCHAR query schema
    test::VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    // Create TabletSchema for query with VARCHAR column (schema evolution from CHAR to VARCHAR)
    test::TabletSchemaBuilder query_schema_builder;
    std::shared_ptr<TabletSchema> query_tablet_schema =
            query_schema_builder.create(0, false, TYPE_INT, true).create(1, false, TYPE_VARCHAR, true).build();

    // Test 1: VARCHAR predicate that should match CHAR data
    {
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        OlapReaderStatistics stats;
        seg_opts.stats = &stats;
        seg_opts.tablet_schema = query_tablet_schema;

        ObjectPool pool;
        auto type_varchar = get_type_info(TYPE_VARCHAR);
        auto predicate = pool.add(new_column_eq_predicate(type_varchar, 1, "abc"));
        PredicateAndNode pred_root;
        pred_root.add_child(PredicateColumnNode{predicate});
        seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

        // Set up zone map predicate tree for zone map filtering
        ASSERT_OK(ZonemapPredicatesRewriter::rewrite_predicate_tree(&pool, seg_opts.pred_tree,
                                                                    seg_opts.pred_tree_for_zone_map));

        auto chunk_iter_res = segment->new_iterator(vec_schema, seg_opts);
        ASSERT_OK(chunk_iter_res.status());
        auto chunk_iter = chunk_iter_res.value();
        ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));

        auto res_chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 1024);
        ASSERT_OK(chunk_iter->get_next(res_chunk.get()));

        // Should return exactly one row: c0=0, c1="abc"
        ASSERT_EQ(res_chunk->num_rows(), 1);
        auto int_col = down_cast<Int32Column*>(res_chunk->get_column_by_index(0).get());
        auto varchar_col = down_cast<BinaryColumn*>(res_chunk->get_column_by_index(1).get());
        ASSERT_EQ(int_col->get_data()[0], 0);
        ASSERT_EQ(varchar_col->get_slice(0), Slice("abc"));

        // Should be no more data
        res_chunk->reset();
        ASSERT_TRUE(chunk_iter->get_next(res_chunk.get()).is_end_of_file());
        ASSERT_EQ(0, stats.segment_stats_filtered);
        ASSERT_EQ(0, stats.rows_stats_filtered);
    }

    // Test 2: VARCHAR predicate that should not match any CHAR data which is filtered by segment-level zonemap index
    {
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        OlapReaderStatistics stats;
        seg_opts.stats = &stats;
        seg_opts.tablet_schema = query_tablet_schema;

        ObjectPool pool;
        auto type_varchar = get_type_info(TYPE_VARCHAR);
        auto predicate = pool.add(new_column_eq_predicate(type_varchar, 1, "xyz"));
        PredicateAndNode pred_root;
        pred_root.add_child(PredicateColumnNode{predicate});
        seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

        // Set up zone map predicate tree for zone map filtering
        ASSERT_OK(ZonemapPredicatesRewriter::rewrite_predicate_tree(&pool, seg_opts.pred_tree,
                                                                    seg_opts.pred_tree_for_zone_map));

        auto chunk_iter_res = segment->new_iterator(vec_schema, seg_opts);
        ASSERT_TRUE(chunk_iter_res.status().is_end_of_file());
        ASSERT_EQ(4, stats.segment_stats_filtered);
        ASSERT_EQ(0, stats.rows_stats_filtered);
    }

    // Test 3: VARCHAR predicate that should not match any CHAR data which is filtered by page-level zonemap
    {
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        OlapReaderStatistics stats;
        seg_opts.stats = &stats;
        seg_opts.tablet_schema = query_tablet_schema;

        ObjectPool pool;
        auto type_varchar = get_type_info(TYPE_VARCHAR);
        auto predicate = pool.add(new_column_eq_predicate(type_varchar, 1, "aa"));
        PredicateAndNode pred_root;
        pred_root.add_child(PredicateColumnNode{predicate});
        seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

        // Set up zone map predicate tree for zone map filtering
        ASSERT_OK(ZonemapPredicatesRewriter::rewrite_predicate_tree(&pool, seg_opts.pred_tree,
                                                                    seg_opts.pred_tree_for_zone_map));

        config::enable_index_segment_level_zonemap_filter = false;
        DeferOp op([&]() { config::enable_index_segment_level_zonemap_filter = true; });
        auto chunk_iter_res = segment->new_iterator(vec_schema, seg_opts);
        ASSERT_OK(chunk_iter_res.status());
        auto chunk_iter = chunk_iter_res.value();
        ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
        auto res_chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 1024);
        auto status = chunk_iter->get_next(res_chunk.get());
        ASSERT_TRUE(status.is_end_of_file());
        ASSERT_EQ(res_chunk->num_rows(), 0);
        ASSERT_EQ(0, stats.segment_stats_filtered);
        ASSERT_EQ(4, stats.rows_stats_filtered);
    }

    // Test 4: VARCHAR range predicate
    {
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        OlapReaderStatistics stats;
        seg_opts.stats = &stats;
        seg_opts.tablet_schema = query_tablet_schema;

        ObjectPool pool;
        auto type_varchar = get_type_info(TYPE_VARCHAR);
        auto predicate1 = pool.add(new_column_ge_predicate(type_varchar, 1, "def"));
        auto predicate2 = pool.add(new_column_le_predicate(type_varchar, 1, "ghi"));
        PredicateAndNode pred_root;
        pred_root.add_child(PredicateColumnNode{predicate1});
        pred_root.add_child(PredicateColumnNode{predicate2});
        seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

        // Set up zone map predicate tree for zone map filtering
        ASSERT_OK(ZonemapPredicatesRewriter::rewrite_predicate_tree(&pool, seg_opts.pred_tree,
                                                                    seg_opts.pred_tree_for_zone_map));

        auto chunk_iter_res = segment->new_iterator(vec_schema, seg_opts);
        ASSERT_OK(chunk_iter_res.status());
        auto chunk_iter = chunk_iter_res.value();
        ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));

        auto res_chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 1024);
        ASSERT_OK(chunk_iter->get_next(res_chunk.get()));

        // Should return two rows: c0=1,2 with c1="def","ghi"
        ASSERT_EQ(res_chunk->num_rows(), 2);
        auto int_col = down_cast<Int32Column*>(res_chunk->get_column_by_index(0).get());
        auto varchar_col = down_cast<BinaryColumn*>(res_chunk->get_column_by_index(1).get());
        ASSERT_EQ(int_col->get_data()[0], 1);
        ASSERT_EQ(int_col->get_data()[1], 2);
        ASSERT_EQ(varchar_col->get_slice(0), Slice("def"));
        ASSERT_EQ(varchar_col->get_slice(1), Slice("ghi"));
        ASSERT_EQ(0, stats.segment_stats_filtered);
        ASSERT_EQ(0, stats.rows_stats_filtered);
    }
}

TEST_F(SegmentIteratorTest, TestResetFunction) {
    using namespace starrocks::test;

    // Create a simple segment with integer data
    std::string file_name = kSegmentDir + "/reset_test";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema =
            builder.create(1, false, TYPE_INT, true).create(2, false, TYPE_INT, false).build();
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    const size_t num_rows = 100; // Create 100 rows of data

    // Create data: row i has values [i, i*2]
    auto col1_provider = [](int32_t i) { return i; };
    auto col2_provider = [](int32_t i) { return i * 2; };

    // Build the segment
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, col1_provider));
    ASSERT_OK(segment_data_builder.append(1, col2_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    // Open the segment
    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    // Setup read options
    SegmentReadOptions seg_options;
    OlapReaderStatistics stats;
    seg_options.fs = _fs;
    seg_options.stats = &stats;
    seg_options.chunk_size = 20; // Small chunk size for testing

    // Setup schema
    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_INT);
    auto vec_schema = schema_builder.build();

    // Create iterator
    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_options);
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);

    // Test 1: Read first 20 rows (0-19)
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    ASSERT_EQ(res_chunk->num_rows(), 20);
    ASSERT_EQ(res_chunk->num_columns(), 2);

    // Verify first few rows
    auto col0 = down_cast<Int32Column*>(res_chunk->get_column_by_index(0).get());
    auto col1 = down_cast<Int32Column*>(res_chunk->get_column_by_index(1).get());
    ASSERT_EQ(col0->get_data()[0], 0);
    ASSERT_EQ(col1->get_data()[0], 0);
    ASSERT_EQ(col0->get_data()[1], 1);
    ASSERT_EQ(col1->get_data()[1], 2);

    // Test 2: Reset to scan rows 50-69
    SparseRange<> new_range;
    new_range.add(Range<>(50, 70));
    ASSERT_OK(chunk_iter->reset(new_range));

    // Read from the new range
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    ASSERT_EQ(res_chunk->num_rows(), 20);

    // Verify we're reading from the correct range
    col0 = down_cast<Int32Column*>(res_chunk->get_column_by_index(0).get());
    col1 = down_cast<Int32Column*>(res_chunk->get_column_by_index(1).get());
    ASSERT_EQ(col0->get_data()[0], 50);
    ASSERT_EQ(col1->get_data()[0], 100);
    ASSERT_EQ(col0->get_data()[1], 51);
    ASSERT_EQ(col1->get_data()[1], 102);

    // Test 3: Reset to scan rows 80-99 (last 20 rows)
    new_range.clear();
    new_range.add(Range<>(80, 100));
    ASSERT_OK(chunk_iter->reset(new_range));

    // Read from the new range
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    ASSERT_EQ(res_chunk->num_rows(), 20);

    // Verify we're reading from the correct range
    col0 = down_cast<Int32Column*>(res_chunk->get_column_by_index(0).get());
    col1 = down_cast<Int32Column*>(res_chunk->get_column_by_index(1).get());
    ASSERT_EQ(col0->get_data()[0], 80);
    ASSERT_EQ(col1->get_data()[0], 160);
    ASSERT_EQ(col0->get_data()[19], 99);
    ASSERT_EQ(col1->get_data()[19], 198);

    // Test 4: Reset to scan a smaller range (rows 10-15)
    new_range.clear();
    new_range.add(Range<>(10, 16));
    ASSERT_OK(chunk_iter->reset(new_range));

    // Read from the new range
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    ASSERT_EQ(res_chunk->num_rows(), 6);

    // Verify we're reading from the correct range
    col0 = down_cast<Int32Column*>(res_chunk->get_column_by_index(0).get());
    col1 = down_cast<Int32Column*>(res_chunk->get_column_by_index(1).get());
    ASSERT_EQ(col0->get_data()[0], 10);
    ASSERT_EQ(col1->get_data()[0], 20);
    ASSERT_EQ(col0->get_data()[5], 15);
    ASSERT_EQ(col1->get_data()[5], 30);

    // Test 5: Reset to scan multiple ranges (rows 5-10 and 25-30)
    new_range.clear();
    new_range.add(Range<>(5, 11));
    new_range.add(Range<>(25, 31));
    ASSERT_OK(chunk_iter->reset(new_range));

    // Read from the new range
    res_chunk->reset();
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    ASSERT_EQ(res_chunk->num_rows(), 12); // 6 + 6 = 12 rows

    // Verify we're reading from the correct ranges
    col0 = down_cast<Int32Column*>(res_chunk->get_column_by_index(0).get());
    col1 = down_cast<Int32Column*>(res_chunk->get_column_by_index(1).get());

    // First range: 5-10
    ASSERT_EQ(col0->get_data()[0], 5);
    ASSERT_EQ(col1->get_data()[0], 10);
    ASSERT_EQ(col0->get_data()[5], 10);
    ASSERT_EQ(col1->get_data()[5], 20);

    // Second range: 25-30
    ASSERT_EQ(col0->get_data()[6], 25);
    ASSERT_EQ(col1->get_data()[6], 50);
    ASSERT_EQ(col0->get_data()[11], 30);
    ASSERT_EQ(col1->get_data()[11], 60);

    // Test 6: Test error handling - reset before initialization
    auto uninitialized_iter = new_segment_iterator(segment, vec_schema, seg_options);
    new_range.clear();
    new_range.add(Range<>(0, 10));
    auto status = uninitialized_iter->reset(new_range);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.is_internal_error());
}

} // namespace starrocks
