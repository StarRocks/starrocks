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

#include "column/column_helper.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "fs/fs_memory.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "gtest/gtest.h"
#include "runtime/global_dict/types.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/delta_column_group.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/runtime_filter_predicate.h"
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
    ColumnPB _create_pb(int32_t id, const std::string& name, bool nullable, LogicalType type, bool key) {
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
    TabletSchemaBuilder& set_bitmap_index() {
        _column_pbs.back().set_has_bitmap_index(true);
        return *this;
    }

    std::unique_ptr<TabletSchema> build() { return TabletSchemaHelper::create_tablet_schema(_column_pbs); }
};

struct TabletDataBuilder {
    TabletDataBuilder(SegmentWriter& writer_, std::shared_ptr<TabletSchema> schema, size_t chunk_size_,
                      size_t num_rows_)
            : writer(writer_), _schema(std::move(schema)), chunk_size(chunk_size_), num_rows(num_rows_) {}

    template <class Provider>
    Status append(int32_t idx, Provider&& provider) {
        std::vector<uint32_t> column_indexes = {static_cast<unsigned int>(idx)};

        RETURN_IF_ERROR(writer.init(column_indexes, true));

        auto schema = ChunkHelper::convert_schema(_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->as_mutable_ptr()->append_datum(provider(static_cast<int32_t>(i * chunk_size + j)));
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
    VecSchemaBuilder& add(int32_t id, const std::string& name, LogicalType type, bool nullable = false) {
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

// Regression test for an out-of-bounds read in `SegmentIterator::_switch_context`.
//
// Shape of the crash seen in production (4.0.12, SIGSEGV @0x0 with the PC in
// `_switch_context`): a string column carries a global dictionary that cannot be
// mapped onto the segment's local dictionary (the pages are not all dict-encoded),
// so `_has_force_dict_encode` is set and the final-chunk schema is rebuilt from
// `_encoded_schema`. That rebuild walks every field of `_encoded_schema` while
// indexing `output_schema()` with a cursor that is never bounds-checked. As soon as
// the LAST output field has been matched, every remaining encoded field reads
// `output_schema().field(num_output_fields)` -- one past the end of the output
// schema's field vector. `Schema::field()` only DCHECKs the index, so a RELEASE
// binary dereferences whatever the read returns; a zeroed slot yields a null
// FieldPtr and `->id()` faults at address 0.
//
// The trailing column here (`c2`) is predicate-only, i.e. exactly what
// filter_unused_columns prunes from the output on a query like
// `SELECT c0, c1 FROM t WHERE c2 >= '...'`.
TEST_F(SegmentIteratorTest, TestForceGlobalDictEncodeWithTrailingUnusedColumn) {
    // Two values big enough to overflow the dict page builder, so the column ends up
    // with no usable local dictionary while a global dictionary still exists for it.
    const int slice_num = 2;
    const int overflow_sz = 1024 * 1024 + 10; // 1M
    std::vector<std::string> values;
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

    std::string file_name = kSegmentDir + "/force_encode_trailing_unused_column";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_INT, true)
                                                          .create(2, false, TYPE_VARCHAR)
                                                          .create(3, false, TYPE_VARCHAR)
                                                          .set_length(overflow_sz + 10)
                                                          .build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 1024;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    const size_t num_rows = slice_num;

    std::vector<std::string> small_values{"aaa", "bbb"};
    auto i32_provider = [](int32_t i) { return i; };
    auto small_slice_provider = [&small_values](int32_t i) { return Slice(small_values[i % small_values.size()]); };
    auto big_slice_provider = [&data_strs](int32_t i) { return data_strs[i % data_strs.size()]; };

    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, i32_provider));
    ASSERT_OK(segment_data_builder.append(1, small_slice_provider));
    ASSERT_OK(segment_data_builder.append(2, big_slice_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    OlapReaderStatistics stats;

    // The trailing column must have no local dictionary, otherwise the global dict is
    // applied directly and `_has_force_dict_encode` stays false.
    ColumnIteratorOptions iter_opts;
    ASSIGN_OR_ABORT(auto read_file, _fs->new_random_access_file(segment->file_name()));
    iter_opts.stats = &stats;
    iter_opts.use_page_cache = false;
    iter_opts.read_file = read_file.get();
    iter_opts.check_dict_encoding = true;
    iter_opts.reader_type = READER_QUERY;
    ASSIGN_OR_ABORT(auto scalar_iter, segment->new_column_iterator(tablet_schema->column(2), nullptr));
    ASSERT_OK(scalar_iter->init(iter_opts));
    ASSERT_FALSE(scalar_iter->all_page_dict_encoded());

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_VARCHAR).add(2, "c2", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;

    ColumnIdToGlobalDictMap dict_map;
    GlobalDictMap g_dict;
    for (int i = 0; i < slice_num; ++i) {
        g_dict[Slice(values[i])] = i;
    }
    dict_map[2] = &g_dict;
    seg_opts.global_dictmaps = &dict_map;

    // Every scanned column carries a pushed-down predicate. That keeps `new_segment_iterator()` on
    // the branch that hands the schema to the iterator AS IS -- with fewer predicate columns than
    // fields it would instead `reorder_schema()` the predicate columns to the front and wrap the
    // iterator in a projection, which moves the unused column away from the tail and hides the bug.
    std::unique_ptr<ColumnPredicate> pred_c0(new_column_ge_predicate(get_type_info(TYPE_INT), 0, "0"));
    std::unique_ptr<ColumnPredicate> pred_c1(new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 1, "a"));
    std::unique_ptr<ColumnPredicate> pred_c2(
            new_column_ge_predicate(get_type_info(TYPE_VARCHAR), 2, values[0].c_str()));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{pred_c0.get()});
    pred_root.add_child(PredicateColumnNode{pred_c1.get()});
    pred_root.add_child(PredicateColumnNode{pred_c2.get()});
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(dict_map));
    // c2 is used by the pushed-down predicate only -- pruned from the output, and it is
    // the last field of the schema.
    std::unordered_set<uint32_t> unused_output_column_ids{2};
    ASSERT_OK(chunk_iter->init_output_schema(unused_output_column_ids));
    ASSERT_EQ(2, chunk_iter->output_schema().num_fields());

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), chunk_size);
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));

    ASSERT_EQ(2, res_chunk->num_columns());
    ASSERT_EQ(num_rows, res_chunk->num_rows());
    const auto& c0 = res_chunk->get_column_by_index(0);
    const auto& c1 = res_chunk->get_column_by_index(1);
    for (size_t i = 0; i < num_rows; ++i) {
        EXPECT_EQ(static_cast<int32_t>(i), c0->get(i).get_int32());
        EXPECT_EQ(small_values[i % small_values.size()], c1->get(i).get_slice().to_string());
    }
    res_chunk->reset();
}

// Verify predicate late materialization keeps non-predicate columns correct.
TEST_F(SegmentIteratorTest, TestPredicateLateMaterializationMaterializesRestColumns) {
    using namespace starrocks::test;

    // Force late materialization always on for determinism.
    auto prev_ratio = config::late_materialization_ratio;
    config::late_materialization_ratio = 1000;
    DeferOp reset_ratio([&]() { config::late_materialization_ratio = prev_ratio; });

    std::string file_name = kSegmentDir + "/predicate_late_materialize_all";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_INT, true)
                                                          .create(2, false, TYPE_INT, false)
                                                          .create(3, false, TYPE_INT, false)
                                                          .build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 32;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = 64;
    const size_t num_rows = 50;

    auto c0_provider = [](int32_t i) { return i; };
    auto c1_provider = [](int32_t i) { return i % 10; };   // predicate column
    auto c2_provider = [](int32_t i) { return 1000 + i; }; // late materialized column

    TabletDataBuilder data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(data_builder.append(0, c0_provider));
    ASSERT_OK(data_builder.append(1, c1_provider));
    ASSERT_OK(data_builder.append(2, c2_provider));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    VecSchemaBuilder schema_builder;
    // ids must be ordinal, keep contiguous from 0
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_INT).add(2, "c2", TYPE_INT);
    auto vec_schema = schema_builder.build();

    std::unique_ptr<ColumnPredicate> predicate(new_column_eq_predicate(get_type_info(TYPE_INT), 1, "5"));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate.get()});

    SegmentReadOptions seg_opts;
    OlapReaderStatistics stats;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.enable_predicate_col_late_materialize = true;
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), config::vector_chunk_size);
    ASSERT_OK(chunk_iter->get_next(res_chunk.get()));
    ASSERT_EQ(res_chunk->num_rows(), 5); // rows where c1 == 5

    auto c0_col = ColumnHelper::cast_to_raw<TYPE_INT>(res_chunk->get_column_by_index(0));
    auto c1_col = ColumnHelper::cast_to_raw<TYPE_INT>(res_chunk->get_column_by_index(1));
    auto c2_col = ColumnHelper::cast_to_raw<TYPE_INT>(res_chunk->get_column_by_index(2));
    for (size_t i = 0; i < res_chunk->num_rows(); ++i) {
        ASSERT_EQ(c1_col->get_data()[i], 5);
        ASSERT_EQ(c2_col->get_data()[i] - c0_col->get_data()[i], 1000);
    }

    res_chunk->reset();
    ASSERT_TRUE(chunk_iter->get_next(res_chunk.get()).is_end_of_file());
}

// Verify `_only_output_one_predicate_col_with_filter_push_down` fast path.
TEST_F(SegmentIteratorTest, TestPredicateLateMaterializationSingleColumnPushdown) {
    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/predicate_late_materialize_pushdown";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_VARCHAR, true).set_length(16).build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 32;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = 64;
    const size_t num_rows = 100;
    std::string keep = "keep";
    std::string drop = "drop";
    auto val_provider = [&](int32_t i) { return Slice(i < 50 ? keep : drop); };

    TabletDataBuilder data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(data_builder.append(0, val_provider));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    std::unique_ptr<ColumnPredicate> predicate(new_column_eq_predicate(get_type_info(TYPE_VARCHAR), 0, keep.c_str()));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate.get()});

    SegmentReadOptions seg_opts;
    OlapReaderStatistics stats;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.enable_predicate_col_late_materialize = true;
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), config::vector_chunk_size);
    size_t total = 0;
    while (true) {
        res_chunk->reset();
        auto st = chunk_iter->get_next(res_chunk.get());
        if (st.is_end_of_file()) break;
        ASSERT_OK(st);
        total += res_chunk->num_rows();
        auto col = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(res_chunk->get_column_by_index(0));
        for (size_t i = 0; i < res_chunk->num_rows(); ++i) {
            ASSERT_EQ(col->get_slice(i), Slice(keep));
        }
    }
    ASSERT_EQ(total, 50);
    ASSERT_GE(stats.rows_vec_cond_filtered, 50);
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
        values.emplace_back(bigstr);
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
    auto cols = chunk->columns();
    for (int i = 0; i < 4; ++i) {
        cols[0]->as_mutable_ptr()->append_datum(int_provider(i));
        cols[1]->as_mutable_ptr()->append_datum(char_provider(i));
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
        const auto& chunk_iter = chunk_iter_res.value();
        ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));

        auto res_chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 1024);
        ASSERT_OK(chunk_iter->get_next(res_chunk.get()));

        // Should return exactly one row: c0=0, c1="abc"
        ASSERT_EQ(res_chunk->num_rows(), 1);
        auto int_col = ColumnHelper::cast_to_raw<TYPE_INT>(res_chunk->get_column_by_index(0));
        auto varchar_col = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(res_chunk->get_column_by_index(1));
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
        const auto& chunk_iter = chunk_iter_res.value();
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
        const auto& chunk_iter = chunk_iter_res.value();
        ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));

        auto res_chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 1024);
        ASSERT_OK(chunk_iter->get_next(res_chunk.get()));

        // Should return two rows: c0=1,2 with c1="def","ghi"
        ASSERT_EQ(res_chunk->num_rows(), 2);
        auto int_col = down_cast<const Int32Column*>(res_chunk->get_column_raw_ptr_by_index(0));
        auto varchar_col = down_cast<const BinaryColumn*>(res_chunk->get_column_raw_ptr_by_index(1));
        ASSERT_EQ(int_col->get_data()[0], 1);
        ASSERT_EQ(int_col->get_data()[1], 2);
        ASSERT_EQ(varchar_col->get_slice(0), Slice("def"));
        ASSERT_EQ(varchar_col->get_slice(1), Slice("ghi"));
        ASSERT_EQ(0, stats.segment_stats_filtered);
        ASSERT_EQ(0, stats.rows_stats_filtered);
    }
}

<<<<<<< HEAD
=======
// (c1 = 1 AND (c2 IN (1, 2))) OR (c1 = 2 AND c2 = 3), with bitmap indexes on c1 and c2. The inner OR fails the
// bitmap selectivity gate (2 of 10 c2 values) while both AND chains pass it, so the outer OR is used and the second
// chain is fully erased. The residual must still keep that chain, or row 3001 (c1 = 2, c2 = 3) is lost.
TEST_F(SegmentIteratorTest, BitmapIndexKeepsPartiallyErasedOr) {
    using namespace starrocks::test;

    std::shared_ptr<TabletSchema> tablet_schema = TabletSchemaBuilder()
                                                          .create(0, false, TYPE_INT, true)
                                                          .create(1, false, TYPE_INT, false)
                                                          .set_bitmap_index()
                                                          .create(2, false, TYPE_INT, false)
                                                          .set_bitmap_index()
                                                          .build();

    auto chunk = ChunkFactory::new_chunk(ChunkHelper::convert_schema(tablet_schema), 4096);
    auto append_row = [&](int32_t id, int32_t c1, int32_t c2) {
        chunk->get_column_by_index(0)->as_mutable_ptr()->append_datum(Datum(id));
        chunk->get_column_by_index(1)->as_mutable_ptr()->append_datum(Datum(c1));
        chunk->get_column_by_index(2)->as_mutable_ptr()->append_datum(Datum(c2));
    };
    for (int32_t i = 0; i < 3000; i++) {
        append_row(i, 1000 + i, i % 10);
    }
    append_row(3000, 1, 1);
    append_row(3001, 2, 3);
    append_row(3002, 1, 3);
    append_row(3003, 2, 1);

    auto segment =
            write_and_open_segment(_fs, kSegmentDir + "/bitmap_index_partially_erased_or", tablet_schema, *chunk, 1024);

    auto vec_schema = VecSchemaBuilder().add(0, "c0", TYPE_INT).add(1, "c1", TYPE_INT).add(2, "c2", TYPE_INT).build();

    const auto type_int = get_type_info(TYPE_INT);
    std::unique_ptr<ColumnPredicate> c1_eq_1(new_column_eq_predicate(type_int, 1, "1"));
    std::unique_ptr<ColumnPredicate> c2_in_1_2(new_column_in_predicate(type_int, 2, {"1", "2"}));
    std::unique_ptr<ColumnPredicate> c1_eq_2(new_column_eq_predicate(type_int, 1, "2"));
    std::unique_ptr<ColumnPredicate> c2_eq_3(new_column_eq_predicate(type_int, 2, "3"));

    PredicateOrNode inner_or;
    inner_or.add_child(PredicateColumnNode{c2_in_1_2.get()});
    PredicateAndNode chain1;
    chain1.add_child(PredicateColumnNode{c1_eq_1.get()});
    chain1.add_child(std::move(inner_or));
    PredicateAndNode chain2;
    chain2.add_child(PredicateColumnNode{c1_eq_2.get()});
    chain2.add_child(PredicateColumnNode{c2_eq_3.get()});
    PredicateOrNode outer_or;
    outer_or.add_child(std::move(chain1));
    outer_or.add_child(std::move(chain2));
    PredicateAndNode pred_root;
    pred_root.add_child(std::move(outer_or));

    SegmentReadOptions seg_opts;
    OlapReaderStatistics stats;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    std::vector<int32_t> ids;
    auto res_chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), config::vector_chunk_size);
    while (true) {
        res_chunk->reset();
        auto st = chunk_iter->get_next(res_chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_OK(st);
        auto id_col = ColumnHelper::cast_to_raw<TYPE_INT>(res_chunk->get_column_by_index(0));
        for (size_t i = 0; i < res_chunk->num_rows(); i++) {
            ids.push_back(id_col->get_data()[i]);
        }
    }

    ASSERT_EQ(3001, stats.rows_bitmap_index_filtered);
    ASSERT_EQ((std::vector<int32_t>{3000, 3001}), ids);
}

// Tests for dict_lookup_batch optimization: large IN predicate on dict-encoded VARCHAR column.
// The hash-based batch lookup (kDictLookupHashThreshold=100) is exercised when either
// dict_size >= 100 or the IN list has >= 100 values.

// Helper to extract the integer suffix from a formatted test value like "value_042" or "v_042".
static int _extract_idx(Slice s, size_t prefix_len) {
    return std::stoi(std::string(s.data + prefix_len, s.size - prefix_len));
}

// IN predicate with large value list on dict-encoded VARCHAR column.
// dict_size=128 >= 100 and IN list=150 >= 100, triggering the hash-based batch lookup.
TEST_F(SegmentIteratorTest, TestDictLookupBatchInListLarge) {
    using namespace starrocks::test;

    const int num_distinct = 128;
    const int num_in_dict = 80;
    const int num_not_in_dict = 70;
    const size_t num_rows = 10000;

    std::vector<std::string> values(num_distinct);
    for (int i = 0; i < num_distinct; ++i) {
        values[i] = fmt::format("value_{:03d}", i);
    }
    auto slice_provider = [&](int32_t i) { return Slice(values[i % num_distinct]); };

    std::string file_name = kSegmentDir + "/dict_lookup_batch_inlist_large";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_VARCHAR).set_length(32).build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    TabletDataBuilder data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(data_builder.append(0, slice_provider));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    ObjectPool pool;
    std::vector<std::string> in_values;
    for (int i = 0; i < num_in_dict; ++i) {
        in_values.push_back(values[i]); // "value_000" to "value_079"
    }
    for (int i = 0; i < num_not_in_dict; ++i) {
        in_values.push_back(fmt::format("not_in_dict_{:03d}", i));
    }

    auto* predicate = pool.add(new_column_in_predicate(get_type_info(TYPE_VARCHAR), 0, in_values));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate});

    SegmentReadOptions seg_opts;
    OlapReaderStatistics stats;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), chunk_size);
    size_t total = 0;
    while (true) {
        res_chunk->reset();
        auto st = chunk_iter->get_next(res_chunk.get());
        if (st.is_end_of_file()) break;
        ASSERT_OK(st);
        size_t n = res_chunk->num_rows();
        total += n;
        auto* col = down_cast<const BinaryColumn*>(res_chunk->get_column_raw_ptr_by_index(0));
        for (size_t i = 0; i < n; ++i) {
            Slice s = col->get_slice(i);
            ASSERT_TRUE(s.starts_with(Slice("value_"))) << "Unexpected value: " << s.to_string();
            int idx = _extract_idx(s, 6);
            ASSERT_LT(idx, num_in_dict) << "v=" << s.to_string() << " idx=" << idx;
        }
    }

    // Distribution of 10000 rows / 128 distinct: first 16 values appear 79 times, rest 78.
    // Matching values 0-79: 16*79 + 64*78 = 1264 + 4992 = 6256
    ASSERT_EQ(total, 6256);
}

// NOT IN predicate with large value list on dict-encoded VARCHAR column.
TEST_F(SegmentIteratorTest, TestDictLookupBatchNotInListLarge) {
    using namespace starrocks::test;

    const int num_distinct = 128;
    const int num_excluded = 80;
    const size_t num_rows = 10000;

    std::vector<std::string> values(num_distinct);
    for (int i = 0; i < num_distinct; ++i) {
        values[i] = fmt::format("value_{:03d}", i);
    }
    auto slice_provider = [&](int32_t i) { return Slice(values[i % num_distinct]); };

    std::string file_name = kSegmentDir + "/dict_lookup_batch_notinlist_large";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_VARCHAR).set_length(32).build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    TabletDataBuilder data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(data_builder.append(0, slice_provider));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    ObjectPool pool;
    std::vector<std::string> not_in_values;
    for (int i = 0; i < num_excluded; ++i) {
        not_in_values.push_back(values[i]);
    }
    for (int i = 0; i < 70; ++i) {
        not_in_values.push_back(fmt::format("not_in_dict_{:03d}", i));
    }

    auto* predicate = pool.add(new_column_not_in_predicate(get_type_info(TYPE_VARCHAR), 0, not_in_values));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate});

    SegmentReadOptions seg_opts;
    OlapReaderStatistics stats;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), chunk_size);
    size_t total = 0;
    while (true) {
        res_chunk->reset();
        auto st = chunk_iter->get_next(res_chunk.get());
        if (st.is_end_of_file()) break;
        ASSERT_OK(st);
        size_t n = res_chunk->num_rows();
        total += n;
        auto* col = down_cast<const BinaryColumn*>(res_chunk->get_column_raw_ptr_by_index(0));
        for (size_t i = 0; i < n; ++i) {
            Slice s = col->get_slice(i);
            ASSERT_TRUE(s.starts_with(Slice("value_"))) << "Unexpected value: " << s.to_string();
            int idx = _extract_idx(s, 6);
            ASSERT_GE(idx, num_excluded) << "v=" << s.to_string() << " idx=" << idx << " should be excluded by NOT IN";
        }
    }
    // Excluded values 0-79: 16*79 + 64*78 = 6256 excluded. Remaining: 10000-6256 = 3744
    ASSERT_EQ(total, 3744);
}

// Small IN list below the batch threshold (max(64,10) < 100): falls back to per-element dict_lookup.
TEST_F(SegmentIteratorTest, TestDictLookupBatchInListSmall) {
    using namespace starrocks::test;

    const int num_distinct = 64;
    const int num_in_dict = 8;
    const size_t num_rows = 5000;

    std::vector<std::string> values(num_distinct);
    for (int i = 0; i < num_distinct; ++i) {
        values[i] = fmt::format("v_{:03d}", i);
    }
    auto slice_provider = [&](int32_t i) { return Slice(values[i % num_distinct]); };

    std::string file_name = kSegmentDir + "/dict_lookup_batch_inlist_small";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_VARCHAR).set_length(32).build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    TabletDataBuilder data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(data_builder.append(0, slice_provider));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    ObjectPool pool;
    std::vector<std::string> in_values;
    for (int i = 0; i < num_in_dict; ++i) {
        in_values.push_back(values[i]);
    }
    in_values.push_back("not_in_dict_a");
    in_values.push_back("not_in_dict_b");

    auto* predicate = pool.add(new_column_in_predicate(get_type_info(TYPE_VARCHAR), 0, in_values));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate});

    SegmentReadOptions seg_opts;
    OlapReaderStatistics stats;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), chunk_size);
    size_t total = 0;
    while (true) {
        res_chunk->reset();
        auto st = chunk_iter->get_next(res_chunk.get());
        if (st.is_end_of_file()) break;
        ASSERT_OK(st);
        size_t n = res_chunk->num_rows();
        total += n;
        auto* col = down_cast<const BinaryColumn*>(res_chunk->get_column_raw_ptr_by_index(0));
        for (size_t i = 0; i < n; ++i) {
            int idx = _extract_idx(col->get_slice(i), 2);
            ASSERT_LT(idx, num_in_dict) << "Unexpected v=" << col->get_slice(i).to_string();
        }
    }
    // 5000/64 = 78 rem 8. Values 0-7 each appear 79 times. 8*79 = 632
    ASSERT_EQ(total, 632);
}

// IN predicate where no values match the dictionary -> ALWAYS_FALSE.
TEST_F(SegmentIteratorTest, TestDictLookupBatchInListAlwaysFalse) {
    using namespace starrocks::test;

    const int num_distinct = 20;
    const size_t num_rows = 1000;

    std::vector<std::string> values(num_distinct);
    for (int i = 0; i < num_distinct; ++i) {
        values[i] = fmt::format("v_{:03d}", i);
    }
    auto slice_provider = [&](int32_t i) { return Slice(values[i % num_distinct]); };

    std::string file_name = kSegmentDir + "/dict_lookup_batch_inlist_false";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_VARCHAR).set_length(32).build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    TabletDataBuilder data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(data_builder.append(0, slice_provider));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    ObjectPool pool;
    std::vector<std::string> in_values;
    for (int i = 0; i < 150; ++i) {
        in_values.push_back(fmt::format("not_found_{:03d}", i));
    }

    auto* predicate = pool.add(new_column_in_predicate(get_type_info(TYPE_VARCHAR), 0, in_values));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate});

    SegmentReadOptions seg_opts;
    OlapReaderStatistics stats;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), chunk_size);
    auto st = chunk_iter->get_next(res_chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
    ASSERT_EQ(res_chunk->num_rows(), 0);
}

// NOT IN predicate where no values match dictionary (non-nullable column) -> ALWAYS_TRUE.
TEST_F(SegmentIteratorTest, TestDictLookupBatchNotInListAlwaysTrue) {
    using namespace starrocks::test;

    const int num_distinct = 20;
    const size_t num_rows = 1000;

    std::vector<std::string> values(num_distinct);
    for (int i = 0; i < num_distinct; ++i) {
        values[i] = fmt::format("v_{:03d}", i);
    }
    auto slice_provider = [&](int32_t i) { return Slice(values[i % num_distinct]); };

    std::string file_name = kSegmentDir + "/dict_lookup_batch_notinlist_true";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_VARCHAR).set_length(32).build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    TabletDataBuilder data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(data_builder.append(0, slice_provider));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    ObjectPool pool;
    std::vector<std::string> not_in_values;
    for (int i = 0; i < 150; ++i) {
        not_in_values.push_back(fmt::format("not_found_{:03d}", i));
    }

    auto* predicate = pool.add(new_column_not_in_predicate(get_type_info(TYPE_VARCHAR), 0, not_in_values));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate});

    SegmentReadOptions seg_opts;
    OlapReaderStatistics stats;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), chunk_size);
    size_t total = 0;
    while (true) {
        res_chunk->reset();
        auto st = chunk_iter->get_next(res_chunk.get());
        if (st.is_end_of_file()) break;
        ASSERT_OK(st);
        total += res_chunk->num_rows();
    }
    ASSERT_EQ(total, num_rows);
}

// Duplicate values in IN list should be handled correctly (dedup by dict code).
TEST_F(SegmentIteratorTest, TestDictLookupBatchDuplicateValues) {
    using namespace starrocks::test;

    const int num_distinct = 50;
    const size_t num_rows = 2000;

    std::vector<std::string> values(num_distinct);
    for (int i = 0; i < num_distinct; ++i) {
        values[i] = fmt::format("v_{:03d}", i);
    }
    auto slice_provider = [&](int32_t i) { return Slice(values[i % num_distinct]); };

    std::string file_name = kSegmentDir + "/dict_lookup_batch_duplicates";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema = builder.create(1, false, TYPE_VARCHAR).set_length(32).build();

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    TabletDataBuilder data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(data_builder.append(0, slice_provider));
    ASSERT_OK(data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_VARCHAR);
    auto vec_schema = schema_builder.build();

    ObjectPool pool;
    // Duplicates: v_000 appears 5 times, v_001 appears 3 times
    std::vector<std::string> in_values;
    for (int i = 0; i < 5; ++i) in_values.push_back(values[0]);
    for (int i = 0; i < 3; ++i) in_values.push_back(values[1]);
    for (int i = 2; i < 10; ++i) in_values.push_back(values[i]);
    for (int i = 0; i < 100; ++i) in_values.push_back(fmt::format("nf_{:03d}", i));

    auto* predicate = pool.add(new_column_in_predicate(get_type_info(TYPE_VARCHAR), 0, in_values));
    PredicateAndNode pred_root;
    pred_root.add_child(PredicateColumnNode{predicate});

    SegmentReadOptions seg_opts;
    OlapReaderStatistics stats;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.pred_tree = PredicateTree::create(std::move(pred_root));

    auto chunk_iter = new_segment_iterator(segment, vec_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
    ASSERT_OK(chunk_iter->init_output_schema(std::unordered_set<uint32_t>()));

    auto res_chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), chunk_size);
    size_t total = 0;
    while (true) {
        res_chunk->reset();
        auto st = chunk_iter->get_next(res_chunk.get());
        if (st.is_end_of_file()) break;
        ASSERT_OK(st);
        size_t n = res_chunk->num_rows();
        total += n;
        auto* col = down_cast<const BinaryColumn*>(res_chunk->get_column_raw_ptr_by_index(0));
        for (size_t i = 0; i < n; ++i) {
            int idx = _extract_idx(col->get_slice(i), 2);
            ASSERT_LT(idx, 10) << "Unexpected v=" << col->get_slice(i).to_string();
        }
    }
    // 2000/50 = 40 each. 10 matching values -> 400
    ASSERT_EQ(total, 400);
}

// Verifies the reusable-segment-iterator path: a single underlying SegmentIterator, kept in a
// caller-owned slot, is driven for two independent scans. The second scan must go through
// reset_for_reuse() (not rebuild a fresh iterator) and still return the full result set.
TEST_F(SegmentIteratorTest, ReuseSegmentIteratorAcrossScans) {
    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/reusable_iterator";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema =
            builder.create(1, false, TYPE_INT, true).create(2, false, TYPE_INT).build();
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    const size_t num_rows = 100;
    auto key_provider = [](int32_t i) { return i; };
    auto val_provider = [](int32_t i) { return i * 2; };
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, key_provider));
    ASSERT_OK(segment_data_builder.append(1, val_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    OlapReaderStatistics stats;
    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_INT);
    auto vec_schema = schema_builder.build();

    auto make_opts = [&]() {
        SegmentReadOptions o;
        o.fs = _fs;
        o.stats = &stats;
        o.chunk_size = chunk_size;
        return o;
    };
    auto drain = [&](const ChunkIteratorPtr& it) -> size_t {
        auto chunk = ChunkFactory::new_chunk(it->output_schema(), chunk_size);
        size_t total = 0;
        while (true) {
            chunk->reset();
            auto st = it->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            total += chunk->num_rows();
        }
        return total;
    };

    // Empty slot -> the first call builds the underlying SegmentIterator and stores it in the slot.
    ChunkIteratorPtr reusable_slot;
    ASSIGN_OR_ABORT(auto iter1, segment->new_reusable_iterator(vec_schema, vec_schema, make_opts(), &reusable_slot));
    ASSERT_NE(nullptr, reusable_slot.get());
    void* underlying = reusable_slot.get();
    ASSERT_EQ(num_rows, drain(iter1));
    // NonClosingChunkIterator: closing the returned iterator must NOT tear down the reused iterator.
    iter1->close();

    // Second scan reuses the same slot: new_reusable_iterator takes the reset_for_reuse() branch,
    // so the SAME underlying SegmentIterator instance re-drives the scan and returns all rows again.
    ASSIGN_OR_ABORT(auto iter2, segment->new_reusable_iterator(vec_schema, vec_schema, make_opts(), &reusable_slot));
    ASSERT_EQ(underlying, reusable_slot.get()) << "expected the slot's iterator to be reused, not rebuilt";
    ASSERT_EQ(num_rows, drain(iter2));
    iter2->close();

    // Third scan reuses the slot with a precomputed scan range (as the prepared-split seed supplies one).
    // This exercises SegmentReadStateCache + _apply_precomputed_scan_range: the reused iterator applies the
    // narrowed range instead of re-pruning, so it returns exactly the precomputed subset.
    const size_t precomputed_rows = 50;
    auto precomputed = std::make_shared<SparseRange<>>();
    precomputed->add(Range<rowid_t>(0, precomputed_rows));
    auto opts3 = make_opts();
    opts3.read_state_cache.scan_range = precomputed; // borrowed during the scan; must outlive iter3
    ASSIGN_OR_ABORT(auto iter3, segment->new_reusable_iterator(vec_schema, vec_schema, opts3, &reusable_slot));
    ASSERT_EQ(underlying, reusable_slot.get());
    ASSERT_EQ(precomputed_rows, drain(iter3));
    iter3->close();
}

// Reusing a slot whose predicate column layout differs from the first scan must rebuild the underlying
// iterator, not reset it: the stored schema was reordered (predicate columns first) for the first scan, and
// reusing that stale ordering would mis-map predicate columns. Here scan 1 filters c1 and scan 2 filters c2,
// so the reorder differs; the slot must be rebuilt and each scan must return its own correct row count.
TEST_F(SegmentIteratorTest, ReuseRebuildsWhenPredicateLayoutChanges) {
    using namespace starrocks::test;

    std::string file_name = kSegmentDir + "/reusable_pred_layout";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema =
            builder.create(1, false, TYPE_INT, true).create(2, false, TYPE_INT).create(3, false, TYPE_INT).build();
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    const int32_t chunk_size = config::vector_chunk_size;
    const size_t num_rows = 100;
    auto k_provider = [](int32_t i) { return i; };       // c0 key: 0..99
    auto c1_provider = [](int32_t i) { return i % 10; }; // c1 == 5 -> 10 rows
    auto c2_provider = [](int32_t i) { return i % 20; }; // c2 == 7 -> 5 rows
    TabletDataBuilder segment_data_builder(writer, tablet_schema, chunk_size, num_rows);
    ASSERT_OK(segment_data_builder.append(0, k_provider));
    ASSERT_OK(segment_data_builder.append(1, c1_provider));
    ASSERT_OK(segment_data_builder.append(2, c2_provider));
    ASSERT_OK(segment_data_builder.finalize_footer());

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    OlapReaderStatistics stats;
    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_INT).add(2, "c2", TYPE_INT);
    auto vec_schema = schema_builder.build();

    // Predicate on a single (non-key) column, so reorder_schema() moves it to the front and the schema
    // ordering depends on which column is filtered.
    auto make_pred_opts = [&](ColumnId cid, const std::string& value, std::unique_ptr<ColumnPredicate>& holder) {
        holder.reset(new_column_eq_predicate(get_type_info(TYPE_INT), cid, value));
        PredicateAndNode root;
        root.add_child(PredicateColumnNode{holder.get()});
        SegmentReadOptions o;
        o.fs = _fs;
        o.stats = &stats;
        o.chunk_size = chunk_size;
        o.enable_predicate_col_late_materialize = true;
        o.pred_tree = PredicateTree::create(std::move(root));
        return o;
    };
    auto drain = [&](const ChunkIteratorPtr& it) -> size_t {
        CHECK_OK(it->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
        CHECK_OK(it->init_output_schema(std::unordered_set<uint32_t>()));
        auto chunk = ChunkFactory::new_chunk(it->output_schema(), chunk_size);
        size_t total = 0;
        while (true) {
            chunk->reset();
            auto st = it->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            total += chunk->num_rows();
        }
        return total;
    };

    ChunkIteratorPtr reusable_slot;

    // First scan filters c1 -> reorder_schema puts c1 first.
    std::unique_ptr<ColumnPredicate> pred_c1;
    auto opts_c1 = make_pred_opts(1, "5", pred_c1);
    ASSIGN_OR_ABORT(auto iter_c1, segment->new_reusable_iterator(vec_schema, vec_schema, opts_c1, &reusable_slot));
    void* first_slot = reusable_slot.get();
    ASSERT_NE(nullptr, first_slot);
    ASSERT_EQ(10u, drain(iter_c1));
    iter_c1->close();

    // Second scan filters c2: the predicate layout no longer matches the stored schema, so the slot must be
    // rebuilt (different underlying iterator) and still return c2's correct row count.
    std::unique_ptr<ColumnPredicate> pred_c2;
    auto opts_c2 = make_pred_opts(2, "7", pred_c2);
    ASSIGN_OR_ABORT(auto iter_c2, segment->new_reusable_iterator(vec_schema, vec_schema, opts_c2, &reusable_slot));
    ASSERT_NE(first_slot, reusable_slot.get()) << "predicate layout changed -> slot must be rebuilt";
    ASSERT_EQ(5u, drain(iter_c2));
    iter_c2->close();
}

// Directly exercises the exported seed/rowid helpers used by the lake prepared-split seed path:
// get_prepared_pruned_row_ranges (seed pruning), block_aligned_rowid_range_from_seek_ranges and
// segment_seek_ranges_to_rowid_ranges (key-range -> rowid-range resolution), for both the empty-range
// fast paths and a non-empty key range that drives the block-aligned bound lookups.
TEST_F(SegmentIteratorTest, PreparedRowRangeAndSeekHelpers) {
    using namespace starrocks::test;

    // block_aligned_rowid_range_from_seek_ranges / segment_seek_ranges_to_rowid_ranges re-derive the
    // FileSystem from the segment's stored path via FileSystemFactory::CreateSharedFromString, so the
    // segment must live on a real (posix) path -- the in-memory MemoryFileSystem the other cases use is
    // invisible to the derived posix fs. Write it under a private temp dir and clean it up.
    const std::string dir = "/tmp/sr_prepared_row_range_helpers";
    const std::string file_name = dir + "/seg.dat";
    ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString(file_name));
    (void)fs->delete_dir_recursive(dir);
    ASSERT_OK(fs->create_dir_recursive(dir));
    DeferOp cleanup([&] { (void)fs->delete_dir_recursive(dir); });

    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema =
            builder.create(1, false, TYPE_INT, true).create(2, false, TYPE_INT).build();
    const size_t num_rows = 100;
    auto written_schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkFactory::new_chunk(written_schema, num_rows);
    for (int32_t row = 0; row < num_rows; ++row) {
        chunk->get_column_by_index(0)->as_mutable_ptr()->append_datum(Datum(row));
        chunk->get_column_by_index(1)->as_mutable_ptr()->append_datum(Datum(row * 2));
    }
    auto segment = write_and_open_segment(fs, file_name, tablet_schema, *chunk, 10);
    ASSERT_EQ(segment->num_rows(), num_rows);

    OlapReaderStatistics stats;
    VecSchemaBuilder schema_builder;
    schema_builder.add(0, "c0", TYPE_INT).add(1, "c1", TYPE_INT);
    auto vec_schema = schema_builder.build();

    // get_prepared_pruned_row_ranges: no predicate -> the seed pruning keeps every row.
    {
        SegmentReadOptions o;
        o.fs = fs;
        o.stats = &stats;
        o.chunk_size = config::vector_chunk_size;
        o.tablet_schema = tablet_schema;
        ASSIGN_OR_ABORT(auto pruned, get_prepared_pruned_row_ranges(segment, vec_schema, o));
        ASSERT_EQ(num_rows, pruned.span_size());
    }

    // Empty-range fast paths: no key bounds -> both helpers cover the whole segment without an index lookup.
    ASSIGN_OR_ABORT(auto full_block_range, block_aligned_rowid_range_from_seek_ranges(segment.get(), {}));
    ASSERT_EQ(num_rows, full_block_range.span_size());
    ASSIGN_OR_ABORT(auto empty_rowid_ranges, segment_seek_ranges_to_rowid_ranges(segment, {}, LakeIOOptions{}));
    ASSERT_TRUE(empty_rowid_ranges.empty());

    // A non-empty key range [10, 50) on the key column drives the short-key bound lookups.
    // block_aligned_rowid_range_from_seek_ranges requires the index to be loaded first (per its contract);
    // segment_seek_ranges_to_rowid_ranges loads it internally.
    LakeIOOptions io_opts{.fill_data_cache = false};
    ASSERT_OK(segment->load_index());
    // The SeekTuple key column must carry the segment's short-key length (INT index_length = 4). Without it
    // short_key_encode emits only a marker byte and every bound collapses to the same block (empty window).
    auto key_field = std::make_shared<Field>(0, "c0", TYPE_INT, -1, -1, false);
    key_field->set_uid(0);
    key_field->set_is_key(true);
    key_field->set_short_key_length(4);
    Schema key_schema({key_field});
    std::vector<SeekRange> seek_ranges;
    seek_ranges.emplace_back(SeekTuple(key_schema, {Datum(10)}), SeekTuple(key_schema, {Datum(50)}));
    seek_ranges.back().set_inclusive_lower(true);

    // The block-aligned window is a non-empty subset of the segment (keys 10..49 live in blocks of 10 rows).
    ASSIGN_OR_ABORT(auto block_range, block_aligned_rowid_range_from_seek_ranges(segment.get(), seek_ranges));
    ASSERT_GT(block_range.span_size(), 0u);
    ASSERT_LE(block_range.span_size(), num_rows);

    // One resolved rowid range per input SeekRange.
    ASSIGN_OR_ABORT(auto rowid_ranges, segment_seek_ranges_to_rowid_ranges(segment, seek_ranges, io_opts));
    ASSERT_EQ(1u, rowid_ranges.size());
    ASSERT_TRUE(rowid_ranges.front().has_value());
    EXPECT_EQ(Range<>(10, 50), *rowid_ranges.front());
}

// A stream-provided IO snapshot must reach the footer and short-key index statistics exactly once,
// including both the local and remote parts. MemoryFileSystem itself does not report an IO breakdown.
TEST_F(SegmentIteratorTest, FooterAndIndexReadsReportStreamStatsOnce) {
    using namespace starrocks::test;

    class ReadStatsStream : public io::SeekableInputStreamWrapper {
    public:
        explicit ReadStatsStream(std::unique_ptr<RandomAccessFile> file)
                : io::SeekableInputStreamWrapper(std::move(file)) {}

        io::IoStatsSnapshot get_io_stats_snapshot() const override {
            return {.bytes_read_local_disk = 128,
                    .bytes_read_remote = 256,
                    .io_count_local_disk = 2,
                    .io_count_remote = 3,
                    .io_ns_read_local_disk = 1000,
                    .io_ns_read_remote = 2000};
        }
    };
    class ReadStatsFileSystem : public MemoryFileSystem {
    public:
        StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                           const std::string& path) override {
            ASSIGN_OR_RETURN(auto file, MemoryFileSystem::new_random_access_file(opts, path));
            return std::make_unique<RandomAccessFile>(std::make_shared<ReadStatsStream>(std::move(file)), path);
        }
    };

    auto fs = std::make_shared<ReadStatsFileSystem>();
    const std::string file_name = "/prepare_read_stats.dat";
    ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(file_name));
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema =
            builder.create(1, false, TYPE_INT, true).create(2, false, TYPE_INT).build();
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, SegmentWriterOptions{});
    TabletDataBuilder data(writer, tablet_schema, config::vector_chunk_size, 100);
    ASSERT_OK(data.append(0, [](int32_t i) { return i; }));
    ASSERT_OK(data.append(1, [](int32_t i) { return i * 2; }));
    ASSERT_OK(data.finalize_footer());
    auto segment = std::make_shared<Segment>(fs, FileInfo{file_name}, 0, tablet_schema, nullptr);

    auto expect_stream_stats = [](const OlapReaderStatistics& stats) {
        EXPECT_EQ(128, stats.compressed_bytes_read_local_disk);
        EXPECT_EQ(256, stats.compressed_bytes_read_remote);
        EXPECT_EQ(384, stats.compressed_bytes_read);
        EXPECT_EQ(2, stats.io_count_local_disk);
        EXPECT_EQ(3, stats.io_count_remote);
        EXPECT_EQ(5, stats.io_count);
        EXPECT_EQ(1000, stats.io_ns_read_local_disk);
        EXPECT_EQ(2000, stats.io_ns_remote);
        EXPECT_GT(stats.io_ns, 0);
    };
    LakeIOOptions io_opts;
    io_opts.use_page_cache = false;
    OlapReaderStatistics footer_stats;
    ASSERT_OK(segment->open(nullptr, nullptr, io_opts, &footer_stats));
    EXPECT_EQ(1, footer_stats.segments_opened);
    EXPECT_EQ(100, segment->num_rows());
    expect_stream_stats(footer_stats);
    OlapReaderStatistics index_stats;
    ASSERT_OK(segment->load_index(io_opts, &index_stats));
    EXPECT_EQ(0, index_stats.segments_opened);
    EXPECT_EQ(1, index_stats.io_count_request);
    expect_stream_stats(index_stats);

    OlapReaderStatistics repeated_stats;
    ASSERT_OK(segment->open(nullptr, nullptr, io_opts, &repeated_stats));
    ASSERT_OK(segment->load_index(io_opts, &repeated_stats));
    EXPECT_EQ(0, repeated_stats.segments_opened);
    EXPECT_EQ(0, repeated_stats.io_count);
    EXPECT_EQ(0, repeated_stats.compressed_bytes_read);
    EXPECT_EQ(0, repeated_stats.io_ns);
    EXPECT_EQ(0, repeated_stats.io_count_request);

    // Corrupt real footer/index bytes after writing a valid segment. Reads before the parse error
    // still reach the caller; an unsuccessful footer open must not count as an opened segment.
    ASSIGN_OR_ABORT(auto original_file, fs->new_random_access_file(RandomAccessFileOptions{}, file_name));
    ASSIGN_OR_ABORT(auto original_bytes, original_file->read_all());
    SegmentFooterPB original_footer;
    ASSERT_TRUE(Segment::parse_segment_footer(original_file.get(), &original_footer, nullptr, nullptr).ok());
    auto write_corrupt_file = [&](const std::string& path, size_t offset) {
        std::string bytes = original_bytes;
        ASSERT_LT(offset, bytes.size());
        bytes[offset] ^= 1;
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(path));
        ASSERT_OK(file->append(Slice(bytes)));
        ASSERT_OK(file->close());
    };
    const std::string bad_footer_path = "/bad_prepare_footer.dat";
    ASSERT_NO_FATAL_FAILURE(write_corrupt_file(bad_footer_path, original_bytes.size() - 1));
    auto bad_footer = std::make_shared<Segment>(fs, FileInfo{bad_footer_path}, 0, tablet_schema, nullptr);
    OlapReaderStatistics failed_footer_stats;
    EXPECT_TRUE(bad_footer->open(nullptr, nullptr, io_opts, &failed_footer_stats).is_corruption());
    EXPECT_EQ(0, failed_footer_stats.segments_opened);
    expect_stream_stats(failed_footer_stats);

    const std::string bad_index_path = "/bad_prepare_index.dat";
    ASSERT_NO_FATAL_FAILURE(write_corrupt_file(bad_index_path, original_footer.short_key_index_page().offset()));
    auto bad_index = std::make_shared<Segment>(fs, FileInfo{bad_index_path}, 0, tablet_schema, nullptr);
    ASSERT_OK(bad_index->open(nullptr, nullptr, io_opts));
    OlapReaderStatistics failed_index_stats;
    EXPECT_TRUE(bad_index->load_index(io_opts, &failed_index_stats).is_corruption());
    EXPECT_EQ(1, failed_index_stats.io_count_request);
    expect_stream_stats(failed_index_stats);
}

>>>>>>> 78e136a ([BugFix] Fix wrong results when bitmap index partially erases a pushed-down OR (#82841))
// A `.cols` segment is opened by every scan and never cached, so its footer read has to follow the
// caller's cache options and report to the caller's statistics. Opening it with default options made
// the read skip the local data cache fill and left it out of the scan's IO statistics.
TEST_F(SegmentIteratorTest, DcgSegmentOpenFollowsCallerIOOptionsAndStats) {
    using namespace starrocks::test;

    class ReadStatsStream : public io::SeekableInputStreamWrapper {
    public:
        explicit ReadStatsStream(std::unique_ptr<RandomAccessFile> file)
                : io::SeekableInputStreamWrapper(std::move(file)) {}

        io::IoStatsSnapshot get_io_stats_snapshot() const override {
            return {.bytes_read_remote = 256, .io_count_remote = 3, .io_ns_read_remote = 2000};
        }
    };
    class RecordingFileSystem : public MemoryFileSystem {
    public:
        StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                           const std::string& path) override {
            skip_fill_by_path[path].push_back(opts.skip_fill_local_cache);
            ASSIGN_OR_RETURN(auto file, MemoryFileSystem::new_random_access_file(opts, path));
            return std::make_unique<RandomAccessFile>(std::make_shared<ReadStatsStream>(std::move(file)), path);
        }

        // One entry per open of the path, in order.
        std::unordered_map<std::string, std::vector<bool>> skip_fill_by_path;
    };
    // Serves one delta column group that moves column uid 2 into `update.cols`.
    class OneDcgLoader : public DeltaColumnGroupLoader {
    public:
        Status load(const TabletSegmentId& tsid, int64_t version, DeltaColumnGroupList* pdcgs) override {
            return _load(pdcgs);
        }
        Status load(int64_t tablet_id, RowsetId rowsetid, uint32_t segment_id, int64_t version,
                    DeltaColumnGroupList* pdcgs) override {
            return _load(pdcgs);
        }

    private:
        static Status _load(DeltaColumnGroupList* pdcgs) {
            auto dcg = std::make_shared<DeltaColumnGroup>();
            dcg->init(1, {{2}}, {"update.cols"});
            pdcgs->push_back(std::move(dcg));
            return Status::OK();
        }
    };

    auto fs = std::make_shared<RecordingFileSystem>();
    ASSERT_OK(fs->create_dir("/dcg"));
    TabletSchemaBuilder builder;
    std::shared_ptr<TabletSchema> tablet_schema =
            builder.create(1, false, TYPE_INT, true).create(2, false, TYPE_INT).build();
    // The second column holds i * |factor|, so a scan shows which file it read the column from.
    auto write_segment = [&](const std::string& path, int32_t factor) {
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(path));
        SegmentWriter writer(std::move(wfile), 0, tablet_schema, SegmentWriterOptions{});
        TabletDataBuilder data(writer, tablet_schema, config::vector_chunk_size, 100);
        ASSERT_OK(data.append(0, [](int32_t i) { return i; }));
        ASSERT_OK(data.append(1, [factor](int32_t i) { return i * factor; }));
        ASSERT_OK(data.finalize_footer());
    };
    const std::string segment_path = "/dcg/seg.dat";
    const std::string cols_path = "/dcg/update.cols";
    ASSERT_NO_FATAL_FAILURE(write_segment(segment_path, 2));
    ASSERT_NO_FATAL_FAILURE(write_segment(cols_path, 3));

    auto segment = std::make_shared<Segment>(fs, FileInfo{segment_path}, 0, tablet_schema, nullptr);
    ASSERT_OK(segment->open(nullptr, nullptr, LakeIOOptions{}));
    DeltaColumnGroup dcg;
    dcg.init(1, {{2}}, {"update.cols"});

    // The caller asks for the cache to be filled: the footer read must not skip the fill, and it is
    // added to the caller's statistics.
    LakeIOOptions fill_opts;
    fill_opts.fill_data_cache = true;
    OlapReaderStatistics stats;
    ASSIGN_OR_ABORT(auto filled, segment->new_dcg_segment(dcg, 0, tablet_schema, fill_opts, &stats));
    EXPECT_EQ(100, filled->num_rows());
    ASSERT_EQ(1, fs->skip_fill_by_path[cols_path].size());
    EXPECT_FALSE(fs->skip_fill_by_path[cols_path].back());
    EXPECT_EQ(1, stats.segments_read_count);
    EXPECT_EQ(3, stats.io_count_remote);
    EXPECT_EQ(3, stats.io_count);
    EXPECT_EQ(256, stats.compressed_bytes_read_remote);
    EXPECT_EQ(2000, stats.io_ns_remote);
    EXPECT_GT(stats.io_ns, 0);

    // A caller that does not want the cache filled keeps that behavior.
    LakeIOOptions no_fill_opts;
    no_fill_opts.fill_data_cache = false;
    ASSIGN_OR_ABORT(auto not_filled, segment->new_dcg_segment(dcg, 0, tablet_schema, no_fill_opts));
    EXPECT_EQ(100, not_filled->num_rows());
    EXPECT_TRUE(fs->skip_fill_by_path[cols_path].back());

    // The scan path: SegmentIterator has to hand its own cache options and statistics to the `.cols`
    // open. It opens the file twice, once for the footer and once to read the column, and neither
    // open may skip the cache fill.
    fs->skip_fill_by_path.clear();
    OlapReaderStatistics scan_stats;
    SegmentReadOptions seg_opts;
    seg_opts.fs = fs;
    seg_opts.stats = &scan_stats;
    seg_opts.tablet_schema = tablet_schema;
    seg_opts.dcg_loader = std::make_shared<OneDcgLoader>();
    seg_opts.lake_io_opts.fill_data_cache = true;
    auto scan_schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk_iter = new_segment_iterator(segment, scan_schema, seg_opts);
    auto chunk = ChunkHelper::new_chunk(scan_schema, config::vector_chunk_size);
    ASSERT_OK(chunk_iter->get_next(chunk.get()));
    ASSERT_EQ(100, chunk->num_rows());
    // The column came from the `.cols` file, not from the base segment.
    EXPECT_EQ(3 * 7, chunk->get_column_by_index(1)->get(7).get_int32());
    chunk_iter->close();

    const auto& cols_opens = fs->skip_fill_by_path[cols_path];
    ASSERT_EQ(2, cols_opens.size());
    EXPECT_FALSE(cols_opens[0]);
    EXPECT_FALSE(cols_opens[1]);
    // The base segment was opened above without statistics, so the one segment counted here is the
    // `.cols` segment the scan opened.
    EXPECT_EQ(1, scan_stats.segments_read_count);
}

} // namespace starrocks
