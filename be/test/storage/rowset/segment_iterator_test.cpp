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
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
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

    void TearDown() override { StoragePageCache::instance()->prune(); }

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

<<<<<<< HEAD
=======
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

    auto res_chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), chunk_size);
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

    auto res_chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), config::vector_chunk_size);
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

    auto res_chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), config::vector_chunk_size);
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

>>>>>>> 5390eed48a ([BugFix] Stop reading past the end of the output schema in _switch_context (#77660))
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

} // namespace starrocks
