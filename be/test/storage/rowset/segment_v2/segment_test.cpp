// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/segment_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/rowset/segment_v2/segment.h"

#include <gtest/gtest.h>

#include <functional>
#include <iostream>

#include "common/logging.h"
#include "env/env_memory.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/comparison_predicate.h"
#include "storage/fs/file_block_manager.h"
#include "storage/in_list_predicate.h"
#include "storage/olap_common.h"
#include "storage/row_block2.h"
#include "storage/row_cursor.h"
#include "storage/rowset/segment_v2/column_iterator.h"
#include "storage/rowset/segment_v2/column_reader.h"
#include "storage/rowset/segment_v2/segment_iterator.h"
#include "storage/rowset/segment_v2/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "util/file_utils.h"

#define ASSERT_OK(expr)                                   \
    do {                                                  \
        Status _status = (expr);                          \
        ASSERT_TRUE(_status.ok()) << _status.to_string(); \
    } while (0)

namespace starrocks {
namespace segment_v2 {

using std::string;
using std::shared_ptr;

using std::vector;

using ValueGenerator = std::function<void(size_t rid, int cid, int block_id, RowCursorCell& cell)>;

// 0,  1,  2,  3
// 10, 11, 12, 13
// 20, 21, 22, 23
static void DefaultIntGenerator(size_t rid, int cid, int block_id, RowCursorCell& cell) {
    cell.set_not_null();
    *(int*)cell.mutable_cell_ptr() = rid * 10 + cid;
}

class SegmentReaderWriterTest : public ::testing::Test {
protected:
    void SetUp() override {
        _env = new EnvMemory();
        _block_mgr = new fs::FileBlockManager(_env, fs::BlockManagerOptions());
        ASSERT_TRUE(_env->create_dir(kSegmentDir).ok());
        _page_cache_mem_tracker = std::make_unique<MemTracker>();
        StoragePageCache::create_global_cache(_page_cache_mem_tracker.get(), 1000000000);
    }

    void TearDown() override {
        delete _block_mgr;
        delete _env;
        StoragePageCache::release_global_cache();
    }

    TabletSchema create_schema(const std::vector<TabletColumn>& columns, int num_short_key_columns = -1) {
        TabletSchema res;
        int num_key_columns = 0;
        for (auto& col : columns) {
            if (col.is_key()) {
                num_key_columns++;
            }
            res._cols.push_back(col);
        }
        res._num_key_columns = num_key_columns;
        res._num_short_key_columns = num_short_key_columns != -1 ? num_short_key_columns : num_key_columns;
        return res;
    }

    void build_segment(SegmentWriterOptions opts, const TabletSchema& build_schema, const TabletSchema& query_schema,
                       size_t nrows, const ValueGenerator& generator, shared_ptr<Segment>* res) {
        static int seg_id = 0;
        // must use unique filename for each segment, otherwise page cache kicks in and produces
        // the wrong answer (it use (filename,offset) as cache key)
        std::string filename = strings::Substitute("$0/seg_$1.dat", kSegmentDir, seg_id++);
        std::unique_ptr<fs::WritableBlock> wblock;
        fs::CreateBlockOptions block_opts({filename});
        ASSERT_OK(_block_mgr->create_block(block_opts, &wblock));
        SegmentWriter writer(std::move(wblock), 0, &build_schema, opts);
        ASSERT_OK(writer.init(10));

        RowCursor row;
        auto olap_st = row.init(build_schema);
        ASSERT_EQ(OLAP_SUCCESS, olap_st);

        for (size_t rid = 0; rid < nrows; ++rid) {
            for (int cid = 0; cid < build_schema.num_columns(); ++cid) {
                int row_block_id = rid / opts.num_rows_per_block;
                RowCursorCell cell = row.cell(cid);
                generator(rid, cid, row_block_id, cell);
            }
            ASSERT_OK(writer.append_row(row));
        }

        uint64_t file_size, index_size;
        ASSERT_OK(writer.finalize(&file_size, &index_size));

        *res = *Segment::open(_block_mgr, filename, 0, &query_schema);
        ASSERT_EQ(nrows, (*res)->num_rows());
    }

    const std::string kSegmentDir = "/segment_test";

    EnvMemory* _env = nullptr;
    fs::FileBlockManager* _block_mgr = nullptr;
    std::unique_ptr<MemTracker> _page_cache_mem_tracker = nullptr;
};

TEST_F(SegmentReaderWriterTest, normal) {
    TabletSchema tablet_schema =
            create_schema({create_int_key(1), create_int_key(2), create_int_value(3), create_int_value(4)});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;

    shared_ptr<Segment> segment;
    build_segment(opts, tablet_schema, tablet_schema, 4096, DefaultIntGenerator, &segment);

    // reader
    {
        Schema schema(tablet_schema);
        OlapReaderStatistics stats;
        // scan all rows
        {
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            int left = 4096;

            int rowid = 0;
            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                ASSERT_OK(iter->next_batch(&block));
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        ASSERT_FALSE(column_block.is_null(i));
                        ASSERT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i));
                    }
                }
                rowid += rows_read;
            }
        }
        // test seek, key
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(tablet_schema, 2);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = 100;
            }
            {
                auto cell = lower_bound->cell(1);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = 100;
            }

            // upper bound
            std::unique_ptr<RowCursor> upper_bound(new RowCursor());
            upper_bound->init(tablet_schema, 1);
            {
                auto cell = upper_bound->cell(0);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = 200;
            }

            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, upper_bound.get(), true);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            ASSERT_OK(iter->next_batch(&block));
            ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
            ASSERT_EQ(11, block.num_rows());
            auto column_block = block.column_block(0);
            for (int i = 0; i < 11; ++i) {
                ASSERT_EQ(100 + i * 10, *(int*)column_block.cell_ptr(i));
            }
        }
        // test seek, key
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(tablet_schema, 1);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = 40970;
            }

            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, nullptr, false);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            ASSERT_TRUE(iter->next_batch(&block).is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }
        // test seek, key (-2, -1)
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(tablet_schema, 1);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = -2;
            }

            std::unique_ptr<RowCursor> upper_bound(new RowCursor());
            upper_bound->init(tablet_schema, 1);
            {
                auto cell = upper_bound->cell(0);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = -1;
            }

            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, upper_bound.get(), false);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            ASSERT_TRUE(iter->next_batch(&block).is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }
    }
}

TEST_F(SegmentReaderWriterTest, LateMaterialization) {
    TabletSchema tablet_schema = create_schema({create_int_key(1), create_int_value(2)});
    ValueGenerator data_gen = [](size_t rid, int cid, int block_id, RowCursorCell& cell) {
        cell.set_not_null();
        if (cid == 0) {
            *(int*)(cell.mutable_cell_ptr()) = rid;
        } else if (cid == 1) {
            *(int*)(cell.mutable_cell_ptr()) = rid * 10;
        }
    };

    {
        shared_ptr<Segment> segment;
        SegmentWriterOptions opts;
        build_segment(opts, tablet_schema, tablet_schema, 100, data_gen, &segment);
        {
            // lazy enabled when predicate is subset of returned columns:
            // select c1, c2 where c2 = 30;
            Schema read_schema(tablet_schema);
            std::unique_ptr<ColumnPredicate> predicate(new EqualPredicate<int32_t>(1, 30));
            std::vector<const ColumnPredicate*> predicates = {predicate.get()};

            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.column_predicates = &predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            ASSERT_OK(segment->new_iterator(read_schema, read_opts, &iter));

            RowBlockV2 block(read_schema, 1024);
            ASSERT_OK(iter->next_batch(&block));
            ASSERT_TRUE(iter->is_lazy_materialization_read());
            ASSERT_EQ(1, block.selected_size());
            ASSERT_EQ(99, stats.rows_vec_cond_filtered);
            auto row = block.row(block.selection_vector()[0]);
            ASSERT_EQ("[3,30]", row.debug_string());
        }
        {
            // lazy disabled when all return columns have predicates:
            // select c1, c2 where c1 = 10 and c2 = 100;
            Schema read_schema(tablet_schema);
            std::unique_ptr<ColumnPredicate> p0(new EqualPredicate<int32_t>(0, 10));
            std::unique_ptr<ColumnPredicate> p1(new EqualPredicate<int32_t>(1, 100));
            std::vector<const ColumnPredicate*> predicates = {p0.get(), p1.get()};

            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.column_predicates = &predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            ASSERT_OK(segment->new_iterator(read_schema, read_opts, &iter));

            RowBlockV2 block(read_schema, 1024);
            ASSERT_OK(iter->next_batch(&block));
            ASSERT_FALSE(iter->is_lazy_materialization_read());
            ASSERT_EQ(1, block.selected_size());
            ASSERT_EQ(99, stats.rows_vec_cond_filtered);
            auto row = block.row(block.selection_vector()[0]);
            ASSERT_EQ("[10,100]", row.debug_string());
        }
        {
            // lazy disabled when no predicate:
            // select c2
            std::vector<ColumnId> read_cols = {1};
            Schema read_schema(tablet_schema.columns(), read_cols);
            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            ASSERT_OK(segment->new_iterator(read_schema, read_opts, &iter));

            RowBlockV2 block(read_schema, 1024);
            ASSERT_OK(iter->next_batch(&block));
            ASSERT_FALSE(iter->is_lazy_materialization_read());
            ASSERT_EQ(100, block.selected_size());
            for (int i = 0; i < block.selected_size(); ++i) {
                auto row = block.row(block.selection_vector()[i]);
                ASSERT_EQ(strings::Substitute("[$0]", i * 10), row.debug_string());
            }
        }
    }

    {
        tablet_schema = create_schema({create_int_key(1, true, false, true), create_int_value(2)});
        shared_ptr<Segment> segment;
        SegmentWriterOptions write_opts;
        build_segment(write_opts, tablet_schema, tablet_schema, 100, data_gen, &segment);
        ASSERT_TRUE(segment->column(0)->has_bitmap_index());
        {
            // lazy disabled when all predicates are removed by bitmap index:
            // select c1, c2 where c2 = 30;
            Schema read_schema(tablet_schema);
            std::unique_ptr<ColumnPredicate> predicate(new EqualPredicate<int32_t>(0, 20));
            std::vector<const ColumnPredicate*> predicates = {predicate.get()};

            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.column_predicates = &predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            ASSERT_OK(segment->new_iterator(read_schema, read_opts, &iter));

            RowBlockV2 block(read_schema, 1024);
            ASSERT_OK(iter->next_batch(&block));
            ASSERT_FALSE(iter->is_lazy_materialization_read());
            ASSERT_EQ(1, block.selected_size());
            ASSERT_EQ(99, stats.rows_bitmap_index_filtered);
            ASSERT_EQ(0, stats.rows_vec_cond_filtered);
            auto row = block.row(block.selection_vector()[0]);
            ASSERT_EQ("[20,200]", row.debug_string());
        }
    }
}

TEST_F(SegmentReaderWriterTest, TestIndex) {
    TabletSchema tablet_schema =
            create_schema({create_int_key(1), create_int_key(2, true, true), create_int_key(3), create_int_value(4)});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;

    std::shared_ptr<Segment> segment;
    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    // ...
    // 64k int will generate 4 pages
    build_segment(
            opts, tablet_schema, tablet_schema, 64 * 1024,
            [](size_t rid, int cid, int block_id, RowCursorCell& cell) {
                cell.set_not_null();
                if (rid >= 16 * 1024 && rid < 32 * 1024) {
                    // make second page all rows equal
                    *(int*)cell.mutable_cell_ptr() = 164000 + cid;

                } else {
                    *(int*)cell.mutable_cell_ptr() = rid * 10 + cid;
                }
            },
            &segment);

    // reader with condition
    {
        Schema schema(tablet_schema);
        OlapReaderStatistics stats;
        // test empty segment iterator
        {
            // the first two page will be read by this condition
            TCondition condition;
            condition.__set_column_name("3");
            condition.__set_condition_op("<");
            std::vector<std::string> vals = {"2"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(&tablet_schema);
            conditions->append_condition(condition);

            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1);

            ASSERT_TRUE(iter->next_batch(&block).is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }
        // scan all rows
        {
            TCondition condition;
            condition.__set_column_name("2");
            condition.__set_condition_op("<");
            std::vector<std::string> vals = {"100"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(&tablet_schema);
            conditions->append_condition(condition);

            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            // only first page will be read because of zone map
            int left = 16 * 1024;

            int rowid = 0;
            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                ASSERT_OK(iter->next_batch(&block));
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        ASSERT_FALSE(column_block.is_null(i));
                        ASSERT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i)) << "rid:" << rid << ", i:" << i;
                    }
                }
                rowid += rows_read;
            }
            ASSERT_EQ(16 * 1024, rowid);
            ASSERT_TRUE(iter->next_batch(&block).is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }
        // test zone map with query predicate an delete predicate
        {
            // the first two page will be read by this condition
            TCondition condition;
            condition.__set_column_name("2");
            condition.__set_condition_op("<");
            std::vector<std::string> vals = {"165000"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(&tablet_schema);
            conditions->append_condition(condition);

            // the second page read will be pruned by the following delete predicate
            TCondition delete_condition;
            delete_condition.__set_column_name("2");
            delete_condition.__set_condition_op("=");
            std::vector<std::string> vals2 = {"164001"};
            delete_condition.__set_condition_values(vals2);
            std::shared_ptr<Conditions> delete_conditions(new Conditions());
            delete_conditions->set_tablet_schema(&tablet_schema);
            delete_conditions->append_condition(delete_condition);

            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();
            read_opts.delete_conditions.push_back(delete_conditions.get());

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            // so the first page will be read because of zone map
            int left = 16 * 1024;

            int rowid = 0;
            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                ASSERT_OK(iter->next_batch(&block));
                ASSERT_EQ(rows_read, block.num_rows());
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        ASSERT_FALSE(column_block.is_null(i));
                        ASSERT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i)) << "rid:" << rid << ", i:" << i;
                    }
                }
                rowid += rows_read;
            }
            ASSERT_EQ(16 * 1024, rowid);
            ASSERT_TRUE(iter->next_batch(&block).is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }
        // test bloom filter
        {
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            TCondition condition;
            condition.__set_column_name("2");
            condition.__set_condition_op("=");
            // 102 is not in page 1
            std::vector<std::string> vals = {"102"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(&tablet_schema);
            conditions->append_condition(condition);
            read_opts.conditions = conditions.get();
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            ASSERT_TRUE(iter->next_batch(&block).is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }
    }
}

TEST_F(SegmentReaderWriterTest, estimate_segment_size) {
    size_t num_rows_per_block = 10;

    std::shared_ptr<TabletSchema> tablet_schema(new TabletSchema());
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 2;
    tablet_schema->_num_rows_per_row_block = num_rows_per_block;
    tablet_schema->_cols.push_back(create_int_key(1));
    tablet_schema->_cols.push_back(create_int_key(2));
    tablet_schema->_cols.push_back(create_int_key(3));
    tablet_schema->_cols.push_back(create_int_value(4));

    // segment write
    std::string dname = "/segment_write_size";
    ASSERT_OK(_env->create_dir(dname));

    SegmentWriterOptions opts;
    opts.num_rows_per_block = num_rows_per_block;

    std::string fname = dname + "/int_case";
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts({fname});
    ASSERT_OK(_block_mgr->create_block(wblock_opts, &wblock));
    SegmentWriter writer(std::move(wblock), 0, tablet_schema.get(), opts);
    ASSERT_OK(writer.init(10));

    RowCursor row;
    auto olap_st = row.init(*tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    for (int i = 0; i < 1048576; ++i) {
        for (int j = 0; j < 4; ++j) {
            auto cell = row.cell(j);
            cell.set_not_null();
            *(int*)cell.mutable_cell_ptr() = i * 10 + j;
        }
        writer.append_row(row);
    }

    uint32_t segment_size = writer.estimate_segment_size();
    LOG(INFO) << "estimated segment sizes=" << segment_size;

    uint64_t file_size = 0;
    uint64_t index_size;
    ASSERT_OK(writer.finalize(&file_size, &index_size));

    ASSERT_OK(_env->get_file_size(fname, &file_size));
    LOG(INFO) << "segment file size=" << file_size;

    ASSERT_NE(segment_size, 0);
}

TEST_F(SegmentReaderWriterTest, TestDefaultValueColumn) {
    std::vector<TabletColumn> columns = {create_int_key(1), create_int_key(2), create_int_value(3),
                                         create_int_value(4)};
    TabletSchema build_schema = create_schema(columns);

    // add a column with null default value
    {
        std::vector<TabletColumn> read_columns = columns;
        read_columns.push_back(create_int_value(5, OLAP_FIELD_AGGREGATION_SUM, true, "NULL"));
        TabletSchema query_schema = create_schema(read_columns);

        std::shared_ptr<Segment> segment;
        SegmentWriterOptions opts;
        build_segment(opts, build_schema, query_schema, 4096, DefaultIntGenerator, &segment);

        Schema schema(query_schema);
        OlapReaderStatistics stats;
        // scan all rows
        {
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            int left = 4096;

            int rowid = 0;
            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                ASSERT_OK(iter->next_batch(&block));
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        if (cid == 4) {
                            ASSERT_TRUE(column_block.is_null(i));
                        } else {
                            ASSERT_FALSE(column_block.is_null(i));
                            ASSERT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i));
                        }
                    }
                }
                rowid += rows_read;
            }
        }
    }

    // add a column with non-null default value
    {
        std::vector<TabletColumn> read_columns = columns;
        read_columns.push_back(create_int_value(5, OLAP_FIELD_AGGREGATION_SUM, true, "10086"));
        TabletSchema query_schema = create_schema(read_columns);

        std::shared_ptr<Segment> segment;
        SegmentWriterOptions opts;
        build_segment(opts, build_schema, query_schema, 4096, DefaultIntGenerator, &segment);

        Schema schema(query_schema);
        OlapReaderStatistics stats;
        // scan all rows
        {
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            int left = 4096;

            int rowid = 0;
            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                ASSERT_OK(iter->next_batch(&block));
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        if (cid == 4) {
                            ASSERT_FALSE(column_block.is_null(i));
                            ASSERT_EQ(10086, *(int*)column_block.cell_ptr(i));
                        } else {
                            ASSERT_FALSE(column_block.is_null(i));
                            ASSERT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i));
                        }
                    }
                }
                rowid += rows_read;
            }
        }
    }
}

TEST_F(SegmentReaderWriterTest, TestStringDict) {
    size_t num_rows_per_block = 10;
    MemPool pool;

    std::shared_ptr<TabletSchema> tablet_schema(new TabletSchema());
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 2;
    tablet_schema->_num_rows_per_row_block = num_rows_per_block;
    tablet_schema->_cols.push_back(create_char_key(1));
    tablet_schema->_cols.push_back(create_char_key(2));
    tablet_schema->_cols.push_back(create_varchar_key(3));
    tablet_schema->_cols.push_back(create_varchar_key(4));

    SegmentWriterOptions opts;
    opts.num_rows_per_block = num_rows_per_block;

    std::string fname = kSegmentDir + "/string_case";
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts({fname});
    ASSERT_OK(_block_mgr->create_block(wblock_opts, &wblock));
    SegmentWriter writer(std::move(wblock), 0, tablet_schema.get(), opts);
    ASSERT_OK(writer.init(10));

    RowCursor row;
    auto olap_st = row.init(*tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    // convert int to string
    for (int i = 0; i < 4096; ++i) {
        for (int j = 0; j < 4; ++j) {
            auto cell = row.cell(j);
            cell.set_not_null();
            set_column_value_by_type(tablet_schema->_cols[j].type(), i * 10 + j, (char*)cell.mutable_cell_ptr(), &pool,
                                     tablet_schema->_cols[j].length());
        }
        ASSERT_OK(writer.append_row(row));
    }

    uint64_t file_size = 0;
    uint64_t index_size;
    ASSERT_OK(writer.finalize(&file_size, &index_size));

    {
        auto segment = *Segment::open(_block_mgr, fname, 0, tablet_schema.get());
        ASSERT_EQ(4096, segment->num_rows());
        Schema schema(*tablet_schema);
        OlapReaderStatistics stats;
        // scan all rows
        {
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            int left = 4096;
            int rowid = 0;

            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                ASSERT_OK(iter->next_batch(&block));
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        ASSERT_FALSE(column_block.is_null(i));
                        const Slice* actual = reinterpret_cast<const Slice*>(column_block.cell_ptr(i));

                        Slice expect;
                        set_column_value_by_type(tablet_schema->_cols[j].type(), rid * 10 + cid,
                                                 reinterpret_cast<char*>(&expect), &pool,
                                                 tablet_schema->_cols[j].length());
                        ASSERT_EQ(expect.to_string(), actual->to_string());
                    }
                }
                rowid += rows_read;
            }
        }

        // test seek, key
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(*tablet_schema, 1);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                set_column_value_by_type(OLAP_FIELD_TYPE_CHAR, 40970, (char*)cell.mutable_cell_ptr(), &pool,
                                         tablet_schema->_cols[0].length());
            }

            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, nullptr, false);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            ASSERT_TRUE(iter->next_batch(&block).is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }

        // test seek, key (-2, -1)
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(*tablet_schema, 1);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                set_column_value_by_type(OLAP_FIELD_TYPE_CHAR, -2, (char*)cell.mutable_cell_ptr(), &pool,
                                         tablet_schema->_cols[0].length());
            }

            std::unique_ptr<RowCursor> upper_bound(new RowCursor());
            upper_bound->init(*tablet_schema, 1);
            {
                auto cell = upper_bound->cell(0);
                cell.set_not_null();
                set_column_value_by_type(OLAP_FIELD_TYPE_CHAR, -1, (char*)cell.mutable_cell_ptr(), &pool,
                                         tablet_schema->_cols[0].length());
            }

            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, upper_bound.get(), false);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            ASSERT_TRUE(iter->next_batch(&block).is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }

        // test char zone_map query hit;should read whole page
        {
            TCondition condition;
            condition.__set_column_name("1");
            condition.__set_condition_op(">");
            std::vector<std::string> vals = {"100"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(tablet_schema.get());
            conditions->append_condition(condition);

            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            int left = 4 * 1024;
            int rowid = 0;

            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                ASSERT_OK(iter->next_batch(&block));
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        ASSERT_FALSE(column_block.is_null(i));

                        const Slice* actual = reinterpret_cast<const Slice*>(column_block.cell_ptr(i));
                        Slice expect;
                        set_column_value_by_type(tablet_schema->_cols[j].type(), rid * 10 + cid,
                                                 reinterpret_cast<char*>(&expect), &pool,
                                                 tablet_schema->_cols[j].length());
                        ASSERT_EQ(expect.to_string(), actual->to_string()) << "rid:" << rid << ", i:" << i;
                    }
                }
                rowid += rows_read;
            }
            ASSERT_EQ(4 * 1024, rowid);
            ASSERT_TRUE(iter->next_batch(&block).is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }

        // test char zone_map query miss;col < -1
        {
            TCondition condition;
            condition.__set_column_name("1");
            condition.__set_condition_op("<");
            std::vector<std::string> vals = {"-2"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(tablet_schema.get());
            conditions->append_condition(condition);

            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            ASSERT_TRUE(iter->next_batch(&block).is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }
    }
}

TEST_F(SegmentReaderWriterTest, TestBitmapPredicate) {
    TabletSchema tablet_schema =
            create_schema({create_int_key(1, true, false, true), create_int_key(2, true, false, true),
                           create_int_value(3), create_int_value(4)});

    SegmentWriterOptions opts;
    shared_ptr<Segment> segment;
    build_segment(opts, tablet_schema, tablet_schema, 4096, DefaultIntGenerator, &segment);
    ASSERT_TRUE(segment->column(0)->has_bitmap_index());
    ASSERT_TRUE(segment->column(1)->has_bitmap_index());

    {
        Schema schema(tablet_schema);

        // test where v1=10
        {
            std::vector<const ColumnPredicate*> column_predicates;
            std::unique_ptr<ColumnPredicate> predicate(new EqualPredicate<int32_t>(0, 10));
            column_predicates.emplace_back(predicate.get());

            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.column_predicates = &column_predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            ASSERT_OK(iter->next_batch(&block));
            ASSERT_EQ(block.num_rows(), 1);
            ASSERT_EQ(read_opts.stats->raw_rows_read, 1);
        }

        // test where v1=10 and v2=11
        {
            std::vector<const ColumnPredicate*> column_predicates;
            std::unique_ptr<ColumnPredicate> predicate(new EqualPredicate<int32_t>(0, 10));
            std::unique_ptr<ColumnPredicate> predicate2(new EqualPredicate<int32_t>(1, 11));
            column_predicates.emplace_back(predicate.get());
            column_predicates.emplace_back(predicate2.get());

            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.column_predicates = &column_predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            ASSERT_OK(iter->next_batch(&block));
            ASSERT_EQ(block.num_rows(), 1);
            ASSERT_EQ(read_opts.stats->raw_rows_read, 1);
        }

        // test where v1=10 and v2=15
        {
            std::vector<const ColumnPredicate*> column_predicates;
            std::unique_ptr<ColumnPredicate> predicate(new EqualPredicate<int32_t>(0, 10));
            std::unique_ptr<ColumnPredicate> predicate2(new EqualPredicate<int32_t>(1, 15));
            column_predicates.emplace_back(predicate.get());
            column_predicates.emplace_back(predicate2.get());

            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.column_predicates = &column_predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            ASSERT_TRUE(iter->next_batch(&block).is_end_of_file());
            ASSERT_EQ(read_opts.stats->raw_rows_read, 0);
        }

        // test where v1 in (10,20,1)
        {
            std::vector<const ColumnPredicate*> column_predicates;
            std::set<int32_t> values;
            values.insert(10);
            values.insert(20);
            values.insert(1);
            std::unique_ptr<ColumnPredicate> predicate(new InListPredicate<int32_t>(0, std::move(values)));
            column_predicates.emplace_back(predicate.get());

            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.column_predicates = &column_predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            ASSERT_OK(iter->next_batch(&block));
            ASSERT_EQ(read_opts.stats->raw_rows_read, 2);
        }

        // test where v1 not in (10,20)
        {
            std::vector<const ColumnPredicate*> column_predicates;
            std::set<int32_t> values;
            values.insert(10);
            values.insert(20);
            std::unique_ptr<ColumnPredicate> predicate(new NotInListPredicate<int32_t>(0, std::move(values)));
            column_predicates.emplace_back(predicate.get());

            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.block_mgr = _block_mgr;
            read_opts.column_predicates = &column_predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            Status st;
            do {
                block.clear();
                st = iter->next_batch(&block);
            } while (st.ok());
            ASSERT_EQ(4094, read_opts.stats->raw_rows_read);
        }
    }
}

TEST_F(SegmentReaderWriterTest, TestBloomFilterIndexUniqueModel) {
    TabletSchema schema = create_schema({create_int_key(1), create_int_key(2), create_int_key(3),
                                         create_int_value(4, OLAP_FIELD_AGGREGATION_REPLACE, true, "", true)});

    // for not base segment
    SegmentWriterOptions opts1;
    shared_ptr<Segment> seg1;
    build_segment(opts1, schema, schema, 100, DefaultIntGenerator, &seg1);
    ASSERT_TRUE(seg1->column(3)->has_bloom_filter_index());

    // for base segment
    SegmentWriterOptions opts2;
    shared_ptr<Segment> seg2;
    build_segment(opts2, schema, schema, 100, DefaultIntGenerator, &seg2);
    ASSERT_TRUE(seg2->column(3)->has_bloom_filter_index());
}

} // namespace segment_v2
} // namespace starrocks
