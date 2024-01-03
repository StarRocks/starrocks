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

#include <gtest/gtest.h>

#include <filesystem>
#include <random>
#include <vector>

#include "column/column_helper.h"
#include "exec/hdfs_scanner.h"
#include "exprs/binary_predicate.h"
#include "exprs/expr_context.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/group_reader.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/parquet/parquet_ut_base.h"
#include "fs/fs.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptor_helper.h"
#include "runtime/mem_tracker.h"

namespace starrocks::parquet {

static HdfsScanStats g_hdfs_scan_stats;
using starrocks::HdfsScannerContext;

class PageIndexTest : public testing::Test {
public:
    void SetUp() override { _runtime_state = _pool.add(new RuntimeState(TQueryGlobals())); }

    void TearDown() override {}

protected:
    std::unique_ptr<RandomAccessFile> _create_file(const std::string& file_path);

    HdfsScannerContext* _create_scan_context();

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t scan_length = 0);

    HdfsScannerContext* _create_file_random_read_context(const std::string& file_path);
    HdfsScannerContext* _create_file_only_c0_context(const std::string& file_path);
    HdfsScannerContext* _create_file_c0_c1_c2_context(const std::string& file_path);

    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;
};

std::unique_ptr<RandomAccessFile> PageIndexTest::_create_file(const std::string& file_path) {
    return *FileSystem::Default()->new_random_access_file(file_path);
}

HdfsScannerContext* PageIndexTest::_create_scan_context() {
    auto* ctx = _pool.add(new HdfsScannerContext());
    auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
    ctx->lazy_column_coalesce_counter = lazy_column_coalesce_counter;
    ctx->timezone = "Asia/Shanghai";
    ctx->stats = &g_hdfs_scan_stats;
    return ctx;
}

HdfsScannerContext* PageIndexTest::_create_file_random_read_context(const std::string& file_path) {
    auto ctx = _create_scan_context();

    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"c3", type_array},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(file_path));

    return ctx;
}

HdfsScannerContext* PageIndexTest::_create_file_only_c0_context(const std::string& file_path) {
    auto ctx = _create_scan_context();

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(file_path));

    return ctx;
}

HdfsScannerContext* PageIndexTest::_create_file_c0_c1_c2_context(const std::string& file_path) {
    auto ctx = _create_scan_context();

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {""},
    };
    ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
    ctx->scan_ranges.emplace_back(_create_scan_range(file_path));

    return ctx;
}

THdfsScanRange* PageIndexTest::_create_scan_range(const std::string& file_path, size_t scan_length) {
    auto* scan_range = _pool.add(new THdfsScanRange());

    scan_range->relative_path = file_path;
    scan_range->file_length = std::filesystem::file_size(file_path);
    scan_range->offset = 4;
    scan_range->length = scan_length > 0 ? scan_length : scan_range->file_length;

    return scan_range;
}

TEST_F(PageIndexTest, TestRandomReadWith2PageSize) {
    std::random_device rd;
    std::mt19937 rng(rd());

    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(
            ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
            chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_array, true), chunk->num_columns());

    // c0 = np.arange(1, 20001)
    // c1 = np.arange(20000, 0, -1)
    // data = {
    //     'c0': c0,
    //     'c1': c1
    // }
    // df = pd.DataFrame(data)
    // df_with_dict = pd.DataFrame({
    //     "c0": df["c0"],
    //     "c1": df["c1"],
    //     "c2": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else str(x["c0"] % 100), axis = 1),
    //     "c3": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else [x["c0"] % 1000, pd.NA, x["c1"] % 1000], axis = 1)
    // })
    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    // c0 = np.arange(1, 100001)
    // c1 = np.arange(100000, 0, -1)
    // data = {
    //     'c0': c0,
    //     'c1': c1
    // }
    // df = pd.DataFrame(data)
    // df_with_dict = pd.DataFrame({
    //     "c0": df["c0"],
    //     "c1": df["c1"],
    //     "c2": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else str(x["c0"] % 100), axis = 1),
    //     "c3": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else [x["c0"] % 1000, pd.NA, x["c1"] % 1000], axis = 1)
    // })
    const std::string big_page_file = "./be/test/formats/parquet/test_data/page_index_big_page.parquet";

    // for small page 1000 values / page
    // for big page 10000 values / page
    for (size_t index = 0; index < 2; index++) {
        const std::string& file_path = index == 0 ? small_page_file : big_page_file;
        std::cout << "file_path: " << file_path << std::endl;

        std::uniform_int_distribution<int> dist_small(1, 20000);
        std::uniform_int_distribution<int> dist_big(1, 100000);

        std::vector<int> oprands;
        size_t expected_row = 0;
        auto _print_predicate = [&](bool single) {
            std::stringstream ss;
            ss << "expected_row: " << expected_row << " predicate: c0 > " << oprands[0] << " and c0 < " << oprands[1];
            if (single) {
                return ss.str();
            }
            ss << " and c1 > " << oprands[2] << " and c1 < " << oprands[3];
            return ss.str();
        };

        std::vector<bool> single_or_not{true, false};

        for (bool single_flag : single_or_not) {
            // use 20 to save ci's time, change bigger to test more case
            for (int32_t i = 0; i < 20; i++) {
                oprands.clear();
                for (int32_t j = 0; j < 4; j++) {
                    int num = index == 0 ? dist_small(rng) : dist_big(rng);
                    oprands.emplace_back(num);
                }
                for (int k : std::vector<int>{0, 2}) {
                    if (oprands[k] > oprands[k + 1]) {
                        int temp = oprands[k];
                        oprands[k] = oprands[k + 1];
                        oprands[k + 1] = temp;
                    }
                }

                auto ctx = _create_file_random_read_context(file_path);
                auto file = _create_file(file_path);
                ctx->conjunct_ctxs_by_slot[0].clear();
                ctx->min_max_conjunct_ctxs.clear();

                if (single_flag) {
                    Utils::SlotDesc min_max_slots[] = {
                            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
                            {""},
                    };
                    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

                    std::vector<TExpr> t_conjuncts;
                    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, oprands[0], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 0, oprands[1], &t_conjuncts);

                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                                        &ctx->min_max_conjunct_ctxs);
                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                                        &ctx->conjunct_ctxs_by_slot[0]);

                    expected_row = std::max(oprands[1] - oprands[0] - 1, 0);
                } else {
                    ctx->conjunct_ctxs_by_slot[1].clear();
                    Utils::SlotDesc min_max_slots[] = {
                            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
                            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 1},
                            {""},
                    };
                    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

                    std::vector<TExpr> t_conjuncts;
                    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, oprands[0], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 0, oprands[1], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 1, oprands[2], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 1, oprands[3], &t_conjuncts);

                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                                        &ctx->min_max_conjunct_ctxs);

                    std::vector<TExpr> t_conjuncts_slot0{t_conjuncts[0], t_conjuncts[1]};
                    std::vector<TExpr> t_conjuncts_slot1{t_conjuncts[2], t_conjuncts[3]};

                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot0,
                                                        &ctx->conjunct_ctxs_by_slot[0]);
                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot1,
                                                        &ctx->conjunct_ctxs_by_slot[1]);

                    int low_bound = std::max(oprands[0], index == 0 ? 20001 - oprands[3] : 100001 - oprands[3]);
                    int up_bound = std::min(oprands[1], index == 0 ? 20001 - oprands[2] : 100001 - oprands[2]);
                    expected_row = std::max(up_bound - low_bound - 1, 0);
                }

                std::cout << "file path: " << file_path << ", " << _print_predicate(single_flag) << std::endl;

                auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                                std::filesystem::file_size(file_path), 100000);

                Status status = file_reader->init(ctx);
                ASSERT_TRUE(status.ok());
                size_t total_row_nums = 0;
                while (!status.is_end_of_file()) {
                    chunk->reset();
                    status = file_reader->get_next(&chunk);
                    chunk->check_or_die();
                    total_row_nums += chunk->num_rows();
                    if (!status.ok() && !status.is_end_of_file()) {
                        std::cout << status.message() << std::endl;
                        DCHECK(false) << "file path: " << file_path << ", " << _print_predicate(single_flag);
                    }
                    // check row value
                    if (chunk->num_rows() > 0) {
                        ColumnPtr c0 = chunk->get_column_by_index(0);
                        ColumnPtr c1 = chunk->get_column_by_index(1);
                        ColumnPtr c2 = chunk->get_column_by_index(2);
                        ColumnPtr c3 = chunk->get_column_by_index(3);
                        for (size_t row_index = 0; row_index < chunk->num_rows(); row_index++) {
                            int32_t c0_value = c0->get(row_index).get_int32();
                            int32_t c1_value = c1->get(row_index).get_int32();
                            bool flag = index == 0 ? c0_value + c1_value == 20001 : c0_value + c1_value == 100001;
                            if (c0_value % 10 == 0) {
                                flag &= c2->is_null(row_index);
                                flag &= c3->is_null(row_index);
                            } else {
                                flag &= (!c2->is_null(row_index));
                                flag &= (!c3->is_null(row_index));
                                if (!flag) {
                                    std::cout << "file path: " << file_path << ", " << _print_predicate(single_flag);
                                }
                                EXPECT_TRUE(flag);
                                std::string expected_string = std::to_string(c0_value % 100);
                                Slice expected_value = Slice(expected_string);
                                Slice c2_value = c2->get(row_index).get_slice();
                                flag &= (c2_value == expected_value);
                                DatumArray c3_value = c3->get(row_index).get_array();
                                flag &= (c3_value.size() == 3) && (!c3_value[0].is_null()) &&
                                        (c3_value[0].get_int32() == (c0_value % 1000)) && (c3_value[1].is_null()) &&
                                        (!c3_value[2].is_null()) && (c3_value[2].get_int32() == (c1_value % 1000));
                            }
                            if (!flag) {
                                std::cout << "file path: " << file_path << ", " << _print_predicate(single_flag);
                            }
                            EXPECT_TRUE(flag);
                        }
                    }
                }
                EXPECT_EQ(total_row_nums, expected_row);
            }
        }
    }
}

TEST_F(PageIndexTest, TestCollectIORangeWithPageIndex) {
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());

    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    Utils::SlotDesc min_max_slots[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
            {""},
    };

    auto ctx = _create_file_only_c0_context(small_page_file);
    auto file = _create_file(small_page_file);
    ctx->conjunct_ctxs_by_slot[0].clear();
    ctx->min_max_conjunct_ctxs.clear();
    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, 5500, &t_conjuncts);
    ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 0, 7500, &t_conjuncts);

    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->min_max_conjunct_ctxs);
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->conjunct_ctxs_by_slot[0]);

    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(small_page_file), 100000);

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // two row groups, but one is filtered.
    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;

    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGE_INDEX);
    // collect io of column index and offset index for active column.
    EXPECT_EQ(ranges.size(), 2);
    // offset_index_offset = 293436, offset_index_length = 113, column_index_offset = 291196, column_index_length = 211
    EXPECT_EQ(ranges[1].offset, 293436);
    EXPECT_EQ(ranges[1].size, 113);

    ranges.clear();
    end_offset = 0;

    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);
    // 3 pages, 1 range 5000-8000
    EXPECT_EQ(file_reader->_row_group_readers[0]->_range.size(), 1);
    // only collect io of 3 pages, 5001-6000, 6001-7000, 7001-8000 and a dict page.
    EXPECT_EQ(ranges.size(), 4);
    // page 7001-8000: offset 50814, size 1660
    EXPECT_EQ(end_offset, 52474);

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
    }
    EXPECT_EQ(total_row_nums, 1999);
}

TEST_F(PageIndexTest, TestTwoColumnIntersectPageIndex) {
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(
            ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
            chunk->num_columns());

    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    Utils::SlotDesc min_max_slots[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 1},
            {""},
    };

    auto ctx = _create_file_c0_c1_c2_context(small_page_file);
    auto file = _create_file(small_page_file);
    ctx->conjunct_ctxs_by_slot[0].clear();
    ctx->conjunct_ctxs_by_slot[1].clear();
    ctx->min_max_conjunct_ctxs.clear();
    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

    std::vector<TExpr> t_conjuncts;
    // c0: 1->20000, c0 > 5000
    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, 5000, &t_conjuncts);
    // c1: 20000->1, c1 > 5000
    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 1, 5000, &t_conjuncts);

    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->min_max_conjunct_ctxs);

    std::vector<TExpr> t_conjuncts_slot0{t_conjuncts[0]};
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot0, &ctx->conjunct_ctxs_by_slot[0]);

    std::vector<TExpr> t_conjuncts_slot1{t_conjuncts[1]};
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot1, &ctx->conjunct_ctxs_by_slot[1]);

    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(small_page_file), 100000);

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // two row groups.
    EXPECT_EQ(file_reader->_row_group_readers.size(), 2);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;

    for (auto& r : file_reader->_row_group_readers) {
        r->collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGE_INDEX);
    }

    // collect io of column index and offset index for active column,
    // and offset index for lazy column
    // and two group collect together. (2 + 2 + 1) * 2 = 10
    EXPECT_EQ(ranges.size(), 10);

    ranges.clear();
    end_offset = 0;

    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);
    // only collect io of 5 pages, 5001-6000, 6001-7000, 7001-8000, 8001-9000, 9001-10000 and a dict page.
    // three columns, (5 + 1) * 3 = 18
    EXPECT_EQ(ranges.size(), 18);

    // The second row group is not prepare yet

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
    }
    EXPECT_EQ(total_row_nums, 10000);
}

} // namespace starrocks::parquet
