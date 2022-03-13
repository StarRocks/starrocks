// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "column/json_column.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sort_merge.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exprs/slot_ref.h"
#include "fmt/core.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks::vectorized {

class ChunksSorterTest : public ::testing::Test {
public:
    void SetUp() override {
        config::vector_chunk_size = 1024;

        const auto& int_type_desc = TypeDescriptor(TYPE_INT);
        const auto& varchar_type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        ColumnPtr col_cust_key_1 = ColumnHelper::create_column(int_type_desc, false);
        ColumnPtr col_cust_key_2 = ColumnHelper::create_column(int_type_desc, false);
        ColumnPtr col_cust_key_3 = ColumnHelper::create_column(int_type_desc, false);
        ColumnPtr col_nation_1 = ColumnHelper::create_column(varchar_type_desc, true);
        ColumnPtr col_nation_2 = ColumnHelper::create_column(varchar_type_desc, true);
        ColumnPtr col_nation_3 = ColumnHelper::create_column(varchar_type_desc, true);
        ColumnPtr col_region_1 = ColumnHelper::create_column(varchar_type_desc, true);
        ColumnPtr col_region_2 = ColumnHelper::create_column(varchar_type_desc, true);
        ColumnPtr col_region_3 = ColumnHelper::create_column(varchar_type_desc, true);
        ColumnPtr col_mkt_sgmt_1 = ColumnHelper::create_column(varchar_type_desc, true);
        ColumnPtr col_mkt_sgmt_2 = ColumnHelper::create_column(varchar_type_desc, true);
        ColumnPtr col_mkt_sgmt_3 = ColumnHelper::create_column(varchar_type_desc, true);

        col_cust_key_1->append_datum(Datum(int32_t(2)));
        col_nation_1->append_datum(Datum(Slice("JORDAN")));
        col_region_1->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_1->append_datum(Datum(Slice("AUTOMOBILE")));
        col_cust_key_2->append_datum(Datum(int32_t(4)));
        col_nation_2->append_datum(Datum(Slice("EGYPT")));
        col_region_2->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_2->append_datum(Datum(Slice("MACHINERY")));
        col_cust_key_3->append_datum(Datum(int32_t(6)));
        col_nation_3->append_datum(Datum(Slice("SAUDI ARABIA")));
        col_region_3->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_3->append_datum(Datum(Slice("AUTOMOBILE")));

        col_cust_key_1->append_datum(Datum(int32_t(12)));
        col_nation_1->append_datum(Datum(Slice("JORDAN")));
        col_region_1->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_1->append_datum(Datum(Slice("HOUSEHOLD")));
        col_cust_key_2->append_datum(Datum(int32_t(16)));
        col_nation_2->append_datum(Datum(Slice("IRAN")));
        col_region_2->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_2->append_datum(Datum(Slice("FURNITURE")));
        col_cust_key_3->append_datum(Datum(int32_t(24)));
        col_nation_3->append_datum(Datum(Slice("JORDAN")));
        col_region_3->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_3->append_datum(Datum(Slice("MACHINERY")));

        col_cust_key_1->append_datum(Datum(int32_t(41)));
        col_nation_1->append_datum(Datum(Slice("IRAN")));
        col_region_1->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_1->append_datum(Datum(Slice("HOUSEHOLD")));
        col_cust_key_2->append_datum(Datum(int32_t(49)));
        col_nation_2->append_datum(Datum(Slice("IRAN")));
        col_region_2->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_2->append_datum(Datum(Slice("FURNITURE")));
        col_cust_key_3->append_datum(Datum(int32_t(52)));
        col_nation_3->append_datum(Datum(Slice("IRAQ")));
        col_region_3->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_3->append_datum(Datum(Slice("HOUSEHOLD")));

        col_cust_key_1->append_datum(Datum(int32_t(54)));
        col_nation_1->append_datum(Datum(Slice("EGYPT")));
        col_region_1->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_1->append_datum(Datum(Slice("AUTOMOBILE")));
        col_cust_key_2->append_datum(Datum(int32_t(55)));
        col_nation_2->append_datum(Datum(Slice("IRAN")));
        col_region_2->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_2->append_datum(Datum(Slice("MACHINERY")));
        col_cust_key_3->append_datum(Datum(int32_t(56)));
        col_nation_3->append_datum(Datum(Slice("IRAN")));
        col_region_3->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_3->append_datum(Datum(Slice("FURNITURE")));

        col_cust_key_1->append_datum(Datum(int32_t(58)));
        col_nation_1->append_datum(Datum(Slice("JORDAN")));
        col_region_1->append_datum(Datum(Slice("MIDDLE EAST")));
        col_mkt_sgmt_1->append_datum(Datum(Slice("HOUSEHOLD")));
        col_cust_key_2->append_datum(Datum(int32_t(69)));
        col_nation_2->append_datum(Datum());
        col_region_2->append_datum(Datum());
        col_mkt_sgmt_2->append_datum(Datum());
        col_cust_key_3->append_datum(Datum(int32_t(70)));
        col_nation_3->append_datum(Datum());
        col_region_3->append_datum(Datum());
        col_mkt_sgmt_3->append_datum(Datum());

        col_cust_key_1->append_datum(Datum(int32_t(71)));
        col_nation_1->append_datum(Datum());
        col_region_1->append_datum(Datum());
        col_mkt_sgmt_1->append_datum(Datum());

        Columns columns1 = {col_cust_key_1, col_nation_1, col_region_1, col_mkt_sgmt_1};
        Columns columns2 = {col_cust_key_2, col_nation_2, col_region_2, col_mkt_sgmt_2};
        Columns columns3 = {col_cust_key_3, col_nation_3, col_region_3, col_mkt_sgmt_3};

        Chunk::SlotHashMap map;
        map.reserve(columns1.size() * 2);
        for (int i = 0; i < columns1.size(); ++i) {
            map[i] = i;
        }

        _chunk_1 = std::make_shared<Chunk>(columns1, map);
        _chunk_2 = std::make_shared<Chunk>(columns2, map);
        _chunk_3 = std::make_shared<Chunk>(columns3, map);

        _expr_cust_key = std::make_unique<SlotRef>(TypeDescriptor(TYPE_INT), 0, 0);     // refer to cust_key
        _expr_nation = std::make_unique<SlotRef>(TypeDescriptor(TYPE_VARCHAR), 0, 1);   // refer to nation
        _expr_region = std::make_unique<SlotRef>(TypeDescriptor(TYPE_VARCHAR), 0, 2);   // refer to region
        _expr_mkt_sgmt = std::make_unique<SlotRef>(TypeDescriptor(TYPE_VARCHAR), 0, 3); // refer to mkt_sgmt
        _expr_constant = std::make_unique<SlotRef>(TypeDescriptor(TYPE_SMALLINT), 0,
                                                   4); // refer to constant value
        _runtime_state = _create_runtime_state();
    }

    void TearDown() override {}

protected:
    std::shared_ptr<RuntimeState> _create_runtime_state() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = config::vector_chunk_size;
        TQueryGlobals query_globals;
        auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        runtime_state->init_instance_mem_tracker();
        return runtime_state;
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    ChunkPtr _chunk_1, _chunk_2, _chunk_3;
    std::unique_ptr<SlotRef> _expr_cust_key, _expr_nation, _expr_region, _expr_mkt_sgmt, _expr_constant;
};

void clear_sort_exprs(std::vector<ExprContext*>& exprs) {
    for (ExprContext* ctx : exprs) {
        delete ctx;
    }
    exprs.clear();
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, fullsort_column_wise) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // cust_key
    is_asc.push_back(true);  // cust_key
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));

    ChunksSorterFullSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, 2);
    sorter.set_compare_strategy(ColumnInc);
    size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
    sorter.update(_runtime_state.get(), _chunk_1);
    sorter.update(_runtime_state.get(), _chunk_2);
    sorter.update(_runtime_state.get(), _chunk_3);
    sorter.done(_runtime_state.get());

    bool eos = false;
    ChunkPtr page_1, page_2;
    sorter.get_next(&page_1, &eos);
    ASSERT_FALSE(eos);
    ASSERT_TRUE(page_1 != nullptr);
    sorter.get_next(&page_2, &eos);
    ASSERT_TRUE(eos);
    ASSERT_TRUE(page_2 == nullptr);

    ASSERT_EQ(16, total_rows);
    ASSERT_EQ(16, page_1->num_rows());
    const size_t Size = 16;
    std::vector<int32_t> permutation{69, 70, 71, 2, 4, 6, 12, 16, 24, 41, 49, 52, 54, 55, 56, 58};
    std::vector<int> result;
    for (size_t i = 0; i < Size; ++i) {
        result.push_back(page_1->get(i).get(0).get_int32());
    }
    EXPECT_EQ(permutation, result);

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, topn_column_wise) {
    std::vector<bool> is_asc{false, true};
    std::vector<bool> is_null_first{true, true};
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));

    // prepare answer
    ChunksSorterFullSort full_sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, 16);
    full_sorter.set_compare_strategy(RowWise);
    full_sorter.update(_runtime_state.get(), _chunk_1);
    full_sorter.update(_runtime_state.get(), _chunk_2);
    full_sorter.update(_runtime_state.get(), _chunk_3);
    full_sorter.done(_runtime_state.get());
    ChunkPtr correct_result;
    bool eos = false;
    while (!eos) {
        ChunkPtr page;
        full_sorter.get_next(&page, &eos);
        correct_result->append(*page);
    }

    size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
    ASSERT_EQ(16, total_rows);
    for (int limit = 1; limit <= total_rows; limit++) {
        ChunksSorterTopn sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, 0, limit);
        sorter.set_compare_strategy(ColumnInc);
        sorter.update(_runtime_state.get(), _chunk_1);
        sorter.update(_runtime_state.get(), _chunk_2);
        sorter.update(_runtime_state.get(), _chunk_3);
        sorter.done(_runtime_state.get());

        bool eos = false;
        ChunkPtr page_1, page_2;
        sorter.get_next(&page_1, &eos);
        ASSERT_FALSE(eos);
        ASSERT_TRUE(page_1 != nullptr);
        sorter.get_next(&page_2, &eos);
        ASSERT_TRUE(eos);
        ASSERT_TRUE(page_2 == nullptr);

        ASSERT_EQ(limit, page_1->num_rows());
        std::vector<int32_t> col1 =
                ColumnHelper::cast_to_raw<TYPE_INT>(correct_result->get_column_by_index(0))->get_data();
        std::vector<int32_t> col3 =
                ColumnHelper::cast_to_raw<TYPE_INT>(correct_result->get_column_by_index(2))->get_data();
        col1.resize(limit);
        col3.resize(limit);
        std::vector<int> result_col1;
        std::vector<int> result_col3;
        for (size_t i = 0; i < limit; ++i) {
            result_col1.push_back(page_1->get(i).get(0).get_int32());
            result_col3.push_back(page_1->get(i).get(2).get_int32());
        }
        ASSERT_EQ(col1, result_col1);
        ASSERT_EQ(col3, result_col3);
    }

    clear_sort_exprs(sort_exprs);
}

static std::vector<CompareStrategy> all_compare_strategy() {
    return {RowWise, ColumnWise, ColumnInc};
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, full_sort_by_2_columns_null_first) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // region
    is_asc.push_back(true);  // cust_key
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));

    for (auto strategy : all_compare_strategy()) {
        std::cerr << "sort with strategy: " << strategy << std::endl;
        ChunksSorterFullSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, 2);
        sorter.set_compare_strategy(strategy);
        size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
        sorter.update(_runtime_state.get(), _chunk_1);
        sorter.update(_runtime_state.get(), _chunk_2);
        sorter.update(_runtime_state.get(), _chunk_3);
        sorter.done(_runtime_state.get());

        bool eos = false;
        ChunkPtr page_1, page_2;
        sorter.get_next(&page_1, &eos);
        ASSERT_FALSE(eos);
        ASSERT_TRUE(page_1 != nullptr);
        sorter.get_next(&page_2, &eos);
        ASSERT_TRUE(eos);
        ASSERT_TRUE(page_2 == nullptr);

        // print_chunk(page_1);

        ASSERT_EQ(16, total_rows);
        ASSERT_EQ(16, page_1->num_rows());
        const size_t Size = 16;
        int32_t permutation[Size] = {69, 70, 71, 2, 4, 6, 12, 16, 24, 41, 49, 52, 54, 55, 56, 58};
        std::vector<int32_t> result;
        for (size_t i = 0; i < Size; ++i) {
            EXPECT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
            result.push_back(page_1->get(i).get(0).get_int32());
        }
        fmt::print("result: {}\n", fmt::join(result, ","));
    }

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, full_sort_by_2_columns_null_last) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // region
    is_asc.push_back(true);  // cust_key
    is_null_first.push_back(false);
    is_null_first.push_back(false);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));

    for (auto strategy : all_compare_strategy()) {
        std::cerr << "sort with strategy: " << strategy << std::endl;
        ChunksSorterFullSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, 2);
        sorter.set_compare_strategy(strategy);
        size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
        sorter.update(_runtime_state.get(), _chunk_1);
        sorter.update(_runtime_state.get(), _chunk_2);
        sorter.update(_runtime_state.get(), _chunk_3);
        sorter.done(_runtime_state.get());

        bool eos = false;
        ChunkPtr page_1, page_2;
        sorter.get_next(&page_1, &eos);
        ASSERT_FALSE(eos);
        ASSERT_TRUE(page_1 != nullptr);
        sorter.get_next(&page_2, &eos);
        ASSERT_TRUE(eos);
        ASSERT_TRUE(page_2 == nullptr);

        // print_chunk(page_1);

        ASSERT_EQ(16, total_rows);
        ASSERT_EQ(16, page_1->num_rows());
        const size_t Size = 16;
        std::vector<int32_t> permutation{2, 4, 6, 12, 16, 24, 41, 49, 52, 54, 55, 56, 58, 69, 70, 71};
        std::vector<int32_t> result;
        for (size_t i = 0; i < Size; ++i) {
            result.push_back(page_1->get(i).get(0).get_int32());
        }
        ASSERT_EQ(permutation, result);
    }

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, full_sort_by_3_columns) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // region
    is_asc.push_back(true);  // nation
    is_asc.push_back(false); // cust_key
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_nation.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));

    for (auto strategy : all_compare_strategy()) {
        std::cerr << "sort with strategy: " << strategy << std::endl;
        ChunksSorterFullSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, 2);
        sorter.set_compare_strategy(strategy);
        size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
        sorter.update(_runtime_state.get(), _chunk_1);
        sorter.update(_runtime_state.get(), _chunk_2);
        sorter.update(_runtime_state.get(), _chunk_3);
        sorter.done(_runtime_state.get());

        bool eos = false;
        ChunkPtr page_1, page_2;
        sorter.get_next(&page_1, &eos);
        ASSERT_FALSE(eos);
        ASSERT_TRUE(page_1 != nullptr);
        sorter.get_next(&page_2, &eos);
        ASSERT_TRUE(eos);
        ASSERT_TRUE(page_2 == nullptr);

        ASSERT_EQ(16, total_rows);
        ASSERT_EQ(16, page_1->num_rows());
        const size_t Size = 16;
        std::vector<int32_t> permutation{71, 70, 69, 54, 4, 56, 55, 49, 41, 16, 52, 58, 24, 12, 2, 6};
        std::vector<int32_t> result;
        for (size_t i = 0; i < Size; ++i) {
            result.push_back(page_1->get(i).get(0).get_int32());
        }
        ASSERT_EQ(permutation, result);
    }

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, full_sort_by_4_columns) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // mtk_sgmt
    is_asc.push_back(true);  // region
    is_asc.push_back(false); // nation
    is_asc.push_back(false); // cust_key
    is_null_first.push_back(false);
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    is_null_first.push_back(false);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_mkt_sgmt.get()));
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_nation.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));

    for (auto strategy : all_compare_strategy()) {
        std::cerr << "sort with strategy: " << strategy << std::endl;
        ChunksSorterFullSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, 2);
        sorter.set_compare_strategy(ColumnInc);
        size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
        sorter.update(_runtime_state.get(), _chunk_1);
        sorter.update(_runtime_state.get(), _chunk_2);
        sorter.update(_runtime_state.get(), _chunk_3);
        sorter.done(_runtime_state.get());

        bool eos = false;
        ChunkPtr page_1, page_2;
        sorter.get_next(&page_1, &eos);
        ASSERT_FALSE(eos);
        ASSERT_TRUE(page_1 != nullptr);
        sorter.get_next(&page_2, &eos);
        ASSERT_TRUE(eos);
        ASSERT_TRUE(page_2 == nullptr);

        // print_chunk(page_1);

        ASSERT_EQ(16, total_rows);
        ASSERT_EQ(16, page_1->num_rows());
        const size_t Size = 16;
        int32_t permutation[Size] = {24, 55, 4, 58, 12, 52, 41, 56, 49, 16, 6, 2, 54, 71, 70, 69};
        for (size_t i = 0; i < Size; ++i) {
            ASSERT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
        }
    }

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, part_sort_by_3_columns_null_fisrt) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // region
    is_asc.push_back(true);  // nation
    is_asc.push_back(true);  // cust_key
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_nation.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));

    ChunksSorterTopn sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, 2, 7, 2);
    size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
    sorter.update(_runtime_state.get(), _chunk_1);
    sorter.update(_runtime_state.get(), _chunk_2);
    sorter.update(_runtime_state.get(), _chunk_3);
    sorter.done(_runtime_state.get());

    bool eos = false;
    ChunkPtr page_1, page_2;
    sorter.get_next(&page_1, &eos);
    ASSERT_FALSE(eos);
    ASSERT_TRUE(page_1 != nullptr);
    sorter.get_next(&page_2, &eos);
    ASSERT_TRUE(eos);
    ASSERT_TRUE(page_2 == nullptr);

    ASSERT_EQ(16, total_rows);
    ASSERT_EQ(7, page_1->num_rows());
    // full sort: {69, 70, 71, 4, 54, 16, 41, 49, 55, 56, 52, 2, 12, 24, 58, 6};
    const size_t Size = 7;
    int32_t permutation[Size] = {71, 4, 54, 16, 41, 49, 55};
    for (size_t i = 0; i < Size; ++i) {
        ASSERT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
    }

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, part_sort_by_3_columns_null_last) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // region
    is_asc.push_back(true);  // nation
    is_asc.push_back(true);  // cust_key
    is_null_first.push_back(false);
    is_null_first.push_back(false);
    is_null_first.push_back(false);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_nation.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));

    ChunksSorterTopn sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, 7, 7, 2);
    size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
    sorter.update(_runtime_state.get(), _chunk_1);
    sorter.update(_runtime_state.get(), _chunk_2);
    sorter.update(_runtime_state.get(), _chunk_3);
    sorter.done(_runtime_state.get());

    bool eos = false;
    ChunkPtr page_1, page_2;
    sorter.get_next(&page_1, &eos);
    ASSERT_FALSE(eos);
    ASSERT_TRUE(page_1 != nullptr);
    sorter.get_next(&page_2, &eos);
    ASSERT_TRUE(eos);
    ASSERT_TRUE(page_2 == nullptr);

    ASSERT_EQ(16, total_rows);
    ASSERT_EQ(7, page_1->num_rows());
    // full sort: {4, 54, 16, 41, 49, 55, 56, 52, 2, 12, 24, 58, 6, 69, 70, 71};
    const size_t Size = 7;
    int32_t permutation[Size] = {52, 2, 12, 24, 58, 6, 69};
    for (size_t i = 0; i < Size; ++i) {
        ASSERT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
    }

    // part sort with large offset
    ChunksSorterTopn sorter2(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, 100, 2, 2);
    sorter2.update(_runtime_state.get(), _chunk_1);
    sorter2.update(_runtime_state.get(), _chunk_2);
    sorter2.update(_runtime_state.get(), _chunk_3);
    sorter2.done(_runtime_state.get());
    eos = false;
    page_1->reset();
    sorter2.get_next(&page_1, &eos);
    ASSERT_TRUE(eos);
    ASSERT_TRUE(page_1 == nullptr);

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, order_by_with_unequal_sized_chunks) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // nation
    is_asc.push_back(false); // cust_key
    is_null_first.push_back(false);
    is_null_first.push_back(false);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_nation.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));

    // partial sort
    ChunksSorterTopn full_sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, 1, 6, 2);
    ChunkPtr chunk_1 = _chunk_1->clone_empty();
    ChunkPtr chunk_2 = _chunk_2->clone_empty();
    for (size_t i = 0; i < _chunk_1->num_columns(); ++i) {
        chunk_1->get_column_by_index(i)->append(*(_chunk_1->get_column_by_index(i)), 0, 1);
        chunk_2->get_column_by_index(i)->append(*(_chunk_2->get_column_by_index(i)), 0, 1);
    }
    full_sorter.update(_runtime_state.get(), chunk_1);
    full_sorter.update(_runtime_state.get(), chunk_2);
    full_sorter.update(_runtime_state.get(), _chunk_3);
    full_sorter.done(_runtime_state.get());

    bool eos = false;
    ChunkPtr page_1, page_2;
    full_sorter.get_next(&page_1, &eos);
    ASSERT_FALSE(eos);
    ASSERT_TRUE(page_1 != nullptr);
    full_sorter.get_next(&page_2, &eos);
    ASSERT_TRUE(eos);
    ASSERT_TRUE(page_2 == nullptr);
    ASSERT_EQ(6, page_1->num_rows());
    const size_t Size = 6;
    int32_t permutation[Size] = {24, 2, 52, 56, 4, 70};
    for (size_t i = 0; i < Size; ++i) {
        ASSERT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
    }

    clear_sort_exprs(sort_exprs);
}

static void reset_permutation(SmallPermutation& permutation, int n) {
    permutation.resize(n);
    for (int i = 0; i < permutation.size(); i++) {
        permutation[i].index_in_chunk = i;
    }
}

TEST_F(ChunksSorterTest, column_incremental_sort) {
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
    ColumnPtr nullable_column = ColumnHelper::create_column(type_desc, true);

    // sort empty column
    SmallPermutation permutation;
    Tie tie;
    std::pair<int, int> range{0, 0};
    nullable_column->sort_and_tie(false, true, true, permutation, tie, range, false);
    nullable_column->sort_and_tie(false, true, false, permutation, tie, range, false);
    nullable_column->sort_and_tie(false, false, false, permutation, tie, range, false);
    nullable_column->sort_and_tie(false, false, true, permutation, tie, range, false);

    // sort all null column
    const int kNullCount = 5;
    nullable_column->append_nulls(kNullCount);
    permutation.resize(kNullCount);
    for (int i = 0; i < permutation.size(); i++) {
        permutation[i].index_in_chunk = i;
    }
    tie.resize(kNullCount);
    range = {0, kNullCount};
    nullable_column->sort_and_tie(false, true, true, permutation, tie, range, false);
    nullable_column->sort_and_tie(false, true, false, permutation, tie, range, false);
    nullable_column->sort_and_tie(false, false, false, permutation, tie, range, false);
    nullable_column->sort_and_tie(false, false, true, permutation, tie, range, false);

    // sort 1 element with 5 nulls
    SmallPermutation expect_perm;
    nullable_column->append_datum(Datum(1));
    reset_permutation(permutation, kNullCount + 1);
    tie = Tie(kNullCount + 1, 0);

    nullable_column->sort_and_tie(false, true, true, permutation, tie, range, false);
    reset_permutation(expect_perm, kNullCount + 1);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({0, 0, 0, 0, 0, 0}), tie);

    reset_permutation(permutation, kNullCount + 1);
    tie = Tie(kNullCount + 1, 0);
    nullable_column->sort_and_tie(false, true, false, permutation, tie, range, false);
    reset_permutation(expect_perm, kNullCount + 1);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({0, 0, 0, 0, 0, 0}), tie);

    reset_permutation(permutation, kNullCount + 1);
    tie = Tie(kNullCount + 1, 0);
    nullable_column->sort_and_tie(false, false, false, permutation, tie, range, false);
    reset_permutation(expect_perm, kNullCount + 1);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({0, 0, 0, 0, 0, 0}), tie);

    reset_permutation(permutation, kNullCount + 1);
    tie = Tie(kNullCount + 1, 0);
    nullable_column->sort_and_tie(false, false, true, permutation, tie, range, false);
    reset_permutation(expect_perm, kNullCount + 1);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({0, 0, 0, 0, 0, 0}), tie);

    // sort not-null elements
    nullable_column = nullable_column->clone_empty();
    nullable_column->append_datum(Datum(1));
    reset_permutation(expect_perm, 1);
    reset_permutation(permutation, 1);
    tie = Tie(1, 1);

    nullable_column->sort_and_tie(false, true, true, permutation, tie, range, false);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({1}), tie);

    nullable_column->sort_and_tie(false, true, false, permutation, tie, range, false);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({1}), tie);

    nullable_column->sort_and_tie(false, false, false, permutation, tie, range, false);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({1}), tie);

    nullable_column->sort_and_tie(false, false, true, permutation, tie, range, false);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({1}), tie);
}

TEST_F(ChunksSorterTest, find_zero) {
    std::vector<uint8_t> bytes;
    for (int len : std::vector<int>{1, 3, 7, 8, 12, 15, 16, 17, 127, 128}) {
        for (int zero_pos = 0; zero_pos < len; zero_pos++) {
            bytes = std::vector<uint8_t>(len, 1);
            bytes[zero_pos] = 0;

            size_t result = SIMD::find_zero(bytes, 0);
            EXPECT_EQ(zero_pos, result);

            // test non-zero
            std::fill(bytes.begin(), bytes.end(), 0);
            bytes[zero_pos] = 1;
            result = SIMD::find_nonzero(bytes, 0);
            EXPECT_EQ(zero_pos, result);
        }

        bytes = std::vector<uint8_t>(len, 1);
        EXPECT_EQ(len, SIMD::find_zero(bytes, 0));
        // test nonzero
        std::fill(bytes.begin(), bytes.end(), 0);
        EXPECT_EQ(len, SIMD::find_nonzero(bytes, 0));
    }
}

TEST_F(ChunksSorterTest, test_tie) {
    using Ranges = std::vector<std::pair<int, int>>;
    Tie tie{0, 1, 1, 1, 0, 1, 1, 0, 1, 0, 1};
    TieIterator iterator(tie);
    Ranges ranges;

    while (iterator.next()) {
        ranges.emplace_back(iterator.range_first, iterator.range_last);
    }
    Ranges expected = {
            {0, 4},
            {4, 7},
            {7, 9},
            {9, 11},
    };
    ASSERT_EQ(expected, ranges);
}

static ColumnPtr make_int_column(const std::vector<int>& data) {
    auto res = Int32Column::create();
    for (int x : data) {
        res->append(x);
    }
    return res;
}

static ColumnPtr make_string_column(const std::vector<std::string>& data) {
    auto res = BinaryColumn::create();
    for (auto& x : data) {
        res->append_string(x);
    }
    return res;
}

static Permutation make_perm(int size) {
    Permutation res(size);
    for (int i = 0; i < size; i++) {
        res[i].index_in_chunk = i;
    }
    return res;
}

template <PrimitiveType ptype, class T = RunTimeCppType<ptype>>
static const std::vector<T> unnest_column_to_vector(ColumnPtr column) {
    auto real_column = ColumnHelper::cast_to<ptype>(column);
    std::vector<T> res;
    if constexpr (ptype == TYPE_VARCHAR) {
        auto slice_array = real_column->get_data();
        for (int i = 0; i < slice_array.size(); i++) {
            res.push_back(slice_array[i].to_string());
        }
    } else {
        res = real_column->get_data();
    }
    return res;
}

TEST_F(ChunksSorterTest, test_merge_sorted) {
    // clang-format off
    std::vector<std::tuple<std::vector<int>, std::vector<int>>> inputs = {
            {{1, 3, 5}, {2, 4, 6}},
            {{1, 3, 5}, {}},
            {{}, {2, 4, 6}},
            {{1, 2, 3}, {4, 5, 6}},
            {{1, 1, 1}, {2, 2, 2}},
            {{1, 1, 2}, {2, 2, 3}},
    };
    // clang-format on
    for (auto [lhs_input, rhs_input] : inputs) {
        int size = lhs_input.size() + rhs_input.size();
        for (int limit = 1; limit <= size; limit++) {
            fmt::print("merge ({}) and ({}) with limit {} \n", fmt::join(lhs_input, ","), fmt::join(rhs_input, ","),
                       limit);
            auto lhs_column = make_int_column(lhs_input);
            auto rhs_column = make_int_column(rhs_input);
            Permutation lhs_perm = make_perm(lhs_column->size());
            Permutation rhs_perm = make_perm(rhs_column->size());
            PermutatedColumn lhs_perm_column(*lhs_column, lhs_perm);
            PermutatedColumn rhs_perm_column(*rhs_column, rhs_perm);

            Permutation result_perm;
            Tie tie(size, 1);
            merge_sorted(lhs_perm_column, rhs_perm_column, result_perm, tie, {0, lhs_column->size()},
                         {0, rhs_column->size()}, 1, 1, limit, 0);
            auto result = unpermute_column(Columns({lhs_column, rhs_column}), result_perm, result_perm.size());
            std::vector<int> sorted_result = unnest_column_to_vector<TYPE_INT>(result);
            ASSERT_TRUE(std::is_sorted(sorted_result.begin(), sorted_result.end()));
            ASSERT_GE(result_perm.size(), limit);

            Tie expected_tie(sorted_result.size(), 1);
            for (int i = 1; i < sorted_result.size(); i++) {
                expected_tie[i] = (sorted_result[i - 1] == sorted_result[i]);
            }
            ASSERT_EQ(expected_tie, tie);
        }
    }
}

static int compare_columns(const Columns& columns, int lhs_idx, int rhs_idx) {
    for (int i = 0; i < columns.size(); i++) {
        int x = columns[i]->compare_at(lhs_idx, rhs_idx, *columns[i], 1);
        if (x != 0) {
            return x;
        }
    }
    return 0;
}

TEST_F(ChunksSorterTest, test_merge_chunk) {
    Columns columns1, columns2;
    // setup chunk
    columns1.emplace_back(make_int_column({1, 2, 2, 3, 4, 4}));
    columns1.emplace_back(make_string_column({"a", "a", "b", "b", "b", "b"}));
    columns1.emplace_back(make_int_column({5, 6, 7, 8, 9, 10}));

    columns2.emplace_back(make_int_column({2, 3, 3, 3, 4, 5}));
    columns2.emplace_back(make_string_column({"a", "a", "b", "b", "b", "b"}));
    columns2.emplace_back(make_int_column({1, 2, 3, 4, 5, 6}));
    Chunk::SlotHashMap slot_map;
    for (int i = 0; i < 3; i++) {
        slot_map[i] = i;
    }
    ChunkPtr chunk1 = std::make_shared<Chunk>(columns1, slot_map);
    ChunkPtr chunk2 = std::make_shared<Chunk>(columns2, slot_map);

    int total_rows = columns1[0]->size() + columns2[0]->size();
    std::vector<int> sort_orders(3, 1);
    std::vector<int> null_firsts(3, 1);

    for (int limit = total_rows; limit >= 1; limit--) {
        Permutation merge_perm;
        Permutation lhs_perm = make_perm(total_rows);
        Permutation rhs_perm = make_perm(total_rows);
        PermutatedChunk lhs_chunk(chunk1, lhs_perm);
        PermutatedChunk rhs_chunk(chunk2, lhs_perm);

        merge_sorted_chunks(lhs_chunk, rhs_chunk, sort_orders, null_firsts, merge_perm, limit);
        auto result_col1 = unpermute_column(Columns({columns1[0], columns2[0]}), merge_perm, limit);
        auto result_col2 = unpermute_column(Columns({columns1[1], columns2[1]}), merge_perm, limit);
        auto result_col3 = unpermute_column(Columns({columns1[2], columns2[2]}), merge_perm, limit);
        auto unnest_col1 = unnest_column_to_vector<TYPE_INT>(result_col1);
        auto unnest_col2 = unnest_column_to_vector<TYPE_VARCHAR, std::string>(result_col2);
        auto unnest_col3 = unnest_column_to_vector<TYPE_INT>(result_col3);
        Columns sorted_columns{result_col1, result_col2, result_col3};
        fmt::print("merge into column1 limit {}: {}\n", limit, fmt::join(unnest_col1, ","));
        fmt::print("merge into column2 limit {}: {}\n", limit, fmt::join(unnest_col2, ","));
        fmt::print("merge into column3 limit {}: {}\n", limit, fmt::join(unnest_col3, ","));
        for (int i = 1; i < limit; i++) {
            ASSERT_EQ(unnest_col1[0], 1) << "start from 1";
            ASSERT_EQ(unnest_col2[0], "a") << "start from 'a'";
            ASSERT_EQ(unnest_col3[0], 5) << "start from 1";
            ASSERT_TRUE(compare_columns(sorted_columns, i - 1, i) <= 0) << " the " << i << " row";
        }
    }
}

} // namespace starrocks::vectorized

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}