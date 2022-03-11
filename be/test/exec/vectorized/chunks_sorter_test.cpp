// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
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
TEST_F(ChunksSorterTest, full_sort_incremental) {
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

template <PrimitiveType ptype>
static const std::vector<RunTimeCppType<ptype>> unnest_column_to_vector(ColumnPtr column) {
    return ColumnHelper::cast_to<ptype>(column)->get_data();
}

// Combine multiple columns according to permutation
static ColumnPtr combine_column(const Columns& columns, const Permutation& perm) {
    if (columns.empty()) {
        return {};
    }
    auto res = columns[0]->clone_empty();
    for (auto& p : perm) {
        DCHECK_LT(p.chunk_index, columns.size());
        res->append_datum(columns[p.chunk_index]->get(p.index_in_chunk));
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
        fmt::print("merge ({}) and ({})\n", fmt::join(lhs_input, ","), fmt::join(rhs_input, ","));
        auto lhs_column = make_int_column(lhs_input);
        auto rhs_column = make_int_column(rhs_input);

        Permutation perm;
        Tie tie;
        merge_sorted(lhs_column.get(), rhs_column.get(), 0, perm, tie);
        auto result = combine_column(Columns({lhs_column, rhs_column}), perm);
        std::vector<int> expected;
        std::copy(lhs_input.begin(), lhs_input.end(), std::back_inserter(expected));
        std::copy(rhs_input.begin(), rhs_input.end(), std::back_inserter(expected));
        std::sort(expected.begin(), expected.end());
        ASSERT_EQ(expected, unnest_column_to_vector<TYPE_INT>(result));
        Tie expected_tie(expected.size(), 0);
        for (int i = 1; i < expected.size(); i++) {
            expected_tie[i] = expected[i - 1] == expected[i];
        }
        ASSERT_EQ(expected_tie, tie);
    }
}

TEST_F(ChunksSorterTest, test_merge_chunk) {
    Columns chunk1, chunk2;
    // setup chunk
    chunk1.emplace_back(make_int_column({1, 2, 2, 3, 4, 4}));
    chunk1.emplace_back(make_string_column({"a", "a", "b", "b", "b", "b"}));
    chunk1.emplace_back(make_int_column({5, 6, 7, 8, 9, 10}));

    chunk2.emplace_back(make_int_column({2, 3, 3, 3, 4, 5}));
    chunk2.emplace_back(make_string_column({"a", "a", "b", "b", "b", "b"}));
    chunk2.emplace_back(make_int_column({1, 2, 3, 4, 5, 6}));

    Permutation perm;
    merge_sorted_chunks(chunk1, chunk2, perm);
    auto result_col1 = combine_column(Columns({chunk1[0], chunk2[0]}), perm);
    auto result_col2 = combine_column(Columns({chunk1[1], chunk2[1]}), perm);
    auto result_col3 = combine_column(Columns({chunk1[2], chunk2[2]}), perm);
    auto unnest_col1 = unnest_column_to_vector<TYPE_INT>(result_col1);
    auto unnest_col2 = unnest_column_to_vector<TYPE_VARCHAR>(result_col2);
    auto unnest_col3 = unnest_column_to_vector<TYPE_INT>(result_col3);
    ASSERT_TRUE(std::is_sorted(unnest_col1.begin(), unnest_col1.end()));
    ASSERT_TRUE(std::is_sorted(unnest_col2.begin(), unnest_col2.end()));
    ASSERT_TRUE(std::is_sorted(unnest_col3.begin(), unnest_col3.end()));
}

} // namespace starrocks::vectorized

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}