// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "exprs/slot_ref.h"
#include "runtime/runtime_state.h"

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

        butil::FlatMap<SlotId, size_t> map;
        map.init(columns1.size() * 2);
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
TEST_F(ChunksSorterTest, full_sort_by_2_columns_null_first) {
    auto runtime_state = _create_runtime_state();
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // region
    is_asc.push_back(true);  // cust_key
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));

    ChunksSorterFullSort sorter(&sort_exprs, &is_asc, &is_null_first, 2);
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
    for (size_t i = 0; i < Size; ++i) {
        ASSERT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
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

    ChunksSorterFullSort sorter(&sort_exprs, &is_asc, &is_null_first, 2);
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
    int32_t permutation[Size] = {2, 4, 6, 12, 16, 24, 41, 49, 52, 54, 55, 56, 58, 69, 70, 71};
    for (size_t i = 0; i < Size; ++i) {
        ASSERT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
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

    ChunksSorterFullSort sorter(&sort_exprs, &is_asc, &is_null_first, 2);
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
    int32_t permutation[Size] = {71, 70, 69, 54, 4, 56, 55, 49, 41, 16, 52, 58, 24, 12, 2, 6};
    for (size_t i = 0; i < Size; ++i) {
        ASSERT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
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

    ChunksSorterFullSort sorter(&sort_exprs, &is_asc, &is_null_first, 2);
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

    ChunksSorterTopn sorter(&sort_exprs, &is_asc, &is_null_first, 2, 7, 2);
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

    ChunksSorterTopn sorter(&sort_exprs, &is_asc, &is_null_first, 7, 7, 2);
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
    ChunksSorterTopn sorter2(&sort_exprs, &is_asc, &is_null_first, 100, 2, 2);
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
    ChunksSorterTopn full_sorter(&sort_exprs, &is_asc, &is_null_first, 1, 6, 2);
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

} // namespace starrocks::vectorized
