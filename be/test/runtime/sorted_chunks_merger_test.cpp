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

#include "runtime/sorted_chunks_merger.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

class SortedChunksMergerTest : public ::testing::Test {
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

        col_cust_key_1->append_datum(int32_t(71));
        col_cust_key_1->append_datum(int32_t(70));
        col_cust_key_1->append_datum(int32_t(69));
        col_cust_key_1->append_datum(int32_t(55));
        col_cust_key_1->append_datum(int32_t(49));
        col_cust_key_1->append_datum(int32_t(41));
        col_cust_key_1->append_datum(int32_t(24));
        col_cust_key_1->append_datum(int32_t(12));
        col_cust_key_1->append_datum(int32_t(2));
        col_nation_1->append_nulls(3);
        col_nation_1->append_datum(Slice("IRAN"));
        col_nation_1->append_datum(Slice("IRAN"));
        col_nation_1->append_datum(Slice("IRAN"));
        col_nation_1->append_datum(Slice("JORDAN"));
        col_nation_1->append_datum(Slice("JORDAN"));
        col_nation_1->append_datum(Slice("JORDAN"));
        col_region_1->append_nulls(3);
        col_region_1->append_datum(Slice("MIDDLE EAST"));
        col_region_1->append_datum(Slice("MIDDLE EAST"));
        col_region_1->append_datum(Slice("MIDDLE EAST"));
        col_region_1->append_datum(Slice("MIDDLE EAST"));
        col_region_1->append_datum(Slice("MIDDLE EAST"));
        col_region_1->append_datum(Slice("MIDDLE EAST"));

        col_cust_key_2->append_datum(int32_t(54));
        col_cust_key_2->append_datum(int32_t(4));
        col_cust_key_2->append_datum(int32_t(16));
        col_cust_key_2->append_datum(int32_t(52));
        col_cust_key_2->append_datum(int32_t(6));
        col_nation_2->append_datum(Slice("EGYPT"));
        col_nation_2->append_datum(Slice("EGYPT"));
        col_nation_2->append_datum(Slice("IRAN"));
        col_nation_2->append_datum(Slice("IRAQ"));
        col_nation_2->append_datum(Slice("SAUDI ARABIA"));
        col_region_2->append_datum(Slice("MIDDLE EAST"));
        col_region_2->append_datum(Slice("MIDDLE EAST"));
        col_region_2->append_datum(Slice("MIDDLE EAST"));
        col_region_2->append_datum(Slice("MIDDLE EAST"));
        col_region_2->append_datum(Slice("MIDDLE EAST"));

        col_cust_key_3->append_datum(int32_t(56));
        col_cust_key_3->append_datum(int32_t(58));
        col_nation_3->append_datum(Slice("IRAN"));
        col_nation_3->append_datum(Slice("JORDAN"));
        col_region_3->append_datum(Slice("MIDDLE EAST"));
        col_region_3->append_datum(Slice("MIDDLE EAST"));

        Columns columns_1 = {col_cust_key_1, col_nation_1, col_region_1};
        Columns columns_2 = {col_cust_key_2, col_nation_2, col_region_2};
        Columns columns_3 = {col_cust_key_3, col_nation_3, col_region_3};

        Chunk::SlotHashMap map;
        map.reserve(columns_1.size() * 2);
        for (int i = 0; i < columns_1.size(); ++i) {
            map[i] = i;
        }

        _chunk_1 = std::make_shared<Chunk>(columns_1, map);
        _chunk_2 = std::make_shared<Chunk>(columns_2, map);
        _chunk_3 = std::make_shared<Chunk>(columns_3, map);

        auto* expr1 = new ColumnRef(TypeDescriptor(TYPE_VARCHAR), 2); // refer to region
        auto* expr2 = new ColumnRef(TypeDescriptor(TYPE_VARCHAR), 1); // refer to nation
        auto* expr3 = new ColumnRef(TypeDescriptor(TYPE_INT), 0);     // refer to cust_key
        _exprs.push_back(expr1);
        _exprs.push_back(expr2);
        _exprs.push_back(expr3);

        _sort_exprs.push_back(new ExprContext(expr1));
        _sort_exprs.push_back(new ExprContext(expr2));
        _sort_exprs.push_back(new ExprContext(expr3));

        _is_asc.push_back(false);
        _is_asc.push_back(true);
        _is_asc.push_back(false);
        _is_null_first.push_back(true);
        _is_null_first.push_back(true);
        _is_null_first.push_back(true);

        _runtime_state = _create_runtime_state();

        ASSERT_OK(Expr::prepare(_sort_exprs, _runtime_state.get()));
        ASSERT_OK(Expr::open(_sort_exprs, _runtime_state.get()));
    }

    void TearDown() override {
        for (ExprContext* ctx : _sort_exprs) {
            delete ctx;
        }
        for (Expr* expr : _exprs) {
            delete expr;
        }
    }

protected:
    ChunkPtr _chunk_1, _chunk_2, _chunk_3;
    std::vector<Expr*> _exprs;
    std::vector<ExprContext*> _sort_exprs;
    std::vector<bool> _is_asc, _is_null_first;

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
};

[[maybe_unused]] static void print_chunk(const ChunkPtr& chunk) {
    std::cout << "==========" << std::endl;
    for (size_t i = 0; i < chunk->num_rows(); ++i) {
        std::cout << "\t" << i << ": ";
        DatumTuple dt = chunk->get(i);
        for (size_t j = 0; j < dt.size(); ++j) {
            if (j == 0) {
                std::cout << dt.get(j).get_int32();
            } else {
                if (dt.get(j).is_null()) {
                    std::cout << ", NULL";
                } else {
                    std::cout << ", " << dt.get(j).get_slice().to_string();
                }
            }
        }
        std::cout << std::endl;
    }
}

TEST_F(SortedChunksMergerTest, one_supplier) {
    int chunk_index = 0;
    std::vector<ChunkPtr> chunks = {_chunk_1};
    auto supplier = [&chunk_index, &chunks](Chunk** cnk) -> Status {
        if (chunk_index < chunks.size()) {
            ChunkPtr& src_chunk = chunks[chunk_index];
            size_t row_num = src_chunk->num_rows();
            *cnk = src_chunk->clone_empty_with_slot(row_num).release();
            for (size_t c = 0; c < src_chunk->num_columns(); ++c) {
                (*cnk)->get_column_by_index(c)->append(*(src_chunk->get_column_by_index(c)), 0, row_num);
            }
            ++chunk_index;
        } else {
            *cnk = nullptr;
        }
        return Status::OK();
    };
    auto probe_supplier = [](Chunk** cnk) -> bool { return false; };
    auto has_supplier = []() -> bool { return false; };

    ChunkSuppliers suppliers = {supplier};
    ChunkProbeSuppliers probe_suppliers = {probe_supplier};
    ChunkHasSuppliers has_suppliers = {has_supplier};
    SortedChunksMerger merger(_runtime_state.get(), false);
    merger.init(suppliers, probe_suppliers, has_suppliers, &_sort_exprs, &_is_asc, &_is_null_first);

    bool eos = false;
    ChunkPtr page_1, page_2;
    merger.get_next(&page_1, &eos);
    ASSERT_FALSE(eos);
    ASSERT_TRUE(page_1 != nullptr);
    merger.get_next(&page_2, &eos);
    ASSERT_TRUE(eos);
    ASSERT_TRUE(page_2 == nullptr);

    // print_chunk(page_1);

    ASSERT_EQ(_chunk_1->num_rows(), page_1->num_rows());
    for (size_t i = 0; i < _chunk_1->num_rows(); ++i) {
        ASSERT_EQ(_chunk_1->get(i).get(0).get_int32(), page_1->get(i).get(0).get_int32());
    }
}

TEST_F(SortedChunksMergerTest, two_suppliers) {
    ChunkSuppliers suppliers;
    ChunkProbeSuppliers probe_suppliers;
    ChunkHasSuppliers has_suppliers;
    std::vector<ChunkPtr> chunks = {_chunk_1, _chunk_2};
    for (auto& chunk : chunks) {
        auto supplier = [&chunks, &chunk](Chunk** cnk) -> Status {
            if (chunk != nullptr) {
                ChunkPtr& src_chunk = chunk;
                size_t row_num = src_chunk->num_rows();
                *cnk = src_chunk->clone_empty_with_slot(row_num).release();
                for (size_t c = 0; c < src_chunk->num_columns(); ++c) {
                    (*cnk)->get_column_by_index(c)->append(*(src_chunk->get_column_by_index(c)), 0, row_num);
                }
                chunk = nullptr;
            } else {
                *cnk = nullptr;
            }
            return Status::OK();
        };
        auto probe_supplier = [](Chunk** cnk) -> bool { return false; };
        auto has_supplier = []() -> bool { return false; };
        suppliers.push_back(supplier);
        probe_suppliers.push_back(probe_supplier);
        has_suppliers.push_back(has_supplier);
    }

    SortedChunksMerger merger(_runtime_state.get(), false);
    merger.init(suppliers, probe_suppliers, has_suppliers, &_sort_exprs, &_is_asc, &_is_null_first);

    bool eos = false;
    ChunkPtr page_1, page_2;
    merger.get_next(&page_1, &eos);
    ASSERT_FALSE(eos);
    ASSERT_TRUE(page_1 != nullptr);
    merger.get_next(&page_2, &eos);
    ASSERT_TRUE(eos);
    ASSERT_TRUE(page_2 == nullptr);

    // print_chunk(page_1);

    ASSERT_EQ(14, _chunk_1->num_rows() + _chunk_2->num_rows());
    ASSERT_EQ(14, page_1->num_rows());
    const size_t Size = 14;
    int32_t permutation[Size] = {71, 70, 69, 54, 4, 55, 49, 41, 16, 52, 24, 12, 2, 6};
    for (size_t i = 0; i < Size; ++i) {
        ASSERT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
    }
}

TEST_F(SortedChunksMergerTest, three_suppliers) {
    ChunkSuppliers suppliers;
    ChunkProbeSuppliers probe_suppliers;
    ChunkHasSuppliers has_suppliers;
    std::vector<ChunkPtr> chunks = {_chunk_1, _chunk_2, _chunk_3};
    for (auto& chunk : chunks) {
        auto supplier = [&chunks, &chunk](Chunk** cnk) -> Status {
            if (chunk != nullptr) {
                ChunkPtr& src_chunk = chunk;
                size_t row_num = src_chunk->num_rows();
                *cnk = src_chunk->clone_empty_with_slot(row_num).release();
                for (size_t c = 0; c < src_chunk->num_columns(); ++c) {
                    (*cnk)->get_column_by_index(c)->append(*(src_chunk->get_column_by_index(c)), 0, row_num);
                }
                chunk = nullptr;
            } else {
                *cnk = nullptr;
            }
            return Status::OK();
        };
        auto probe_supplier = [](Chunk** cnk) -> bool { return false; };
        auto has_supplier = []() -> bool { return false; };
        suppliers.push_back(supplier);
        probe_suppliers.push_back(probe_supplier);
        has_suppliers.push_back(has_supplier);
    }

    SortedChunksMerger merger(_runtime_state.get(), false);
    merger.init(suppliers, probe_suppliers, has_suppliers, &_sort_exprs, &_is_asc, &_is_null_first);

    bool eos = false;
    ChunkPtr page_1, page_2;
    merger.get_next(&page_1, &eos);
    ASSERT_FALSE(eos);
    ASSERT_TRUE(page_1 != nullptr);
    merger.get_next(&page_2, &eos);
    ASSERT_TRUE(eos);
    ASSERT_TRUE(page_2 == nullptr);

    // print_chunk(page_1);

    ASSERT_EQ(16, _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows());
    ASSERT_EQ(16, page_1->num_rows());
    const size_t Size = 16;
    int32_t permutation[Size] = {71, 70, 69, 54, 4, 56, 55, 49, 41, 16, 52, 58, 24, 12, 2, 6};
    for (size_t i = 0; i < Size; ++i) {
        ASSERT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
    }
}

} // namespace starrocks
