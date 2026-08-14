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

#include "compute_env/sorting/sorted_chunks_merger.h"

#include <gtest/gtest.h>

#include <deque>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "common/config_exec_fwd.h"
#include "compute_env/sorting/chunk_cursor.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class SortedChunksMergerTest : public ::testing::Test {
public:
    void SetUp() override {
        config::vector_chunk_size = 1024;

        const auto& int_type_desc = TypeDescriptor(TYPE_INT);
        const auto& varchar_type_desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        MutableColumnPtr col_cust_key_1 = ColumnHelper::create_column(int_type_desc, false);
        MutableColumnPtr col_cust_key_2 = ColumnHelper::create_column(int_type_desc, false);
        MutableColumnPtr col_cust_key_3 = ColumnHelper::create_column(int_type_desc, false);
        MutableColumnPtr col_nation_1 = ColumnHelper::create_column(varchar_type_desc, true);
        MutableColumnPtr col_nation_2 = ColumnHelper::create_column(varchar_type_desc, true);
        MutableColumnPtr col_nation_3 = ColumnHelper::create_column(varchar_type_desc, true);
        MutableColumnPtr col_region_1 = ColumnHelper::create_column(varchar_type_desc, true);
        MutableColumnPtr col_region_2 = ColumnHelper::create_column(varchar_type_desc, true);
        MutableColumnPtr col_region_3 = ColumnHelper::create_column(varchar_type_desc, true);

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

        _chunk_1 = std::make_shared<Chunk>(std::move(columns_1), map);
        _chunk_2 = std::make_shared<Chunk>(std::move(columns_2), map);
        _chunk_3 = std::make_shared<Chunk>(std::move(columns_3), map);

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

        ASSERT_OK(ExprExecutor::prepare(_sort_exprs, _runtime_state.get()));
        ASSERT_OK(ExprExecutor::open(_sort_exprs, _runtime_state.get()));
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
                (*cnk)->get_column_raw_ptr_by_index(c)->append(*(src_chunk->get_column_raw_ptr_by_index(c)), 0,
                                                               row_num);
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
        auto supplier = [&chunk](Chunk** cnk) -> Status {
            if (chunk != nullptr) {
                ChunkPtr& src_chunk = chunk;
                size_t row_num = src_chunk->num_rows();
                *cnk = src_chunk->clone_empty_with_slot(row_num).release();
                for (size_t c = 0; c < src_chunk->num_columns(); ++c) {
                    (*cnk)->get_column_raw_ptr_by_index(c)->append(*(src_chunk->get_column_raw_ptr_by_index(c)), 0,
                                                                   row_num);
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
        auto supplier = [&chunk](Chunk** cnk) -> Status {
            if (chunk != nullptr) {
                ChunkPtr& src_chunk = chunk;
                size_t row_num = src_chunk->num_rows();
                *cnk = src_chunk->clone_empty_with_slot(row_num).release();
                for (size_t c = 0; c < src_chunk->num_columns(); ++c) {
                    (*cnk)->get_column_raw_ptr_by_index(c)->append(*(src_chunk->get_column_raw_ptr_by_index(c)), 0,
                                                                   row_num);
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

// An order-by expression that fails only on a poisoned value, so a cursor can fail on its *second*
// chunk -- after the merger has already built its heap out of the first ones.
// sorting_test.cpp has the same helper and links into the same binary, so keep this copy's linkage
// internal: two identical definitions are legal today, but they must not have to stay identical.
namespace {

class PoisonSortExpr final : public Expr {
public:
    PoisonSortExpr(const TypeDescriptor& type, int32_t poison) : Expr(type, false), _poison(poison) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new PoisonSortExpr(*this)); }

    bool is_constant() const override { return false; }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ColumnPtr column = ptr->get_column_by_index(0);
        const auto* data = ColumnHelper::cast_to_raw<TYPE_INT>(column.get());
        for (size_t i = 0; i < data->size(); i++) {
            if (data->get_data()[i] == _poison) {
                return Status::InternalError("poisoned order-by value");
            }
        }
        return column;
    }

private:
    int32_t _poison;
};

} // namespace

// A cursor whose order-by expressions fail mid-stream. The order-by columns are built from a
// constructor and from void next(), so the failure can only be latched -- and the merger keeps
// comparing cursors before it looks at that latch.
class ChunkCursorFailureTest : public ::testing::Test {
public:
    static constexpr int32_t kPoison = 999;

    void SetUp() override {
        config::vector_chunk_size = 1024;
        _runtime_state = _create_runtime_state();
        // Two order-by expressions: the first one always succeeds, so a failure of the second one
        // leaves a half-built column set behind.
        _column_ref = std::make_unique<ColumnRef>(TypeDescriptor(TYPE_INT), 0);
        _poison_expr = std::make_unique<PoisonSortExpr>(TypeDescriptor(TYPE_INT), kPoison);
        _sort_exprs.push_back(new ExprContext(_column_ref.get()));
        _sort_exprs.push_back(new ExprContext(_poison_expr.get()));
        _is_asc = {true, true};
        _is_null_first = {true, true};
        ASSERT_OK(ExprExecutor::prepare(_sort_exprs, _runtime_state.get()));
        ASSERT_OK(ExprExecutor::open(_sort_exprs, _runtime_state.get()));
    }

    void TearDown() override {
        ExprExecutor::close(_sort_exprs, _runtime_state.get());
        for (ExprContext* ctx : _sort_exprs) {
            delete ctx;
        }
    }

protected:
    static ChunkPtr make_chunk(const std::vector<int32_t>& values) {
        MutableColumnPtr column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        for (int32_t value : values) {
            column->append_datum(Datum(value));
        }
        Chunk::SlotHashMap map;
        map[0] = 0;
        return std::make_shared<Chunk>(Columns{std::move(column)}, map);
    }

    // Chunk is not copyable, so hand the cursor a fresh copy the way the other tests here do.
    static Chunk* clone_chunk(const ChunkPtr& src) {
        const size_t row_num = src->num_rows();
        Chunk* copy = src->clone_empty_with_slot(row_num).release();
        for (size_t c = 0; c < src->num_columns(); ++c) {
            copy->get_column_raw_ptr_by_index(c)->append(*(src->get_column_raw_ptr_by_index(c)), 0, row_num);
        }
        return copy;
    }

    // Hands out |chunks| one by one, then signals EOS with a null chunk.
    static ChunkSupplier make_supplier(const std::shared_ptr<std::deque<ChunkPtr>>& queue) {
        return [queue](Chunk** cnk) -> Status {
            if (queue->empty()) {
                *cnk = nullptr;
                return Status::OK();
            }
            *cnk = clone_chunk(queue->front());
            queue->pop_front();
            return Status::OK();
        };
    }

    static ChunkProbeSupplier make_probe_supplier(const std::shared_ptr<std::deque<ChunkPtr>>& queue) {
        return [queue](Chunk** cnk) -> bool {
            if (queue->empty()) {
                *cnk = nullptr;
                return false;
            }
            *cnk = clone_chunk(queue->front());
            queue->pop_front();
            return true;
        };
    }

    static ChunkHasSupplier make_has_supplier(const std::shared_ptr<std::deque<ChunkPtr>>& queue) {
        return [queue]() -> bool { return !queue->empty(); };
    }

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
    std::unique_ptr<ColumnRef> _column_ref;
    std::unique_ptr<PoisonSortExpr> _poison_expr;
    std::vector<ExprContext*> _sort_exprs;
    std::vector<bool> _is_asc, _is_null_first;
};

// Building the order-by columns stops at the failing expression, so the column set is shorter than
// the expression list. ChunkCursor::operator<() sizes its loop by one cursor and indexes the other,
// so a cursor left half-built while still reporting is_valid() would be read out of bounds by the
// merger's heap -- long before anyone looks at the latched status.
TEST_F(ChunkCursorFailureTest, failed_order_by_build_leaves_the_cursor_exhausted) {
    auto queue = std::make_shared<std::deque<ChunkPtr>>();
    queue->push_back(make_chunk({1, 3, 5}));
    queue->push_back(make_chunk({7, kPoison}));

    ChunkCursor cursor(make_supplier(queue), make_probe_supplier(queue), make_has_supplier(queue), &_sort_exprs,
                       &_is_asc, &_is_null_first, false);
    cursor.next();
    ASSERT_TRUE(cursor.is_valid());
    ASSERT_OK(cursor.status());

    // Walk off the end of the clean chunk, which pulls the poisoned one. Stop as soon as the failure
    // is latched: walking any further runs the cursor off the end of the input, where it looks
    // exhausted whether or not the failure left it comparable.
    for (int i = 0; i < 8; i++) {
        cursor.next();
        if (!cursor.status().ok()) {
            break;
        }
    }

    ASSERT_FALSE(cursor.status().ok()) << "the failure was not latched";
    ASSERT_TRUE(cursor.status().is_internal_error()) << cursor.status();
    ASSERT_FALSE(cursor.is_valid()) << "a cursor with a half-built order-by column set must not stay comparable";
}

// The blocking path: the failure happens inside get_next(), while the other cursor is still live.
TEST_F(ChunkCursorFailureTest, blocking_merge_reports_a_mid_stream_order_by_failure) {
    auto left = std::make_shared<std::deque<ChunkPtr>>();
    left->push_back(make_chunk({1, 3, 5}));
    left->push_back(make_chunk({7, kPoison}));
    auto right = std::make_shared<std::deque<ChunkPtr>>();
    right->push_back(make_chunk({2, 4, 6}));

    ChunkSuppliers suppliers = {make_supplier(left), make_supplier(right)};
    ChunkProbeSuppliers probe_suppliers = {make_probe_supplier(left), make_probe_supplier(right)};
    ChunkHasSuppliers has_suppliers = {make_has_supplier(left), make_has_supplier(right)};

    SortedChunksMerger merger(_runtime_state.get(), false);
    ASSERT_OK(merger.init(suppliers, probe_suppliers, has_suppliers, &_sort_exprs, &_is_asc, &_is_null_first));

    // Everything fits in one chunk, so the poisoned chunk is pulled inside this very call. The call
    // must not hand out its merged chunk as a success and leave the error for a later call that the
    // owning operator has no reason to make.
    ChunkPtr chunk;
    bool eos = false;
    Status st = merger.get_next(&chunk, &eos);
    ASSERT_FALSE(st.ok()) << "a failing order-by expression was reported as a successful merge";
    ASSERT_TRUE(st.is_internal_error()) << st;
}

// The pipeline path drops a cursor out of the heap and reports eos in the *same* call, so checking
// the latched status only on the way in lets the last failure escape: the operator sees eos, stops
// calling, and the query returns fewer rows without an error.
TEST_F(ChunkCursorFailureTest, pipeline_merge_does_not_report_eos_instead_of_the_failure) {
    auto left = std::make_shared<std::deque<ChunkPtr>>();
    left->push_back(make_chunk({1, 3, 5}));
    left->push_back(make_chunk({7, kPoison}));
    auto right = std::make_shared<std::deque<ChunkPtr>>();
    right->push_back(make_chunk({2, 4, 6}));
    right->push_back(make_chunk({8, kPoison}));

    ChunkSuppliers suppliers = {make_supplier(left), make_supplier(right)};
    ChunkProbeSuppliers probe_suppliers = {make_probe_supplier(left), make_probe_supplier(right)};
    ChunkHasSuppliers has_suppliers = {make_has_supplier(left), make_has_supplier(right)};

    SortedChunksMerger merger(_runtime_state.get(), true);
    ASSERT_OK(merger.init_for_pipeline(suppliers, probe_suppliers, has_suppliers, &_sort_exprs, &_is_asc,
                                       &_is_null_first));
    ASSERT_TRUE(merger.is_data_ready());

    Status st;
    std::atomic<bool> eos{false};
    for (int i = 0; i < 16; i++) {
        ChunkPtr chunk;
        bool should_exit = false;
        st = merger.get_next_for_pipeline(&chunk, &eos, &should_exit);
        if (!st.ok() || eos) {
            break;
        }
    }
    ASSERT_FALSE(st.ok()) << "the merge reported eos and swallowed the order-by failure";
    ASSERT_TRUE(st.is_internal_error()) << st;
}

} // namespace starrocks
