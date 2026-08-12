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

#include "column/sorting/sorting.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/sorting/sort_permute.h"
#include "column/struct_column.h"
#include "common/config_exec_fwd.h"
#include "compute_env/sorting/merge.h"
#include "compute_env/sorting/sort_cursor.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "runtime/runtime_state.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

static MutableColumnPtr build_int_column(const std::vector<int32_t>& values) {
    MutableColumnPtr column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
    for (int32_t value : values) {
        column->append_datum(Datum(value));
    }
    return column;
}

// A row of an ARRAY<INT> column with nullable elements, std::nullopt is a NULL element.
using ArrayRow = std::vector<std::optional<int32_t>>;

static MutableColumnPtr build_nullable_int_column(const std::vector<std::optional<int32_t>>& values) {
    auto column = NullableColumn::create(Int32Column::create(), NullColumn::create());
    for (const auto& value : values) {
        if (value.has_value()) {
            column->append_datum(Datum(value.value()));
        } else {
            column->append_nulls(1);
        }
    }
    return column;
}

// Build an ARRAY<INT> column with nullable elements. A row without a value is a NULL row, which makes
// the whole column nullable.
static MutableColumnPtr build_array_column(const std::vector<std::optional<ArrayRow>>& rows) {
    std::vector<std::optional<int32_t>> elements;
    auto offsets = UInt32Column::create();
    offsets->append(0);
    for (const auto& row : rows) {
        if (row.has_value()) {
            elements.insert(elements.end(), row->begin(), row->end());
        }
        offsets->append(elements.size());
    }
    MutableColumnPtr column = ArrayColumn::create(build_nullable_int_column(elements), std::move(offsets));

    if (std::all_of(rows.begin(), rows.end(), [](const auto& row) { return row.has_value(); })) {
        return column;
    }
    auto nulls = NullColumn::create();
    for (const auto& row : rows) {
        nulls->append(row.has_value() ? 0 : 1);
    }
    return NullableColumn::create(std::move(column), std::move(nulls));
}

// Build a STRUCT<f0 INT, f1 INT> column, both fields are nullable.
static MutableColumnPtr build_struct_column(const std::vector<std::optional<int32_t>>& f0,
                                            const std::vector<std::optional<int32_t>>& f1) {
    MutableColumns fields;
    fields.emplace_back(build_nullable_int_column(f0));
    fields.emplace_back(build_nullable_int_column(f1));
    return StructColumn::create(std::move(fields), std::vector<std::string>{"f0", "f1"});
}

static void clear_exprs(std::vector<ExprContext*>& exprs) {
    for (ExprContext* ctx : exprs) {
        delete ctx;
    }
    exprs.clear();
}

static std::shared_ptr<RuntimeState> create_runtime_state() {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = config::vector_chunk_size;
    TQueryGlobals query_globals;
    auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
    runtime_state->init_instance_mem_tracker();
    return runtime_state;
}

class SortingCoreTest : public testing::Test {
protected:
    void SetUp() override {
        _runtime_state = create_runtime_state();
        _exprs.emplace_back(std::make_unique<ColumnRef>(TypeDescriptor(TYPE_INT), 0));
        _sort_exprs.emplace_back(new ExprContext(_exprs.back().get()));
        ASSERT_OK(ExprExecutor::prepare(_sort_exprs, _runtime_state.get()));
        ASSERT_OK(ExprExecutor::open(_sort_exprs, _runtime_state.get()));
    }

    void TearDown() override { clear_exprs(_sort_exprs); }

    ChunkUniquePtr make_chunk(const std::vector<int32_t>& values) {
        return std::make_unique<Chunk>(Columns{build_int_column(values)}, Chunk::SlotHashMap{{0, 0}});
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    std::vector<std::unique_ptr<ColumnRef>> _exprs;
    std::vector<ExprContext*> _sort_exprs;
};

TEST_F(SortingCoreTest, simple_cursor_materializes_sort_columns) {
    auto input = make_chunk({1, 3, 5});
    bool emitted = false;

    ChunkProvider provider = [&input, &emitted](ChunkUniquePtr* output, bool* eos) mutable {
        if (output == nullptr || eos == nullptr) {
            return true;
        }
        if (emitted) {
            *output = nullptr;
            *eos = true;
            return false;
        }
        *output = std::move(input);
        *eos = false;
        emitted = true;
        return true;
    };

    SimpleChunkSortCursor cursor(std::move(provider), &_sort_exprs);
    ASSERT_TRUE(cursor.is_data_ready());

    auto [chunk, sort_columns] = cursor.try_get_next();
    ASSERT_NE(nullptr, chunk);
    ASSERT_EQ(3, chunk->num_rows());
    ASSERT_EQ(1, sort_columns.size());
    ASSERT_EQ(3, sort_columns[0]->size());
    EXPECT_EQ(1, sort_columns[0]->get(0).get_int32());
    EXPECT_EQ(3, sort_columns[0]->get(1).get_int32());
    EXPECT_EQ(5, sort_columns[0]->get(2).get_int32());
}

TEST_F(SortingCoreTest, merge_sorted_chunks) {
    std::vector<ChunkUniquePtr> input_chunks;
    input_chunks.emplace_back(make_chunk({-2074, -1691, -1400, -969, -767, -725}));
    input_chunks.emplace_back(make_chunk({-680, -571, -568}));
    input_chunks.emplace_back(make_chunk({-2118, -2065, -1328, -1103, -1099, -1093}));
    input_chunks.emplace_back(make_chunk({-950, -807, -604}));

    SortDescs sort_desc(std::vector<int>{1}, std::vector<int>{-1});
    SortedRuns output;
    ASSERT_OK(merge_sorted_chunks(sort_desc, &_sort_exprs, input_chunks, &output));
    ASSERT_TRUE(output.is_sorted(sort_desc));
    ASSERT_EQ(18, output.num_rows());
}

// A sorted run is compared twice on its way out of a full sort: once while the run itself is sorted,
// and once again while the runs are merged. Both phases must agree on where a NULL goes, otherwise the
// merge reorders rows that were already in the right order. These tests pin that agreement down by
// running the very same rows through one run (no merge) and through several runs (merged), and by
// pinning the resulting order to a literal so a change of the NULL semantics cannot slip through.
class SortRunConsistencyTest : public testing::Test {
protected:
    void SetUp() override { _runtime_state = create_runtime_state(); }

    void TearDown() override { clear_exprs(_sort_exprs); }

    // Sort every run on its own and merge the sorted runs afterwards, exactly what a full sort does
    // once its input no longer fits into a single buffered run. `run_sizes` splits the rows into runs,
    // a single run holding every row is the path that never merges anything.
    // Returns the row ids in output order.
    std::vector<int32_t> sort_and_merge(const TypeDescriptor& key_type, const ColumnPtr& key_column,
                                        const std::vector<size_t>& run_sizes, const SortDescs& sort_desc) {
        clear_exprs(_sort_exprs);
        _exprs.emplace_back(std::make_unique<ColumnRef>(key_type, 0));
        _sort_exprs.emplace_back(new ExprContext(_exprs.back().get()));
        CHECK(ExprExecutor::prepare(_sort_exprs, _runtime_state.get()).ok());
        CHECK(ExprExecutor::open(_sort_exprs, _runtime_state.get()).ok());

        const Chunk::SlotHashMap slot_map{{0, 0}, {1, 1}};
        const std::atomic<bool> cancel{false};

        std::vector<ChunkUniquePtr> sorted_runs;
        size_t offset = 0;
        for (size_t run_size : run_sizes) {
            auto key = key_column->clone_empty();
            key->append(*key_column, offset, run_size);
            auto row_id = Int32Column::create();
            for (size_t i = 0; i < run_size; i++) {
                row_id->append(static_cast<int32_t>(offset + i));
            }
            ChunkPtr run = std::make_shared<Chunk>(Columns{std::move(key), std::move(row_id)}, slot_map);

            SmallPermutation permutation = create_small_permutation(run_size);
            CHECK(sort_and_tie_columns(cancel, Columns{run->get_column_by_index(0)}, sort_desc, permutation).ok());
            auto sorted_run = run->clone_empty_with_slot(run_size);
            materialize_by_permutation_single(sorted_run.get(), run, permutation);
            sorted_runs.emplace_back(std::move(sorted_run));

            offset += run_size;
        }
        CHECK_EQ(key_column->size(), offset);

        SortedRuns output;
        CHECK(merge_sorted_chunks(sort_desc, &_sort_exprs, sorted_runs, &output).ok());
        // The merged runs must be sorted by the very comparator the merge itself used.
        EXPECT_TRUE(output.is_sorted(sort_desc));

        ChunkPtr merged = output.assemble();
        std::vector<int32_t> order;
        for (size_t i = 0; i < merged->num_rows(); i++) {
            order.emplace_back(merged->get_column_by_index(1)->get(i).get_int32());
        }
        return order;
    }

    // Every split of the same rows must produce the same order as the single run that is never merged.
    void expect_order(const TypeDescriptor& key_type, const ColumnPtr& key_column, const SortDescs& sort_desc,
                      const std::vector<int32_t>& expected) {
        const size_t num_rows = key_column->size();
        ASSERT_EQ(num_rows, expected.size());

        EXPECT_EQ(expected, sort_and_merge(key_type, key_column, {num_rows}, sort_desc)) << "single run";
        for (const auto& run_sizes : std::vector<std::vector<size_t>>{{4, 4}, {3, 5}, {2, 2, 2, 2}, {1, 2, 3, 1, 1}}) {
            EXPECT_EQ(expected, sort_and_merge(key_type, key_column, run_sizes, sort_desc))
                    << "runs: " << run_sizes.size();
        }
    }

    static SortDescs asc_nulls_first() { return SortDescs(std::vector<bool>{true}, std::vector<bool>{true}); }
    static SortDescs desc_nulls_last() { return SortDescs(std::vector<bool>{false}, std::vector<bool>{false}); }
    static SortDescs desc_nulls_first() { return SortDescs(std::vector<bool>{false}, std::vector<bool>{true}); }

    std::shared_ptr<RuntimeState> _runtime_state;
    std::vector<std::unique_ptr<ColumnRef>> _exprs;
    std::vector<ExprContext*> _sort_exprs;
};

// Whether a NULL is the smallest or the greatest value is `SortDesc::nan_direction()`, which the sort
// order mirrors: ascending nulls-first and descending nulls-last both make a NULL element the smallest
// value in ascending space, so under `DESC NULLS LAST` the rows holding a NULL element come out first.
// No two of the eight rows below compare equal, which makes every expected order unambiguous.
TEST_F(SortRunConsistencyTest, array) {
    ColumnPtr column =
            build_array_column({ArrayRow{std::nullopt}, ArrayRow{5}, ArrayRow{4}, ArrayRow{3},
                                ArrayRow{std::nullopt, 2}, ArrayRow{7}, ArrayRow{std::nullopt, 9}, ArrayRow{8}});
    const auto type = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT));

    expect_order(type, column, asc_nulls_first(), {0, 4, 6, 3, 2, 1, 5, 7});
    // Before this was fixed, every run sorted the NULL elements first while the merge ordered them
    // last, so merging several runs degenerated the output into a concatenation of the runs.
    expect_order(type, column, desc_nulls_last(), {6, 4, 0, 7, 5, 1, 2, 3});
}

TEST_F(SortRunConsistencyTest, nullable_array) {
    // Row 2 is a NULL row, which the null_first flag places, unlike the NULL elements of rows 0/4/6.
    ColumnPtr column =
            build_array_column({ArrayRow{std::nullopt}, ArrayRow{5}, std::nullopt, ArrayRow{3},
                                ArrayRow{std::nullopt, 2}, ArrayRow{7}, ArrayRow{std::nullopt, 9}, ArrayRow{8}});
    const auto type = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT));

    expect_order(type, column, desc_nulls_last(), {6, 4, 0, 7, 5, 1, 3, 2});
    expect_order(type, column, desc_nulls_first(), {2, 7, 5, 1, 3, 6, 4, 0});
}

TEST_F(SortRunConsistencyTest, struct_with_null_field) {
    // STRUCT hands the same hint down to its fields, so a NULL field is ordered like a NULL element.
    ColumnPtr column =
            build_struct_column({std::nullopt, 5, 3, std::nullopt, 8, std::nullopt, 7, 1}, {1, 0, 0, 2, 0, 3, 0, 0});
    const auto type =
            TypeDescriptor::create_struct_type({"f0", "f1"}, {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_INT)});

    expect_order(type, column, asc_nulls_first(), {0, 3, 5, 7, 2, 1, 6, 4});
    expect_order(type, column, desc_nulls_last(), {5, 3, 0, 4, 6, 1, 2, 7});
}

// A row-level NULL of a scalar column is placed by the null_first flag alone. The per-run sort
// partitions those rows off instead of comparing them, so the merge is the only phase reading the flag
// and its output must not move.
TEST_F(SortRunConsistencyTest, nullable_int_keeps_null_position) {
    ColumnPtr column = build_nullable_int_column({5, std::nullopt, 3, 9, 8, 1, 6, 7});
    const auto type = TypeDescriptor(TYPE_INT);

    expect_order(type, column, asc_nulls_first(), {1, 5, 2, 0, 6, 7, 4, 3});
    expect_order(type, column, desc_nulls_last(), {3, 4, 7, 6, 0, 2, 5, 1});
    expect_order(type, column, desc_nulls_first(), {1, 3, 4, 7, 6, 0, 2, 5});
}

static MutableColumnPtr build_adaptive_int_column(AdaptiveNullableColumn::State state) {
    auto column = AdaptiveNullableColumn::create(Int32Column::create(), NullColumn::create());
    switch (state) {
    case AdaptiveNullableColumn::State::kUninitialized:
        break;
    case AdaptiveNullableColumn::State::kNull:
        CHECK(column->append_nulls(3));
        break;
    case AdaptiveNullableColumn::State::kConstant:
        for (int i = 0; i < 3; i++) {
            column->append_default_not_null_value();
        }
        break;
    case AdaptiveNullableColumn::State::kNotConstant:
        for (int i = 0; i < 3; i++) {
            column->append_datum(Datum(i));
        }
        break;
    case AdaptiveNullableColumn::State::kMaterialized:
        CHECK(column->append_nulls(1));
        column->append_datum(Datum(1));
        break;
    }
    CHECK(state == column->state());
    return column;
}

// A column is unpacked into its data and null columns only if it says both are readable as is. Everything
// else keeps the general path, and asking must not materialize anything, because the merge asks from every
// worker thread at once while the inputs are supposed to be read-only.
class ColumnCompareViewTest : public testing::Test {
protected:
    static void expect_general_path(const Column& column) {
        ColumnCompareView view(column);
        EXPECT_EQ(&column, view.original);
        EXPECT_EQ(&column, view.data);
        EXPECT_EQ(nullptr, view.nulls);
    }

    static void expect_unpacked(const Column& column) {
        const auto& nullable = down_cast<const NullableColumn&>(column);
        ColumnCompareView view(column);
        EXPECT_EQ(&column, view.original);
        EXPECT_EQ(nullable.data_column().get(), view.data);
        EXPECT_EQ(nullable.null_column().get(), view.nulls);
    }
};

TEST_F(ColumnCompareViewTest, nullable_column_is_unpacked) {
    MutableColumnPtr column = build_nullable_int_column({1, std::nullopt, 3});
    ASSERT_TRUE(column->can_access_nullable_data());
    expect_unpacked(*column);
}

TEST_F(ColumnCompareViewTest, plain_and_constant_columns_keep_the_general_path) {
    MutableColumnPtr plain = build_int_column({1, 2, 3});
    EXPECT_FALSE(plain->can_access_nullable_data());
    expect_general_path(*plain);

    // A constant NULL column reports the is_nullable() of the column it wraps, yet down_cast-ing it would be UB.
    auto constant_null = ConstColumn::create(build_nullable_int_column({std::nullopt}), 3);
    EXPECT_TRUE(constant_null->is_nullable());
    EXPECT_FALSE(constant_null->can_access_nullable_data());
    expect_general_path(*constant_null);

    expect_general_path(*ConstColumn::create(build_int_column({7}), 3));
}

TEST_F(ColumnCompareViewTest, adaptive_column_is_unpacked_only_once_materialized) {
    for (auto state : {AdaptiveNullableColumn::State::kUninitialized, AdaptiveNullableColumn::State::kNull,
                       AdaptiveNullableColumn::State::kConstant, AdaptiveNullableColumn::State::kNotConstant}) {
        MutableColumnPtr column = build_adaptive_int_column(state);
        EXPECT_FALSE(column->can_access_nullable_data());

        expect_general_path(*column);
        // Building the view is the only thing asserted here: a full comparison would materialize the column.
        EXPECT_TRUE(state == down_cast<AdaptiveNullableColumn*>(column.get())->state());
    }

    MutableColumnPtr materialized = build_adaptive_int_column(AdaptiveNullableColumn::State::kMaterialized);
    ASSERT_TRUE(materialized->can_access_nullable_data());
    expect_unpacked(*materialized);
}

// Several call sites write the null bitmap directly and leave has_null() false (BoolOrAggregateFunction's
// empty_result, resize()). What the merge must agree with is the per-run sort, which skips NULL handling
// entirely when has_null() is false -- so the comparison has to consult the flag before the bitmap, exactly
// like is_null() does.
TEST_F(ColumnCompareViewTest, stale_has_null_is_ordered_the_way_the_run_sort_orders_it) {
    MutableColumnPtr owner = build_nullable_int_column({1, 2});
    auto* nullable = down_cast<NullableColumn*>(owner.get());
    nullable->null_column_data()[1] = 1;
    ASSERT_FALSE(nullable->has_null());
    ASSERT_FALSE(nullable->is_null(1)) << "is_null() consults has_null() before the bitmap";

    ColumnPtr column = std::move(owner);
    const ColumnCompareView view(*column);
    ASSERT_NE(nullptr, view.nulls) << "the column is unpacked, so this exercises the fast path";

    for (const SortDesc& desc : {SortDesc(true, true), SortDesc(false, false)}) {
        const SortDescs descs(std::vector<int>{desc.sort_order}, std::vector<int>{desc.null_first});
        SmallPermutation permutation = create_small_permutation(column->size());
        const std::atomic<bool> cancel{false};
        ASSERT_OK(sort_and_tie_columns(cancel, Columns{column}, descs, permutation));

        const bool sort_puts_first_row_first = permutation[0].index_in_chunk == 0;
        const int merged = compare_column_row(desc, view, 0, view, 1) * desc.sort_order;
        EXPECT_EQ(sort_puts_first_row_first, merged < 0) << "sort_order: " << desc.sort_order;
    }
}

// The two runs have to overlap to reach MergeTwoColumn at all: two disjoint runs are concatenated by
// merge_sorted_chunks_two_way without ever comparing a row, which would leave these cases green and blind.
class MergeTwoColumnTest : public testing::Test {
protected:
    using Values = std::vector<std::optional<int32_t>>;

    static Values read_values(const Column& column, const std::vector<uint32_t>& rows) {
        Values values;
        for (uint32_t row : rows) {
            values.emplace_back(column.is_null(row) ? std::nullopt
                                                    : std::optional<int32_t>(column.get(row).get_int32()));
        }
        return values;
    }

    // Merge two single column runs, and report whether the output interleaved them, which only a row by row
    // merge can produce.
    static Values merge_two_runs(const SortDescs& descs, const ColumnPtr& left, const ColumnPtr& right,
                                 bool* interleaved) {
        auto make_run = [](const ColumnPtr& column) {
            ChunkPtr chunk = std::make_shared<Chunk>(Columns{column}, Chunk::SlotHashMap{{0, 0}});
            return SortedRun(chunk, Columns{column});
        };

        Permutation perm;
        CHECK(merge_sorted_chunks_two_way(descs, make_run(left), make_run(right), &perm).ok());

        Values values;
        bool seen_right = false;
        *interleaved = false;
        for (const auto& item : perm) {
            const Column& column = item.chunk_index == 0 ? *left : *right;
            values.emplace_back(column.is_null(item.index_in_chunk)
                                        ? std::nullopt
                                        : std::optional<int32_t>(column.get(item.index_in_chunk).get_int32()));
            if (item.chunk_index == 1) {
                seen_right = true;
            } else if (seen_right) {
                *interleaved = true;
            }
        }
        return values;
    }

    // The order the per-run sort itself produces for all the rows at once, the merge must reproduce it.
    static Values sort_all(const SortDescs& descs, const Values& all) {
        ColumnPtr column = build_nullable_int_column(all);
        SmallPermutation perm = create_small_permutation(column->size());
        const std::atomic<bool> cancel{false};
        CHECK(sort_and_tie_columns(cancel, Columns{column}, descs, perm).ok());

        std::vector<uint32_t> rows;
        for (const auto& item : perm) {
            rows.emplace_back(item.index_in_chunk);
        }
        return read_values(*column, rows);
    }

    static SortDescs desc_nulls_last() { return SortDescs(std::vector<bool>{false}, std::vector<bool>{false}); }
    static SortDescs asc_nulls_first() { return SortDescs(std::vector<bool>{true}, std::vector<bool>{true}); }
};

TEST_F(MergeTwoColumnTest, merges_nullable_columns_the_way_the_sort_would) {
    // Distinct values, so a single order satisfies both phases and the comparison below is unambiguous.
    const Values left_values{9, 7, 4, std::nullopt};
    const Values right_values{8, 6, 5, std::nullopt};
    Values all = left_values;
    all.insert(all.end(), right_values.begin(), right_values.end());

    for (const auto& descs : {desc_nulls_last(), asc_nulls_first()}) {
        Values left_sorted = sort_all(descs, left_values);
        Values right_sorted = sort_all(descs, right_values);

        bool interleaved = false;
        Values merged = merge_two_runs(descs, build_nullable_int_column(left_sorted),
                                       build_nullable_int_column(right_sorted), &interleaved);
        EXPECT_TRUE(interleaved);
        EXPECT_EQ(sort_all(descs, all), merged);
    }
}

TEST_F(MergeTwoColumnTest, merges_materialized_adaptive_columns_the_way_the_sort_would) {
    auto build_adaptive = [](const Values& values) {
        auto column = AdaptiveNullableColumn::create(Int32Column::create(), NullColumn::create());
        for (const auto& value : values) {
            value.has_value() ? column->append_datum(Datum(value.value())) : (void)column->append_nulls(1);
        }
        column->materialized_nullable();
        CHECK(column->can_access_nullable_data());
        return column;
    };

    const Values left_values{9, 7, 4, std::nullopt};
    const Values right_values{8, 6, 5, std::nullopt};
    Values all = left_values;
    all.insert(all.end(), right_values.begin(), right_values.end());

    const SortDescs descs = desc_nulls_last();
    bool interleaved = false;
    Values merged = merge_two_runs(descs, build_adaptive(sort_all(descs, left_values)),
                                   build_adaptive(sort_all(descs, right_values)), &interleaved);
    EXPECT_TRUE(interleaved);
    EXPECT_EQ(sort_all(descs, all), merged);
}

} // namespace
} // namespace starrocks
