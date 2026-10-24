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

#include <atomic>
#include <cmath>
#include <cstring>
#include <latch>
#include <limits>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/sorting/radix_sort.h"
#include "column/sorting/sort_helper.h"
#include "column/sorting/sort_permute.h"
#include "column/sorting/sorting.h"
#include "common/config_exec_flow_fwd.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {
namespace {

ColumnPtr int_column(const std::vector<int32_t>& values) {
    auto column = Int32Column::create();
    for (auto value : values) {
        column->append(value);
    }
    return column;
}

TEST(ColumnSortCoreTest, sort_and_tie_columns_orders_single_column) {
    std::atomic<bool> cancel{false};
    Columns columns{int_column({3, 1, 2})};
    SortDescs sort_desc(std::vector<int>{1}, std::vector<int>{-1});
    auto perm = create_small_permutation(3);

    ASSERT_OK(sort_and_tie_columns(cancel, columns, sort_desc, perm));

    ASSERT_EQ(3, perm.size());
    EXPECT_EQ(1, perm[0].index_in_chunk);
    EXPECT_EQ(2, perm[1].index_in_chunk);
    EXPECT_EQ(0, perm[2].index_in_chunk);
}

TEST(ColumnSortCoreTest, sort_and_tie_columns_orders_nullable_column) {
    std::atomic<bool> cancel{false};
    auto nullable = NullableColumn::create(Int32Column::create(), NullColumn::create());
    nullable->append_datum(Datum(3));
    nullable->append_nulls(1);
    nullable->append_datum(Datum(1));
    Columns columns{std::move(nullable)};
    SortDescs sort_desc(std::vector<int>{1}, std::vector<int>{-1});
    auto perm = create_small_permutation(3);

    ASSERT_OK(sort_and_tie_columns(cancel, columns, sort_desc, perm));

    ASSERT_EQ(3, perm.size());
    EXPECT_EQ(1, perm[0].index_in_chunk);
    EXPECT_EQ(2, perm[1].index_in_chunk);
    EXPECT_EQ(0, perm[2].index_in_chunk);
}

TEST(ColumnSortCoreTest, stable_sort_and_tie_columns_preserves_equal_order) {
    std::atomic<bool> cancel{false};
    Columns columns{int_column({2, 1, 1, 2})};
    SortDescs sort_desc(std::vector<int>{1}, std::vector<int>{-1});
    auto perm = create_small_permutation(4);

    ASSERT_OK(stable_sort_and_tie_columns(cancel, columns, sort_desc, &perm));

    ASSERT_EQ(4, perm.size());
    EXPECT_EQ(1, perm[0].index_in_chunk);
    EXPECT_EQ(2, perm[1].index_in_chunk);
    EXPECT_EQ(0, perm[2].index_in_chunk);
    EXPECT_EQ(3, perm[3].index_in_chunk);
}

TEST(ColumnSortCoreTest, materialize_column_by_permutation_single_reorders_values) {
    auto input = Int32Column::create();
    input->append(10);
    input->append(20);
    input->append(30);
    auto output = Int32Column::create();
    SmallPermutation perm{{2}, {0}, {1}};

    materialize_column_by_permutation_single(output.get(), input.get(), perm);

    ASSERT_EQ("[30, 10, 20]", output->debug_string());
}

// Regression test for https://github.com/StarRocks/starrocks/issues/77374
//
// A full sort compares the rows twice: once when a buffered run is sorted, and once when the sorted runs are
// merged. The merge phase compares by `compare_chunk_row`, so a run sorted by `sort_and_tie_columns` must be
// ordered by the very same comparator, no matter where the NULLs nested in an ARRAY/MAP/STRUCT end up.
class NestedNullSortTest : public testing::TestWithParam<std::pair<SortDescs, std::vector<uint32_t>>> {
protected:
    // The `arr` values of issue #77374
    static ColumnPtr array_column() {
        const std::vector<std::vector<std::optional<int32_t>>> arrays = {
                {std::nullopt}, {5}, {4}, {3}, {std::nullopt, 2}, {7}, {std::nullopt, 9}, {8}};

        auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
        auto offsets = UInt32Column::create();
        offsets->append(0);
        uint32_t offset = 0;
        for (const auto& array : arrays) {
            for (const auto& element : array) {
                if (element.has_value()) {
                    elements->append_datum(Datum(element.value()));
                } else {
                    elements->append_nulls(1);
                }
                offset++;
            }
            offsets->append(offset);
        }
        return ArrayColumn::create(std::move(elements), std::move(offsets));
    }
};

TEST_P(NestedNullSortTest, sorted_run_agrees_with_the_merge_comparator) {
    const auto& [sort_desc, expected] = GetParam();
    std::atomic<bool> cancel{false};
    Columns columns{array_column()};
    auto perm = create_small_permutation(columns[0]->size());

    ASSERT_OK(sort_and_tie_columns(cancel, columns, sort_desc, perm));

    std::vector<uint32_t> sorted;
    for (const auto& item : perm) {
        sorted.push_back(item.index_in_chunk);
    }
    EXPECT_EQ(expected, sorted);

    // The order of the run must be non-descending for the comparator the merge phase uses
    for (size_t i = 1; i < perm.size(); i++) {
        EXPECT_LE(compare_chunk_row(sort_desc, columns, columns, perm[i - 1].index_in_chunk, perm[i].index_in_chunk), 0)
                << "row " << perm[i - 1].index_in_chunk << " must not succeed row " << perm[i].index_in_chunk;
    }
}

INSTANTIATE_TEST_SUITE_P(ArrayWithNestedNull, NestedNullSortTest,
                         testing::Values(
                                 // asc, null first
                                 std::make_pair(SortDescs(std::vector<int>{1}, std::vector<int>{-1}),
                                                std::vector<uint32_t>{0, 4, 6, 3, 2, 1, 5, 7}),
                                 // desc, null last: the reverse of `asc, null first`
                                 std::make_pair(SortDescs(std::vector<int>{-1}, std::vector<int>{-1}),
                                                std::vector<uint32_t>{7, 5, 1, 2, 3, 6, 4, 0}),
                                 // asc, null last
                                 std::make_pair(SortDescs(std::vector<int>{1}, std::vector<int>{1}),
                                                std::vector<uint32_t>{3, 2, 1, 5, 7, 0, 4, 6}),
                                 // desc, null first: the reverse of `asc, null last`
                                 std::make_pair(SortDescs(std::vector<int>{-1}, std::vector<int>{1}),
                                                std::vector<uint32_t>{6, 4, 0, 7, 5, 1, 2, 3})));

struct ThresholdRestoreGuard {
    int32_t saved;
    ThresholdRestoreGuard() : saved(config::radix_sort_threshold) {}
    ~ThresholdRestoreGuard() { config::radix_sort_threshold = saved; }
};

TEST(SortCoreTest, RadixSortStableTieOrderPreservation) {
    ThresholdRestoreGuard guard;
    config::radix_sort_threshold = 256;

    constexpr size_t N = 512;
    auto col = Int64Column::create();
    for (size_t i = 0; i < N; ++i) {
        col->append(static_cast<int64_t>((i * 7) % 5));
    }

    std::atomic<bool> cancel{false};
    SmallPermutation permutation = create_small_permutation(N);
    Tie tie(N, 1);
    ColumnPtr col_ptr = std::move(col);

    ASSERT_OK(sort_and_tie_column(cancel, col_ptr, SortDesc(true, true), permutation, tie,
                                  std::pair<int, int>{0, static_cast<int>(N)}, true));

    const auto* typed_col = down_cast<const Int64Column*>(col_ptr.get());
    const auto data = typed_col->immutable_data();
    for (size_t i = 1; i < N; ++i) {
        int64_t prev_val = data[permutation[i - 1].index_in_chunk];
        int64_t curr_val = data[permutation[i].index_in_chunk];
        ASSERT_LE(prev_val, curr_val);
        EXPECT_EQ(tie[i], static_cast<uint8_t>(prev_val == curr_val));
        if (tie[i] == 1) {
            EXPECT_LT(permutation[i - 1].index_in_chunk, permutation[i].index_in_chunk)
                    << "Tie order not preserved at i=" << i;
        }
    }
}

template <typename ColumnType, typename BuilderFn>
void verify_column_across_thresholds_and_sizes(BuilderFn&& build_col) {
    const std::vector<size_t> sizes = {0, 1, 64, 255, 256, 257, 512, 1024, 4096};
    const std::vector<int32_t> thresholds = {256, 16, 0};
    std::atomic<bool> cancel{false};

    for (size_t n : sizes) {
        ColumnPtr col = build_col(n);
        for (bool asc : {true, false}) {
            SortDesc single_desc(asc, true);
            SortDescs multi_desc(std::vector<bool>{asc}, std::vector<bool>{true});

            std::vector<SmallPermutation> stable_perms;
            std::vector<Tie> single_ties;

            for (int32_t thresh : thresholds) {
                config::radix_sort_threshold = thresh;

                // 1. Test sort_and_tie_column
                SmallPermutation perm_single = create_small_permutation(static_cast<uint32_t>(n));
                Tie tie_single(n, 1);
                ASSERT_OK(sort_and_tie_column(cancel, col, single_desc, perm_single, tie_single,
                                              std::pair<int, int>{0, static_cast<int>(n)}, true));
                single_ties.push_back(std::move(tie_single));

                // 2. Test stable_sort_and_tie_columns
                SmallPermutation perm_stable = create_small_permutation(static_cast<uint32_t>(n));
                Columns cols{col};
                ASSERT_OK(stable_sort_and_tie_columns(cancel, cols, multi_desc, &perm_stable));
                stable_perms.push_back(std::move(perm_stable));
            }

            // Verify all thresholds (256, 16, 0) produce identical Tie bitmask and identical stable permutation
            EXPECT_EQ(single_ties[0], single_ties[1]) << "Tie mismatch (256 vs 16) at N=" << n << " asc=" << asc;
            EXPECT_EQ(single_ties[0], single_ties[2]) << "Tie mismatch (256 vs 0) at N=" << n << " asc=" << asc;

            EXPECT_EQ(stable_perms[0], stable_perms[1])
                    << "Stable permutation mismatch (256 vs 16) at N=" << n << " asc=" << asc;
            EXPECT_EQ(stable_perms[0], stable_perms[2])
                    << "Stable permutation mismatch (256 vs 0) at N=" << n << " asc=" << asc;

            // Verify tie preservation in stable_perms[0]
            for (size_t i = 1; i < n; ++i) {
                if (single_ties[0][i] == 1) {
                    EXPECT_LT(stable_perms[0][i - 1].index_in_chunk, stable_perms[0][i].index_in_chunk)
                            << "Stability violated at N=" << n << " i=" << i;
                }
            }
        }
    }
}

TEST(SortCoreTest, RadixSortThresholdBoundariesAndKillSwitch) {
    ThresholdRestoreGuard guard;

    verify_column_across_thresholds_and_sizes<Int8Column>([](size_t n) -> ColumnPtr {
        auto col = Int8Column::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(static_cast<int8_t>(static_cast<int>((i * 13) % 31) - 15));
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<Int16Column>([](size_t n) -> ColumnPtr {
        auto col = Int16Column::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(static_cast<int16_t>(static_cast<int>((i * 37) % 101) - 50));
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<Int32Column>([](size_t n) -> ColumnPtr {
        auto col = Int32Column::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(static_cast<int32_t>(static_cast<int64_t>((i * 73) % 127) - 63));
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<Int64Column>([](size_t n) -> ColumnPtr {
        auto col = Int64Column::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(static_cast<int64_t>((i * 97) % 131) - 65);
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<UInt8Column>([](size_t n) -> ColumnPtr {
        auto col = UInt8Column::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(static_cast<uint8_t>((i * 11) % 29));
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<UInt16Column>([](size_t n) -> ColumnPtr {
        auto col = UInt16Column::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(static_cast<uint16_t>((i * 41) % 103));
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<UInt32Column>([](size_t n) -> ColumnPtr {
        auto col = UInt32Column::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(static_cast<uint32_t>((i * 59) % 137));
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<UInt64Column>([](size_t n) -> ColumnPtr {
        auto col = UInt64Column::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(static_cast<uint64_t>((i * 83) % 149));
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<FloatColumn>([](size_t n) -> ColumnPtr {
        using Limits = std::numeric_limits<float>;
        float custom_nan1, custom_nan2;
        uint32_t nan1_bits = 0x7fc00123U;
        uint32_t nan2_bits = 0xffc04567U;
        std::memcpy(&custom_nan1, &nan1_bits, sizeof(float));
        std::memcpy(&custom_nan2, &nan2_bits, sizeof(float));

        const std::vector<float> pool = {
                -0.0f,
                +0.0f,
                -Limits::infinity(),
                Limits::infinity(),
                Limits::quiet_NaN(),
                Limits::signaling_NaN(),
                -Limits::quiet_NaN(),
                custom_nan1,
                custom_nan2,
                -100.5f,
                -1.0f,
                -Limits::denorm_min(),
                Limits::denorm_min(),
                1.0f,
                100.5f,
        };
        auto col = FloatColumn::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(pool[(i * 7) % pool.size()]);
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<DoubleColumn>([](size_t n) -> ColumnPtr {
        using Limits = std::numeric_limits<double>;
        double custom_nan1, custom_nan2;
        uint64_t nan1_bits = 0x7ff8000000000123ULL;
        uint64_t nan2_bits = 0xfff8000000004567ULL;
        std::memcpy(&custom_nan1, &nan1_bits, sizeof(double));
        std::memcpy(&custom_nan2, &nan2_bits, sizeof(double));

        const std::vector<double> pool = {
                -0.0,
                +0.0,
                -Limits::infinity(),
                Limits::infinity(),
                Limits::quiet_NaN(),
                Limits::signaling_NaN(),
                -Limits::quiet_NaN(),
                custom_nan1,
                custom_nan2,
                -1000.25,
                -1.0,
                -Limits::denorm_min(),
                Limits::denorm_min(),
                1.0,
                1000.25,
        };
        auto col = DoubleColumn::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(pool[(i * 11) % pool.size()]);
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<DateColumn>([](size_t n) -> ColumnPtr {
        auto col = DateColumn::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(DateValue{static_cast<int32_t>(2450000 + static_cast<int>((i * 19) % 47) - 23)});
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<TimestampColumn>([](size_t n) -> ColumnPtr {
        auto col = TimestampColumn::create();
        for (size_t i = 0; i < n; ++i) {
            col->append(TimestampValue{static_cast<int64_t>((i * 23) % 53) - 26});
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<Decimal32Column>([](size_t n) -> ColumnPtr {
        auto col = Decimal32Column::create(9, 2);
        for (size_t i = 0; i < n; ++i) {
            col->append(static_cast<int32_t>(static_cast<int>((i * 29) % 61) - 30));
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<Decimal64Column>([](size_t n) -> ColumnPtr {
        auto col = Decimal64Column::create(18, 4);
        for (size_t i = 0; i < n; ++i) {
            col->append(static_cast<int64_t>((i * 31) % 67) - 33);
        }
        return col;
    });

    verify_column_across_thresholds_and_sizes<Decimal128Column>([](size_t n) -> ColumnPtr {
        auto col = Decimal128Column::create(38, 6);
        for (size_t i = 0; i < n; ++i) {
            col->append(static_cast<int128_t>(static_cast<int64_t>((i * 43) % 71) - 35));
        }
        return col;
    });

    config::radix_sort_threshold = 256;
}

TEST(SortCoreTest, MultiColumnMixedRadixAndPdqsortStability) {
    ThresholdRestoreGuard guard;
    std::atomic<bool> cancel{false};

    // 1. Multi-column all-radix (Int32Column + FloatColumn, N = 1024, tie groups >= 256)
    //    Here all_columns_stably_sorted == true elides the post-sort stabilization loop.
    {
        constexpr size_t N = 1024;
        auto col0 = Int32Column::create();
        auto col1 = FloatColumn::create();
        for (size_t i = 0; i < N; ++i) {
            // 3 distinct keys -> tie group sizes ~341 >= 256
            col0->append(static_cast<int32_t>((i * 7) % 3) - 1);
            // 5 distinct float keys -> final ties of size ~68
            col1->append(static_cast<float>(static_cast<int>((i * 13) % 5) - 2) * 1.5f);
        }

        Columns cols{col0, col1};
        SortDescs descs(std::vector<bool>{true, false}, std::vector<bool>{true, true});

        config::radix_sort_threshold = 256;
        SmallPermutation perm_radix = create_small_permutation(N);
        ASSERT_OK(stable_sort_and_tie_columns(cancel, cols, descs, &perm_radix));

        config::radix_sort_threshold = 0;
        SmallPermutation perm_pdq = create_small_permutation(N);
        ASSERT_OK(stable_sort_and_tie_columns(cancel, cols, descs, &perm_pdq));

        EXPECT_EQ(perm_radix, perm_pdq);

        const auto d0 = col0->immutable_data();
        const auto d1 = col1->immutable_data();
        for (size_t i = 1; i < N; ++i) {
            uint32_t prev_idx = perm_radix[i - 1].index_in_chunk;
            uint32_t curr_idx = perm_radix[i].index_in_chunk;
            ASSERT_LE(d0[prev_idx], d0[curr_idx]);
            if (d0[prev_idx] == d0[curr_idx]) {
                ASSERT_GE(d1[prev_idx], d1[curr_idx]);
                if (d1[prev_idx] == d1[curr_idx]) {
                    EXPECT_LT(prev_idx, curr_idx) << "All-radix tie order violated at i=" << i;
                }
            }
        }
    }

    // 2. Multi-column with small tie groups (Int32Column with tie groups of size 16 < 256 + Int64Column)
    //    Primary column uses radix sort (N = 512 >= 256), secondary column sees sub-ranges of size 16 < 256
    //    and falls back to pdqsort, triggering the stabilization pass on final ties.
    {
        constexpr size_t N = 512;
        auto col0 = Int32Column::create();
        auto col1 = Int64Column::create();
        for (size_t i = 0; i < N; ++i) {
            // 32 distinct keys -> each tie group has exact size 16 (< 256)
            col0->append(static_cast<int32_t>((i * 9) % 32));
            // 3 distinct keys within each group -> ~5 tied rows per final tie group
            col1->append(static_cast<int64_t>((i * 5) % 3));
        }

        Columns cols{col0, col1};
        SortDescs descs(std::vector<bool>{true, true}, std::vector<bool>{true, true});

        config::radix_sort_threshold = 256;
        SmallPermutation perm_mixed = create_small_permutation(N);
        ASSERT_OK(stable_sort_and_tie_columns(cancel, cols, descs, &perm_mixed));

        config::radix_sort_threshold = 0;
        SmallPermutation perm_pdq = create_small_permutation(N);
        ASSERT_OK(stable_sort_and_tie_columns(cancel, cols, descs, &perm_pdq));

        EXPECT_EQ(perm_mixed, perm_pdq);

        const auto d0 = col0->immutable_data();
        const auto d1 = col1->immutable_data();
        for (size_t i = 1; i < N; ++i) {
            uint32_t prev_idx = perm_mixed[i - 1].index_in_chunk;
            uint32_t curr_idx = perm_mixed[i].index_in_chunk;
            ASSERT_LE(d0[prev_idx], d0[curr_idx]);
            if (d0[prev_idx] == d0[curr_idx]) {
                ASSERT_LE(d1[prev_idx], d1[curr_idx]);
                if (d1[prev_idx] == d1[curr_idx]) {
                    EXPECT_LT(prev_idx, curr_idx) << "Mixed small-tie stability violated at i=" << i;
                }
            }
        }
    }

    // 3. Multi-column with NullableColumn (both nulls_first = true and nulls_first = false,
    //    with duplicate null rows and duplicate non-null rows >= 256) + Int32Column and BinaryColumn.
    //    Because NullableColumn::do_visit uses std::partition (unstable), it must force _is_stable_sorted = false
    //    even when the inner data_column is sorted via radix sort!
    for (bool nulls_first : {true, false}) {
        constexpr size_t N = 600;
        auto build_nullable_int = [&]() {
            auto data_col = Int32Column::create();
            auto null_col = NullColumn::create();
            for (size_t i = 0; i < N; ++i) {
                if (i % 3 == 0) {
                    // 200 NULL rows with varying underlying garbage before fill_null_with_default
                    data_col->append(static_cast<int32_t>(i));
                    null_col->append(1);
                } else {
                    // 400 non-null rows with only 2 distinct values (each tie group ~200 rows)
                    data_col->append(static_cast<int32_t>((i * 7) % 2));
                    null_col->append(0);
                }
            }
            return NullableColumn::create(std::move(data_col), std::move(null_col));
        };

        auto col1_int = Int32Column::create();
        auto col1_bin = BinaryColumn::create();
        const std::vector<std::string> str_pool = {"alpha", "beta", "gamma"};
        for (size_t i = 0; i < N; ++i) {
            // Only 2 distinct values so large tie groups (>= 256 when combined or within non-nulls) remain tied
            col1_int->append(static_cast<int32_t>((i * 11) % 2));
            col1_bin->append(Slice(str_pool[(i * 5) % str_pool.size()]));
        }

        // Case 3a: NullableColumn(Int32) + Int32Column (with threshold = 16 so inner data_column uses radix sort)
        {
            Columns cols_radix{build_nullable_int(), col1_int};
            Columns cols_pdq{build_nullable_int(), col1_int};
            SortDescs descs(std::vector<bool>{true, true}, std::vector<bool>{nulls_first, true});

            config::radix_sort_threshold = 16;
            SmallPermutation perm_radix = create_small_permutation(N);
            ASSERT_OK(stable_sort_and_tie_columns(cancel, cols_radix, descs, &perm_radix));

            config::radix_sort_threshold = 0;
            SmallPermutation perm_pdq = create_small_permutation(N);
            ASSERT_OK(stable_sort_and_tie_columns(cancel, cols_pdq, descs, &perm_pdq));

            EXPECT_EQ(perm_radix, perm_pdq) << "Nullable+Int32 mismatch for nulls_first=" << nulls_first;

            for (size_t i = 1; i < N; ++i) {
                uint32_t prev_idx = perm_radix[i - 1].index_in_chunk;
                uint32_t curr_idx = perm_radix[i].index_in_chunk;
                int cmp = compare_chunk_row(descs, cols_radix, cols_radix, prev_idx, curr_idx);
                ASSERT_LE(cmp, 0);
                if (cmp == 0) {
                    EXPECT_LT(prev_idx, curr_idx)
                            << "Nullable+Int32 tie order violated at i=" << i << " nulls_first=" << nulls_first;
                }
            }
        }

        // Case 3b: Int32Column (radix) + BinaryColumn (pdqsort)
        {
            Columns cols_radix{col1_int, col1_bin};
            Columns cols_pdq{col1_int, col1_bin};
            SortDescs descs(std::vector<bool>{true, true}, std::vector<bool>{true, true});

            config::radix_sort_threshold = 256;
            SmallPermutation perm_radix = create_small_permutation(N);
            ASSERT_OK(stable_sort_and_tie_columns(cancel, cols_radix, descs, &perm_radix));

            config::radix_sort_threshold = 0;
            SmallPermutation perm_pdq = create_small_permutation(N);
            ASSERT_OK(stable_sort_and_tie_columns(cancel, cols_pdq, descs, &perm_pdq));

            EXPECT_EQ(perm_radix, perm_pdq);

            for (size_t i = 1; i < N; ++i) {
                uint32_t prev_idx = perm_radix[i - 1].index_in_chunk;
                uint32_t curr_idx = perm_radix[i].index_in_chunk;
                int cmp = compare_chunk_row(descs, cols_radix, cols_radix, prev_idx, curr_idx);
                ASSERT_LE(cmp, 0);
                if (cmp == 0) {
                    EXPECT_LT(prev_idx, curr_idx) << "Int32+Binary tie order violated at i=" << i;
                }
            }
        }
    }

    config::radix_sort_threshold = 256;
}

TEST(SortCoreTest, RadixSortMidExecutionCancellationReturnsCancelled) {
    ThresholdRestoreGuard guard;
    config::radix_sort_threshold = 256;
    constexpr size_t N = 1024;

    auto col = Int64Column::create();
    for (size_t i = 0; i < N; ++i) {
        col->append(static_cast<int64_t>((i * 6364136223846793005ULL + 1442695040888963407ULL)));
    }
    ColumnPtr col_ptr = col;

    // 1. Pre-set atomic cancellation on sort_and_tie_column and stable_sort_and_tie_columns
    {
        std::atomic<bool> cancel{true};
        SmallPermutation perm = create_small_permutation(N);
        Tie tie(N, 1);
        Status st = sort_and_tie_column(cancel, col_ptr, SortDesc(1, 1), perm, tie, {0, static_cast<int>(N)}, false);
        ASSERT_TRUE(st.is_cancelled());

        SmallPermutation stable_perm = create_small_permutation(N);
        Columns cols{col_ptr};
        SortDescs descs(std::vector<bool>{true}, std::vector<bool>{true});
        Status st_stable = stable_sort_and_tie_columns(cancel, cols, descs, &stable_perm);
        ASSERT_TRUE(st_stable.is_cancelled());
    }

    // 2. True mid-execution concurrent cancellation across a 4-column dataset using deterministic SyncPoint barrier
    {
        constexpr size_t kRows = 4096;
        auto col0 = Int32Column::create();
        auto col1 = Int32Column::create();
        auto col2 = Int64Column::create();
        auto col3 = Int64Column::create();
        for (size_t i = 0; i < kRows; ++i) {
            // 16 tie groups of 256 rows (interleaved so col0 permutes perm immediately after 1 byte pass)
            col0->append(static_cast<int32_t>(i % 16));
            // 64 tie groups of 64 rows
            col1->append(static_cast<int32_t>((i / 16) % 4));
            col2->append(static_cast<int64_t>(i * 6364136223846793005ULL + 1442695040888963407ULL));
            col3->append(static_cast<int64_t>(i * 2862933555777941757ULL + 3037000493ULL));
        }
        Columns cols{col0, col1, col2, col3};
        SortDescs descs(std::vector<bool>{true, true, true, true}, std::vector<bool>{true, true, true, true});

        auto is_identity = [](const SmallPermutation& perm) {
            for (size_t i = 0; i < perm.size(); ++i) {
                if (perm[i].index_in_chunk != static_cast<uint32_t>(i)) {
                    return false;
                }
            }
            return true;
        };

        for (bool test_stable : {false, true}) {
            std::atomic<bool> cancel{false};
            std::atomic<int> pass_count{0};
            SmallPermutation perm = create_small_permutation(kRows);
            Status st;
            std::latch in_flight_reached(1);
            std::latch cancel_stored(1);

            // Trigger cancellation on pass_count == 2 (inside col1's first 256-row tie group after col0
            // has already finished and restored its non-identity permutation into `perm`).
            auto* sp = SyncPoint::GetInstance();
            sp->ClearAllCallBacks();
            sp->SetCallBack("radix_sort_core::after_scatter_pass", [&](void*) {
                if (pass_count.fetch_add(1, std::memory_order_relaxed) + 1 == 2) {
                    in_flight_reached.count_down();
                    cancel_stored.wait();
                }
            });
            sp->EnableProcessing();

            std::thread worker([&]() {
                if (test_stable) {
                    st = stable_sort_and_tie_columns(cancel, cols, descs, &perm);
                } else {
                    st = sort_and_tie_columns(cancel, cols, descs, perm);
                }
                if (pass_count.load(std::memory_order_relaxed) < 2) {
                    in_flight_reached.count_down();
                }
            });

            in_flight_reached.wait();
            cancel.store(true, std::memory_order_release);
            cancel_stored.count_down();
            worker.join();

            sp->DisableProcessing();
            sp->ClearAllCallBacks();

            EXPECT_GE(pass_count.load(std::memory_order_relaxed), 2);
            EXPECT_FALSE(is_identity(perm));
            EXPECT_TRUE(st.is_cancelled()) << "Expected Cancelled status for test_stable=" << test_stable;
        }
    }

    // 3. ConstColumn + Int32Column in stable_sort_and_tie_columns (N = 512)
    {
        constexpr size_t kConstRows = 512;
        auto const_col = ConstColumn::create(int_column({42}), kConstRows);
        auto val_col = Int32Column::create();
        for (size_t i = 0; i < kConstRows; ++i) {
            val_col->append(static_cast<int32_t>((kConstRows - i) % 17));
        }
        Columns cols{const_col, val_col};
        SortDescs descs(std::vector<bool>{true, true}, std::vector<bool>{true, true});
        std::atomic<bool> cancel{false};
        SmallPermutation perm = create_small_permutation(kConstRows);
        ASSERT_OK(stable_sort_and_tie_columns(cancel, cols, descs, &perm));
        const auto data = val_col->immutable_data();
        for (size_t i = 1; i < kConstRows; ++i) {
            uint32_t prev_idx = perm[i - 1].index_in_chunk;
            uint32_t curr_idx = perm[i].index_in_chunk;
            ASSERT_LE(data[prev_idx], data[curr_idx]);
            if (data[prev_idx] == data[curr_idx]) {
                EXPECT_LT(prev_idx, curr_idx);
            }
        }
    }
}

TEST(SortCoreTest, StableSortNonIdentityInitialPermutationRestoresIndexOrder) {
    ThresholdRestoreGuard guard;
    std::atomic<bool> cancel{false};

    // 1. Single column with 512 identical keys and reversed initial SmallPermutation
    {
        constexpr size_t N = 512;
        auto col = Int64Column::create();
        for (size_t i = 0; i < N; ++i) {
            col->append(42);
        }
        Columns cols{col};
        SortDescs descs(std::vector<bool>{true}, std::vector<bool>{true});

        SmallPermutation perm_radix(N);
        SmallPermutation perm_pdq(N);
        for (size_t i = 0; i < N; ++i) {
            perm_radix[i].index_in_chunk = static_cast<uint32_t>(N - 1 - i);
            perm_pdq[i].index_in_chunk = static_cast<uint32_t>(N - 1 - i);
        }

        config::radix_sort_threshold = 256;
        ASSERT_OK(stable_sort_and_tie_columns(cancel, cols, descs, &perm_radix));

        config::radix_sort_threshold = 0;
        ASSERT_OK(stable_sort_and_tie_columns(cancel, cols, descs, &perm_pdq));

        EXPECT_EQ(perm_radix, perm_pdq);
        for (size_t i = 0; i < N; ++i) {
            EXPECT_EQ(perm_radix[i].index_in_chunk, static_cast<uint32_t>(i)) << "Mismatch at i=" << i;
        }
    }

    // 2. Two columns (Int32Column with 2 tie groups of 300 rows + Int64Column with 300 identical keys per group)
    //    and reversed initial SmallPermutation
    {
        constexpr size_t N = 600;
        auto col0 = Int32Column::create();
        auto col1 = Int64Column::create();
        for (size_t i = 0; i < N; ++i) {
            col0->append(static_cast<int32_t>(i % 2));
            col1->append(99);
        }
        Columns cols{col0, col1};
        SortDescs descs(std::vector<bool>{true, true}, std::vector<bool>{true, true});

        SmallPermutation perm_radix(N);
        SmallPermutation perm_pdq(N);
        for (size_t i = 0; i < N; ++i) {
            perm_radix[i].index_in_chunk = static_cast<uint32_t>(N - 1 - i);
            perm_pdq[i].index_in_chunk = static_cast<uint32_t>(N - 1 - i);
        }

        config::radix_sort_threshold = 256;
        ASSERT_OK(stable_sort_and_tie_columns(cancel, cols, descs, &perm_radix));

        config::radix_sort_threshold = 0;
        ASSERT_OK(stable_sort_and_tie_columns(cancel, cols, descs, &perm_pdq));

        EXPECT_EQ(perm_radix, perm_pdq);
        const auto d0 = col0->immutable_data();
        for (size_t i = 1; i < N; ++i) {
            uint32_t prev_idx = perm_radix[i - 1].index_in_chunk;
            uint32_t curr_idx = perm_radix[i].index_in_chunk;
            ASSERT_LE(d0[prev_idx], d0[curr_idx]);
            if (d0[prev_idx] == d0[curr_idx]) {
                EXPECT_LT(prev_idx, curr_idx) << "Tie index order violated at i=" << i;
            }
        }
    }

    // 3. Ranges overload of sort_and_tie_columns (IS_RANGES == true, range length >= 256)
    {
        constexpr size_t N = 600;
        auto col0 = Int32Column::create();
        for (size_t i = 0; i < N; ++i) {
            col0->append(static_cast<int32_t>((N - i) % 53));
        }
        std::vector<const Column*> raw_cols{col0.get()};
        SortDescs descs(std::vector<bool>{true}, std::vector<bool>{true});
        std::vector<uint32_t> offsets = {0, 300, 600};
        std::span<const uint32_t> src_offsets(offsets);
        std::vector<std::span<const uint32_t>> offsets_per_key{src_offsets};

        config::radix_sort_threshold = 256;
        SmallPermutation perm_ranges;
        ASSERT_OK(sort_and_tie_columns(cancel, raw_cols, descs, perm_ranges, src_offsets, offsets_per_key));
        ASSERT_EQ(perm_ranges.size(), N);
        const auto d0 = col0->immutable_data();
        for (size_t r = 0; r < 2; ++r) {
            size_t start = offsets[r];
            size_t end = offsets[r + 1];
            for (size_t i = start + 1; i < end; ++i) {
                EXPECT_LE(d0[perm_ranges[i - 1].index_in_chunk], d0[perm_ranges[i].index_in_chunk]);
                EXPECT_GE(perm_ranges[i].index_in_chunk, start);
                EXPECT_LT(perm_ranges[i].index_in_chunk, end);
            }
        }
    }
}

struct CountingTrivialValue {
    int32_t v;
    CountingTrivialValue() noexcept : v(12345) { ++default_ctor_calls; }
    explicit CountingTrivialValue(int32_t x) noexcept : v(x) {}
    bool operator==(const CountingTrivialValue& rhs) const noexcept { return v == rhs.v; }
    static inline size_t default_ctor_calls = 0;
};
static_assert(std::is_trivially_copyable_v<InlinePermuteItem<CountingTrivialValue>>);
static_assert(std::is_trivially_destructible_v<InlinePermuteItem<CountingTrivialValue>>);

TEST(SortCoreTest, CreateInlinePermutationSkipsZeroFillOnlyWhenUncheckedAndTrivial) {
    // 1. Trait verification: only !CheckBound with trivially default-constructible & trivially destructible
    //    InlinePermuteItem<T> uses uninitialized resize (RawAllocator::construct `::new (ptr) U;`).
    static_assert(can_resize_inline_permutation_uninitialized<int32_t, false>);
    static_assert(can_resize_inline_permutation_uninitialized<int64_t, false>);
    static_assert(can_resize_inline_permutation_uninitialized<double, false>);
    static_assert(can_resize_inline_permutation_uninitialized<DateValue, false>);
    static_assert(can_resize_inline_permutation_uninitialized<TimestampValue, false>);
    static_assert(!can_resize_inline_permutation_uninitialized<int32_t, true>);
    static_assert(!can_resize_inline_permutation_uninitialized<int64_t, true>);
    static_assert(!can_resize_inline_permutation_uninitialized<Slice, false>);
    static_assert(!can_resize_inline_permutation_uninitialized<GermanString, false>);
    static_assert(!can_resize_inline_permutation_uninitialized<CountingTrivialValue, false>);

    EXPECT_TRUE((can_resize_inline_permutation_uninitialized<int64_t, false>));
    EXPECT_FALSE((can_resize_inline_permutation_uninitialized<int64_t, true>));
    EXPECT_FALSE((can_resize_inline_permutation_uninitialized<Slice, false>));

    // 2. Empty permutation (rows == 0) edge case
    {
        SmallPermutation empty_small = create_small_permutation(0);
        EXPECT_TRUE(empty_small.empty());
        std::vector<int64_t> empty_data;
        auto empty_inlined = create_inline_permutation<int64_t, false>(empty_small, empty_data);
        EXPECT_TRUE(empty_inlined.empty());
    }

    // 3. CheckBound == false populates all slots accurately for trivial types
    {
        constexpr uint32_t kRows = 64;
        SmallPermutation small = create_small_permutation(kRows);
        std::vector<int64_t> container(kRows);
        for (uint32_t i = 0; i < kRows; ++i) {
            container[i] = static_cast<int64_t>(i * 10) - 320;
        }

        auto inlined = create_inline_permutation<int64_t, false>(small, container);
        ASSERT_EQ(inlined.size(), kRows);
        for (uint32_t i = 0; i < kRows; ++i) {
            EXPECT_EQ(inlined[i].index_in_chunk, i);
            EXPECT_EQ(inlined[i].inline_value, static_cast<int64_t>(i * 10) - 320);
        }
    }

    // 4. CheckBound == true value-initializes all slots so out-of-bounds skipped indices
    //    retain deterministic default values (0 for int64_t, 12345 for CountingTrivialValue)
    {
        constexpr uint32_t kRows = 16;
        SmallPermutation small = create_small_permutation(kRows);
        small[5].index_in_chunk = 999;  // out-of-bounds
        small[15].index_in_chunk = 888; // out-of-bounds
        std::vector<int64_t> int_container(kRows);
        std::vector<CountingTrivialValue> custom_container;
        custom_container.reserve(kRows);
        for (uint32_t i = 0; i < kRows; ++i) {
            int_container[i] = static_cast<int64_t>(i + 100);
            custom_container.emplace_back(static_cast<int32_t>(i + 1));
        }

        auto inlined_int = create_inline_permutation<int64_t, true>(small, int_container);
        ASSERT_EQ(inlined_int.size(), kRows);
        for (uint32_t i = 0; i < kRows; ++i) {
            if (i == 5 || i == 15) {
                EXPECT_EQ(inlined_int[i].inline_value, 0);
            } else {
                EXPECT_EQ(inlined_int[i].inline_value, static_cast<int64_t>(i + 100));
            }
        }

        CountingTrivialValue::default_ctor_calls = 0;
        auto inlined_custom = create_inline_permutation<CountingTrivialValue, true>(small, custom_container);
        EXPECT_EQ(CountingTrivialValue::default_ctor_calls, kRows);
        ASSERT_EQ(inlined_custom.size(), kRows);
        for (uint32_t i = 0; i < kRows; ++i) {
            if (i == 5 || i == 15) {
                EXPECT_EQ(inlined_custom[i].inline_value.v, 12345);
            } else {
                EXPECT_EQ(inlined_custom[i].inline_value.v, static_cast<int32_t>(i + 1));
            }
        }
    }
}

} // namespace
} // namespace starrocks
