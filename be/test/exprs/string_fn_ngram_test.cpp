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

#include "column/column_helper.h"
#include "exprs/function_context.h"
#include "exprs/string_functions.h"

namespace starrocks {

class NgramFunctionNonConstNeedleTest : public ::testing::Test {};

// Build a ConstColumn<TYPE_INT> with value v and logical size n (for gram_num param).
static ColumnPtr make_gram_num_col(int v, size_t n) {
    auto inner = Int32Column::create();
    inner->append(v);
    return ConstColumn::create(std::move(inner), n);
}

// Both haystack and needle are plain non-nullable vectors.  Result must be
// in [0,1] and equal the expected 2/3 similarity for this input pair.
//
// needle = "abcdef" (grams with n=4: "abcd","bcde","cdef")
// haystack = "abcde12"  (grams: "abcd","bcde","cde1","de12")
// 2 of 3 needle grams appear in haystack → similarity = 2/3.
TEST_F(NgramFunctionNonConstNeedleTest, NonConstNeedle_vector_both) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    const size_t N = 5;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    for (size_t i = 0; i < N; i++) {
        haystack->append("abcde12");
        needle->append("abcdef");
    }
    Columns cols;
    cols.emplace_back(std::move(haystack));
    cols.emplace_back(std::move(needle));
    cols.emplace_back(make_gram_num_col(4, N));

    ColumnPtr result = StringFunctions::ngram_search(ctx.get(), cols).value();
    ASSERT_EQ(N, result->size());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);
    float expected = 1.0f - 1.0f / 3.0f;
    for (size_t i = 0; i < N; i++) {
        double sim = v->get_data()[i];
        EXPECT_GE(sim, 0.0);
        EXPECT_LE(sim, 1.0);
        EXPECT_NEAR(sim, expected, 1e-5f);
    }
}

// Result must match manually computed expected value of 2/3 across multiple rows.
TEST_F(NgramFunctionNonConstNeedleTest, NonConstNeedle_result_matches_const_needle) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    const size_t N = 3;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    for (size_t i = 0; i < N; i++) {
        haystack->append("abcde12");
        needle->append("abcdef");
    }
    Columns cols;
    cols.emplace_back(std::move(haystack));
    cols.emplace_back(std::move(needle));
    cols.emplace_back(make_gram_num_col(4, N));

    ColumnPtr result = StringFunctions::ngram_search(ctx.get(), cols).value();
    ASSERT_EQ(N, result->size());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);
    float expected = 1.0f - 1.0f / 3.0f;
    for (size_t i = 0; i < N; i++) {
        EXPECT_NEAR(v->get_data()[i], expected, 1e-5f);
    }
}

// When needle column is nullable the result must be nullable and null positions
// must align with the needle's null mask.
TEST_F(NgramFunctionNonConstNeedleTest, NonConstNeedle_nullable_needle) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    const size_t N = 6;
    auto haystack = BinaryColumn::create();
    auto needle_data = BinaryColumn::create();
    auto needle_null = NullColumn::create();
    for (size_t i = 0; i < N; i++) {
        haystack->append("abcde12");
        needle_data->append("abcdef");
        needle_null->append(i % 2 == 1 ? 1 : 0); // odd rows are null
    }
    Columns cols;
    cols.emplace_back(std::move(haystack));
    cols.emplace_back(NullableColumn::create(std::move(needle_data), std::move(needle_null)));
    cols.emplace_back(make_gram_num_col(4, N));

    ColumnPtr result = StringFunctions::ngram_search(ctx.get(), cols).value();
    ASSERT_EQ(N, result->size());
    ASSERT_TRUE(result->is_nullable());

    auto* nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
    auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(nullable->data_column());
    float expected = 1.0f - 1.0f / 3.0f;
    for (size_t i = 0; i < N; i++) {
        if (i % 2 == 1) {
            EXPECT_TRUE(result->is_null(i)) << "row " << i << " should be null";
        } else {
            EXPECT_FALSE(result->is_null(i)) << "row " << i << " should not be null";
            EXPECT_NEAR(v->get_data()[i], expected, 1e-5f);
        }
    }
}

// Case-insensitive: "ABCDEF" and "abcdef" needles must yield the same
// similarity against a lowercase haystack.
TEST_F(NgramFunctionNonConstNeedleTest, NonConstNeedle_case_insensitive) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    const size_t N = 4;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    for (size_t i = 0; i < N; i++) {
        haystack->append("abcde12");
        needle->append(i % 2 == 0 ? "ABCDEF" : "abcdef");
    }
    Columns cols;
    cols.emplace_back(std::move(haystack));
    cols.emplace_back(std::move(needle));
    cols.emplace_back(make_gram_num_col(4, N));

    ColumnPtr result = StringFunctions::ngram_search_case_insensitive(ctx.get(), cols).value();
    ASSERT_EQ(N, result->size());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);
    float expected = 1.0f - 1.0f / 3.0f;
    for (size_t i = 0; i < N; i++) {
        EXPECT_NEAR(v->get_data()[i], expected, 1e-5f);
    }
}

// When needle length < gram_num no grams can be built → similarity = 0 every row.
TEST_F(NgramFunctionNonConstNeedleTest, NonConstNeedle_needle_too_small) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    const size_t N = 4;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    for (size_t i = 0; i < N; i++) {
        haystack->append("abcde12");
        needle->append("ab"); // length 2 < gram_num 4
    }
    Columns cols;
    cols.emplace_back(std::move(haystack));
    cols.emplace_back(std::move(needle));
    cols.emplace_back(make_gram_num_col(4, N));

    ColumnPtr result = StringFunctions::ngram_search(ctx.get(), cols).value();
    ASSERT_EQ(N, result->size());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);
    for (size_t i = 0; i < N; i++) {
        EXPECT_DOUBLE_EQ(0.0, v->get_data()[i]);
    }
}

// The row_map is recovered between rows that share the same needle.  Verify
// that the result is identical across all rows even though each row dirtied
// the map via the distance calculation.
TEST_F(NgramFunctionNonConstNeedleTest, NonConstNeedle_map_reuse_correctness) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    const size_t N = 10;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    for (size_t i = 0; i < N; i++) {
        haystack->append("abcde12");
        needle->append("abcdef");
    }
    Columns cols;
    cols.emplace_back(std::move(haystack));
    cols.emplace_back(std::move(needle));
    cols.emplace_back(make_gram_num_col(4, N));

    ColumnPtr result = StringFunctions::ngram_search(ctx.get(), cols).value();
    ASSERT_EQ(N, result->size());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);
    float expected = 1.0f - 1.0f / 3.0f;
    for (size_t i = 0; i < N; i++) {
        EXPECT_NEAR(v->get_data()[i], expected, 1e-5f) << "map reuse broken at row " << i;
    }
}

// valid needle → too-small needle → valid needle again.
// Verifies that the map is correctly cleared when needle shrinks below
// gram_num and correctly rebuilt when a valid needle follows.
TEST_F(NgramFunctionNonConstNeedleTest, NonConstNeedle_valid_toosmall_valid_sequence) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    haystack->append("abcde12"); // row 0: valid needle
    needle->append("abcdef");
    haystack->append("abcde12"); // row 1: needle too small
    needle->append("ab");
    haystack->append("abcde12"); // row 2: valid needle again
    needle->append("abcdef");

    Columns cols;
    cols.emplace_back(std::move(haystack));
    cols.emplace_back(std::move(needle));
    cols.emplace_back(make_gram_num_col(4, 3));

    ColumnPtr result = StringFunctions::ngram_search(ctx.get(), cols).value();
    ASSERT_EQ(3u, result->size());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);
    float expected_valid = 1.0f - 1.0f / 3.0f;
    EXPECT_NEAR(v->get_data()[0], expected_valid, 1e-5f);
    EXPECT_DOUBLE_EQ(0.0, v->get_data()[1]);
    EXPECT_NEAR(v->get_data()[2], expected_valid, 1e-5f);
}

// Both haystack and needle are nullable.  Result null mask must be the OR
// of both input null masks.
TEST_F(NgramFunctionNonConstNeedleTest, NonConstNeedle_both_nullable) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    const size_t N = 6;
    auto hs_data = BinaryColumn::create();
    auto hs_null = NullColumn::create();
    auto nd_data = BinaryColumn::create();
    auto nd_null = NullColumn::create();
    for (size_t i = 0; i < N; i++) {
        hs_data->append("abcde12");
        hs_null->append(i % 3 == 0 ? 1 : 0); // rows 0,3 null
        nd_data->append("abcdef");
        nd_null->append(i % 3 == 1 ? 1 : 0); // rows 1,4 null
    }
    Columns cols;
    cols.emplace_back(NullableColumn::create(std::move(hs_data), std::move(hs_null)));
    cols.emplace_back(NullableColumn::create(std::move(nd_data), std::move(nd_null)));
    cols.emplace_back(make_gram_num_col(4, N));

    ColumnPtr result = StringFunctions::ngram_search(ctx.get(), cols).value();
    ASSERT_EQ(N, result->size());
    ASSERT_TRUE(result->is_nullable());

    for (size_t i = 0; i < N; i++) {
        bool hs_is_null = (i % 3 == 0);
        bool nd_is_null = (i % 3 == 1);
        if (hs_is_null || nd_is_null) {
            EXPECT_TRUE(result->is_null(i)) << "row " << i << " should be null";
        } else {
            EXPECT_FALSE(result->is_null(i)) << "row " << i << " should not be null";
        }
    }
}

} // namespace starrocks
