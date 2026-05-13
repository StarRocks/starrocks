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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "exprs/function_context.h"
#include "exprs/string_functions.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class NgramSearchTest : public ::testing::Test {
protected:
    std::unique_ptr<FunctionContext> make_ctx(bool utf8_enabled, std::unique_ptr<RuntimeState>* state_out) {
        TQueryOptions query_options;
        query_options.__set_ngram_search_support_utf8(utf8_enabled);
        TQueryGlobals query_globals;
        *state_out = std::make_unique<RuntimeState>(TUniqueId(), query_options, query_globals, nullptr);
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ctx->set_runtime_state(state_out->get());
        return ctx;
    }

    static Columns build_columns(const std::string& haystack, const std::string& needle, int gram_num) {
        return {ColumnHelper::create_const_column<TYPE_VARCHAR>(haystack, 1),
                ColumnHelper::create_const_column<TYPE_VARCHAR>(needle, 1),
                ColumnHelper::create_const_column<TYPE_INT>(gram_num, 1)};
    }

    enum Mode { CASE_SENSITIVE, CASE_INSENSITIVE };

    static double eval(FunctionContext* ctx, const Columns& columns, Mode mode) {
        ctx->set_constant_columns(columns);
        ColumnPtr result;
        if (mode == CASE_INSENSITIVE) {
            EXPECT_OK(StringFunctions::ngram_search_case_insensitive_prepare(ctx, FunctionContext::FRAGMENT_LOCAL));
            auto r = StringFunctions::ngram_search_case_insensitive(ctx, columns);
            EXPECT_TRUE(r.ok());
            result = r.value();
        } else {
            EXPECT_OK(StringFunctions::ngram_search_prepare(ctx, FunctionContext::FRAGMENT_LOCAL));
            auto r = StringFunctions::ngram_search(ctx, columns);
            EXPECT_TRUE(r.ok());
            result = r.value();
        }
        double v = ColumnHelper::get_const_value<TYPE_DOUBLE>(result);
        EXPECT_OK(StringFunctions::ngram_search_close(ctx, FunctionContext::FRAGMENT_LOCAL));
        return v;
    }
};

// ---------------------------------------------------------------------------
// Matrix invariant: ASCII input must yield the same result regardless of the
// ngram_search_support_utf8 toggle. The UTF-8 mode only adds new behavior for
// non-ASCII input; pure ASCII paths must remain bit-identical.
// ---------------------------------------------------------------------------

TEST_F(NgramSearchTest, AsciiSensitive_ToggleInvariant) {
    auto columns_on = build_columns("hello world", "hello", 3);
    auto columns_off = build_columns("hello world", "hello", 3);

    std::unique_ptr<RuntimeState> s_on;
    std::unique_ptr<RuntimeState> s_off;
    auto ctx_on = make_ctx(true, &s_on);
    auto ctx_off = make_ctx(false, &s_off);

    double v_on = eval(ctx_on.get(), columns_on, CASE_SENSITIVE);
    double v_off = eval(ctx_off.get(), columns_off, CASE_SENSITIVE);
    EXPECT_DOUBLE_EQ(v_on, v_off);
    EXPECT_NEAR(v_on, 1.0, 0.05) << "Substring 'hello' fully covered by 'hello world' ngrams";
}

TEST_F(NgramSearchTest, AsciiInsensitive_ToggleInvariant) {
    auto columns_on = build_columns("HELLO WORLD", "hello", 3);
    auto columns_off = build_columns("HELLO WORLD", "hello", 3);

    std::unique_ptr<RuntimeState> s_on;
    std::unique_ptr<RuntimeState> s_off;
    auto ctx_on = make_ctx(true, &s_on);
    auto ctx_off = make_ctx(false, &s_off);

    double v_on = eval(ctx_on.get(), columns_on, CASE_INSENSITIVE);
    double v_off = eval(ctx_off.get(), columns_off, CASE_INSENSITIVE);
    EXPECT_DOUBLE_EQ(v_on, v_off);
    EXPECT_NEAR(v_on, 1.0, 0.05) << "ASCII case-insensitive match regardless of mode";
}

// ---------------------------------------------------------------------------
// utf8=ON: the new behavior. Non-ASCII characters are treated as units,
// ICU-based lowering produces matching ngrams across case-different strings.
// ---------------------------------------------------------------------------

TEST_F(NgramSearchTest, Utf8On_CyrillicInsensitive_Match) {
    std::unique_ptr<RuntimeState> state;
    auto ctx = make_ctx(true, &state);
    auto columns = build_columns("ПРИВЕТ", "привет", 3);

    double v = eval(ctx.get(), columns, CASE_INSENSITIVE);
    EXPECT_NEAR(v, 1.0, 0.05) << "ICU lowering aligns Cyrillic case variants";
}

TEST_F(NgramSearchTest, Utf8On_CyrillicSensitive_SubstringMatch) {
    std::unique_ptr<RuntimeState> state;
    auto ctx = make_ctx(true, &state);
    auto columns = build_columns("Привет мир", "мир", 3);

    double v = eval(ctx.get(), columns, CASE_SENSITIVE);
    EXPECT_GT(v, 0.5) << "UTF-8 ngrams find needle as a character substring";
}

TEST_F(NgramSearchTest, Utf8On_GermanSharpS_Insensitive) {
    // ICU lowering folds U+1E9E (ẞ) and "SS" toward "ß"/"ss" in a way the byte
    // path cannot. Compare uppercase vs lowercase variants of "Straße".
    std::unique_ptr<RuntimeState> state;
    auto ctx = make_ctx(true, &state);
    auto columns = build_columns("STRASSE", "strasse", 3);

    double v = eval(ctx.get(), columns, CASE_INSENSITIVE);
    EXPECT_NEAR(v, 1.0, 0.05);
}

TEST_F(NgramSearchTest, Utf8On_TurkishCapitalI_ExpandingLowercase_Insensitive) {
    // Regression: ICU lowers U+0130 ("İ", 1 codepoint) into "i" + U+0307 combining
    // dot (2 codepoints). A pre-lower length check would see 1 < gram_num=2 and
    // wrongly return 0; needle_gram_count from prepare is computed post-lower
    // and yields the valid 2-gram, so the match must be found.
    std::unique_ptr<RuntimeState> state;
    auto ctx = make_ctx(true, &state);
    auto columns = build_columns("i\xCC\x87stanbul", "\xC4\xB0", 2);

    double v = eval(ctx.get(), columns, CASE_INSENSITIVE);
    EXPECT_NEAR(v, 1.0, 0.05) << "Expanding ICU lowercase must not be cut off by pre-lower length guard";
}

// ---------------------------------------------------------------------------
// utf8=OFF: the legacy byte-oriented behavior. We pin it here as a regression
// guard - it stays broken for case-insensitive Cyrillic, intentionally, so the
// feature must be opted into. ASCII keeps working.
// ---------------------------------------------------------------------------

TEST_F(NgramSearchTest, Utf8Off_CyrillicInsensitive_LegacyBroken) {
    std::unique_ptr<RuntimeState> state;
    auto ctx = make_ctx(false, &state);
    auto columns = build_columns("ПРИВЕТ", "привет", 3);

    double v = eval(ctx.get(), columns, CASE_INSENSITIVE);
    // Byte-wise std::tolower leaves Cyrillic bytes unchanged, so uppercase and
    // lowercase Cyrillic produce disjoint byte-ngrams and similarity is ~0.
    EXPECT_LT(v, 0.5) << "Legacy ASCII tolower cannot fold Cyrillic case";
}

TEST_F(NgramSearchTest, Utf8Off_AsciiSensitive_StillWorks) {
    std::unique_ptr<RuntimeState> state;
    auto ctx = make_ctx(false, &state);
    auto columns = build_columns("hello world", "hello", 3);

    double v = eval(ctx.get(), columns, CASE_SENSITIVE);
    EXPECT_NEAR(v, 1.0, 0.05);
}

// ---------------------------------------------------------------------------
// Edge cases: empty / oversized needle, non-const haystack vector path.
// ---------------------------------------------------------------------------

TEST_F(NgramSearchTest, Utf8On_EmptyNeedle_Zero) {
    std::unique_ptr<RuntimeState> state;
    auto ctx = make_ctx(true, &state);
    auto columns = build_columns("Привет", "", 3);

    double v = eval(ctx.get(), columns, CASE_SENSITIVE);
    EXPECT_DOUBLE_EQ(v, 0.0);
}

TEST_F(NgramSearchTest, Utf8On_HaystackVector_CyrillicInsensitive) {
    std::unique_ptr<RuntimeState> state;
    auto ctx = make_ctx(true, &state);

    auto haystack_col = BinaryColumn::create();
    haystack_col->append("ПРИВЕТ");
    haystack_col->append("МИР");
    haystack_col->append("hello");
    Columns columns{haystack_col, ColumnHelper::create_const_column<TYPE_VARCHAR>("привет", 3),
                    ColumnHelper::create_const_column<TYPE_INT>(3, 3)};
    ctx->set_constant_columns(columns);

    ASSERT_OK(StringFunctions::ngram_search_case_insensitive_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
    auto result = StringFunctions::ngram_search_case_insensitive(ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result.value());
    EXPECT_NEAR(v->get_data()[0], 1.0, 0.05) << "ПРИВЕТ matches привет";
    EXPECT_LT(v->get_data()[1], 0.5) << "МИР does not match привет";
    EXPECT_LT(v->get_data()[2], 0.5) << "hello does not match привет";
    ASSERT_OK(StringFunctions::ngram_search_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
}

} // namespace starrocks
