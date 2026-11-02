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

#include <base/testutil/assert.h>
#include <gtest/gtest.h>
#include <hs/hs.h>

#include <memory>
#include <random>
#include <vector>

#include "base/utility/defer_op.h"
#include "bench/bench_util.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/string_functions.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

class StringFunctionRegexpReplaceTest : public ::testing::Test {
public:
    void init(size_t ratio) {
        _columns.clear();
        _ratio = ratio;
        auto column = BenchUtil::create_random_string_column(_num_rows, 20);
        auto binary = down_cast<BinaryColumn*>(column.get());
        Bytes& data = binary->get_bytes();
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int> dist(0, 99);
        for (size_t i = 0; i < data.size(); i++) {
            int random_number = dist(rng);
            if (random_number < 100 / _ratio) {
                data[i] = '-';
            }
        }
        _columns.push_back(std::move(column));
        MutableColumnPtr pattern_data = ColumnHelper::create_column(type_desc, false);
        pattern_data->append_datum(Datum(Slice("-")));
        auto pattern_column = ConstColumn::create(std::move(pattern_data), _num_rows);
        _columns.push_back(std::move(pattern_column));
        MutableColumnPtr rpl_data = ColumnHelper::create_column(type_desc, false);
        rpl_data->append_datum(Datum(Slice("")));
        auto rpl_column = ConstColumn::create(std::move(rpl_data), _num_rows);
        _columns.push_back(std::move(rpl_column));
        _state = std::make_shared<StringFunctionsState>();
        _state->options = std::make_unique<re2::RE2::Options>();
        _state->options->set_log_errors(false);
        _state->options->set_longest_match(true);
        _state->options->set_dot_nl(true);
        _state->const_pattern = true;
        std::string pattern = "-";
        _state->pattern = pattern;
        _state->use_hyperscan = true;
        _state->use_hyperscan_vec = true;
        _state->size_of_pattern = int(pattern.size());
        if (hs_compile(pattern.c_str(), HS_FLAG_ALLOWEMPTY | HS_FLAG_DOTALL | HS_FLAG_UTF8 | HS_FLAG_SOM_LEFTMOST,
                       HS_MODE_BLOCK, nullptr, &_state->database, &_state->compile_err) != HS_SUCCESS) {
            std::stringstream error;
            error << "Invalid regex expression: "
                  << "-"
                  << ": " << _state->compile_err->message;
            hs_free_compile_error(_state->compile_err);
            std::cout << error.str();
            return;
        }

        if (hs_alloc_scratch(_state->database, &_state->scratch) != HS_SUCCESS) {
            std::stringstream error;
            error << "ERROR: Unable to allocate scratch space.";
            hs_free_database(_state->database);
            std::cout << error.str();
            return;
        }
    }

private:
    const TypeDescriptor type_desc = TypeDescriptor(TYPE_VARCHAR);
    size_t _ratio = 0;
    Columns _columns{};
    std::shared_ptr<StringFunctionsState> _state;
    std::string _rpl_value = "";
    size_t _num_rows = 4096;
};

TEST_F(StringFunctionRegexpReplaceTest, testHyperscanVec) {
    std::vector<size_t> ratio_cases = {5, 10, 20, 50, 100};
    for (size_t r : ratio_cases) {
        init(r);
        auto r_vec = StringFunctions::regexp_replace_use_hyperscan_vec(_state.get(), _columns);
        ASSERT_TRUE(r_vec.ok());
        auto r_ori = StringFunctions::regexp_replace_use_hyperscan(_state.get(), _columns);
        ASSERT_TRUE(r_ori.ok());
        auto ori = r_ori.value();
        auto vec = r_vec.value();
        for (size_t i = 0; i < _num_rows; i++) {
            ASSERT_EQ(vec->debug_item(i), ori->debug_item(i));
        }
    }
}

TEST_F(StringFunctionRegexpReplaceTest, testMultipleRowsWithPackagePattern) {
    // Test case for the reported issue with '_package_.*' pattern
    const TypeDescriptor type_desc = TypeDescriptor(TYPE_VARCHAR);
    Columns columns;

    // Create input column with two rows
    auto str_column = ColumnTestHelper::build_column<Slice>({"activity_60_package_2", "activity_50_package_7"});
    columns.push_back(std::move(str_column));

    // Create pattern column (constant)
    MutableColumnPtr pattern_data = ColumnHelper::create_column(type_desc, false);
    pattern_data->append_datum(Datum(Slice("_package_.*")));
    auto pattern_column = ConstColumn::create(std::move(pattern_data), 2);
    columns.push_back(std::move(pattern_column));

    // Create replacement column (constant empty string)
    MutableColumnPtr rpl_data = ColumnHelper::create_column(type_desc, false);
    rpl_data->append_datum(Datum(Slice("")));
    auto rpl_column = ConstColumn::create(std::move(rpl_data), 2);
    columns.push_back(std::move(rpl_column));

    // Setup state with hyperscan
    auto state = std::make_shared<StringFunctionsState>();
    state->options = std::make_unique<re2::RE2::Options>();
    state->options->set_log_errors(false);
    state->options->set_longest_match(true);
    state->options->set_dot_nl(true);
    state->const_pattern = true;
    std::string pattern = "_package_.*";
    state->pattern = pattern;
    state->use_hyperscan = false;
    state->global_mode = true; // Pattern doesn't start with ^ or end with $
    state->size_of_pattern = int(pattern.size());

    if (hs_compile(pattern.c_str(), HS_FLAG_ALLOWEMPTY | HS_FLAG_DOTALL | HS_FLAG_UTF8 | HS_FLAG_SOM_LEFTMOST,
                   HS_MODE_BLOCK, nullptr, &state->database, &state->compile_err) != HS_SUCCESS) {
        std::stringstream error;
        error << "Invalid regex expression: " << pattern << ": " << state->compile_err->message;
        hs_free_compile_error(state->compile_err);
        LOG(FATAL) << error.str();
    }

    if (hs_alloc_scratch(state->database, &state->scratch) != HS_SUCCESS) {
        hs_free_database(state->database);
        LOG(FATAL) << "ERROR: Unable to allocate scratch space.";
    }

    // Ensure cleanup happens even if assertions fail
    DeferOp cleanup([&] {
        if (state->scratch != nullptr) {
            hs_free_scratch(state->scratch);
            state->scratch = nullptr;
        }
        if (state->database != nullptr) {
            hs_free_database(state->database);
            state->database = nullptr;
        }
    });

    // Test non-vectorized version (expected to be correct)
    auto r_ori = StringFunctions::regexp_replace_use_hyperscan(state.get(), columns);
    ASSERT_TRUE(r_ori.ok());
    auto ori_result = r_ori.value();

    // Both should produce "activity_60" and "activity_50"
    ASSERT_EQ(ori_result->debug_item(0), "'activity_60'");
    ASSERT_EQ(ori_result->debug_item(1), "'activity_50'");
}

TEST_F(StringFunctionRegexpReplaceTest, testRegexpReplaceTemplateParse) {
    RegexpReplaceTemplate tmpl;

    // 1. Empty replacement
    tmpl.parse(Slice(""));
    ASSERT_EQ(tmpl.nsubmatch, 1);
    ASSERT_EQ(tmpl.max_submatch, 0);
    ASSERT_FALSE(tmpl.has_invalid_escape);
    ASSERT_TRUE(tmpl.segments.empty());

    // 2. Single capture group "\1" (ClickBench Q28)
    tmpl.parse(Slice("\\1"));
    ASSERT_EQ(tmpl.nsubmatch, 2);
    ASSERT_EQ(tmpl.max_submatch, 1);
    ASSERT_FALSE(tmpl.has_invalid_escape);
    ASSERT_EQ(tmpl.segments.size(), 1);
    ASSERT_EQ(tmpl.segments[0].group_idx, 1);

    // 3. Coalesced escaped backslash and multi-digit "\10" -> group 1 + literal "0\\end"
    tmpl.parse(Slice("pre\\\\mid\\10\\\\end\\0"));
    ASSERT_EQ(tmpl.nsubmatch, 2);
    ASSERT_EQ(tmpl.max_submatch, 1);
    ASSERT_FALSE(tmpl.has_invalid_escape);
    ASSERT_EQ(tmpl.segments.size(), 4);
    ASSERT_EQ(tmpl.segments[0].group_idx, -1);
    ASSERT_EQ(tmpl.literal_bytes.substr(tmpl.segments[0].offset, tmpl.segments[0].length), "pre\\mid");
    ASSERT_EQ(tmpl.segments[1].group_idx, 1);
    ASSERT_EQ(tmpl.segments[2].group_idx, -1);
    ASSERT_EQ(tmpl.literal_bytes.substr(tmpl.segments[2].offset, tmpl.segments[2].length), "0\\end");
    ASSERT_EQ(tmpl.segments[3].group_idx, 0);
    {
        re2::StringPiece vec[2] = {re2::StringPiece("WHOLE_MATCH", 11), re2::StringPiece("GRP1", 4)};
        ASSERT_EQ(tmpl.rendered_size(vec), 12 + 4 + 11);
        re2::StringPiece vec_unmatched_opt[2] = {re2::StringPiece("WHOLE", 5), re2::StringPiece()};
        ASSERT_EQ(tmpl.rendered_size(vec_unmatched_opt), 12 + 0 + 5);
    }

    // 4. Invalid escape followed by higher capture group ("prefix\x\2"):
    // segments stop at "\x", but max_submatch still records group 2 like RE2::MaxSubmatch.
    tmpl.parse(Slice("prefix\\x\\2"));
    ASSERT_TRUE(tmpl.has_invalid_escape);
    ASSERT_EQ(tmpl.max_submatch, 2);
    ASSERT_EQ(tmpl.nsubmatch, 3);
    ASSERT_EQ(tmpl.segments.size(), 1);
    ASSERT_EQ(tmpl.segments[0].group_idx, -1);
    ASSERT_EQ(tmpl.literal_bytes.substr(tmpl.segments[0].offset, tmpl.segments[0].length), "prefix");

    re2::RE2 one_group_re("(a)");
    re2::RE2 two_group_re("(a)(b)");
    ASSERT_TRUE(tmpl.exceeds_groups(one_group_re));
    ASSERT_FALSE(tmpl.exceeds_groups(two_group_re));

    // 5. Trailing backslash ("abc\")
    tmpl.parse(Slice("abc\\"));
    ASSERT_TRUE(tmpl.has_invalid_escape);
    ASSERT_EQ(tmpl.max_submatch, 0);
    ASSERT_EQ(tmpl.nsubmatch, 1);
    ASSERT_EQ(tmpl.segments.size(), 1);
    ASSERT_EQ(tmpl.literal_bytes.substr(tmpl.segments[0].offset, tmpl.segments[0].length), "abc");
}

namespace {

StatusOr<ColumnPtr> eval_regexp_replace(const Columns& columns) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto* context = ctx.get();
    context->set_constant_columns(columns);
    RETURN_IF_ERROR(
            StringFunctions::regexp_replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
    DeferOp close_op(
            [&] { (void)StringFunctions::regexp_close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL); });
    return StringFunctions::regexp_replace(context, columns);
}

std::string re2_reference_replace(const std::string& input, const re2::RE2& re, const std::string& rpl,
                                  bool global_mode) {
    re2::StringPiece rpl_sp(rpl.data(), rpl.size());
    std::string out(input.data(), input.size());
    if (global_mode) {
        re2::RE2::GlobalReplace(&out, re, rpl_sp);
    } else {
        re2::RE2::Replace(&out, re, rpl_sp);
    }
    return out;
}

} // namespace

TEST_F(StringFunctionRegexpReplaceTest, testZeroCopyPassthroughOnNoMatch) {
    // 1. 0 rows match on BinaryColumn -> returns exact same ColumnPtr
    {
        ColumnPtr str_col =
                ColumnTestHelper::build_column<Slice>({"not_a_url", "ftp://example.com/a", "plain_text_row"});
        ColumnPtr ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("^https?://([^/]+)/.*$", str_col->size());
        ColumnPtr rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("\\1", str_col->size());
        Columns columns = {str_col, ptn_col, rpl_col};

        auto res = eval_regexp_replace(columns);
        ASSERT_TRUE(res.ok()) << res.status();
        ASSERT_EQ(res.value().get(), columns[0].get());
    }

    // 2. 0 rows match on NullableColumn with nulls -> returns exact same ColumnPtr
    {
        auto data_col = BinaryColumn::create();
        auto null_col = NullColumn::create();
        data_col->append("not_a_url");
        null_col->append(0);
        data_col->append("");
        null_col->append(1);
        data_col->append("still_not_a_url");
        null_col->append(0);
        ColumnPtr nullable_str = NullableColumn::create(std::move(data_col), std::move(null_col));
        ColumnPtr ptn_col =
                ColumnHelper::create_const_column<TYPE_VARCHAR>("^https?://([^/]+)/.*$", nullable_str->size());
        ColumnPtr rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("\\1", nullable_str->size());
        Columns columns = {nullable_str, ptn_col, rpl_col};

        auto res = eval_regexp_replace(columns);
        ASSERT_TRUE(res.ok()) << res.status();
        ASSERT_EQ(res.value().get(), columns[0].get());
    }

    // 3. Out-of-range group (\2 on a 1-group pattern) -> returns exact same ColumnPtr without matching
    {
        ColumnPtr str_col = ColumnTestHelper::build_column<Slice>(
                {"https://www.starrocks.io/docs", "http://example.com/index.html"});
        ColumnPtr ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("^https?://([^/]+)/.*$", str_col->size());
        ColumnPtr rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("\\2", str_col->size());
        Columns columns = {str_col, ptn_col, rpl_col};

        auto res = eval_regexp_replace(columns);
        ASSERT_TRUE(res.ok()) << res.status();
        ASSERT_EQ(res.value().get(), columns[0].get());
    }

    // 4. Invalid escape in !global_mode (^a with replacement "x\y") -> returns exact same ColumnPtr
    {
        ColumnPtr str_col = ColumnTestHelper::build_column<Slice>({"abcdef", "a12345"});
        ColumnPtr ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("^a", str_col->size());
        ColumnPtr rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("x\\y", str_col->size());
        Columns columns = {str_col, ptn_col, rpl_col};

        auto res = eval_regexp_replace(columns);
        ASSERT_TRUE(res.ok()) << res.status();
        ASSERT_EQ(res.value().get(), columns[0].get());
    }

    // 5. NullableColumn with 0 nulls (!has_null()) -> unwraps and returns underlying data_column() pointer
    {
        auto data_col = BinaryColumn::create();
        data_col->append("not_a_url_1");
        data_col->append("not_a_url_2");
        const Column* expected_data_ptr = data_col.get();
        auto null_col = NullColumn::create(2, 0);
        ColumnPtr nullable_str = NullableColumn::create(std::move(data_col), std::move(null_col));
        ASSERT_FALSE(nullable_str->has_null());
        ColumnPtr ptn_col =
                ColumnHelper::create_const_column<TYPE_VARCHAR>("^https?://([^/]+)/.*$", nullable_str->size());
        ColumnPtr rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("\\1", nullable_str->size());
        Columns columns = {nullable_str, ptn_col, rpl_col};

        auto res = eval_regexp_replace(columns);
        ASSERT_TRUE(res.ok()) << res.status();
        ASSERT_EQ(res.value().get(), expected_data_ptr);
    }

    // 6. All ConstColumns with no match -> returns exact same ConstColumn pointer
    {
        ColumnPtr str_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("not_a_url", 8);
        ColumnPtr ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("^https?://([^/]+)/.*$", 8);
        ColumnPtr rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("\\1", 8);
        Columns columns = {str_col, ptn_col, rpl_col};

        auto res = eval_regexp_replace(columns);
        ASSERT_TRUE(res.ok()) << res.status();
        ASSERT_EQ(res.value().get(), columns[0].get());
    }

    // 7. Empty 0-row column -> returns 0-row column cleanly
    {
        ColumnPtr str_col = BinaryColumn::create();
        ColumnPtr ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("^https?://([^/]+)/.*$", 1);
        ColumnPtr rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("\\1", 1);
        Columns columns = {str_col, ptn_col, rpl_col};

        auto res = eval_regexp_replace(columns);
        ASSERT_TRUE(res.ok()) << res.status();
        ASSERT_EQ(res.value()->size(), 0);
    }
}

TEST_F(StringFunctionRegexpReplaceTest, testLazyPrefixCopyAndMultiMatchFirstRow) {
    constexpr size_t kNumRows = 128;
    const std::string pattern = "([0-9]+)";
    const std::string rpl = "<\\1>";

    re2::RE2::Options opts;
    opts.set_log_errors(false);
    opts.set_longest_match(true);
    opts.set_dot_nl(true);
    re2::RE2 ref_re(pattern, opts);
    ASSERT_TRUE(ref_re.ok());

    std::vector<std::string> inputs(kNumRows);
    for (size_t i = 0; i < 64; ++i) {
        inputs[i] = "no_digits_prefix_" + std::string((i % 7) + 1, static_cast<char>('a' + (i % 26)));
    }
    inputs[64] = "id=123,code=456,ver=789";
    for (size_t i = 65; i < kNumRows; ++i) {
        if (i % 2 == 0) {
            inputs[i] = "item_" + std::to_string(i) + "_count_" + std::to_string(i * 10);
        } else {
            inputs[i] = "pure_alpha_row";
        }
    }

    // 1. Non-nullable BinaryColumn
    {
        auto str_col = BinaryColumn::create();
        for (const auto& s : inputs) {
            str_col->append(Slice(s));
        }
        ColumnPtr ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(pattern), kNumRows);
        ColumnPtr rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(rpl), kNumRows);
        Columns columns = {str_col, ptn_col, rpl_col};

        auto res = eval_regexp_replace(columns);
        ASSERT_TRUE(res.ok()) << res.status();
        auto out_col = ColumnHelper::as_column<BinaryColumn>(res.value());
        ASSERT_EQ(out_col->size(), kNumRows);
        for (size_t i = 0; i < kNumRows; ++i) {
            std::string expected = re2_reference_replace(inputs[i], ref_re, rpl, true);
            Slice actual = out_col->get_slice(i);
            ASSERT_EQ(std::string(actual.data, actual.size), expected) << "row=" << i;
        }
    }

    // 2. NullableColumn with nulls before and after first match at row 64
    {
        auto data_col = BinaryColumn::create();
        auto null_col = NullColumn::create();
        for (size_t i = 0; i < kNumRows; ++i) {
            bool is_null = (i < 64 && (i % 5 == 0)) || (i > 64 && (i % 7 == 0));
            if (is_null) {
                data_col->append(Slice("999_ignored_in_null_slot"));
                null_col->append(1);
            } else {
                data_col->append(Slice(inputs[i]));
                null_col->append(0);
            }
        }
        ColumnPtr nullable_str = NullableColumn::create(std::move(data_col), std::move(null_col));
        ColumnPtr ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(pattern), kNumRows);
        ColumnPtr rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(rpl), kNumRows);
        Columns columns = {nullable_str, ptn_col, rpl_col};

        auto res = eval_regexp_replace(columns);
        ASSERT_TRUE(res.ok()) << res.status();
        ASSERT_EQ(res.value()->size(), kNumRows);
        ColumnViewer<TYPE_VARCHAR> viewer(res.value());
        for (size_t i = 0; i < kNumRows; ++i) {
            bool expected_null = (i < 64 && (i % 5 == 0)) || (i > 64 && (i % 7 == 0));
            ASSERT_EQ(viewer.is_null(i), expected_null) << "row=" << i;
            if (!expected_null) {
                std::string expected = re2_reference_replace(inputs[i], ref_re, rpl, true);
                Slice actual = viewer.value(i);
                ASSERT_EQ(std::string(actual.data, actual.size), expected) << "row=" << i;
            }
        }
    }
}

TEST_F(StringFunctionRegexpReplaceTest, testDifferentialOracleAgainstRE2) {
    const std::vector<std::string> patterns = {
            "^https?://(?:www\\.)?([^/]+)/.*$", "([a-z]+)-([0-9]+)", "(a)?(b)", "", "a*", "^", "$", "\\x00+",
    };

    const std::vector<std::string> replacements = {
            "", "\\1", "\\2:\\1:\\0", "\\10", "\\\\\\1\\\\", "prefix\\x", "prefix\\x\\2", "trail\\", "CHINA",
    };

    const std::vector<std::string> inputs = {
            "https://www.starrocks.io/docs/en/sql-reference",
            "http://github.com/StarRocks/starrocks/pull/1",
            "foo-123 bar-456",
            "b",
            "ab",
            "aabaa",
            "",
            std::string("ab\0cd", 5),
            std::string("\0\0", 2),
            "éß",
            "中文",
            "😀🚀",
            std::string("\xFF\x80", 2),
            std::string("\xC0\x80", 2),
            std::string("\xE0\x80\x80", 3),
            std::string("\xF0\x80\x80\x80", 4),
            std::string("a\xE4\xB8", 3),
    };

    re2::RE2::Options opts;
    opts.set_log_errors(false);
    opts.set_longest_match(true);
    opts.set_dot_nl(true);

    for (const auto& pattern : patterns) {
        re2::RE2 ref_re(re2::StringPiece(pattern.data(), pattern.size()), opts);
        ASSERT_TRUE(ref_re.ok());
        const bool const_global_mode = pattern.empty() || (!pattern.starts_with("^") && !pattern.ends_with("$"));

        for (const auto& rpl : replacements) {
            // Path 1: Const pattern + const rpl over BinaryColumn
            {
                auto str_col = BinaryColumn::create();
                for (const auto& s : inputs) {
                    str_col->append(Slice(s.data(), s.size()));
                }
                ColumnPtr ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(
                        Slice(pattern.data(), pattern.size()), inputs.size());
                ColumnPtr rpl_col =
                        ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(rpl.data(), rpl.size()), inputs.size());
                Columns columns = {str_col, ptn_col, rpl_col};

                auto res = eval_regexp_replace(columns);
                ASSERT_TRUE(res.ok()) << res.status();
                ColumnViewer<TYPE_VARCHAR> viewer(res.value());
                for (size_t i = 0; i < inputs.size(); ++i) {
                    std::string expected = re2_reference_replace(inputs[i], ref_re, rpl, const_global_mode);
                    Slice actual = viewer.value(i);
                    ASSERT_EQ(std::string(actual.data, actual.size), expected)
                            << "Path 1 mismatch: pattern=[" << pattern << "] rpl=[" << rpl << "] row=" << i;
                }
            }

            // Path 1b: All ConstColumns
            for (const auto& s : inputs) {
                constexpr size_t kConstSize = 4;
                ColumnPtr str_col =
                        ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(s.data(), s.size()), kConstSize);
                ColumnPtr ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(
                        Slice(pattern.data(), pattern.size()), kConstSize);
                ColumnPtr rpl_col =
                        ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(rpl.data(), rpl.size()), kConstSize);
                Columns columns = {str_col, ptn_col, rpl_col};

                auto res = eval_regexp_replace(columns);
                ASSERT_TRUE(res.ok()) << res.status();
                ASSERT_TRUE(res.value()->is_constant());
                ASSERT_EQ(res.value()->size(), kConstSize);
                ColumnViewer<TYPE_VARCHAR> viewer(res.value());
                std::string expected = re2_reference_replace(s, ref_re, rpl, const_global_mode);
                Slice actual = viewer.value(0);
                ASSERT_EQ(std::string(actual.data, actual.size), expected);
            }

            // Path 2: Const pattern + non-const rpl
            {
                auto str_col = BinaryColumn::create();
                auto rpl_col = BinaryColumn::create();
                for (const auto& s : inputs) {
                    str_col->append(Slice(s.data(), s.size()));
                    rpl_col->append(Slice(rpl.data(), rpl.size()));
                }
                ColumnPtr ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(
                        Slice(pattern.data(), pattern.size()), inputs.size());
                Columns columns = {str_col, ptn_col, rpl_col};

                auto res = eval_regexp_replace(columns);
                ASSERT_TRUE(res.ok()) << res.status();
                ColumnViewer<TYPE_VARCHAR> viewer(res.value());
                for (size_t i = 0; i < inputs.size(); ++i) {
                    std::string expected = re2_reference_replace(inputs[i], ref_re, rpl, const_global_mode);
                    Slice actual = viewer.value(i);
                    ASSERT_EQ(std::string(actual.data, actual.size), expected)
                            << "Path 2 mismatch: pattern=[" << pattern << "] rpl=[" << rpl << "] row=" << i;
                }
            }

            // Path 3: Non-const pattern + non-const rpl (regexp_replace_general always uses GlobalReplace)
            {
                auto str_col = BinaryColumn::create();
                auto ptn_col = BinaryColumn::create();
                auto rpl_col = BinaryColumn::create();
                for (const auto& s : inputs) {
                    str_col->append(Slice(s.data(), s.size()));
                    ptn_col->append(Slice(pattern.data(), pattern.size()));
                    rpl_col->append(Slice(rpl.data(), rpl.size()));
                }
                Columns columns = {str_col, ptn_col, rpl_col};

                auto res = eval_regexp_replace(columns);
                ASSERT_TRUE(res.ok()) << res.status();
                ColumnViewer<TYPE_VARCHAR> viewer(res.value());
                for (size_t i = 0; i < inputs.size(); ++i) {
                    std::string expected = re2_reference_replace(inputs[i], ref_re, rpl, true);
                    Slice actual = viewer.value(i);
                    ASSERT_EQ(std::string(actual.data, actual.size), expected)
                            << "Path 3 mismatch: pattern=[" << pattern << "] rpl=[" << rpl << "] row=" << i;
                }
            }
        }
    }

    // Also verify only_null() handling across all 3 argument positions preserves columns[0]->size()
    {
        auto str_col = ColumnTestHelper::build_column<Slice>({"a", "b", "c"});
        auto ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("a", 3);
        auto rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("x", 3);
        auto null_col_3 = ColumnHelper::create_const_null_column(3);
        auto null_col_1 = ColumnHelper::create_const_null_column(1);

        for (const Columns& cols : std::vector<Columns>{
                     {null_col_3, ptn_col, rpl_col}, {str_col, null_col_1, rpl_col}, {str_col, ptn_col, null_col_1}}) {
            auto res = eval_regexp_replace(cols);
            ASSERT_TRUE(res.ok()) << res.status();
            ASSERT_EQ(res.value()->size(), 3);
            ASSERT_TRUE(res.value()->only_null());
        }
    }
}

TEST_F(StringFunctionRegexpReplaceTest, testShrinkingReplacementDoesNotOverReserve) {
    constexpr size_t kNumRows = 64;
    const std::string blob(64 * 1024, 'a'); // 64 KiB per row -> 4 MiB total input

    auto str_col = BinaryColumn::create();
    auto var_ptn_col = BinaryColumn::create();
    auto var_rpl_col = BinaryColumn::create();
    for (size_t i = 0; i < kNumRows; ++i) {
        str_col->append(Slice(blob));
        var_ptn_col->append(Slice("^.*$"));
        var_rpl_col->append(Slice(""));
    }
    ColumnPtr const_ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("^.*$", kNumRows);
    ColumnPtr const_rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("", kNumRows);

    // 1. Const pattern + const replacement
    {
        auto res = eval_regexp_replace({str_col, const_ptn_col, const_rpl_col});
        ASSERT_TRUE(res.ok()) << res.status();
        auto* out_bin = down_cast<BinaryColumn*>(res.value()->as_mutable_raw_ptr());
        ASSERT_EQ(out_bin->size(), kNumRows);
        ASSERT_EQ(out_bin->get_immutable_bytes().size(), 0u);
        ASSERT_EQ(out_bin->get_bytes().capacity(), 0u);
    }

    // 2. Const pattern + non-const replacement
    {
        auto res = eval_regexp_replace({str_col, const_ptn_col, var_rpl_col});
        ASSERT_TRUE(res.ok()) << res.status();
        auto* out_bin = down_cast<BinaryColumn*>(res.value()->as_mutable_raw_ptr());
        ASSERT_EQ(out_bin->size(), kNumRows);
        ASSERT_EQ(out_bin->get_immutable_bytes().size(), 0u);
        ASSERT_EQ(out_bin->get_bytes().capacity(), 0u);
    }

    // 3. Non-const pattern + non-const replacement (general path)
    {
        auto res = eval_regexp_replace({str_col, var_ptn_col, var_rpl_col});
        ASSERT_TRUE(res.ok()) << res.status();
        auto* out_bin = down_cast<BinaryColumn*>(res.value()->as_mutable_raw_ptr());
        ASSERT_EQ(out_bin->size(), kNumRows);
        ASSERT_EQ(out_bin->get_immutable_bytes().size(), 0u);
        ASSERT_EQ(out_bin->get_bytes().capacity(), 0u);
    }
}

TEST_F(StringFunctionRegexpReplaceTest, testVariablePatternAndReplacementEdgeCases) {
    // 1. regexp_replace_general: non-constant pattern with invalid regex on row 0,
    //    null str on row 1, valid matching regex on row 2, valid non-matching regex on row 3.
    {
        auto str_col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        str_col->append_datum(Datum(Slice("abc123")));
        str_col->append_nulls(1);
        str_col->append_datum(Datum(Slice("abc123")));
        str_col->append_datum(Datum(Slice("abc123")));

        auto ptn_col = BinaryColumn::create();
        ptn_col->append(Slice("["));      // invalid regex
        ptn_col->append(Slice("[0-9]+")); // skipped due to null str
        ptn_col->append(Slice("[0-9]+")); // matches
        ptn_col->append(Slice("XYZ+"));   // does not match

        ColumnPtr rpl_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("X", 4);
        Columns columns = {str_col, ptn_col, rpl_col};

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ctx->set_constant_columns(columns);
        ASSERT_TRUE(
                StringFunctions::regexp_replace_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
        DeferOp close_op([&] {
            (void)StringFunctions::regexp_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        });

        auto res = StringFunctions::regexp_replace(ctx.get(), columns);
        ASSERT_TRUE(res.ok()) << res.status();
        ASSERT_TRUE(ctx->has_error());
        ColumnViewer<TYPE_VARCHAR> viewer(res.value());
        ASSERT_TRUE(viewer.is_null(0));
        ASSERT_TRUE(viewer.is_null(1));
        ASSERT_FALSE(viewer.is_null(2));
        ASSERT_EQ(viewer.value(2), Slice("abcX"));
        ASSERT_FALSE(viewer.is_null(3));
        ASSERT_EQ(viewer.value(3), Slice("abc123"));
    }

    // 2. regexp_replace_const (global_mode = true and false): constant pattern,
    //    variable replacement with nulls, and constant str column with variable replacement.
    for (const char* ptn : {"[0-9]+", "^[a-z]+[0-9]+$"}) {
        auto str_col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        str_col->append_datum(Datum(Slice("abc123")));
        str_col->append_nulls(1);
        str_col->append_datum(Datum(Slice("abc123")));
        str_col->append_datum(Datum(Slice("no_digits")));

        ColumnPtr ptn_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(ptn), 4);

        auto rpl_col = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        rpl_col->append_nulls(1);
        rpl_col->append_datum(Datum(Slice("R1")));
        rpl_col->append_datum(Datum(Slice("R2")));
        rpl_col->append_datum(Datum(Slice("R3")));

        auto res = eval_regexp_replace({str_col, ptn_col, rpl_col});
        ASSERT_TRUE(res.ok()) << res.status();
        ColumnViewer<TYPE_VARCHAR> viewer(res.value());
        ASSERT_TRUE(viewer.is_null(0));
        ASSERT_TRUE(viewer.is_null(1));
        ASSERT_FALSE(viewer.is_null(2));
        ASSERT_EQ(viewer.value(2), Slice(std::string(ptn) == "[0-9]+" ? "abcR2" : "R2"));
        ASSERT_FALSE(viewer.is_null(3));
        ASSERT_EQ(viewer.value(3), Slice("no_digits"));

        // Constant str column with variable replacement
        ColumnPtr const_str = ColumnHelper::create_const_column<TYPE_VARCHAR>("abc123", 4);
        auto res_const_str = eval_regexp_replace({const_str, ptn_col, rpl_col});
        ASSERT_TRUE(res_const_str.ok()) << res_const_str.status();
        ColumnViewer<TYPE_VARCHAR> viewer_const_str(res_const_str.value());
        ASSERT_TRUE(viewer_const_str.is_null(0));
        ASSERT_FALSE(viewer_const_str.is_null(1));
        ASSERT_EQ(viewer_const_str.value(1), Slice(std::string(ptn) == "[0-9]+" ? "abcR1" : "R1"));
    }
}

} // namespace starrocks
