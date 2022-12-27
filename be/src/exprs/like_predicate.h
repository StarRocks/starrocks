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

#pragma once

#include <hs/hs.h>
#include <re2/re2.h>

#include <memory>
#include <string>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/builtin_functions.h"
#include "exprs/function_helper.h"

namespace starrocks {

class LikePredicate {
public:
    // Like method
    static Status like_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    static Status like_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    /**
     * like predicate method interface
     *
     * @param: [string_value, pattern]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(like);

    // regex method
    static Status regex_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    static Status regex_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    /**
     * regex predicate method interface
     *
     * @param: [string_value, pattern]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(regex);

private:
    /**
     * use for:
     *  a like "....", such as "!@#$%^&*"..=
     *
     * @param: [string_value, pattern]
     * @paramType: [BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(like_fn);

    /**
     * use for:
     *  regex match, such as "!@#$%^&*"...
     *
     * @param: [string_value, pattern]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(regex_fn);

    DEFINE_VECTORIZED_FN(regex_fn_with_long_constant_pattern);
    DEFINE_VECTORIZED_FN(like_fn_with_long_constant_pattern);
    /**
     * use for:
     *  a like "xxxx%"
     *
     * pattern from context
     *
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(constant_ends_with_fn);

    /**
     * use for:
     *  a like "%xxxx"
     *
     * pattern from context
     *
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(constant_starts_with_fn);

    /**
     * use for:
     *  a like "xxxx"
     *
     * pattern from context
     *
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(constant_equals_fn);

    /**
     * use for:
     *  a like "%xxxx%"
     *
     * pattern from context
     *
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(constant_substring_fn);

    /**
      * use for:
      *  regex match
      *
      * @param: [string_value, pattern_value]
      * @paramType: [BinaryColumn, BinaryColumn]
      * @return: BooleanColumn
      */
    static StatusOr<ColumnPtr> regex_match(FunctionContext* context, const Columns& columns, bool is_like_pattern);

    static StatusOr<ColumnPtr> regex_match_full(FunctionContext* context, const Columns& columns);

    static StatusOr<ColumnPtr> regex_match_partial(FunctionContext* context, const Columns& columns);

    template <bool full_match>
    static StatusOr<ColumnPtr> match_fn_with_long_constant_pattern(FunctionContext* context, const Columns& columns);

    /// Convert a LIKE pattern (with embedded % and _) into the corresponding
    /// regular expression pattern. Escaped chars are copied verbatim.
    template <bool fullMatch>
    static std::string convert_like_pattern(FunctionContext* context, const Slice& pattern);

    static void remove_escape_character(std::string* search_string);

private:
    static StatusOr<ColumnPtr> _predicate_const_regex(FunctionContext* context, ColumnBuilder<TYPE_BOOLEAN>* result,
                                                      const ColumnViewer<TYPE_VARCHAR>& value_viewer,
                                                      const ColumnPtr& value_column);

    // This is used when pattern is empty string, &_DUMMY_STRING_FOR_EMPTY_PATTERN used as not null pointer
    // to avoid crash with hs_scan.
    static inline char _DUMMY_STRING_FOR_EMPTY_PATTERN = 'A';

    struct LikePredicateState;
    static bool hs_compile_and_alloc_scratch(const std::string&, LikePredicateState*, FunctionContext*,
                                             const Slice& slice);
    template <bool full_match>
    static Status compile_with_hyperscan_or_re2(const std::string& pattern, LikePredicateState* state,
                                                FunctionContext* context, const Slice& slice);
    struct LikePredicateState {
        char escape_char{'\\'};

        std::shared_ptr<re2::RE2> re2 = nullptr;
        /// This is the function, set in the prepare function, that will be used to determine
        /// the value of the predicate. It will be set depending on whether the expression is
        /// a LIKE, RLIKE or REGEXP predicate, whether the pattern is a constant argument
        /// and whether the pattern has any constant substrings. If the pattern is not a
        /// constant argument, none of the following fields can be set because we cannot know
        /// the format of the pattern in the prepare function and must deal with each pattern
        /// separately.
        ScalarFunction function;

        /// Holds the string the StringValue points to and is set any time.
        std::string search_string;

        /// Used for LIKE predicates if the pattern is a constant argument, and is either a
        /// constant string or has a constant string at the beginning or end of the pattern.
        /// This will be set in order to check for that pattern in the corresponding part of
        /// the string.
        Slice search_string_sv;

        ColumnPtr _search_string_column;

        // a pointer to the generated database that responsible for parsed expression.
        hs_database_t* database = nullptr;
        // a type containing error details that is returned by the compile calls on failure.
        hs_compile_error_t* compile_err = nullptr;
        // A Hyperscan scratch space, Used to call hs_scan,
        // one scratch space per thread, or concurrent caller, is required
        hs_scratch_t* scratch = nullptr;

        LikePredicateState() = default;

        ~LikePredicateState() {
            if (scratch != nullptr) {
                hs_free_scratch(scratch);
            }

            if (database != nullptr) {
                hs_free_database(database);
            }
        }

        void set_search_string(const std::string& search_string_arg) {
            search_string = search_string_arg;
            search_string_sv = Slice(search_string);
            _search_string_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(search_string_sv, 1);
        }
    };
};
} // namespace starrocks
