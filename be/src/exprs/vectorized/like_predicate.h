// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <re2/re2.h>

#include <memory>
#include <string>

#include "exprs/vectorized/builtin_functions.h"
#include "exprs/vectorized/function_helper.h"

namespace starrocks {
namespace vectorized {

class LikePredicate {
public:
    // Like method
    static Status like_prepare(starrocks_udf::FunctionContext* context,
                               starrocks_udf::FunctionContext::FunctionStateScope scope);

    static Status like_close(starrocks_udf::FunctionContext* context,
                             starrocks_udf::FunctionContext::FunctionStateScope scope);

    /**
     * like predicate method interface
     *
     * @param: [string_value, pattern]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(like);

    // regex method
    static Status regex_prepare(starrocks_udf::FunctionContext* context,
                                starrocks_udf::FunctionContext::FunctionStateScope scope);

    static Status regex_close(starrocks_udf::FunctionContext* context,
                              starrocks_udf::FunctionContext::FunctionStateScope scope);

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
    static ColumnPtr regex_match(FunctionContext* context, const Columns& columns, bool is_like_pattern);

    static ColumnPtr regex_match_full(FunctionContext* context, const Columns& columns);

    static ColumnPtr regex_match_partial(FunctionContext* context, const Columns& columns);

    /// Convert a LIKE pattern (with embedded % and _) into the corresponding
    /// regular expression pattern. Escaped chars are copied verbatim.
    static std::string convert_like_pattern(starrocks_udf::FunctionContext* context, const Slice& pattern);

    static void remove_escape_character(std::string* search_string);

private:
    struct LikePredicateState {
        char escape_char;

        /// This is the function, set in the prepare function, that will be used to determine
        /// the value of the predicate. It will be set depending on whether the expression is
        /// a LIKE, RLIKE or REGEXP predicate, whether the pattern is a constant argument
        /// and whether the pattern has any constant substrings. If the pattern is not a
        /// constant argument, none of the following fields can be set because we cannot know
        /// the format of the pattern in the prepare function and must deal with each pattern
        /// seperately.
        ScalarFunction function;

        /// Holds the string the StringValue points to and is set any time.
        std::string search_string;

        /// Used for LIKE predicates if the pattern is a constant argument, and is either a
        /// constant string or has a constant string at the beginning or end of the pattern.
        /// This will be set in order to check for that pattern in the corresponding part of
        /// the string.
        Slice search_string_sv;

        ColumnPtr _search_string_column;

        /// Used for RLIKE and REGEXP predicates if the pattern is a constant argument.
        std::unique_ptr<re2::RE2> regex;

        LikePredicateState() : escape_char('\\') {}

        void set_search_string(const std::string& search_string_arg) {
            search_string = search_string_arg;
            search_string_sv = Slice(search_string);
            _search_string_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(search_string_sv, 1);
        }
    };
};
} // namespace vectorized
} // namespace starrocks
