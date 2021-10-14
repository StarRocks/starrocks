// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <runtime/decimalv3.h>

#include <iomanip>

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "exprs/vectorized/function_helper.h"
#include "util/url_parser.h"

namespace starrocks {
namespace vectorized {

struct PadState {
    bool is_const;
    bool fill_is_const;
    Slice fill;
    bool fill_is_utf8;
    std::vector<size_t> fill_utf8_index;
};

struct SubstrState {
    bool is_const = false;
    int32_t pos = std::numeric_limits<int32_t>::lowest();
    int32_t len = std::numeric_limits<int32_t>::max();
};

struct ConcatState {
    bool is_const = false;
    bool is_oversize = false;
    std::string tail;
};

class StringFunctions {
public:
    /**
   * @param: [string_value, position, optional<length>]
   * @paramType: [BinaryColumn, IntColumn, optional<IntColumn>]
   * @return: BinaryColumn
   */
    DEFINE_VECTORIZED_FN(substring);

    /**
     * @param: [string_value, length]
     * @paramType: [BinaryColumn, IntColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(left);

    /**
     * @param: [string_value, length]
     * @paramType: [BinaryColumn, IntColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(right);

    /**
     * @param: [string_value, prefix]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(starts_with);

    /**
     * @param: [string_value, subffix]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(ends_with);

    /**
     * Return a string of the specified number of spaces
     *
     * @param: [length]
     * @paramType: [IntColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(space);

    /**
     * Repeat a string the specified number of times
     * we will truncate the result length to 65535
     *
     * @param: [string_value, times]
     * @paramType: [BinaryColumn, IntColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(repeat);

    /**
     * Return the string argument, left-padded with the specified string
     *
     * @param: [string_value, repeat_number]
     * @paramType: [BinaryColumn, IntColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(lpad);

    /**
     * Append string the specified number of times
     *
     * @param: [string_value, repeat_number]
     * @paramType: [BinaryColumn, IntColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(rpad);

    /**
     * Append the character if the s string is non-empty and does not contain the character at the
     * end, and the character length must be 1.
     *
     * @param: [string_value, tail_char]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(append_trailing_char_if_absent);

    /**
     * Return the length of a string in bytes
     *
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: IntColumn
     */
    DEFINE_VECTORIZED_FN(length);

    /**
     * Return the length of a string in utf8
     *
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: IntColumn
     */
    DEFINE_VECTORIZED_FN(utf8_length);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(lower);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(upper);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(reverse);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(trim);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(ltrim);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(rtrim);

    /**
     * Return numeric value of left-most character
     *
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: IntColumn
     */
    DEFINE_VECTORIZED_FN(ascii);

    /**
     * @param: [IntColumn]
     * @return: StringColumn
     * Get symbols from binary numbers
     */
    DEFINE_VECTORIZED_FN(get_char);

    /**
     * Return the index of the first occurrence of substring
     *
     * @param: [string_value, sub_string_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: IntColumn
     */
    DEFINE_VECTORIZED_FN(instr);

    /**
     * Return the position of the first occurrence of substring
     *
     * @param: [sub_string_value, string_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: IntColumn
     */
    DEFINE_VECTORIZED_FN(locate);

    /**
     * Return the position of the first occurrence of substring start with start_position
     *
     * @param: [sub_string_value, string_value, start_position]
     * @paramType: [BinaryColumn, BinaryColumn, IntColumn]
     * @return: IntColumn
     */
    DEFINE_VECTORIZED_FN(locate_pos);

    /**
     * @param: [string_value, ......]
     * @paramType: [BinaryColumn, ......]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(concat);

    /**
     * Return concatenate with separator
     *
     * @param: [string_value, ......]
     * @paramType: [BinaryColumn, ......]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(concat_ws);

    /**
     * Index (position) of first argument within second argument which is a comma-separated string
     *
     * @param: [string_value, string_set]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: IntColumn
     */
    DEFINE_VECTORIZED_FN(find_in_set);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(null_or_empty);

    /**
     * @param: [string_value, delimiter]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: ArrayColumn
     */
    DEFINE_VECTORIZED_FN(split);

    static Status split_prepare(starrocks_udf::FunctionContext* context,
                                starrocks_udf::FunctionContext::FunctionStateScope scope);
    static Status split_close(starrocks_udf::FunctionContext* context,
                              starrocks_udf::FunctionContext::FunctionStateScope scope);

    /**
     * @param: [string_value, delimiter, field]
     * @paramType: [BinaryColumn, BinaryColumn, IntColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(split_part);

    // regex method
    static Status regexp_prepare(starrocks_udf::FunctionContext* context,
                                 starrocks_udf::FunctionContext::FunctionStateScope scope);
    static Status regexp_close(starrocks_udf::FunctionContext* context,
                               starrocks_udf::FunctionContext::FunctionStateScope scope);

    /**
     * @param: [string_value, pattern_value, index_value
     * @paramType: [BinaryColumn, BinaryColumn, Int64Column]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(regexp_extract);

    /**
     * @param: [string_value, pattern_value, replace_value
     * @paramType: [BinaryColumn, BinaryColumn, BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(regexp_replace);

    /**
     * @param: [DOUBLE]
     * @paramType: [DoubleColumn]
     * @return: BinaryColumn
     */
    static ColumnPtr money_format_double(FunctionContext* context, const starrocks::vectorized::Columns& columns);

    /**
     * @param: [BIGINT]
     * @paramType: [Int64Column]
     * @return: BinaryColumn
     */
    static ColumnPtr money_format_bigint(FunctionContext* context, const starrocks::vectorized::Columns& columns);

    /**
     * @param: [DECIMALV2]
     * @paramType: [DecimalColumn]
     * @return: BinaryColumn
     */
    static ColumnPtr money_format_largeint(FunctionContext* context, const starrocks::vectorized::Columns& columns);

    /**
     * @param: [LARGEINT]
     * @paramType: [Int128Column]
     * @return: BinaryColumn
     */
    static ColumnPtr money_format_decimalv2val(FunctionContext* context, const starrocks::vectorized::Columns& columns);

    template <PrimitiveType Type>
    static ColumnPtr money_format_decimal(FunctionContext* context, const starrocks::vectorized::Columns& columns);

    // parse's auxiliary method
    static Status parse_url_prepare(starrocks_udf::FunctionContext* context,
                                    starrocks_udf::FunctionContext::FunctionStateScope scope);

    static Status parse_url_close(starrocks_udf::FunctionContext* context,
                                  starrocks_udf::FunctionContext::FunctionStateScope scope);

    static Status sub_str_prepare(starrocks_udf::FunctionContext* context,
                                  starrocks_udf::FunctionContext::FunctionStateScope scope);

    static Status sub_str_close(starrocks_udf::FunctionContext* context,
                                starrocks_udf::FunctionContext::FunctionStateScope scope);

    static Status left_or_right_prepare(starrocks_udf::FunctionContext* context,
                                        starrocks_udf::FunctionContext::FunctionStateScope scope);

    static Status left_or_right_close(starrocks_udf::FunctionContext* context,
                                      starrocks_udf::FunctionContext::FunctionStateScope scope);

    static Status concat_prepare(starrocks_udf::FunctionContext* context,
                                 starrocks_udf::FunctionContext::FunctionStateScope scope);

    static Status concat_close(starrocks_udf::FunctionContext* context,
                               starrocks_udf::FunctionContext::FunctionStateScope scope);

    static Status pad_prepare(starrocks_udf::FunctionContext* context,
                              starrocks_udf::FunctionContext::FunctionStateScope scope);

    static Status pad_close(starrocks_udf::FunctionContext* context,
                            starrocks_udf::FunctionContext::FunctionStateScope scope);
    /**
   * string_value is a url, part_value indicate a part of the url, return url's corresponding value;
   * part_values is fixed: "AUTHORITY"/"FILE"/"HOST"/"PROTOCOL" and so on.
   *
   * @param: [string_value, part_value]
   * @paramType: [BinaryColumn, BinaryColumn]
   * @return: BinaryColumn
   */
    DEFINE_VECTORIZED_FN(parse_url);

    /**
     * @param: [BigIntColumn]
     * @return: StringColumn
     * Get the hexadecimal representation of bigint
     */
    DEFINE_VECTORIZED_FN(hex_int);
    /**
     * @param: [StringColumn]
     * @return: StringColumn
     * Get the hexadecimal representation of string
     */
    DEFINE_VECTORIZED_FN(hex_string);
    /**
     * @param: [BigIntColumn]
     * @return: StringColumn
     * Get the string of this hexadecimal representation represents
     */
    DEFINE_VECTORIZED_FN(unhex);

private:
    static int index_of(const char* source, int source_count, const char* target, int target_count, int from_index);

private:
    struct StringFunctionsState {
        std::unique_ptr<re2::RE2> regex;
        std::unique_ptr<re2::RE2::Options> options;
        bool const_pattern;

        StringFunctionsState() : regex(), options(), const_pattern(false) {}
    };

    static ColumnPtr regexp_extract_const(re2::RE2* const_re, const Columns& columns);
    static ColumnPtr regexp_extract_general(FunctionContext* context, re2::RE2::Options* options,
                                            const Columns& columns);

    static ColumnPtr regexp_replace_const(re2::RE2* const_re, const Columns& columns);
    static ColumnPtr regexp_replace_general(FunctionContext* context, re2::RE2::Options* options,
                                            const Columns& columns);

    struct CurrencyFormat : std::moneypunct<char> {
        pattern do_pos_format() const override { return {{none, sign, none, value}}; }
        pattern do_neg_format() const override { return {{none, sign, none, value}}; }
        int do_frac_digits() const override { return 2; }
        char_type do_thousands_sep() const override { return ','; }
        string_type do_grouping() const override { return "\003"; }
        string_type do_negative_sign() const override { return "-"; }
    };

    static std::string transform_currency_format(FunctionContext* context, const std::string& v) {
        std::locale comma_locale(std::locale(), new CurrencyFormat());
        std::stringstream ss;
        ss.imbue(comma_locale);
        ss << std::put_money(v);
        return ss.str();
    };

    struct ParseUrlState {
        bool const_pattern;
        std::unique_ptr<UrlParser::UrlPart> url_part;
        ParseUrlState() : const_pattern(false), url_part() {}
    };

    static ColumnPtr parse_url_general(FunctionContext* context, const starrocks::vectorized::Columns& columns);
    static ColumnPtr parse_url_const(UrlParser::UrlPart* url_part, FunctionContext* context,
                                     const starrocks::vectorized::Columns& columns);

    template <PrimitiveType Type, bool scale_up, bool check_overflow>
    static inline void money_format_decimal_impl(FunctionContext* context, ColumnViewer<Type> const& money_viewer,
                                                 size_t num_rows, int adjust_scale,
                                                 ColumnBuilder<TYPE_VARCHAR>* result);
};

template <PrimitiveType Type, bool scale_up, bool check_overflow>
void StringFunctions::money_format_decimal_impl(FunctionContext* context, ColumnViewer<Type> const& money_viewer,
                                                size_t num_rows, int adjust_scale,
                                                ColumnBuilder<TYPE_VARCHAR>* result) {
    using CppType = RunTimeCppType<Type>;
    const auto scale_factor = get_scale_factor<CppType>(adjust_scale);
    static constexpr auto max_precision = decimal_precision_limit<CppType>;
    for (int row = 0; row < num_rows; ++row) {
        if (money_viewer.is_null(row)) {
            result->append_null();
            continue;
        }

        auto money_value = money_viewer.value(row);
        CppType rounded_cent_money;
        auto overflow = DecimalV3Cast::round<CppType, ROUND_HALF_EVEN, scale_up, check_overflow>(
                money_value, scale_factor, &rounded_cent_money);
        auto str = DecimalV3Cast::to_string<CppType>(rounded_cent_money, max_precision, 0);
        std::string concurr_format = transform_currency_format(context, str);
        result->append(Slice(concurr_format.data(), concurr_format.size()), overflow);
    }
}

template <PrimitiveType Type>
ColumnPtr StringFunctions::money_format_decimal(FunctionContext* context,
                                                const starrocks::vectorized::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    using CppType = RunTimeCppType<Type>;
    static_assert(pt_is_decimal<Type>, "Invalid decimal type");
    auto money_viewer = ColumnViewer<Type>(columns[0]);
    const auto& type = context->get_arg_type(0);
    int scale = type->scale;

    ColumnBuilder<TYPE_VARCHAR> result;
    auto num_rows = columns[0]->size();
    if (scale > 2) {
        // scale down
        money_format_decimal_impl<Type, false, true>(context, money_viewer, num_rows, scale - 2, &result);
    } else {
        // scale up
        money_format_decimal_impl<Type, true, true>(context, money_viewer, num_rows, 2 - scale, &result);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace vectorized
} // namespace starrocks
