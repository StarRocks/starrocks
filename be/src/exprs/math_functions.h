// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/math_functions.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef STARROCKS_BE_SRC_QUERY_EXPRS_MATH_FUNCTIONS_H
#define STARROCKS_BE_SRC_QUERY_EXPRS_MATH_FUNCTIONS_H

#include <stdint.h>

#include "util/string_parser.hpp"

namespace starrocks {

class Expr;
struct ExprValue;
class TupleRow;

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

class MathFunctions {
public:
    static void init();

    static starrocks_udf::DoubleVal pi(starrocks_udf::FunctionContext* ctx);
    static starrocks_udf::DoubleVal e(starrocks_udf::FunctionContext* ctx);

    static starrocks_udf::DoubleVal abs(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);
    static starrocks_udf::FloatVal abs(starrocks_udf::FunctionContext*, const starrocks_udf::FloatVal&);
    static starrocks_udf::DecimalVal abs(starrocks_udf::FunctionContext*, const starrocks_udf::DecimalVal&);
    static starrocks_udf::DecimalV2Val abs(starrocks_udf::FunctionContext*, const starrocks_udf::DecimalV2Val&);

    // For integer math, we have to promote ABS() to the next highest integer type because
    // in two's complement arithmetic, the largest negative value for any bit width is not
    // representable as a positive value within the same width.  For the largest width, we
    // simply overflow.  In the unlikely event a workaround is needed, one can simply cast
    // to a higher precision decimal type.
    static starrocks_udf::LargeIntVal abs(starrocks_udf::FunctionContext*, const starrocks_udf::LargeIntVal&);
    static starrocks_udf::LargeIntVal abs(starrocks_udf::FunctionContext*, const starrocks_udf::BigIntVal&);
    static starrocks_udf::BigIntVal abs(starrocks_udf::FunctionContext*, const starrocks_udf::IntVal&);
    static starrocks_udf::IntVal abs(starrocks_udf::FunctionContext*, const starrocks_udf::SmallIntVal&);
    static starrocks_udf::SmallIntVal abs(starrocks_udf::FunctionContext*, const starrocks_udf::TinyIntVal&);

    static starrocks_udf::FloatVal sign(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DoubleVal& v);

    static starrocks_udf::DoubleVal sin(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);
    static starrocks_udf::DoubleVal asin(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);
    static starrocks_udf::DoubleVal cos(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);
    static starrocks_udf::DoubleVal acos(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);
    static starrocks_udf::DoubleVal tan(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);
    static starrocks_udf::DoubleVal atan(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);

    static starrocks_udf::BigIntVal ceil(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);
    static starrocks_udf::BigIntVal floor(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);
    static starrocks_udf::BigIntVal round(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DoubleVal& v);
    static starrocks_udf::DoubleVal round_up_to(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DoubleVal& v,
                                                const starrocks_udf::IntVal& scale);
    static starrocks_udf::DoubleVal truncate(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DoubleVal& v,
                                             const starrocks_udf::IntVal& scale);

    static starrocks_udf::DoubleVal ln(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);
    static starrocks_udf::DoubleVal log(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DoubleVal& base,
                                        const starrocks_udf::DoubleVal& v);
    static starrocks_udf::DoubleVal log2(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DoubleVal& v);
    static starrocks_udf::DoubleVal log10(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);
    static starrocks_udf::DoubleVal exp(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);

    static starrocks_udf::DoubleVal radians(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DoubleVal& v);
    static starrocks_udf::DoubleVal degrees(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DoubleVal& v);

    static starrocks_udf::DoubleVal sqrt(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&);
    static starrocks_udf::DoubleVal pow(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DoubleVal& base,
                                        const starrocks_udf::DoubleVal& exp);

    /// Used for both Rand() and RandSeed()
    static void rand_prepare(starrocks_udf::FunctionContext*, starrocks_udf::FunctionContext::FunctionStateScope);
    static starrocks_udf::DoubleVal rand(starrocks_udf::FunctionContext*);
    static starrocks_udf::DoubleVal rand_seed(starrocks_udf::FunctionContext*, const starrocks_udf::BigIntVal& seed);

    static starrocks_udf::StringVal bin(starrocks_udf::FunctionContext* ctx, const starrocks_udf::BigIntVal& v);
    static starrocks_udf::StringVal hex_int(starrocks_udf::FunctionContext* ctx, const starrocks_udf::BigIntVal& v);
    static starrocks_udf::StringVal hex_string(starrocks_udf::FunctionContext* ctx, const starrocks_udf::StringVal& s);
    static starrocks_udf::StringVal unhex(starrocks_udf::FunctionContext* ctx, const starrocks_udf::StringVal& s);

    static starrocks_udf::StringVal conv_int(starrocks_udf::FunctionContext* ctx, const starrocks_udf::BigIntVal& num,
                                             const starrocks_udf::TinyIntVal& src_base,
                                             const starrocks_udf::TinyIntVal& dest_base);
    static starrocks_udf::StringVal conv_string(starrocks_udf::FunctionContext* ctx,
                                                const starrocks_udf::StringVal& num_str,
                                                const starrocks_udf::TinyIntVal& src_base,
                                                const starrocks_udf::TinyIntVal& dest_base);

    static starrocks_udf::BigIntVal pmod_bigint(starrocks_udf::FunctionContext* ctx, const starrocks_udf::BigIntVal& a,
                                                const starrocks_udf::BigIntVal& b);
    static starrocks_udf::DoubleVal pmod_double(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DoubleVal& a,
                                                const starrocks_udf::DoubleVal& b);
    static starrocks_udf::FloatVal fmod_float(starrocks_udf::FunctionContext*, const starrocks_udf::FloatVal&,
                                              const starrocks_udf::FloatVal&);
    static starrocks_udf::DoubleVal fmod_double(starrocks_udf::FunctionContext*, const starrocks_udf::DoubleVal&,
                                                const starrocks_udf::DoubleVal&);

    static starrocks_udf::BigIntVal positive_bigint(starrocks_udf::FunctionContext* ctx,
                                                    const starrocks_udf::BigIntVal& val);
    static starrocks_udf::DoubleVal positive_double(starrocks_udf::FunctionContext* ctx,
                                                    const starrocks_udf::DoubleVal& val);
    static starrocks_udf::DecimalVal positive_decimal(starrocks_udf::FunctionContext* ctx,
                                                      const starrocks_udf::DecimalVal& val);
    static starrocks_udf::DecimalV2Val positive_decimal(starrocks_udf::FunctionContext* ctx,
                                                        const starrocks_udf::DecimalV2Val& val);
    static starrocks_udf::BigIntVal negative_bigint(starrocks_udf::FunctionContext* ctx,
                                                    const starrocks_udf::BigIntVal& val);
    static starrocks_udf::DoubleVal negative_double(starrocks_udf::FunctionContext* ctx,
                                                    const starrocks_udf::DoubleVal& val);
    static starrocks_udf::DecimalVal negative_decimal(starrocks_udf::FunctionContext* ctx,
                                                      const starrocks_udf::DecimalVal& val);
    static starrocks_udf::DecimalV2Val negative_decimal(starrocks_udf::FunctionContext* ctx,
                                                        const starrocks_udf::DecimalV2Val& val);

    static starrocks_udf::TinyIntVal least(starrocks_udf::FunctionContext* ctx, int num_args,
                                           const starrocks_udf::TinyIntVal* args);
    static starrocks_udf::TinyIntVal greatest(starrocks_udf::FunctionContext* ctx, int num_args,
                                              const starrocks_udf::TinyIntVal* args);
    static starrocks_udf::SmallIntVal least(starrocks_udf::FunctionContext* ctx, int num_args,
                                            const starrocks_udf::SmallIntVal* val);
    static starrocks_udf::SmallIntVal greatest(starrocks_udf::FunctionContext* ctx, int num_args,
                                               const starrocks_udf::SmallIntVal* val);
    static starrocks_udf::IntVal least(starrocks_udf::FunctionContext* ctx, int num_args,
                                       const starrocks_udf::IntVal* val);
    static starrocks_udf::IntVal greatest(starrocks_udf::FunctionContext* ctx, int num_args,
                                          const starrocks_udf::IntVal* val);
    static starrocks_udf::BigIntVal least(starrocks_udf::FunctionContext* ctx, int num_args,
                                          const starrocks_udf::BigIntVal* val);
    static starrocks_udf::BigIntVal greatest(starrocks_udf::FunctionContext* ctx, int num_args,
                                             const starrocks_udf::BigIntVal* val);
    static starrocks_udf::LargeIntVal least(starrocks_udf::FunctionContext* ctx, int num_args,
                                            const starrocks_udf::LargeIntVal* val);
    static starrocks_udf::LargeIntVal greatest(starrocks_udf::FunctionContext* ctx, int num_args,
                                               const starrocks_udf::LargeIntVal* val);
    static starrocks_udf::FloatVal least(starrocks_udf::FunctionContext* ctx, int num_args,
                                         const starrocks_udf::FloatVal* val);
    static starrocks_udf::FloatVal greatest(starrocks_udf::FunctionContext* ctx, int num_args,
                                            const starrocks_udf::FloatVal* val);
    static starrocks_udf::DoubleVal least(starrocks_udf::FunctionContext* ctx, int num_args,
                                          const starrocks_udf::DoubleVal* val);
    static starrocks_udf::DoubleVal greatest(starrocks_udf::FunctionContext* ctx, int num_args,
                                             const starrocks_udf::DoubleVal* val);
    static starrocks_udf::StringVal least(starrocks_udf::FunctionContext* ctx, int num_args,
                                          const starrocks_udf::StringVal* val);
    static starrocks_udf::StringVal greatest(starrocks_udf::FunctionContext* ctx, int num_args,
                                             const starrocks_udf::StringVal* val);
    static starrocks_udf::DateTimeVal least(starrocks_udf::FunctionContext* ctx, int num_args,
                                            const starrocks_udf::DateTimeVal* val);
    static starrocks_udf::DateTimeVal greatest(starrocks_udf::FunctionContext* ctx, int num_args,
                                               const starrocks_udf::DateTimeVal* val);
    static starrocks_udf::DecimalVal least(starrocks_udf::FunctionContext* ctx, int num_args,
                                           const starrocks_udf::DecimalVal* val);
    static starrocks_udf::DecimalVal greatest(starrocks_udf::FunctionContext* ctx, int num_args,
                                              const starrocks_udf::DecimalVal* val);
    static starrocks_udf::DecimalV2Val least(starrocks_udf::FunctionContext* ctx, int num_args,
                                             const starrocks_udf::DecimalV2Val* val);
    static starrocks_udf::DecimalV2Val greatest(starrocks_udf::FunctionContext* ctx, int num_args,
                                                const starrocks_udf::DecimalV2Val* val);

    static double my_double_round(double value, int64_t dec, bool dec_unsigned, bool truncate);

private:
    static const int32_t MIN_BASE = 2;
    static const int32_t MAX_BASE = 36;
    static const char* _s_alphanumeric_chars;

    // Converts src_num in decimal to dest_base,
    // and fills expr_val.string_val with the result.
    static starrocks_udf::StringVal decimal_to_base(starrocks_udf::FunctionContext* ctx, int64_t src_num,
                                                    int8_t dest_base);

    // Converts src_num representing a number in src_base but encoded in decimal
    // into its actual decimal number.
    // For example, if src_num is 21 and src_base is 5,
    // then this function sets *result to 2*5^1 + 1*5^0 = 11.
    // Returns false if overflow occurred, true upon success.
    static bool decimal_in_base_to_decimal(int64_t src_num, int8_t src_base, int64_t* result);

    // Helper function used in Conv to implement behavior consistent
    // with MySQL and Hive in case of numeric overflow during Conv.
    // Inspects parse_res, and in case of overflow sets num to MAXINT64 if dest_base
    // is positive, otherwise to -1.
    // Returns true if no parse_res == PARSE_SUCCESS || parse_res == PARSE_OVERFLOW.
    // Returns false otherwise, indicating some other error condition.
    static bool handle_parse_result(int8_t dest_base, int64_t* num, StringParser::ParseResult parse_res);
};

} // namespace starrocks

#endif
