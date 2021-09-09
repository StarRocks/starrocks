// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/string_functions.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXPRS_STRING_FUNCTIONS_H
#define STARROCKS_BE_SRC_QUERY_EXPRS_STRING_FUNCTIONS_H

#include <re2/re2.h>

#include <iomanip>
#include <locale>
#include <sstream>

#include "anyval_util.h"
#include "runtime/string_search.hpp"
#include "runtime/string_value.h"

namespace starrocks {

class Expr;
class OpcodeRegistry;
class TupleRow;

class StringFunctions {
public:
    static void init();

    static starrocks_udf::StringVal substring(starrocks_udf::FunctionContext* context,
                                              const starrocks_udf::StringVal& str, const starrocks_udf::IntVal& pos,
                                              const starrocks_udf::IntVal& len);
    static starrocks_udf::StringVal substring(starrocks_udf::FunctionContext* context,
                                              const starrocks_udf::StringVal& str, const starrocks_udf::IntVal& pos);
    static starrocks_udf::StringVal left(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str,
                                         const starrocks_udf::IntVal& len);
    static starrocks_udf::StringVal right(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str,
                                          const starrocks_udf::IntVal& len);
    static starrocks_udf::BooleanVal starts_with(starrocks_udf::FunctionContext* context,
                                                 const starrocks_udf::StringVal& str,
                                                 const starrocks_udf::StringVal& prefix);
    static starrocks_udf::BooleanVal ends_with(starrocks_udf::FunctionContext* context,
                                               const starrocks_udf::StringVal& str,
                                               const starrocks_udf::StringVal& suffix);
    static starrocks_udf::BooleanVal null_or_empty(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::StringVal& str);
    static starrocks_udf::StringVal space(starrocks_udf::FunctionContext* context, const starrocks_udf::IntVal& len);
    static starrocks_udf::StringVal repeat(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str,
                                           const starrocks_udf::IntVal& n);
    static starrocks_udf::StringVal lpad(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str,
                                         const starrocks_udf::IntVal& len, const starrocks_udf::StringVal& pad);
    static starrocks_udf::StringVal rpad(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str,
                                         const starrocks_udf::IntVal& len, const starrocks_udf::StringVal& pad);
    static starrocks_udf::StringVal append_trailing_char_if_absent(starrocks_udf::FunctionContext* context,
                                                                   const starrocks_udf::StringVal& str,
                                                                   const starrocks_udf::StringVal& trailing_char);
    static starrocks_udf::IntVal length(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str);
    static starrocks_udf::IntVal char_utf8_length(starrocks_udf::FunctionContext* context,
                                                  const starrocks_udf::StringVal& str);
    static starrocks_udf::StringVal lower(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str);
    static starrocks_udf::StringVal upper(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str);
    static starrocks_udf::StringVal reverse(starrocks_udf::FunctionContext* context,
                                            const starrocks_udf::StringVal& str);
    static starrocks_udf::StringVal trim(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str);
    static starrocks_udf::StringVal ltrim(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str);
    static starrocks_udf::StringVal rtrim(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str);
    static starrocks_udf::IntVal ascii(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str);
    static starrocks_udf::IntVal instr(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str,
                                       const starrocks_udf::StringVal&);
    static starrocks_udf::IntVal locate(starrocks_udf::FunctionContext* context, const starrocks_udf::StringVal& str,
                                        const starrocks_udf::StringVal&);
    static starrocks_udf::IntVal locate_pos(starrocks_udf::FunctionContext* context,
                                            const starrocks_udf::StringVal& str, const starrocks_udf::StringVal&,
                                            const starrocks_udf::IntVal&);

    static bool set_re2_options(const starrocks_udf::StringVal& match_parameter, std::string* error_str,
                                re2::RE2::Options* opts);

    static void regexp_prepare(starrocks_udf::FunctionContext*, starrocks_udf::FunctionContext::FunctionStateScope);
    static StringVal regexp_extract(starrocks_udf::FunctionContext*, const starrocks_udf::StringVal& str,
                                    const starrocks_udf::StringVal& pattern, const starrocks_udf::BigIntVal& index);
    static StringVal regexp_replace(starrocks_udf::FunctionContext*, const starrocks_udf::StringVal& str,
                                    const starrocks_udf::StringVal& pattern, const starrocks_udf::StringVal& replace);
    static void regexp_close(starrocks_udf::FunctionContext*, starrocks_udf::FunctionContext::FunctionStateScope);
#if 0
    static void RegexpMatchCountPrepare(FunctionContext* context,
                                        FunctionContext::FunctionStateScope scope);
    static IntVal RegexpMatchCount2Args(FunctionContext* context, const StringVal& str,
                                        const StringVal& pattern);
    static IntVal RegexpMatchCount4Args(FunctionContext* context, const StringVal& str,
                                        const StringVal& pattern, const IntVal& start_pos,
                                        const StringVal& match_parameter);
#endif
    static StringVal concat(starrocks_udf::FunctionContext*, int num_children, const StringVal* strs);
    static StringVal concat_ws(starrocks_udf::FunctionContext*, const starrocks_udf::StringVal& sep, int num_children,
                               const starrocks_udf::StringVal* strs);
    static IntVal find_in_set(starrocks_udf::FunctionContext*, const starrocks_udf::StringVal& str,
                              const starrocks_udf::StringVal& str_set);

    static void parse_url_prepare(starrocks_udf::FunctionContext*, starrocks_udf::FunctionContext::FunctionStateScope);
    static StringVal parse_url(starrocks_udf::FunctionContext*, const starrocks_udf::StringVal& url,
                               const starrocks_udf::StringVal& part);
    static StringVal parse_url_key(starrocks_udf::FunctionContext*, const starrocks_udf::StringVal& url,
                                   const starrocks_udf::StringVal& key, const starrocks_udf::StringVal& part);
    static void parse_url_close(starrocks_udf::FunctionContext*, starrocks_udf::FunctionContext::FunctionStateScope);

    static starrocks_udf::StringVal money_format(starrocks_udf::FunctionContext* context,
                                                 const starrocks_udf::DoubleVal& v);

    static starrocks_udf::StringVal money_format(starrocks_udf::FunctionContext* context,
                                                 const starrocks_udf::DecimalVal& v);

    static starrocks_udf::StringVal money_format(starrocks_udf::FunctionContext* context,
                                                 const starrocks_udf::DecimalV2Val& v);

    static starrocks_udf::StringVal money_format(starrocks_udf::FunctionContext* context,
                                                 const starrocks_udf::BigIntVal& v);

    static starrocks_udf::StringVal money_format(starrocks_udf::FunctionContext* context,
                                                 const starrocks_udf::LargeIntVal& v);

    struct CommaMoneypunct : std::moneypunct<char> {
        pattern do_pos_format() const override { return {{none, sign, none, value}}; }
        pattern do_neg_format() const override { return {{none, sign, none, value}}; }
        int do_frac_digits() const override { return 2; }
        char_type do_thousands_sep() const override { return ','; }
        string_type do_grouping() const override { return "\003"; }
        string_type do_negative_sign() const override { return "-"; }
    };

    static StringVal do_money_format(FunctionContext* context, const std::string& v) {
        std::locale comma_locale(std::locale(), new CommaMoneypunct());
        std::stringstream ss;
        ss.imbue(comma_locale);
        ss << std::put_money(v);
        return AnyValUtil::from_string_temp(context, ss.str());
    };

    static StringVal split_part(FunctionContext* context, const StringVal& content, const StringVal& delimiter,
                                const IntVal& field);
};
} // namespace starrocks

#endif
