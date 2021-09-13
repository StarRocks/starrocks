// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/new_in_predicate.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXPRS_NEW_IN_PREDICATE_H
#define STARROCKS_BE_SRC_QUERY_EXPRS_NEW_IN_PREDICATE_H

#include <string>

#include "exprs/predicate.h"
#include "udf/udf.h"

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

/* added by lide */
#define IN_FUNCTIONS_STMT(AnyValType, SetType, type_name)                                                  \
    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,                \
                                                   const starrocks_udf::AnyValType& val, int num_args,     \
                                                   const starrocks_udf::AnyValType* args);                 \
                                                                                                           \
    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,            \
                                                       const starrocks_udf::AnyValType& val, int num_args, \
                                                       const starrocks_udf::AnyValType* args);             \
                                                                                                           \
    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,                   \
                                                const starrocks_udf::AnyValType& val, int num_args,        \
                                                const starrocks_udf::AnyValType* args);                    \
                                                                                                           \
    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,               \
                                                    const starrocks_udf::AnyValType& val, int num_args,    \
                                                    const starrocks_udf::AnyValType* args);                \
                                                                                                           \
    static void set_lookup_prepare_##type_name(starrocks_udf::FunctionContext* ctx,                        \
                                               starrocks_udf::FunctionContext::FunctionStateScope scope);  \
                                                                                                           \
    static void set_lookup_close_##type_name(starrocks_udf::FunctionContext* ctx,                          \
                                             starrocks_udf::FunctionContext::FunctionStateScope scope);

namespace starrocks {

/// Predicate for evaluating expressions of the form "val [NOT] IN (x1, x2, x3...)".
//
/// There are two strategies for evaluating the IN predicate:
//
/// 1) SET_LOOKUP: This strategy is for when all the values in the IN list are constant. In
///    the prepare function, we create a set of the constant values from the IN list, and
///    use this set to lookup a given 'val'.
//
/// 2) ITERATE: This is the fallback strategy for when their are non-constant IN list
///    values, or very few values in the IN list. We simply iterate through every
///    expression and compare it to val. This strategy has no prepare function.
//
/// The FE chooses which strategy we should use by choosing the appropriate function (e.g.,
/// in_iterate() or in_set_lookup()). If it chooses SET_LOOKUP, it also sets the appropriate
/// set_lookup_prepare and set_lookup_close functions.
//
/// TODO: the set lookup logic is not yet implemented for DateTimeVals or DecimalVals
class InPredicate {
public:
    static void init();

    /// Functions for every type
    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::BooleanVal& val, int num_args,
                                                const starrocks_udf::BooleanVal* args);

    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::BooleanVal& val, int num_args,
                                                    const starrocks_udf::BooleanVal* args);

    static void set_lookup_prepare_boolean_val(starrocks_udf::FunctionContext* ctx,
                                               starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_close_boolean_val(starrocks_udf::FunctionContext* ctx,
                                             starrocks_udf::FunctionContext::FunctionStateScope scope);

    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::BooleanVal& val, int num_args,
                                                   const starrocks_udf::BooleanVal* args);

    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,
                                                       const starrocks_udf::BooleanVal& val, int num_args,
                                                       const starrocks_udf::BooleanVal* args);

    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::TinyIntVal& val, int num_args,
                                                const starrocks_udf::TinyIntVal* args);

    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::TinyIntVal& val, int num_args,
                                                    const starrocks_udf::TinyIntVal* args);

    static void set_lookup_prepare_tiny_int_val(starrocks_udf::FunctionContext* ctx,
                                                starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_close_tiny_int_val(starrocks_udf::FunctionContext* ctx,
                                              starrocks_udf::FunctionContext::FunctionStateScope scope);

    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::TinyIntVal& val, int num_args,
                                                   const starrocks_udf::TinyIntVal* args);

    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,
                                                       const starrocks_udf::TinyIntVal& val, int num_args,
                                                       const starrocks_udf::TinyIntVal* args);

    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::SmallIntVal& val, int num_args,
                                                const starrocks_udf::SmallIntVal* args);

    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::SmallIntVal& val, int num_args,
                                                    const starrocks_udf::SmallIntVal* args);

    static void set_lookup_prepare_small_int_val(starrocks_udf::FunctionContext* ctx,
                                                 starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_close_small_int_val(starrocks_udf::FunctionContext* ctx,
                                               starrocks_udf::FunctionContext::FunctionStateScope scope);

    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::SmallIntVal& val, int num_args,
                                                   const starrocks_udf::SmallIntVal* args);

    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,
                                                       const starrocks_udf::SmallIntVal& val, int num_args,
                                                       const starrocks_udf::SmallIntVal* args);

    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::IntVal& val, int num_args,
                                                const starrocks_udf::IntVal* args);

    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::IntVal& val, int num_args,
                                                    const starrocks_udf::IntVal* args);

    static void set_lookup_prepare_int_val(starrocks_udf::FunctionContext* ctx,
                                           starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_close_int_val(starrocks_udf::FunctionContext* ctx,
                                         starrocks_udf::FunctionContext::FunctionStateScope scope);

    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::IntVal& val, int num_args,
                                                   const starrocks_udf::IntVal* args);

    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,
                                                       const starrocks_udf::IntVal& val, int num_args,
                                                       const starrocks_udf::IntVal* args);

    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::BigIntVal& val, int num_args,
                                                const starrocks_udf::BigIntVal* args);

    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::BigIntVal& val, int num_args,
                                                    const starrocks_udf::BigIntVal* args);

    static void set_lookup_prepare_big_int_val(starrocks_udf::FunctionContext* ctx,
                                               starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_close_big_int_val(starrocks_udf::FunctionContext* ctx,
                                             starrocks_udf::FunctionContext::FunctionStateScope scope);

    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::BigIntVal& val, int num_args,
                                                   const starrocks_udf::BigIntVal* args);

    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,
                                                       const starrocks_udf::BigIntVal& val, int num_args,
                                                       const starrocks_udf::BigIntVal* args);

    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::FloatVal& val, int num_args,
                                                const starrocks_udf::FloatVal* args);

    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::FloatVal& val, int num_args,
                                                    const starrocks_udf::FloatVal* args);

    static void set_lookup_prepare_float_val(starrocks_udf::FunctionContext* ctx,
                                             starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_close_float_val(starrocks_udf::FunctionContext* ctx,
                                           starrocks_udf::FunctionContext::FunctionStateScope scope);

    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::FloatVal& val, int num_args,
                                                   const starrocks_udf::FloatVal* args);

    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,
                                                       const starrocks_udf::FloatVal& val, int num_args,
                                                       const starrocks_udf::FloatVal* args);

    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::DoubleVal& val, int num_args,
                                                const starrocks_udf::DoubleVal* args);

    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::DoubleVal& val, int num_args,
                                                    const starrocks_udf::DoubleVal* args);

    static void set_lookup_prepare_double_val(starrocks_udf::FunctionContext* ctx,
                                              starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_close_double_val(starrocks_udf::FunctionContext* ctx,
                                            starrocks_udf::FunctionContext::FunctionStateScope scope);

    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::DoubleVal& val, int num_args,
                                                   const starrocks_udf::DoubleVal* args);

    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,
                                                       const starrocks_udf::DoubleVal& val, int num_args,
                                                       const starrocks_udf::DoubleVal* args);

    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::StringVal& val, int num_args,
                                                const starrocks_udf::StringVal* args);

    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::StringVal& val, int num_args,
                                                    const starrocks_udf::StringVal* args);

    static void set_lookup_prepare_string_val(starrocks_udf::FunctionContext* ctx,
                                              starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_close_string_val(starrocks_udf::FunctionContext* ctx,
                                            starrocks_udf::FunctionContext::FunctionStateScope scope);

    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::StringVal& val, int num_args,
                                                   const starrocks_udf::StringVal* args);

    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,
                                                       const starrocks_udf::StringVal& val, int num_args,
                                                       const starrocks_udf::StringVal* args);

    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::DateTimeVal& val, int num_args,
                                                const starrocks_udf::DateTimeVal* args);

    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::DateTimeVal& val, int num_args,
                                                    const starrocks_udf::DateTimeVal* args);

    static void set_lookup_prepare_datetime_val(starrocks_udf::FunctionContext* ctx,
                                                starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_close_datetime_val(starrocks_udf::FunctionContext* ctx,
                                              starrocks_udf::FunctionContext::FunctionStateScope scope);

    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::DateTimeVal& val, int num_args,
                                                   const starrocks_udf::DateTimeVal* args);

    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,
                                                       const starrocks_udf::DateTimeVal& val, int num_args,
                                                       const starrocks_udf::DateTimeVal* args);

    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::DecimalVal& val, int num_args,
                                                const starrocks_udf::DecimalVal* args);

    static starrocks_udf::BooleanVal in_iterate(starrocks_udf::FunctionContext* context,
                                                const starrocks_udf::DecimalV2Val& val, int num_args,
                                                const starrocks_udf::DecimalV2Val* args);

    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::DecimalVal& val, int num_args,
                                                    const starrocks_udf::DecimalVal* args);

    static starrocks_udf::BooleanVal not_in_iterate(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::DecimalV2Val& val, int num_args,
                                                    const starrocks_udf::DecimalV2Val* args);

    static void set_lookup_prepare_decimal_val(starrocks_udf::FunctionContext* ctx,
                                               starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_prepare_decimalv2_val(starrocks_udf::FunctionContext* ctx,
                                                 starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_close_decimal_val(starrocks_udf::FunctionContext* ctx,
                                             starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void set_lookup_close_decimalv2_val(starrocks_udf::FunctionContext* ctx,
                                               starrocks_udf::FunctionContext::FunctionStateScope scope);

    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::DecimalVal& val, int num_args,
                                                   const starrocks_udf::DecimalVal* args);

    static starrocks_udf::BooleanVal in_set_lookup(starrocks_udf::FunctionContext* context,
                                                   const starrocks_udf::DecimalV2Val& val, int num_args,
                                                   const starrocks_udf::DecimalV2Val* args);

    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,
                                                       const starrocks_udf::DecimalVal& val, int num_args,
                                                       const starrocks_udf::DecimalVal* args);

    static starrocks_udf::BooleanVal not_in_set_lookup(starrocks_udf::FunctionContext* context,
                                                       const starrocks_udf::DecimalV2Val& val, int num_args,
                                                       const starrocks_udf::DecimalV2Val* args);

    /* added by lide */
    IN_FUNCTIONS_STMT(LargeIntVal, __int128, large_int_val)

private:
    friend class InPredicateBenchmark;

    enum Strategy {
        /// Indicates we should use SetLookUp().
        SET_LOOKUP,
        /// Indicates we should use Iterate().
        ITERATE
    };

    template <typename SetType>
    struct SetLookupState {
        /// If true, there is at least one NULL constant in the IN list.
        bool contains_null;

        /// The set of all non-NULL constant values in the IN list.
        /// Note: boost::unordered_set and std::binary_search performed worse based on the
        /// in-predicate-benchmark
        std::set<SetType> val_set;

        /// The type of the arguments
        const FunctionContext::TypeDesc* type;
    };

    /// The templated function that provides the implementation for all the In() and NotIn()
    /// functions.
    template <typename T, typename SetType, bool not_in, Strategy strategy>
    static inline starrocks_udf::BooleanVal templated_in(starrocks_udf::FunctionContext* context, const T& val,
                                                         int num_args, const T* args);

    /// Initializes an SetLookupState in ctx.
    template <typename T, typename SetType>
    static void set_lookup_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

    template <typename SetType>
    static void set_lookup_close(FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

    /// Looks up v in state->val_set.
    template <typename T, typename SetType>
    static BooleanVal set_lookup(SetLookupState<SetType>* state, const T& v);

    /// Iterates through each vararg looking for val. 'type' is the type of 'val' and 'args'.
    template <typename T>
    static BooleanVal iterate(const FunctionContext::TypeDesc* type, const T& val, int num_args, const T* args);
};

} // namespace starrocks

#endif
