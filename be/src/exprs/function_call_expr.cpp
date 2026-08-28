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

#include "exprs/function_call_expr.h"

#include <cstdint>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/builtin_functions.h"
#include "exprs/expr_context.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/user_function_cache.h"
#include "types/logical_type.h"
#include "util/bloom_filter.h"
#include "util/failpoint/fail_point.h"
#include "util/slice.h"
#include "util/utf8.h"

namespace starrocks {

DEFINE_FAIL_POINT(expr_prepare_failed);
DEFINE_FAIL_POINT(expr_prepare_fragment_local_call_failed);
DEFINE_FAIL_POINT(expr_prepare_fragment_thread_local_call_failed);

VectorizedFunctionCallExpr::VectorizedFunctionCallExpr(const TExprNode& node) : Expr(node) {}

const FunctionDescriptor* VectorizedFunctionCallExpr::_get_function_by_fid(const TFunction& fn) {
    // branch-3.0 is 150102~150104, branch-3.1 is 150103~150105
    // refs: https://github.com/StarRocks/starrocks/pull/17803
    // @todo: remove this code when branch-3.0 is deprecated
    int64_t fid = fn.fid;
    if (fn.fid == 150102 && _type.type == TYPE_ARRAY && _type.children[0].type == TYPE_DECIMAL32) {
        fid = 150103;
    } else if (fn.fid == 150103 && _type.type == TYPE_ARRAY && _type.children[0].type == TYPE_DECIMAL64) {
        fid = 150104;
    } else if (fn.fid == 150104 && _type.type == TYPE_ARRAY && _type.children[0].type == TYPE_DECIMAL128) {
        fid = 150105;
    }
    return BuiltinFunctions::find_builtin_function(fid);
}

const FunctionDescriptor* VectorizedFunctionCallExpr::_get_function(const TFunction& fn,
                                                                    const std::vector<TypeDescriptor>& arg_types,
                                                                    const TypeDescriptor& return_type,
                                                                    std::vector<bool> arg_nullables) {
    if (fn.__isset.agg_state_desc) {
        // For _state combinator function, it's created according to the agg_state_desc rather than fid.
        auto agg_state_desc = AggStateDesc::from_thrift(fn.agg_state_desc);
        _agg_state_func = std::make_shared<AggStateFunction>(agg_state_desc, return_type, std::move(arg_nullables));
        auto execute_func = std::bind(&AggStateFunction::execute, _agg_state_func.get(), std::placeholders::_1,
                                      std::placeholders::_2);
        auto prepare_func = std::bind(&AggStateFunction::prepare, _agg_state_func.get(), std::placeholders::_1,
                                      std::placeholders::_2);
        auto close_func = std::bind(&AggStateFunction::close, _agg_state_func.get(), std::placeholders::_1,
                                    std::placeholders::_2);
        _agg_func_desc = std::make_shared<FunctionDescriptor>(fn.name.function_name, arg_types.size(), execute_func,
                                                              prepare_func, close_func, true, false);
        return _agg_func_desc.get();
    } else {
        return _get_function_by_fid(fn);
    }
}

Status VectorizedFunctionCallExpr::prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));

    // parpare result type and arg types
    FunctionContext::TypeDesc return_type = _type;
    if (!_fn.__isset.fid && !_fn.__isset.agg_state_desc) {
        return Status::InternalError("Vectorized engine doesn't implement agg state function " +
                                     _fn.name.function_name);
    }
    std::vector<FunctionContext::TypeDesc> args_types;
    std::vector<bool> arg_nullblaes;
    for (Expr* child : _children) {
        args_types.push_back(child->type());
        arg_nullblaes.emplace_back(child->is_nullable());
    }

    // initialize function descriptor
    _fn_desc = _get_function(_fn, args_types, return_type, arg_nullblaes);
    if (_fn_desc == nullptr || _fn_desc->scalar_function == nullptr) {
        return Status::InternalError("Vectorized engine doesn't implement function " + _fn.name.function_name);
    }

    if (_fn_desc->args_nums > _children.size()) {
        return Status::InternalError(strings::Substitute("Vectorized function $0 requires $1 arguments but given $2",
                                                         _fn.name.function_name, _fn_desc->args_nums,
                                                         _children.size()));
    }

    FAIL_POINT_TRIGGER_RETURN_ERROR(random_error);
    FAIL_POINT_TRIGGER_RETURN_ERROR(expr_prepare_failed);

    // todo: varargs use for allocate slice memory, need compute buffer size
    //  for varargs in vectorized engine?
    _fn_context_index = context->register_func(state, return_type, args_types);

    _is_returning_random_value = _fn.fid == 10300 /* rand */ || _fn.fid == 10301 /* random */ ||
                                 _fn.fid == 10302 /* rand */ || _fn.fid == 10303 /* random */ ||
                                 _fn.fid == 100015 /* uuid */ || _fn.fid == 100016 /* uniq_id */;

    return Status::OK();
}

Status VectorizedFunctionCallExpr::open(starrocks::RuntimeState* state, starrocks::ExprContext* context,
                                        FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, context, scope));

    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);

    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        Columns const_columns;
        const_columns.reserve(_children.size());
        for (const auto& child : _children) {
            ASSIGN_OR_RETURN(auto&& child_col, child->evaluate_const(context))
            const_columns.emplace_back(std::move(child_col));
        }
        fn_ctx->set_constant_columns(std::move(const_columns));
    }

    if (_fn_desc->prepare_function != nullptr) {
        FAIL_POINT_TRIGGER_RETURN_ERROR(random_error);
        FAIL_POINT_TRIGGER_RETURN_ERROR(expr_prepare_fragment_local_call_failed);
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            RETURN_IF_ERROR(_fn_desc->prepare_function(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
        }
        FAIL_POINT_TRIGGER_RETURN_ERROR(expr_prepare_fragment_thread_local_call_failed);
        RETURN_IF_ERROR(_fn_desc->prepare_function(fn_ctx, FunctionContext::THREAD_LOCAL));
    }

    // Todo: We will use output_scale in the result_writer to format the
    //  output in row engine, but we need set output scale in vectorized engine?
    if (_fn.name.function_name == "round" && _type.type == TYPE_DOUBLE) {
        if (_children[1]->is_constant()) {
            ASSIGN_OR_RETURN(ColumnPtr ptr, _children[1]->evaluate_checked(context, nullptr));
            _output_scale = Int32Column::static_pointer_cast(ConstColumn::static_pointer_cast(ptr)->data_column())
                                    ->get_data()[0];
        }
    }

    return Status::OK();
}

void VectorizedFunctionCallExpr::close(starrocks::RuntimeState* state, starrocks::ExprContext* context,
                                       FunctionContext::FunctionStateScope scope) {
    // _fn_context_index >= 0 means this function call has call opened
    if (_fn_desc != nullptr && _fn_desc->close_function != nullptr && _fn_context_index >= 0) {
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        (void)_fn_desc->close_function(fn_ctx, FunctionContext::THREAD_LOCAL);

        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            (void)_fn_desc->close_function(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
        }
    }

    Expr::close(state, context, scope);
}

bool VectorizedFunctionCallExpr::is_constant() const {
    if (_is_returning_random_value) {
        return false;
    }

    return Expr::is_constant();
}

StatusOr<ColumnPtr> VectorizedFunctionCallExpr::evaluate_checked(starrocks::ExprContext* context, Chunk* ptr) {
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);

    Columns args;
    args.reserve(_children.size());
    for (Expr* child : _children) {
        ASSIGN_OR_RETURN(ColumnPtr column, context->evaluate(child, ptr));
        args.emplace_back(column);
    }

    if (_is_returning_random_value) {
        if (ptr != nullptr) {
            args.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(ptr->num_rows(), ptr->num_rows()));
        } else {
            args.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(1, 1));
        }
    }

#ifndef NDEBUG
    if (ptr != nullptr) {
        size_t size = ptr->num_rows();
        // Ensure all columns have the same size
        for (const ColumnPtr& c : args) {
            CHECK_EQ(size, c->size());
        }
    }
#endif

    StatusOr<ColumnPtr> result;
    if (_fn_desc->exception_safe) {
        result = _fn_desc->scalar_function(fn_ctx, args);
    } else {
        SCOPED_SET_CATCHED(false);
        result = _fn_desc->scalar_function(fn_ctx, args);
    }
    RETURN_IF_ERROR(result);
    if (_fn_desc->check_overflow) {
        RETURN_IF_ERROR(result.value()->capacity_limit_reached());
    }

    // For no args function call (pi, e)
    if (result.value()->is_constant() && ptr != nullptr) {
        result.value()->resize(ptr->num_rows());
    }
    RETURN_IF_ERROR(result.value()->unfold_const_children(_type));
    return result;
}

bool VectorizedFunctionCallExpr::ngram_bloom_filter(ExprContext* context, const BloomFilter* bf,
                                                    const NgramBloomFilterReaderOptions& reader_options) const {
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
    std::unique_ptr<NgramBloomFilterState>& ngram_state = fn_ctx->get_ngram_state();

    // initialize ngram_state: determine whether this index useful or not, split needle into ngram_set if useful
    // this is not thread-safe, but every scan thread will has its own ExprContext, so it's ok
    if (ngram_state == nullptr) {
        ngram_state = std::make_unique<NgramBloomFilterState>();
        std::vector<std::string>& ngram_set = ngram_state->ngram_set;
        bool index_useful;

        // checked in support_ngram_bloom_filter(size_t gram_num), so it 's safe to get const column's value

        const auto& needle_column = fn_ctx->get_constant_column(1);
        std::string needle = ColumnHelper::get_const_value<TYPE_VARCHAR>(needle_column).to_string();

        // for case_insensitive, we need to convert needle to lower case
        if (!reader_options.index_case_sensitive) {
            std::transform(needle.begin(), needle.end(), needle.begin(),
                           [](unsigned char c) { return std::tolower(c); });
        }

        if (!simdjson::validate_utf8(needle.data(), needle.size())) {
            index_useful = false;
        } else if (_fn_desc->name == "LIKE") {
            index_useful = split_like_string_to_ngram(needle, reader_options, ngram_set);
        } else {
            index_useful = split_normal_string_to_ngram(needle, fn_ctx, reader_options, ngram_set, _fn_desc->name);
        }
        ngram_state->initialized = true;
        ngram_state->index_useful = index_useful;
    }

    DCHECK(ngram_state != nullptr);
    DCHECK(ngram_state->initialized);

    // this index can not be used to this function
    if (!ngram_state->index_useful) {
        return true;
    }

    // if empty, which means needle is too short, so index_valid should be false
    DCHECK(!ngram_state->ngram_set.empty());
    if (_fn_desc->name == "LIKE") {
        for (auto& ngram : ngram_state->ngram_set) {
            // if any ngram in needle doesn't hit bf, this page has nothing to do with target,so filter it
            if (!bf->test_bytes(ngram.data(), ngram.size())) {
                return false;
            }
        }
        // if all ngram in needle hit bf, this page may have something to do with needle, so don't filter it
        return true;
    } else {
        for (auto& ngram : ngram_state->ngram_set) {
            // if any ngram in needle hit bf, this page may have something to do with needle, so don't filter it
            if (bf->test_bytes(ngram.data(), ngram.size())) {
                return true;
            }
        }
        // if neither ngram in needle hit bf, this page has nothing to do with target,so filter it!
        return false;
    }
}

bool VectorizedFunctionCallExpr::support_ngram_bloom_filter(ExprContext* context) const {
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
    // if second argument is not const, don't support
    if (!fn_ctx->is_notnull_constant_column(1)) {
        return false;
    }

    return _fn_desc->name == "LIKE" || _fn_desc->name == "ngram_search" ||
           _fn_desc->name == "ngram_search_case_insensitive";
}

// return false if this index can not be used, otherwise set ngram_set and return true
bool VectorizedFunctionCallExpr::split_normal_string_to_ngram(const Slice& needle, FunctionContext* fn_ctx,
                                                              const NgramBloomFilterReaderOptions& reader_options,
                                                              std::vector<std::string>& ngram_set,
                                                              const std::string& func_name) {
    size_t index_gram_num = reader_options.index_gram_num;
    bool index_case_sensitive = reader_options.index_case_sensitive;

    auto gram_num_column = fn_ctx->get_constant_column(2);
    if (gram_num_column != nullptr) {
        size_t predicate_gram_num = ColumnHelper::get_const_value<TYPE_INT>(gram_num_column);
        // case like ngram_search(col,"needle", 5) when col has a 4gram bloom filter, don't use this index
        if (index_gram_num != predicate_gram_num) {
            return false;
        }
    }

    // if ngram bloom filter is case_sensitive,but function is case insensitive
    if (index_case_sensitive && func_name == "ngram_search_case_insensitive") {
        return false;
    }

    std::vector<size_t> index;
    size_t slice_gram_num = get_utf8_index(needle, &index);
    // case like "ngram_search
    if (slice_gram_num < index_gram_num) {
        return false;
    }
    ngram_set.reserve(slice_gram_num - index_gram_num + 1);

    size_t j;
    for (j = 0; j + index_gram_num <= slice_gram_num; j++) {
        // find next ngram
        size_t cur_ngram_length = j + index_gram_num < slice_gram_num ? index[j + index_gram_num] - index[j]
                                                                      : needle.get_size() - index[j];
        ngram_set.emplace_back(needle.data + index[j], cur_ngram_length);
    }
    // case like "ngram_search(col, "nee", 3) when col has a 4gram bloom filter, don't use this index
    if (ngram_set.empty()) return false;
    return true;
}

bool VectorizedFunctionCallExpr::split_like_string_to_ngram(const Slice& needle,
                                                            const NgramBloomFilterReaderOptions& reader_options,
                                                            std::vector<std::string>& ngram_set) {
    size_t index_gram_num = reader_options.index_gram_num;

    // below is a window sliding algorithm which consider escaped character
    // cur_grams_begin_index is window's left site, cur_grams_end_index is window's right site
    // in each iteration of while loop, we will keep moving window's right site from cur_grams_begin_index until we find a valid ngram and save it into  ngram_set
    // then move window's left site cur_grams_begin_index to the next utf-8 gram
    size_t cur_grams_begin_index = 0;
    size_t cur_grams_end_index = 0;
    while (cur_grams_end_index < needle.size) {
        size_t cur_valid_grams_num = 0;
        bool escaped = false;
        std::string cur_valid_grams;
        cur_valid_grams.reserve(index_gram_num);
        // when iteration begin,[cur_grams_begin_index, cur_grams_end_index) is the current ngram
        // cur_valid_grams contains the number of utf-8 gram in needle[cur_grams_begin_index, cur_grams_end_index) without '\\'
        // cur_valid_grams_num is the number of utf-8 gram in needle[cur_grams_begin_index, cur_grams_end_index)
        // escaped means needle[cur_grams_end_index - 1] is '\\'
        cur_grams_end_index = cur_grams_begin_index;
        while (cur_grams_end_index < needle.size) {
            if (escaped && (needle[cur_grams_end_index] == '%' || needle[cur_grams_end_index] == '_' ||
                            needle[cur_grams_end_index] == '\\')) {
                cur_valid_grams += needle[cur_grams_end_index];
                ++cur_valid_grams_num;
                escaped = false;
                ++cur_grams_end_index;
            } else if (!escaped && (needle[cur_grams_end_index] == '%' || needle[cur_grams_end_index] == '_')) {
                // not enough grams, so move left site of window to need[i+1]
                ++cur_grams_end_index;
                cur_grams_begin_index = cur_grams_end_index;
                break;
            } else if (!escaped && needle[cur_grams_end_index] == '\\') {
                escaped = true;
                ++cur_grams_end_index;
            } else {
                // add next gram into cur_valid_grams
                size_t cur_gram_length =
                        UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(needle.data[cur_grams_end_index])];
                cur_valid_grams.append(&needle[cur_grams_end_index], cur_gram_length);
                cur_grams_end_index += cur_gram_length;
                ++cur_valid_grams_num;
                escaped = false;
            }

            if (cur_valid_grams_num == index_gram_num) {
                // got enough grams, add them to ngram_set and move window's left site(cur_grams_begin_index) to the next utf-8 gram
                ngram_set.push_back(std::move(cur_valid_grams));
                cur_valid_grams.clear();
                cur_valid_grams_num = 0;
                cur_grams_begin_index +=
                        UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(needle.data[cur_grams_begin_index])];
                break;
            }
        }
    }

    // case like "like(col, "nee") when col has a 4gram bloom filter, don't use this index
    if (ngram_set.empty()) return false;
    return true;
}
} // namespace starrocks
