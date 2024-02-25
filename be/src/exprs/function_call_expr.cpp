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

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/anyval_util.h"
#include "exprs/builtin_functions.h"
#include "exprs/expr_context.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/user_function_cache.h"
#include "storage/rowset/bloom_filter.h"
#include "util/failpoint/fail_point.h"
#include "util/slice.h"
#include "util/utf8.h"

namespace starrocks {

VectorizedFunctionCallExpr::VectorizedFunctionCallExpr(const TExprNode& node) : Expr(node) {}

Status VectorizedFunctionCallExpr::prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));

    if (!_fn.__isset.fid) {
        return Status::InternalError("Vectorized engine doesn't implement function " + _fn.name.function_name);
    }

    _fn_desc = BuiltinFunctions::find_builtin_function(_fn.fid);

    if (_fn_desc == nullptr || _fn_desc->scalar_function == nullptr) {
        return Status::InternalError("Vectorized engine doesn't implement function " + _fn.name.function_name);
    }

    if (_fn_desc->args_nums > _children.size()) {
        return Status::InternalError(strings::Substitute("Vectorized function $0 requires $1 arguments but given $2",
                                                         _fn.name.function_name, _fn_desc->args_nums,
                                                         _children.size()));
    }

    FunctionContext::TypeDesc return_type = AnyValUtil::column_type_to_type_desc(_type);
    std::vector<FunctionContext::TypeDesc> args_types;

    for (Expr* child : _children) {
        args_types.push_back(AnyValUtil::column_type_to_type_desc(child->type()));
    }

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
        std::vector<ColumnPtr> const_columns;
        const_columns.reserve(_children.size());
        for (const auto& child : _children) {
            ASSIGN_OR_RETURN(auto&& child_col, child->evaluate_const(context))
            const_columns.emplace_back(std::move(child_col));
        }
        fn_ctx->set_constant_columns(std::move(const_columns));
    }

    if (_fn_desc->prepare_function != nullptr) {
        FAIL_POINT_TRIGGER_RETURN_ERROR(random_error);
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            RETURN_IF_ERROR(_fn_desc->prepare_function(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
        }

        RETURN_IF_ERROR(_fn_desc->prepare_function(fn_ctx, FunctionContext::THREAD_LOCAL));
    }

    // Todo: We will use output_scale in the result_writer to format the
    //  output in row engine, but we need set output scale in vectorized engine?
    if (_fn.name.function_name == "round" && _type.type == TYPE_DOUBLE) {
        if (_children[1]->is_constant()) {
            ASSIGN_OR_RETURN(ColumnPtr ptr, _children[1]->evaluate_checked(context, nullptr));
            _output_scale =
                    std::static_pointer_cast<Int32Column>(std::static_pointer_cast<ConstColumn>(ptr)->data_column())
                            ->get_data()[0];
        }
    }

    return Status::OK();
}

void VectorizedFunctionCallExpr::close(starrocks::RuntimeState* state, starrocks::ExprContext* context,
                                       FunctionContext::FunctionStateScope scope) {
    if (_fn_desc != nullptr && _fn_desc->close_function != nullptr) {
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
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(context, child, ptr);
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
    if (ngram_state == nullptr) {
        ngram_state = std::make_unique<NgramBloomFilterState>();
        std::vector<Slice>& ngram_set = ngram_state->ngram_set;
        bool index_useful;
        if (_fn_desc->name == "LIKE") {
            index_useful = split_like_string_to_ngram(fn_ctx, reader_options, ngram_set);
        } else {
            index_useful = split_normal_string_to_ngram(fn_ctx, reader_options, ngram_state.get(), _fn_desc->name);
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
            if (!bf->test_bytes(ngram.get_data(), ngram.get_size())) {
                return false;
            }
        }
        // if all ngram in needle hit bf, this page may have something to do with needle, so don't filter it
        return true;
    } else {
        for (auto& ngram : ngram_state->ngram_set) {
            // if any ngram in needle hit bf, this page may have something to do with needle, so don't filter it
            if (bf->test_bytes(ngram.get_data(), ngram.get_size())) {
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
bool VectorizedFunctionCallExpr::split_normal_string_to_ngram(FunctionContext* fn_ctx,
                                                              const NgramBloomFilterReaderOptions& reader_options,
                                                              NgramBloomFilterState* ngram_state,
                                                              const string& func_name) const {
    size_t index_gram_num = reader_options.index_gram_num;
    bool index_case_sensitive = reader_options.index_case_sensitive;
    std::vector<Slice>& ngram_set = ngram_state->ngram_set;

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

    // checked in support_ngram_bloom_filter(size_t gram_num), so it 's safe to get const column's value
    Slice needle;
    const auto& needle_column = fn_ctx->get_constant_column(1);

    if (index_case_sensitive) {
        needle = ColumnHelper::get_const_value<TYPE_VARCHAR>(needle_column);
    } else {
        // for case_insensitive, we need to convert needle to lower case
        std::string& buf = ngram_state->buffer;
        needle = ColumnHelper::get_const_value<TYPE_VARCHAR>(needle_column).tolower(buf);
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
        Slice cur_ngram = Slice(needle.data + index[j], cur_ngram_length);

        ngram_set.push_back(cur_ngram);
    }
    // case like "ngram_search(col, "nee", 3) when col has a 4gram bloom filter, don't use this index
    if (ngram_set.empty()) return false;
    return true;
}

bool VectorizedFunctionCallExpr::split_like_string_to_ngram(FunctionContext* fn_ctx,
                                                            const NgramBloomFilterReaderOptions& reader_options,
                                                            std::vector<Slice>& ngram_set) const {
    size_t index_gram_num = reader_options.index_gram_num;
    auto needle_column = fn_ctx->get_constant_column(1);
    if (needle_column == nullptr) {
        return false;
    }

    Slice needle = ColumnHelper::get_const_value<TYPE_VARCHAR>(needle_column);

    size_t cur_valid_grams_num = 0;
    size_t cur_grams_begin_index = 0;
    bool escaped = false;

    // when iteration begin,[cur_grams_begin_index, i) is the current ngram
    // cur_valid_grams_num is the number of utf-8 gram in slice[cur_grams_begin_index, i)
    // escaped means needle[i - 1] is '\\'
    for (size_t i = 0; i < needle.size;) {
        if (escaped && (needle[i] == '%' || needle[i] == '_' || needle[i] == '\\')) {
            ++cur_valid_grams_num;
            escaped = false;
            ++i;
        } else if (!escaped && (needle[i] == '%' || needle[i] == '_')) {
            cur_valid_grams_num = 0;
            escaped = false;
            ++i;
            cur_grams_begin_index = i;
        } else if (!escaped && needle[i] == '\\') {
            escaped = true;
            ++i;
        } else {
            size_t cur_gram_length = UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(needle.data[i])];
            i += cur_gram_length;
            ++cur_valid_grams_num;
            escaped = false;
        }

        if (cur_valid_grams_num == index_gram_num) {
            ngram_set.emplace_back(needle.data + cur_grams_begin_index, i - cur_grams_begin_index);
            cur_valid_grams_num = 0;
            cur_grams_begin_index = i;
        }
    }
    // case like "like(col, "nee") when col has a 4gram bloom filter, don't use this index
    if (ngram_set.empty()) return false;
    return true;
}
} // namespace starrocks
