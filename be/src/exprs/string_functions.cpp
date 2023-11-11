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

#include "exprs/string_functions.h"

#include <hs/hs.h>
#ifdef __x86_64__
#include <immintrin.h>
#include <mmintrin.h>
#endif
#include <re2/re2.h>

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "common/constexpr.h"
#include "common/status.h"
#include "exprs/binary_function.h"
#include "exprs/math_functions.h"
#include "exprs/unary_function.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/large_int_value.h"
#include "storage/olap_define.h"
#include "util/phmap/phmap.h"
#include "util/raw_container.h"
#include "util/sm3.h"
#include "util/utf8.h"
#include "util/utf8_encoding.h"

namespace starrocks {
// A regex to match any regex pattern is equivalent to a substring search.
static const RE2 SUBSTRING_RE(R"((?:\.\*)*([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]+)(?:\.\*)*)", re2::RE2::Quiet);

#define THROW_RUNTIME_ERROR_IF_EXCEED_LIMIT(col, func_name)                          \
    if (UNLIKELY(col->capacity_limit_reached())) {                                   \
        col->reset_column();                                                         \
        throw std::runtime_error("binary column exceed 4G in function " #func_name); \
    }

#define RETURN_COLUMN(stmt, func_name)                                    \
    auto VARNAME_LINENUM(res) = stmt;                                     \
    THROW_RUNTIME_ERROR_IF_EXCEED_LIMIT(VARNAME_LINENUM(res), func_name); \
    return VARNAME_LINENUM(res);

constexpr size_t CONCAT_SMALL_OPTIMIZE_THRESHOLD = 16 << 20;

// ascii_substr_per_slice can compute substr directly via pointer arithmetics
// if s is an ASCII string, so it is faster than its utf8 counterparts significantly.
//
// template parameters
// - off_off_negative: true if off < 0, otherwise false; used for substr_const and right_const
//   optimization in which cases the off is a constant.
// - allow_out_of_left_bound: for substr and left functions, it's always false; for right function,
//   it's always true, because the n > char_length(s) in right(s, n) is legal and return the
//   entire s.
// - EmptyOp, NonEmptyOp: type of empty_op and type of non_empty_op
// - Args: variadic parameter type for empty_op and non_empty_op
//
// function parameters
// - s, off, len: parameters for substr(s, off, len); when off > 0, from left side on, counting from
//   1 till n (inclusive) locates the start of the substr; when off < 0, scan from right side on,
//   counting from 1 till abs(n) (inclusive) locates the start.
//   based 1 from left side; when off < 0,
// - empty_op: invoked when result is a empty string;
// - non_empty_op: invoked when result is non-empty string;
// - args: extra variadic arguments to emtpy_op and non_empty_op;
template <bool off_is_negative, bool allow_out_of_left_bound, typename EmptyOp, typename NonEmptyOp, typename... Args>
static inline void ascii_substr_per_slice(Slice* s, int off, int len, EmptyOp empty_op, NonEmptyOp non_empty_op,
                                          Args&&... args) {
    const char* begin = s->data;
    auto from_pos = off;
    if constexpr (off_is_negative) {
        // negative offset, count bytes from right side;
        from_pos = from_pos + s->size;
        if constexpr (allow_out_of_left_bound) {
            from_pos = std::max(0, from_pos);
        }
        if (UNLIKELY(from_pos < 0)) {
            empty_op(std::forward<Args>(args)...);
            return;
        }
    } else {
        from_pos -= 1;
        if (UNLIKELY(from_pos >= s->size)) {
            empty_op(std::forward<Args>(args)...);
            return;
        }
    }
    // set end to s.size when end exceeds the string tail.
    auto to_pos = from_pos + len;
    if (to_pos < 0 || to_pos > s->size) {
        to_pos = s->size;
    }
    // In GCC 7.3, inserting uint8_t* iterator into std::vector<uint8_t> is +65% faster than
    // other iterators, such as char*, int8_t* and default(no type casting);
    // but in GCC 10.0.0.2, uint8_t* is very slow.
    non_empty_op((uint8_t*)begin + from_pos, (uint8_t*)begin + to_pos, std::forward<Args>(args)...);
}

// utf8_substr_from_left_per_slice scans utf8 string from left to right to compute the substr in
// case that the origin off argument to substr is positive integer.
//
// template parameters:
// - EmptyOp, NonEmptyOp, Args: type of empty_op, non_empty_op and args.
//
// function parameters:
// - s, off, len: off equals to substract 1 from origin off argument to substr, off = 0 means
//   the first utf8 char of the utf8 string.
// - empty_op: invoked when result is a empty string;
// - non_empty_op: invoked when result is non-empty string;
// - args: extra variadic arguments to emtpy_op and non_empty_op;
template <typename EmptyOp, typename NonEmptyOp, typename... Args>
static inline void utf8_substr_from_left_per_slice(Slice* s, int off, int len, EmptyOp empty_op,
                                                   NonEmptyOp non_empty_op, Args&&... args) {
    const char* begin = s->data;
    const char* end = s->data + s->size;
    const char* from_ptr = skip_leading_utf8(begin, end, off);
    const char* to_ptr = end;
    if (from_ptr >= end) {
        empty_op(std::forward<Args>(args)...);
        return;
    } else {
        to_ptr = skip_leading_utf8(from_ptr, end, len);
    }
    non_empty_op((uint8_t*)from_ptr, (uint8_t*)to_ptr, std::forward<Args>(args)...);
}

// utf8_substr_from_right_per_slice scans utf8 string from right to left to compute the substr in
// case that the origin off argument is negative integer and abs(off) > 32; when abs(off) <= 32,
// a small array on stack is used to compute start pointer in byte string for each utf8 chars, then
// compute substr from this array, for short strings, this optimization has a good performance.
//
// template parameters
// - off_off_negative: true if off < 0, otherwise false; used for substr_const and right_const
//   optimization in which cases the off is a constant.
// - allow_out_of_left_bound: for substr and left functions, it's always false; for right function,
//   it's always true, because the n > char_length(s) in right(s, n) is legal and return the
//   entire s.
// - EmptyOp, NonEmptyOp: type of empty_op and type of non_empty_op
// - Args: variadic parameter type for empty_op and non_empty_op
//
// function parameters
// - s, off, len: off is absolute value of the origin off argument to substr, off = 1 means the
//   last utf8 char of the utf8 string.
// - empty_op: invoked when result is a empty string;
// - non_empty_op: invoked when result is non-empty string;
// - args: extra variadic arguments to emtpy_op and non_empty_op;
template <bool allow_out_of_left_bound, typename EmptyOp, typename NonEmptyOp, typename... Args>
static inline void utf8_substr_from_right_per_slice(Slice* s, int off, int len, EmptyOp empty_op,
                                                    NonEmptyOp non_empty_op, Args&&... args) {
    const char* begin = s->data;
    const char* end = s->data + s->size;
    if (off > s->size) {
        if (allow_out_of_left_bound) {
            non_empty_op((uint8_t*)begin, (uint8_t*)end, std::forward<Args>(args)...);
        } else {
            empty_op(std::forward<Args>(args)...);
        }
        return;
    }

    // if string size is less than SMALL_INDEX_MAX, using small_index is faster than
    // skip_trailing_utf8 significantly; however, in case of string size greater than
    // SMALL_INDEX_MAX, skip_trailing_utf8 is efficient.
    constexpr size_t SMALL_INDEX_MAX = 32;
    uint8_t small_index[SMALL_INDEX_MAX] = {0};
    if (s->size <= SMALL_INDEX_MAX) {
        auto small_index_size = get_utf8_small_index(*s, small_index);
        if (off > small_index_size) {
            if constexpr (allow_out_of_left_bound) {
                non_empty_op((uint8_t*)begin, (uint8_t*)end, std::forward<Args>(args)...);
            } else {
                empty_op(std::forward<Args>(args)...);
            }
            return;
        }
        auto from_idx = small_index_size - off;
        const char* from_ptr = begin + small_index[from_idx];
        const char* to_ptr = end;
        // take the first `len` bytes from the trailing `off` bytes, so if
        // len >= off, at most `off` bytes can be taken.
        auto to_idx = from_idx + len;
        if (len < off) {
            to_ptr = begin + small_index[to_idx];
        }
        non_empty_op((uint8_t*)from_ptr, (uint8_t*)to_ptr, std::forward<Args>(args)...);
    } else {
        const char* from_ptr = skip_trailing_utf8(end, begin, off);
        if (from_ptr < begin) {
            if constexpr (allow_out_of_left_bound) {
                non_empty_op((uint8_t*)begin, (uint8_t*)end, std::forward<Args>(args)...);
            } else {
                empty_op(std::forward<Args>(args)...);
            }
            return;
        }
        const char* to_ptr = end;
        if (len < end - from_ptr) {
            to_ptr = skip_leading_utf8(from_ptr, end, len);
        }
        non_empty_op((uint8_t*)from_ptr, (uint8_t*)to_ptr, std::forward<Args>(args)...);
    }
}

static inline void binary_column_empty_op(Bytes* bytes, Offsets* offsets, size_t i) {
    (*offsets)[i + 1] = bytes->size();
}

static inline void binary_column_non_empty_op(uint8_t* begin, uint8_t* end, Bytes* bytes, Offsets* offsets, size_t i) {
    bytes->insert(bytes->end(), begin, end);
    (*offsets)[i + 1] = bytes->size();
}

template <bool off_is_negative, bool allow_out_of_left_bound>
static inline void ascii_substr(BinaryColumn* src, Bytes* bytes, Offsets* offsets, int off, int len) {
    const auto size = src->size();
    size_t i = 0;
    for (; i < size; ++i) {
        auto s = src->get_slice(i);
        ascii_substr_per_slice<off_is_negative, allow_out_of_left_bound>(&s, off, len, binary_column_empty_op,
                                                                         binary_column_non_empty_op, bytes, offsets, i);
    }
}

static inline void utf8_substr_from_left(BinaryColumn* src, Bytes* bytes, Offsets* offsets, int off, int len) {
    const auto size = src->size();
    size_t i = 0;
    for (; i < size; ++i) {
        auto s = src->get_slice(i);
        utf8_substr_from_left_per_slice(&s, off, len, binary_column_empty_op, binary_column_non_empty_op, bytes,
                                        offsets, i);
    }
}

template <bool allow_out_of_left_bound>
static inline void utf8_substr_from_right(BinaryColumn* src, Bytes* bytes, Offsets* offsets, int off, int len) {
    const auto size = src->size();
    size_t i = 0;
    for (; i < size; ++i) {
        auto s = src->get_slice(i);
        utf8_substr_from_right_per_slice<allow_out_of_left_bound>(&s, off, len, binary_column_empty_op,
                                                                  binary_column_non_empty_op, bytes, offsets, i);
    }
}

Status StringFunctions::sub_str_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = new SubstrState();
    context->set_function_state(scope, state);

    // Don't improve for const string, which is rare case.
    if (context->is_constant_column(0)) {
        return Status::OK();
    }

    // TODO(kks): improve this case if necessary
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    // TODO(kks): improve this case if necessary
    if (context->get_num_args() == 3 && !context->is_notnull_constant_column(2)) {
        return Status::OK();
    }

    state->is_const = true;

    ColumnPtr column_pos = context->get_constant_column(1);
    state->pos = ColumnHelper::get_const_value<TYPE_INT>(column_pos);

    if (context->get_num_args() == 3) {
        ColumnPtr column_len = context->get_constant_column(2);
        state->len = ColumnHelper::get_const_value<TYPE_INT>(column_len);
    }

    return Status::OK();
}

Status unregister_substr_state(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<SubstrState*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

Status StringFunctions::sub_str_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    return unregister_substr_state(context, scope);
}

Status StringFunctions::left_or_right_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }
    // const null case is handled by non_const implementation.
    if (!context->is_notnull_constant_column(0) || !context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    auto len_column = context->get_constant_column(1);
    auto* state = new SubstrState();
    context->set_function_state(scope, state);
    state->is_const = true;
    int len = ColumnHelper::get_const_value<TYPE_INT>(len_column);
    // right_const ignores state->pos, just use state->len
    state->pos = 1;
    state->len = len;
    return Status::OK();
}

Status StringFunctions::left_or_right_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    return unregister_substr_state(context, scope);
}

Status StringFunctions::concat_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }
    auto* state = new ConcatState();
    state->is_const = true;
    state->is_oversize = false;
    const auto num_args = context->get_num_args();
    for (auto i = 1; i < num_args; ++i) {
        // For code simpleness, we only handle const varchar args in the following code.
        // For only null column, we let concat_not_const function handle
        if (!context->is_notnull_constant_column(i)) {
            state->is_const = false;
        }
    }

    context->set_function_state(scope, state);
    if (!state->is_const) {
        return Status::OK();
    }

    std::string tail;
    // size of concatenation of tail columns(i.e. columns except the 1st one)
    // must not exceeds SIZE_LIMIT, otherwise the result is oversize in which
    // case NULL is returned according to mysql.
    raw::make_room(&tail, OLAP_STRING_MAX_LENGTH);
    auto* tail_begin = (uint8_t*)tail.data();
    size_t tail_off = 0;

    for (auto i = 1; i < num_args; ++i) {
        auto const_arg = context->get_constant_column(i);
        auto s = ColumnHelper::get_const_value<TYPE_VARCHAR>(const_arg);
        if (tail_off + s.size > OLAP_STRING_MAX_LENGTH) {
            //oversize
            state->is_oversize = true;
            break;
        }
        strings::memcpy_inlined(tail_begin + tail_off, s.data, s.size);
        tail_off += s.size;
    }
    if (!state->is_oversize) {
        state->tail.assign(tail_begin, tail_begin + tail_off);
    }
    return Status::OK();
}

Status StringFunctions::concat_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<ConcatState*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

static inline void column_builder_null_op(NullableBinaryColumnBuilder* builder, size_t i) {
    builder->set_null(i);
}

static inline void column_builder_empty_op(NullableBinaryColumnBuilder* builder, size_t i) {
    builder->append_empty(i);
}

static inline void column_builder_non_empty_op(uint8_t* begin, uint8_t* end, NullableBinaryColumnBuilder* builder,
                                               size_t i) {
    builder->append(begin, end, i);
}

ColumnPtr substr_const_not_null(const Columns& columns, BinaryColumn* src, SubstrState* state) {
    ColumnPtr result = BinaryColumn::create();
    auto* binary = down_cast<BinaryColumn*>(result.get());
    Bytes& bytes = binary->get_bytes();
    Offsets& offsets = binary->get_offset();
    int len = state->len;
    int off = state->pos;

    const size_t size = src->size();
    // return a vector of NULL if off or len is trivial or invalid
    if (off == INT_MIN || off == 0 || len <= 0) {
        offsets.resize(size + 1, 0);
        return result;
    }

    if (len > 0) {
        // the size of substr result never exceeds the counterpart of the source column.
        size_t reserved = src->get_bytes().size();
        // when start pos is negative, the result of substr take last abs(pos) chars,
        // thus length of the result is less than abs(pos) and len.
        int min_len = len;
        if (off < 0) {
            min_len = std::min(-off, min_len);
        }
        // prevent a big exaggerated or invalid len from causing malfunction.
        if (INT_MAX / min_len > size) {
            reserved = std::min(min_len * size, reserved);
        }
        bytes.reserve(reserved);
    }

    raw::RawVector<Offsets::value_type> raw_offsets;
    raw_offsets.resize(size + 1);
    raw_offsets[0] = 0;
    offsets.swap(reinterpret_cast<Offsets&>(raw_offsets));

    auto& src_bytes = src->get_bytes();
    auto is_ascii = validate_ascii_fast((const char*)src_bytes.data(), src_bytes.size());
    if (is_ascii) {
        if (off > 0) {
            // off_is_negative=false
            // allow_out_of_left_bound=false
            ascii_substr<false, false>(src, &bytes, &offsets, off, len);
        } else {
            // off_is_negative=true
            // allow_out_of_left_bound=false
            ascii_substr<true, false>(src, &bytes, &offsets, off, len);
        }
    } else {
        if (off > 0) {
            utf8_substr_from_left(src, &bytes, &offsets, off - 1, len);
        } else if (off < 0) {
            // allow_out_of_left_bound=false
            utf8_substr_from_right<false>(src, &bytes, &offsets, -off, len);
        }
    }
    return result;
}

ColumnPtr right_const_not_null(const Columns& columns, BinaryColumn* src, SubstrState* state) {
    ColumnPtr result = BinaryColumn::create();
    auto* binary = down_cast<BinaryColumn*>(result.get());
    Bytes& bytes = binary->get_bytes();
    Offsets& offsets = binary->get_offset();
    int len = state->len;

    const size_t size = src->size();

    if (len <= 0) {
        offsets.resize(size + 1);
        return result;
    }

    auto& src_bytes = src->get_bytes();
    const size_t src_bytes_size = src_bytes.size();
    auto reserved = src_bytes_size;
    if (INT_MAX / size > len) {
        reserved = std::min(reserved, size * len);
    }

    bytes.reserve(reserved);
    raw::RawVector<Offsets::value_type> raw_offsets;
    raw_offsets.resize(size + 1);
    raw_offsets[0] = 0;
    offsets.swap(reinterpret_cast<Offsets&>(raw_offsets));
    auto is_ascii = validate_ascii_fast((const char*)src_bytes.data(), src_bytes_size);
    if (is_ascii) {
        // off_is_negative=true, off=-len
        // allow_out_of_left_bound=true
        ascii_substr<true, true>(src, &bytes, &offsets, -len, len);
    } else {
        // allow_out_of_left_bound=true
        utf8_substr_from_right<true>(src, &bytes, &offsets, len, len);
    }
    return result;
}

template <typename StringConstFuncType, typename... Args>
ColumnPtr string_func_const(StringConstFuncType func, const Columns& columns, Args&&... args) {
    if (columns[0]->is_nullable()) {
        auto* src_nullable = down_cast<NullableColumn*>(columns[0].get());
        if (src_nullable->has_null()) {
            auto* src_binary = down_cast<BinaryColumn*>(src_nullable->data_column().get());
            ColumnPtr binary = func(columns, src_binary, std::forward<Args>(args)...);
            NullColumnPtr src_null = NullColumn::create(*(src_nullable->null_column()));

            // - if binary is null ConstColumn, just return it.
            // - if binary is non-null ConstColumn, unfold it and wrap with src_null.
            // - if binary is NullableColumn, then the null column inside the binary should
            //   be merged with the null column inside of columns[0].
            if (binary->only_null()) {
                return binary;
            }
            if (binary->is_constant()) {
                auto* dst_const = down_cast<ConstColumn*>(binary.get());
                dst_const->data_column()->assign(dst_const->size(), 0);
                return NullableColumn::create(dst_const->data_column(), src_null);
            }
            if (binary->is_nullable()) {
                auto* binary_nullable = down_cast<NullableColumn*>(binary.get());
                if (binary_nullable->has_null()) {
                    // case 2: some rows are nulls and some rows are non-nulls, merge the column
                    // inside original result and the null column inside the columns[0].
                    NullColumnPtr binary_null = binary_nullable->null_column();
                    auto union_null = FunctionHelper::union_null_column(src_null, binary_null);
                    return NullableColumn::create(binary_nullable->data_column(), union_null);
                } else {
                    // case 3: any of the result rows is not null, so return the original result.
                    // no merge is needed.
                    return NullableColumn::create(binary_nullable->data_column(), src_null);
                }
            } else {
                return NullableColumn::create(binary, src_null);
            }
        } else {
            auto* src = down_cast<BinaryColumn*>(src_nullable->data_column().get());
            return func(columns, src, std::forward<Args>(args)...);
        }
    } else if (columns[0]->is_constant()) {
        auto* src_constant = down_cast<ConstColumn*>(columns[0].get());
        auto* src_binary = down_cast<BinaryColumn*>(src_constant->data_column().get());
        ColumnPtr binary = func(columns, src_binary, std::forward<Args>(args)...);
        if (binary->is_constant()) {
            return binary;
        } else {
            return ConstColumn::create(binary, src_constant->size());
        }
    } else {
        auto* src = down_cast<BinaryColumn*>(columns[0].get());
        return func(columns, src, std::forward<Args>(args)...);
    }
}

ColumnPtr substr_const(SubstrState* state, const Columns& columns) {
    return string_func_const(substr_const_not_null, columns, state);
}

ColumnPtr right_const(SubstrState* state, const Columns& columns) {
    return string_func_const(right_const_not_null, columns, state);
}

static inline void ascii_substr_not_const(const size_t row_nums, ColumnViewer<TYPE_VARCHAR>* str_viewer,
                                          ColumnViewer<TYPE_INT>* off_viewer, ColumnViewer<TYPE_INT>* len_viewer,
                                          NullableBinaryColumnBuilder* builder) {
    for (int row = 0; row < row_nums; row++) {
        if (str_viewer->is_null(row) || off_viewer->is_null(row) || len_viewer->is_null(row)) {
            column_builder_null_op(builder, row);
            continue;
        }

        auto s = str_viewer->value(row);
        auto off = off_viewer->value(row);
        auto len = len_viewer->value(row);
        if (off == 0 || len <= 0 || s.size == 0) {
            column_builder_empty_op(builder, row);
            continue;
        }
        if (off > 0) {
            // off_is_negative=false
            // allow_out_of_left_bound=false
            ascii_substr_per_slice<false, false>(&s, off, len, column_builder_empty_op, column_builder_non_empty_op,
                                                 builder, row);
        } else {
            // off_is_negative=true
            // allow_out_of_left_bound=false
            ascii_substr_per_slice<true, false>(&s, off, len, column_builder_empty_op, column_builder_non_empty_op,
                                                builder, row);
        }
    }
}

static inline void ascii_right_not_const(const size_t num_rows, ColumnViewer<TYPE_VARCHAR>* str_viewer,
                                         ColumnViewer<TYPE_INT>* len_viewer, NullableBinaryColumnBuilder* builder) {
    for (int row = 0; row < num_rows; row++) {
        if (str_viewer->is_null(row) || len_viewer->is_null(row)) {
            column_builder_null_op(builder, row);
            continue;
        }

        auto s = str_viewer->value(row);
        auto len = len_viewer->value(row);
        if (len <= 0 || s.size == 0) {
            column_builder_empty_op(builder, row);
            continue;
        }
        // off_is_negative=true, off=-len
        // allow_out_of_left_bound=true
        ascii_substr_per_slice<true, true>(&s, -len, len, column_builder_empty_op, column_builder_non_empty_op, builder,
                                           row);
    }
}
static inline void utf8_substr_not_const(const size_t num_rows, ColumnViewer<TYPE_VARCHAR>* str_viewer,
                                         ColumnViewer<TYPE_INT>* off_viewer, ColumnViewer<TYPE_INT>* len_viewer,
                                         NullableBinaryColumnBuilder* builder) {
    for (int row = 0; row < num_rows; row++) {
        if (str_viewer->is_null(row) || off_viewer->is_null(row) || len_viewer->is_null(row)) {
            column_builder_null_op(builder, row);
            continue;
        }
        auto s = str_viewer->value(row);
        auto off = off_viewer->value(row);
        auto len = len_viewer->value(row);
        if (off == INT_MIN || off == 0 || len <= 0 || s.size == 0) {
            column_builder_empty_op(builder, row);
            continue;
        }
        if (off > 0) {
            utf8_substr_from_left_per_slice(&s, off - 1, len, column_builder_empty_op, column_builder_non_empty_op,
                                            builder, row);
        } else {
            utf8_substr_from_right_per_slice<false>(&s, -off, len, column_builder_empty_op, column_builder_non_empty_op,
                                                    builder, row);
        }
    }
}

static inline void utf8_right_not_const(const size_t row_nums, ColumnViewer<TYPE_VARCHAR>* str_viewer,
                                        ColumnViewer<TYPE_INT>* len_viewer, NullableBinaryColumnBuilder* builder) {
    for (int row = 0; row < row_nums; row++) {
        if (str_viewer->is_null(row) || len_viewer->is_null(row)) {
            column_builder_null_op(builder, row);
            continue;
        }
        auto s = str_viewer->value(row);
        auto len = len_viewer->value(row);
        if (len <= 0 || s.size == 0) {
            column_builder_empty_op(builder, row);
            continue;
        }
        utf8_substr_from_right_per_slice<true>(&s, len, len, column_builder_empty_op, column_builder_non_empty_op,
                                               builder, row);
    }
}

static inline ColumnPtr substr_not_const(FunctionContext* context, const starrocks::Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> str_viewer(columns[0]);
    ColumnViewer<TYPE_INT> off_viewer(columns[1]);

    ColumnPtr len_column = nullptr;
    // has length
    if (columns.size() > 2) {
        len_column = columns[2];
    } else {
        len_column = ColumnHelper::create_const_column<TYPE_INT>(INT32_MAX, columns[0]->size());
    }

    ColumnViewer<TYPE_INT> len_viewer(len_column);

    auto data_column = ColumnHelper::get_data_column(columns[0].get());
    auto* src = down_cast<BinaryColumn*>(data_column);

    const auto rows_num = columns[0]->size();
    NullableBinaryColumnBuilder result;
    result.resize(rows_num, src->byte_size());

    Bytes& src_bytes = src->get_bytes();
    auto is_ascii = validate_ascii_fast((const char*)src_bytes.data(), src_bytes.size());
    if (is_ascii) {
        ascii_substr_not_const(rows_num, &str_viewer, &off_viewer, &len_viewer, &result);
    } else {
        utf8_substr_not_const(rows_num, &str_viewer, &off_viewer, &len_viewer, &result);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

static inline ColumnPtr right_not_const(FunctionContext* context, const starrocks::Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> str_viewer(columns[0]);
    ColumnViewer<TYPE_INT> len_viewer(columns[1]);

    auto data_column = ColumnHelper::get_data_column(columns[0].get());
    auto* src = down_cast<BinaryColumn*>(data_column);
    const auto rows_num = columns[0]->size();

    NullableBinaryColumnBuilder result;

    Bytes& src_bytes = src->get_bytes();
    auto is_ascii = validate_ascii_fast((const char*)src_bytes.data(), src_bytes.size());
    result.resize(rows_num, src->byte_size());

    if (is_ascii) {
        ascii_right_not_const(rows_num, &str_viewer, &len_viewer, &result);
    } else {
        utf8_right_not_const(rows_num, &str_viewer, &len_viewer, &result);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

/**
 * @param: [string_value, position, length]
 * @paramType: [BinaryColumn, IntColumn, IntColumn]
 * @return: BinaryColumn
 */
StatusOr<ColumnPtr> StringFunctions::substring(FunctionContext* context, const starrocks::Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto state = reinterpret_cast<SubstrState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state != nullptr && state->is_const) {
        return substr_const(state, columns);
    }
    return substr_not_const(context, columns);
}

// left
// left(s, n) equals to substr(s, 1, n)
StatusOr<ColumnPtr> StringFunctions::left(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    Columns values;
    values.emplace_back(columns[0]);
    values.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(1, columns[0]->size()));
    values.emplace_back(columns[1]);

    return substring(context, values);
}

// right
// right(s, n) equals to substr(s, -n, n) except the case len(s) < n under which
// right(s, n) return the entire s.
StatusOr<ColumnPtr> StringFunctions::right(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto state = reinterpret_cast<SubstrState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state != nullptr && state->is_const) {
        return right_const(state, columns);
    } else {
        return right_not_const(context, columns);
    }
}

// starts_with
DEFINE_BINARY_FUNCTION_WITH_IMPL(starts_withImpl, str, prefix) {
    re2::StringPiece str_sp(str.data, str.size);
    re2::StringPiece prefix_sp(prefix.data, prefix.size);
    return str_sp.starts_with(prefix_sp);
}

StatusOr<ColumnPtr> StringFunctions::starts_with(FunctionContext* context, const Columns& columns) {
    return VectorizedStrictBinaryFunction<starts_withImpl>::evaluate<TYPE_VARCHAR, TYPE_BOOLEAN>(columns[0],
                                                                                                 columns[1]);
}

// ends_with
DEFINE_BINARY_FUNCTION_WITH_IMPL(ends_withImpl, str, suffix) {
    re2::StringPiece str_sp(str.data, str.size);
    re2::StringPiece suffix_sp(suffix.data, suffix.size);
    return str_sp.ends_with(suffix_sp);
}

StatusOr<ColumnPtr> StringFunctions::ends_with(FunctionContext* context, const Columns& columns) {
    return VectorizedStrictBinaryFunction<ends_withImpl>::evaluate<TYPE_VARCHAR, TYPE_BOOLEAN>(columns[0], columns[1]);
}

struct SpaceFunction {
public:
    template <LogicalType Type, LogicalType ResultType>
    static ColumnPtr evaluate(const ColumnPtr& v1) {
        auto len_column = down_cast<Int32Column*>(v1.get());
        auto& len_array = len_column->get_data();
        const auto num_rows = len_column->size();
        NullableBinaryColumnBuilder builder;
        auto& dst_bytes = builder.data_column()->get_bytes();
        auto& dst_offsets = builder.data_column()->get_offset();
        auto& nulls = builder.get_null_data();

        raw::make_room(&dst_offsets, num_rows + 1);
        dst_offsets[0] = 0;
        nulls.resize(num_rows);
        bool has_null = false;
        size_t dst_off = 0;
        for (auto i = 0; i < num_rows; ++i) {
            auto len = len_array[i];
            if (UNLIKELY((uint32_t)len > OLAP_STRING_MAX_LENGTH)) {
                dst_offsets[i + 1] = dst_off;
                has_null = true;
                nulls[i] = 1;
            } else {
                dst_off += len;
                dst_offsets[i + 1] = dst_off;
            }
        }
        dst_bytes.resize(dst_off, ' ');
        builder.set_has_null(has_null);

        RETURN_COLUMN(builder.build(v1->is_constant()), "space");
    }
};

StatusOr<ColumnPtr> StringFunctions::space(FunctionContext* context, const Columns& columns) {
    return VectorizedUnaryFunction<SpaceFunction>::evaluate<TYPE_INT, TYPE_VARCHAR>(columns[0]);
}

// O(logN) repeat algorithm
// repeat('x', 10) is computed as follows:
//
// repeat_times = 10 in binary format is 0b1010. use
// is_odd to record the last bit of the repeat_times, and
// update repeat_times by sra (shift-right-logical) 1.
//
// initialization: copy 1-repitition to dst
// k = 0, is_odd = 0, repeat_times = 0b101;
// dst = 'x'.
//
// round 1: k = 0, is_odd = 0, repeat_times = 0b101;
// copy 2^k = 1 repetition once, because is_odd == 0;
// dst = 'xx'
//
// round 2: k = 1, is_odd = 1, repeat_times = 0b10;
// copy 2^k = 2 repetitions twice, because is_odd == 1;
// dst = 'xxxxxx'.
//
// round 3: k = 2, is_odd = 0, repeat_times = 0b1;
// copy 2^k = 4 repetitions once, because is_odd == 0;
// dst= 'xxxxxxxxxx'.
//
// round 4: k = 3, is_odd = 1, repeat_times = 0;
// when repeat_times becomes 0, the algorithm finishes
//
// k never exceeds position(from right) of the most signifcant bit of the
// repeat_times, so k = log2(repeat_times), for each round, memcpy is
// called for at most twice.  so total memcpy call count is 2log2(repeat_times),
// time complexity is O(logN), drastically decreased call count implies
// more data will be copy and give memcpy more change to speedup via SIMD optimization.
void fast_repeat(uint8_t* dst, const uint8_t* src, size_t src_size, int32_t repeat_times) {
    if (UNLIKELY(repeat_times <= 0)) {
        return;
    }
    uint8_t* dst_begin = dst;
    uint8_t* dst_curr = dst;
    int32_t k = 0;
    int32_t is_odd = repeat_times & 1;
    repeat_times >>= 1;

    strings::memcpy_inlined(dst_curr, src, src_size);
    dst_curr += src_size;
    for (; repeat_times > 0; k += 1, is_odd = repeat_times & 1, repeat_times >>= 1) {
        int32_t len = src_size * (1 << k);
        strings::memcpy_inlined(dst_curr, dst_begin, len);
        dst_curr += len;
        if (is_odd) {
            strings::memcpy_inlined(dst_curr, dst_begin, len);
            dst_curr += len;
        }
    }
}

static inline ColumnPtr repeat_const_not_null(const Columns& columns, const BinaryColumn* src) {
    auto times = ColumnHelper::get_const_value<TYPE_INT>(columns[1]);

    auto& src_offsets = src->get_offset();
    const auto num_rows = src->size();

    NullableBinaryColumnBuilder builder;
    auto& dst_nulls = builder.get_null_data();
    auto& dst_offsets = builder.data_column()->get_offset();
    auto& dst_bytes = builder.data_column()->get_bytes();

    dst_nulls.resize(num_rows);
    bool has_null = false;

    if (times <= 0) {
        dst_offsets.resize(num_rows + 1);
        return builder.build(ColumnHelper::is_all_const(columns));
    } else {
        raw::make_room(&dst_offsets, num_rows + 1);
        dst_offsets[0] = 0;
        size_t reserved = static_cast<size_t>(times) * src_offsets.back();
        if (reserved > OLAP_STRING_MAX_LENGTH * num_rows) {
            reserved = 0;
            for (int i = 0; i < num_rows; ++i) {
                size_t slice_sz = src_offsets[i + 1] - src_offsets[i];
                if (slice_sz * times < OLAP_STRING_MAX_LENGTH) {
                    reserved += slice_sz * times;
                }
            }
        }
        dst_bytes.resize(reserved);
    }

    uint8_t* dst_curr = dst_bytes.data();
    size_t dst_off = 0;
    for (auto i = 0; i < num_rows; ++i) {
        auto s = src->get_slice(i);
        if (s.size == 0) {
            dst_offsets[i + 1] = dst_off;
            continue;
        }
        // if result exceed STRING_MAX_LENGTH
        // return null
        if (s.size * times > OLAP_STRING_MAX_LENGTH) {
            dst_nulls[i] = 1;
            has_null = true;
            dst_offsets[i + 1] = dst_off;
            continue;
        }
        fast_repeat(dst_curr, (uint8_t*)s.data, s.size, times);
        const size_t dst_slice_size = s.size * times;
        dst_curr += dst_slice_size;
        dst_off += dst_slice_size;
        dst_offsets[i + 1] = dst_off;
    }

    dst_bytes.resize(dst_off);
    builder.set_has_null(has_null);
    RETURN_COLUMN(builder.build(ColumnHelper::is_all_const(columns)), "repeat");
}

static inline ColumnPtr repeat_const(const Columns& columns) {
    return string_func_const(repeat_const_not_null, columns);
}

static inline ColumnPtr repeat_not_const(const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> str_viewer(columns[0]);
    ColumnViewer<TYPE_INT> times_viewer(columns[1]);

    const size_t num_rows = columns[0]->size();
    NullableBinaryColumnBuilder builder;
    auto& dst_nulls = builder.get_null_data();
    auto& dst_offsets = builder.data_column()->get_offset();
    auto& dst_bytes = builder.data_column()->get_bytes();
    dst_nulls.resize(num_rows);
    raw::make_room(&dst_offsets, num_rows + 1);
    dst_offsets[0] = 0;

    bool has_null = false;
    size_t dst_off = 0;

    for (int i = 0; i < num_rows; ++i) {
        if (str_viewer.is_null(i) || times_viewer.is_null(i)) {
            dst_nulls[i] = 1;
            has_null = true;
            dst_offsets[i + 1] = dst_off;
            continue;
        }

        if (str_viewer.value(i).size == 0 || times_viewer.value(i) <= 0) {
            dst_offsets[i + 1] = dst_off;
            continue;
        }

        auto s = str_viewer.value(i);
        int32_t n = times_viewer.value(i);

        if (s.size * n > OLAP_STRING_MAX_LENGTH) {
            dst_nulls[i] = 1;
            has_null = true;
            dst_offsets[i + 1] = dst_off;
            continue;
        }

        dst_off += n * s.size;
        dst_offsets[i + 1] = dst_off;
    }

    dst_bytes.resize(dst_off);
    uint8_t* dst_begin = dst_bytes.data();
    for (auto i = 0; i < num_rows; ++i) {
        auto dst_slice_size = dst_offsets[i + 1] - dst_offsets[i];
        if (dst_slice_size == 0) {
            continue;
        }
        auto s = str_viewer.value(i);
        fast_repeat(dst_begin + dst_offsets[i], (uint8_t*)s.data, s.size, dst_slice_size / s.size);
    }
    builder.set_has_null(has_null);
    RETURN_COLUMN(builder.build(ColumnHelper::is_all_const(columns)), "repeat");
}

// repeat
StatusOr<ColumnPtr> StringFunctions::repeat(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    if (columns[1]->is_constant()) {
        return repeat_const(columns);
    } else {
        return repeat_not_const(columns);
    }
}

// ------------------------------------------------------------------------------------
// Methods for TRANSLATE.
// ------------------------------------------------------------------------------------

struct TranslateState {
    using ASCII_MAP = char[0xff];
    using UTF8_MAP = phmap::flat_hash_map<EncodedUtf8Char, EncodedUtf8Char, EncodedUtf8CharHash>;

    static constexpr int SRC_STR_INDEX = 0;
    static constexpr int FROM_STR_INDEX = 1;
    static constexpr int TO_STR_INDEX = 2;

    // The bytes not less than 0b1111'1000 will never occur in a UTF-8 character.
    static constexpr char UNINIT_ASCII = 0b1111'1001;
    static constexpr char DELETED_ASCII = 0b1111'1111;

    bool is_from_and_to_const = false;

    bool is_ascii_map = false;
    ASCII_MAP ascii_map; // effective when is_ascii_map is true.
    UTF8_MAP utf8_map;   // effective when is_ascii_map is false.

    void clear_ascii_map() { std::memset(ascii_map, UNINIT_ASCII, sizeof(ascii_map)); }

    void clear_utf8_map() { utf8_map.clear(); }
};

/**
 * Build the translate map.
 * - If all the characters in `from_str` and `to_str`, it will set `dst_state.is_ascii_map` to true and store the map
 *   to `dst_state.ascii_map`.
 * - Otherwise, it will set `dst_state.is_ascii_map` to false and store the map to `dst_state.utf8_map`.
 * @param from_str the from string.
 * @param to_str the to string.
 * @param dst_state the destination state to store map.
 */
static inline void build_translate_map(const Slice& from_str, const Slice& to_str, TranslateState* dst_state) {
    std::vector<EncodedUtf8Char> encoded_from_values;
    encode_utf8_chars(from_str, &encoded_from_values);

    std::vector<EncodedUtf8Char> encoded_to_values;
    encode_utf8_chars(to_str, &encoded_to_values);

    const bool is_ascii_map = encoded_from_values.size() == from_str.size && encoded_to_values.size() == to_str.size;
    dst_state->is_ascii_map = is_ascii_map;
    if (is_ascii_map) {
        dst_state->clear_ascii_map();

        size_t common_size = std::min(to_str.size, from_str.size);
        int i = 0;
        for (; i < common_size; i++) {
            auto& v = dst_state->ascii_map[static_cast<uint8_t>(from_str[i])];
            if (v == TranslateState::UNINIT_ASCII) {
                v = to_str[i];
            }
        }
        for (; i < from_str.size; i++) {
            auto& v = dst_state->ascii_map[static_cast<uint8_t>(from_str[i])];
            if (v == TranslateState::UNINIT_ASCII) {
                v = TranslateState::DELETED_ASCII;
            }
        }
    } else {
        dst_state->clear_utf8_map();

        size_t common_size = std::min(encoded_to_values.size(), encoded_from_values.size());
        int i = 0;
        for (; i < common_size; i++) {
            dst_state->utf8_map.emplace(encoded_from_values[i], encoded_to_values[i]);
        }
        for (; i < encoded_from_values.size(); i++) {
            dst_state->utf8_map.emplace(encoded_from_values[i], EncodedUtf8Char{});
        }
    }
}

/**
 * Translate a string by the UTF-8 map.
 *
 * @param src_encoded_values the string to translate, which has already been encoded.
 * @param utf8_map the UTF-8 map.
 * @param dst the destination to store the translated chars. The caller must guarantee there is enough room.
 * @return [is_null, num_bytes].
 * - `is_null` will be true, if the number of translated chars exceeds `OLAP_STRING_MAX_LENGTH`.
 * - `num_bytes`: the number of translated chars.
 */
static inline std::pair<bool, size_t> translate_string_with_utf8_map(
        const std::vector<EncodedUtf8Char>& src_encoded_values, const TranslateState::UTF8_MAP& utf8_map,
        uint8_t* dst) {
    size_t num_bytes = 0;
    Slice dst_utf_char;
    for (const auto& src_encoded_value : src_encoded_values) {
        auto it = utf8_map.find(src_encoded_value);
        if (it == utf8_map.end()) {
            dst_utf_char = Slice(src_encoded_value);
        } else if (!it->second.is_empty()) {
            dst_utf_char = Slice(it->second);
        } else {
            continue;
        }

        if (num_bytes + dst_utf_char.size > OLAP_STRING_MAX_LENGTH) {
            return {true, 0};
        }
        strings::memcpy_inlined(dst + num_bytes, dst_utf_char.data, dst_utf_char.size);
        num_bytes += dst_utf_char.size;
    }

    return {false, num_bytes};
}

/**
 * Translate a string by the ASCII map.
 *
 * @param src the string to translate.
 * @param ascii_map the ASCII map.
 * @param dst the destination to store the translated chars. The caller must guarantee there is enough room.
 * @return the number of translated chars.
 */
static inline size_t translate_string_with_ascii_map(const Slice& src, const TranslateState::ASCII_MAP& ascii_map,
                                                     uint8_t* dst) {
    uint8_t* const dst_begin = dst;
    for (int i = 0; i < src.size; i++) {
        char v = ascii_map[static_cast<uint8_t>(src[i])];
        if (v == TranslateState::UNINIT_ASCII) {
            *(dst++) = src[i];
        } else if (v != TranslateState::DELETED_ASCII) {
            *(dst++) = v;
        }
    }

    return dst - dst_begin;
}

Status StringFunctions::translate_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = new TranslateState();
    context->set_function_state(FunctionContext::FRAGMENT_LOCAL, state);

    // const null case is handled by non_const implementation.
    if (!context->is_notnull_constant_column(TranslateState::FROM_STR_INDEX) ||
        !context->is_notnull_constant_column(TranslateState::TO_STR_INDEX)) {
        return Status::OK();
    }
    state->is_from_and_to_const = true;

    const auto from_str_col = context->get_constant_column(TranslateState::FROM_STR_INDEX);
    const Slice from_str = ColumnHelper::get_const_value<TYPE_CHAR>(from_str_col);

    const auto to_str_col = context->get_constant_column(TranslateState::TO_STR_INDEX);
    const Slice to_str = ColumnHelper::get_const_value<TYPE_CHAR>(to_str_col);

    build_translate_map(from_str, to_str, state);

    return Status::OK();
}

Status StringFunctions::translate_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto state = (TranslateState*)(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

/**
 * Translate strings of `src_column` with the constant ASCII map.
 *
 * The `src_column` can be a non-constant column.
 * The `from_string` and `to_string` column must be constant columns and only contain ASCII characters,
 * to make all the rows able to use the same ASCII map, which is stored in `state`.
 *
 * @param columns the input columns, including `SRC_STR_INDEX`, `FROM_STR_INDEX`, `TO_STR_INDEX`.
 * @param src the source column, which may be de-wrapped from NullableColumn.
 * @param state stores the ASCII map.
 * @return The translated column, which is a non-nullable BinaryColumn.
 */
static inline ColumnPtr translate_with_ascii_const_nonnull_from_and_to(const Columns& columns, BinaryColumn* src,
                                                                       const TranslateState* state) {
    DCHECK(state->is_from_and_to_const);
    DCHECK(state->is_ascii_map);

    auto dst = BinaryColumn::create();
    auto& dst_offsets = dst->get_offset();
    auto& dst_bytes = dst->get_bytes();
    const auto& src_offsets = src->get_offset();

    const size_t num_rows = src->size();
    if (num_rows == 0) {
        return dst;
    }

    const int num_src_bytes = src_offsets.back();
    dst_bytes.resize(num_src_bytes);
    raw::make_room(&dst_offsets, num_rows + 1);
    dst_offsets[0] = 0;

    uint8_t* dst_begin = dst_bytes.data();
    size_t dst_offset = 0;

    const auto& ascii_map = state->ascii_map;
    for (int i = 0; i < num_rows; i++) {
        const Slice s = src->get_slice(i);
        size_t num_row_bytes = translate_string_with_ascii_map(s, ascii_map, dst_begin + dst_offset);
        dst_offset += num_row_bytes;
        dst_offsets[i + 1] = dst_offset;
    }

    dst_bytes.resize(dst_offset);

    return dst;
}

/**
 * Translate strings of `src_column` with the constant UTF-8 map.
 *
 * The `src_column` can be a non-constant column.
 * The `from_string` and `to_string` column must be constant columns and contain non-ASCII characters,
 * to make all the rows able to use the same UTF-8 map, which is stored in `state`.
 *
 * @param columns the input columns, including `SRC_STR_INDEX`, `FROM_STR_INDEX`, `TO_STR_INDEX`.
 * @param src the source column, which may be de-wrapped from NullableColumn.
 * @param state stores the UTF-8 map.
 * @return the translated column, which may be a nullable BinaryColumn.
 *  The row will be null, if it exceeds OLAP_STRING_MAX_LENGTH after translated.
 */
static inline ColumnPtr translate_with_utf8_const_nonnull_from_and_to(const Columns& columns, BinaryColumn* src,
                                                                      const TranslateState* state) {
    DCHECK(state->is_from_and_to_const);
    DCHECK(!state->is_ascii_map);

    NullableBinaryColumnBuilder builder;
    auto& dst_offsets = builder.data_column()->get_offset();
    auto& dst_bytes = builder.data_column()->get_bytes();
    auto& dst_nulls = builder.get_null_data();

    const size_t num_rows = src->size();
    if (num_rows == 0) {
        return builder.build(ColumnHelper::is_all_const(columns));
    }

    const auto& src_offsets = src->get_offset();
    const int num_src_bytes = src_offsets.back();
    // The `dst_bytes` can be at most four times larger than `src_bytes`, as in the worst-case scenario, each
    // `src_bytes` corresponds to a one-byte UTF-8 character, while each dst_bytes is replaced by a four-byte
    // UTF-8 character.
    dst_bytes.reserve(std::min<size_t>(16ULL, num_src_bytes * 4));
    raw::make_room(&dst_offsets, num_rows + 1);
    dst_offsets[0] = 0;
    dst_nulls.resize(num_rows);

    bool has_null = false;
    size_t dst_offset = 0;
    std::vector<EncodedUtf8Char> src_encoded_values;

    const auto& utf8_map = state->utf8_map;
    for (int i = 0; i < num_rows; i++) {
        const Slice s = src->get_slice(i);

        src_encoded_values.clear();
        src_encoded_values.reserve(s.size);
        encode_utf8_chars(s, &src_encoded_values);

        dst_bytes.resize(dst_offset + std::min<size_t>(s.size * 4, OLAP_STRING_MAX_LENGTH));
        uint8_t* dst_begin = dst_bytes.data() + dst_offset;

        const auto [is_null, num_row_bytes] = translate_string_with_utf8_map(src_encoded_values, utf8_map, dst_begin);
        if (is_null) {
            has_null = true;
            dst_offsets[i + 1] = dst_offset;
            dst_nulls[i] = 1;
        } else {
            dst_offset += num_row_bytes;
            dst_offsets[i + 1] = dst_offset;
        }
    }

    dst_bytes.resize(dst_offset);
    builder.set_has_null(has_null);

    RETURN_COLUMN(builder.build(ColumnHelper::is_all_const(columns)), "translate");
}

/**
 * Translate strings of `src_column` with the maps different between the source strings.
 *
 * The `src_column`, `from_string`, `to_string` can be non-constant columns.
 *
 * @param columns the input columns, including `SRC_STR_INDEX`, `FROM_STR_INDEX`, `TO_STR_INDEX`.
 * @param state useless, `state` is useful only for the constant `from_string`, `to_string` for now.
 * @return the translated column, which may be a nullable BinaryColumn.
 *  The row will be null, if it exceeds OLAP_STRING_MAX_LENGTH after translated.
 */
ColumnPtr translate_with_non_const_from_or_to(const Columns& columns, const TranslateState* state) {
    DCHECK(state == nullptr || !state->is_from_and_to_const);

    ColumnViewer<TYPE_VARCHAR> src_viewer(columns[TranslateState::SRC_STR_INDEX]);
    ColumnViewer<TYPE_VARCHAR> from_viewer(columns[TranslateState::FROM_STR_INDEX]);
    ColumnViewer<TYPE_VARCHAR> to_viewer(columns[TranslateState::TO_STR_INDEX]);

    NullableBinaryColumnBuilder builder;
    auto& dst_offsets = builder.data_column()->get_offset();
    auto& dst_bytes = builder.data_column()->get_bytes();
    auto& dst_nulls = builder.get_null_data();

    const size_t num_rows = columns[TranslateState::SRC_STR_INDEX]->size();
    if (num_rows == 0) {
        return builder.build(ColumnHelper::is_all_const(columns));
    }

    const auto& src_offsets = src_viewer.column()->get_offset();
    const int num_src_bytes = src_offsets.back();
    dst_bytes.reserve(std::min<size_t>(16ULL, num_src_bytes * 4));
    raw::make_room(&dst_offsets, num_rows + 1);
    dst_offsets[0] = 0;
    dst_nulls.resize(num_rows);

    bool has_null = false;
    size_t dst_offset = 0;
    std::vector<EncodedUtf8Char> src_encoded_values;
    TranslateState local_state;

    for (int i = 0; i < num_rows; i++) {
        if (src_viewer.is_null(i) || from_viewer.is_null(i) || to_viewer.is_null(i)) {
            has_null = true;
            dst_offsets[i + 1] = dst_offset;
            dst_nulls[i] = 1;
            continue;
        }

        const Slice src = src_viewer.value(i);
        const Slice from_str = from_viewer.value(i);
        const Slice to_str = to_viewer.value(i);

        build_translate_map(from_str, to_str, &local_state);
        if (local_state.is_ascii_map) {
            dst_bytes.resize(dst_offset + src.size);
            size_t num_row_bytes =
                    translate_string_with_ascii_map(src, local_state.ascii_map, dst_bytes.data() + dst_offset);
            dst_offset += num_row_bytes;
            dst_offsets[i + 1] = dst_offset;
        } else {
            src_encoded_values.clear();
            src_encoded_values.reserve(src.size);
            encode_utf8_chars(src, &src_encoded_values);

            dst_bytes.resize(dst_offset + std::min<size_t>(src.size * 4, OLAP_STRING_MAX_LENGTH));
            uint8_t* dst_begin = dst_bytes.data() + dst_offset;

            const auto [is_null, num_row_bytes] =
                    translate_string_with_utf8_map(src_encoded_values, local_state.utf8_map, dst_begin);
            if (is_null) {
                has_null = true;
                dst_offsets[i + 1] = dst_offset;
                dst_nulls[i] = 1;
            } else {
                dst_offset += num_row_bytes;
                dst_offsets[i + 1] = dst_offset;
            }
        }
    }

    dst_bytes.resize(dst_offset);
    builder.set_has_null(has_null);

    RETURN_COLUMN(builder.build(ColumnHelper::is_all_const(columns)), "translate");
}

StatusOr<ColumnPtr> StringFunctions::translate(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto state = (TranslateState*)context->get_function_state(FunctionContext::FRAGMENT_LOCAL);
    if (state != nullptr) {
        if (state->is_from_and_to_const) {
            if (state->is_ascii_map) {
                return string_func_const(translate_with_ascii_const_nonnull_from_and_to, columns, state);
            } else {
                return string_func_const(translate_with_utf8_const_nonnull_from_and_to, columns, state);
            }
        } else {
            return translate_with_non_const_from_or_to(columns, state);
        }
    } else {
        return translate_with_non_const_from_or_to(columns, nullptr);
    }
}

// ------------------------------------------------------------------------------------
// Methods for PAD.
// ------------------------------------------------------------------------------------

Status StringFunctions::pad_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }
    auto* state = new PadState();
    state->is_const = false;
    state->fill_is_const = false;
    context->set_function_state(FunctionContext::FRAGMENT_LOCAL, state);

    // const null case is handled by non_const implementation.
    if (!context->is_notnull_constant_column(2)) {
        return Status::OK();
    }

    auto pad_column = context->get_constant_column(2);

    state->fill_is_const = true;
    state->fill = ColumnHelper::get_const_value<TYPE_VARCHAR>(pad_column);
    state->fill_is_utf8 = state->fill.size > get_utf8_index(state->fill, &state->fill_utf8_index);

    // const null case is handled by non_const implementation.
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    state->is_const = true;
    return Status::OK();
}

Status StringFunctions::pad_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto state = (PadState*)(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

enum PadType { PAD_TYPE_LEFT, PAD_TYPE_RIGHT };
template <PadType pad_type>
static inline ColumnPtr ascii_pad_ascii_const(Columns const& columns, BinaryColumn* src, const uint8_t* fill,
                                              const size_t fill_size, const size_t len) {
    DCHECK(0 < len && len <= OLAP_STRING_MAX_LENGTH);
    DCHECK(fill_size > 0);

    const auto num_rows = src->size();
    auto result = BinaryColumn::create();
    auto& dst_offsets = result->get_offset();
    auto& dst_bytes = result->get_bytes();

    dst_bytes.resize(num_rows * len);
    raw::make_room(&dst_offsets, num_rows + 1);
    dst_offsets[0] = 0;

    uint8_t* dst_begin = dst_bytes.data();
    size_t dst_off = 0;

    for (auto i = 0; i < num_rows; ++i) {
        auto s = src->get_slice(i);
        // src slice is ascii, and slice's size is longer than expect len, so truncate it
        if (s.size >= len) {
            strings::memcpy_inlined(dst_begin + dst_off, s.data, len);
            dst_off += len;
            dst_offsets[i + 1] = dst_off;
            continue;
        }
        const size_t fill_len = len - s.size;

        const size_t fill_times = fill_len / fill_size;
        const size_t fill_rest = fill_len % fill_size;
        // for PAD_TYPE_RIGHT, put src slice leftmost
        if constexpr (pad_type == PAD_TYPE_RIGHT) {
            strings::memcpy_inlined(dst_begin + dst_off, s.data, s.size);
            dst_off += s.size;
        }
        // memcpy paddings.
        fast_repeat(dst_begin + dst_off, fill, fill_size, fill_times);
        dst_off += fill_size * fill_times;
        strings::memcpy_inlined(dst_begin + dst_off, fill, fill_rest);
        dst_off += fill_rest;
        // for PAD_TYPE_LEFT, put src slice rightmost
        if constexpr (pad_type == PAD_TYPE_LEFT) {
            strings::memcpy_inlined(dst_begin + dst_off, s.data, s.size);
            dst_off += s.size;
        }

        dst_offsets[i + 1] = dst_off;
    }
    dst_bytes.resize(dst_off);
    return result;
}

template <bool src_is_utf8, bool fill_is_utf8, PadType pad_type>
static inline ColumnPtr pad_utf8_const(Columns const& columns, BinaryColumn* src, const uint8_t* fill,
                                       const size_t fill_size, const size_t len,
                                       std::vector<size_t> const& fill_utf8_index) {
    static_assert(src_is_utf8 || fill_is_utf8);

    const auto num_rows = src->size();
    NullableBinaryColumnBuilder builder;

    auto& dst_offsets = builder.data_column()->get_offset();
    auto& dst_nulls = builder.get_null_data();

    raw::make_room(&dst_offsets, num_rows + 1);
    dst_offsets[0] = 0;

    Bytes dst_bytes;
    dst_bytes.reserve(16ULL << 20);
    size_t dst_off = 0;
    bool has_null = false;
    const size_t fill_utf8_length = fill_utf8_index.size();
    for (auto i = 0; i < num_rows; ++i) {
        auto s = src->get_slice(i);
        size_t fill_len = 0;
        if constexpr (src_is_utf8) {
            // src slice is utf8, and slice's utf8_length is longer than expect len, so truncate it.
            size_t skipped_chars;
            const char* end = skip_leading_utf8<true>(s.data, s.data + s.size, len, &skipped_chars);
            if (skipped_chars >= len) {
                size_t real_len = end - s.data;
                dst_bytes.resize(dst_off + real_len);
                strings::memcpy_inlined(dst_bytes.data() + dst_off, s.data, real_len);
                dst_off += real_len;
                dst_offsets[i + 1] = dst_off;
                continue;
            }
            fill_len = len - skipped_chars;
        } else {
            // src slice is ascii, and slice's size is longer than expect len, so truncate it
            if (s.size >= len) {
                dst_bytes.resize(dst_off + len);
                strings::memcpy_inlined(dst_bytes.data() + dst_off, s.data, len);
                dst_off += len;
                dst_offsets[i + 1] = dst_off;
                continue;
            }
            fill_len = len - s.size;
        }

        size_t fill_times = 0;
        size_t fill_rest = 0;
        if constexpr (fill_is_utf8) {
            fill_times = fill_len / fill_utf8_length;
            fill_rest = fill_utf8_index[fill_len % fill_utf8_length];
        } else {
            fill_times = fill_len / fill_size;
            fill_rest = fill_len % fill_size;
        }

        size_t dst_slice_size = s.size + fill_times * fill_size + fill_rest;
        // oversize
        if (dst_slice_size > OLAP_STRING_MAX_LENGTH) {
            dst_nulls[i] = 1;
            has_null = true;
            dst_offsets[i + 1] = dst_off;
            continue;
        }

        dst_bytes.resize(dst_off + dst_slice_size);
        const auto dst_begin = dst_bytes.data();
        // for PAD_TYPE_RIGHT, put src slice leftmost
        if constexpr (pad_type == PAD_TYPE_RIGHT) {
            strings::memcpy_inlined(dst_begin + dst_off, s.data, s.size);
            dst_off += s.size;
        }
        // memcpy paddings.
        fast_repeat(dst_begin + dst_off, fill, fill_size, fill_times);
        dst_off += fill_size * fill_times;
        strings::memcpy_inlined(dst_begin + dst_off, fill, fill_rest);
        dst_off += fill_rest;
        // for PAD_TYPE_LEFT, put src slice rightmost
        if constexpr (pad_type == PAD_TYPE_LEFT) {
            strings::memcpy_inlined(dst_begin + dst_off, s.data, s.size);
            dst_off += s.size;
        }
        dst_offsets[i + 1] = dst_off;
    }
    dst_bytes.resize(dst_off);
    builder.data_column()->get_bytes().swap(reinterpret_cast<Bytes&>(dst_bytes));
    builder.set_has_null(has_null);
    return builder.build(ColumnHelper::is_all_const(columns));
}

template <PadType pad_type>
static inline ColumnPtr pad_const_not_null(const Columns& columns, BinaryColumn* src, const PadState* pad_state) {
    auto len = ColumnHelper::get_const_value<TYPE_INT>(columns[1]);
    auto fill = ColumnHelper::get_const_value<TYPE_VARCHAR>(columns[2]);

    // illegal length  or too-big length, return NULL
    if (len < 0 || len > OLAP_STRING_MAX_LENGTH) {
        return ColumnHelper::create_const_null_column(columns[1]->size());
    }
    // len == 0, return empty string
    if (len == 0) {
        return ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(), columns[1]->size());
    }
    // pad.size == 0, return substr(s, 1, len) according to Snowflake
    if (fill.size == 0) {
        SubstrState state = {.is_const = true, .pos = 1, .len = len};
        return substr_const_not_null(columns, src, &state);
    }
    auto& src_bytes = src->get_bytes();
    auto src_is_utf8 = !validate_ascii_fast((const char*)src_bytes.data(), src_bytes.size());
    if (src_is_utf8 && pad_state->fill_is_utf8) {
        return pad_utf8_const<true, true, pad_type>(columns, src, (uint8_t*)fill.data, fill.size, len,
                                                    pad_state->fill_utf8_index);
    } else if (src_is_utf8) {
        return pad_utf8_const<true, false, pad_type>(columns, src, (uint8_t*)fill.data, fill.size, len,
                                                     pad_state->fill_utf8_index);
    } else if (pad_state->fill_is_utf8) {
        return pad_utf8_const<false, true, pad_type>(columns, src, (uint8_t*)fill.data, fill.size, len,
                                                     pad_state->fill_utf8_index);
    } else {
        return ascii_pad_ascii_const<pad_type>(columns, src, (uint8_t*)fill.data, fill.size, len);
    }
}

template <PadType pad_type>
ColumnPtr pad_const(const Columns& columns, const PadState* state) {
    return string_func_const(pad_const_not_null<pad_type>, columns, state);
}

template <bool src_is_ascii, bool pad_is_const, PadType pad_type>
ColumnPtr pad_not_const(const Columns& columns, [[maybe_unused]] const PadState* state) {
    ColumnViewer<TYPE_VARCHAR> str_viewer(columns[0]);
    ColumnViewer<TYPE_INT> len_viewer(columns[1]);
    ColumnViewer<TYPE_VARCHAR> fill_viewer(columns[2]);

    std::vector<size_t> non_const_fill_index;
    const std::vector<size_t>* fill_index = nullptr;
    Slice non_const_fill;
    const Slice* fill = nullptr;
    // pad_is_const, compute pad_utf8_index once in pad_prepare
    if constexpr (pad_is_const) {
        fill_index = &state->fill_utf8_index;
        fill = &state->fill;
    } else {
        fill_index = &non_const_fill_index;
        fill = &non_const_fill;
    }

    const auto num_rows = columns[0]->size();
    NullableBinaryColumnBuilder builder;
    builder.resize(num_rows, 0);
    auto& dst_offsets = builder.data_column()->get_offset();
    auto& dst_nulls = builder.get_null_data();
    Bytes dst_bytes;
    dst_bytes.reserve(16ULL << 20);

    size_t dst_off = 0;
    bool has_null = false;
    for (int i = 0; i < num_rows; ++i) {
        // NULL if any [str, len, pad] is NULL
        if (str_viewer.is_null(i) || len_viewer.is_null(i) || fill_viewer.is_null(i)) {
            has_null = true;
            dst_offsets[i + 1] = dst_off;
            dst_nulls[i] = 1;
            continue;
        }

        int len = len_viewer.value(i);
        // NULL if len < 0 || len > OLAP_STRING_MAX_LENGTH
        if ((uint32)len > OLAP_STRING_MAX_LENGTH) {
            has_null = true;
            dst_offsets[i + 1] = dst_off;
            dst_nulls[i] = 1;
            continue;
        }
        // empty string if len = 0
        if (len == 0) {
            dst_offsets[i + 1] = dst_off;
            continue;
        }
        auto str = str_viewer.value(i);
        // compute utf8 index per slice if pad_is_const is false
        if constexpr (!pad_is_const) {
            non_const_fill = fill_viewer.value(i);
            non_const_fill_index.clear();
            get_utf8_index(*fill, &non_const_fill_index);
        }

        size_t fill_len = 0;
        if constexpr (src_is_ascii) {
            // ASCII case: len is less than str.size
            if (str.size >= len) {
                dst_bytes.resize(dst_off + len);
                strings::memcpy_inlined(dst_bytes.data() + dst_off, str.data, len);
                dst_off += len;
                dst_offsets[i + 1] = dst_off;
                continue;
            }
            fill_len = len - str.size;
        } else {
            // UTF8 case: len is less than utf8_length(str)
            size_t skipped_chars = 0;
            const auto end = skip_leading_utf8<true>(str.data, str.data + str.size, len, &skipped_chars);
            if (skipped_chars >= len) {
                const auto real_len = end - str.data;
                dst_bytes.resize(dst_off + real_len);
                strings::memcpy_inlined(dst_bytes.data() + dst_off, str.data, real_len);
                dst_off += real_len;
                dst_offsets[i + 1] = dst_off;
                continue;
            }
            fill_len = len - skipped_chars;
        }

        const size_t fill_size = fill_index->size();
        // if pad_size = 0, return left(str, len);
        if (fill_size == 0) {
            dst_bytes.resize(dst_off + str.size);
            strings::memcpy_inlined(dst_bytes.data() + dst_off, str.data, str.size);
            dst_off += str.size;
            dst_offsets[i + 1] = dst_off;
            continue;
        }

        const size_t fill_times = fill_len / fill_size;
        const size_t fill_rest = (*fill_index)[fill_len % fill_size];
        const size_t dst_slice_size = str.size + fill->size * fill_times + fill_rest;

        // result is oversize, return NULL
        if (dst_slice_size > OLAP_STRING_MAX_LENGTH) {
            has_null = true;
            dst_offsets[i + 1] = dst_off;
            dst_nulls[i] = 1;
            continue;
        }

        DCHECK(dst_bytes.size() == dst_off);
        dst_bytes.resize(dst_off + dst_slice_size);
        const auto dst_begin = dst_bytes.data();
        // put str to leftmost if pad_type is PAD_TYPE_RIGHT
        if constexpr (pad_type == PAD_TYPE_RIGHT) {
            strings::memcpy_inlined(dst_begin + dst_off, str.data, str.size);
            dst_off += str.size;
        }
        // copy the entire pad for the pad_times rounds
        fast_repeat((uint8_t*)dst_begin + dst_off, (uint8_t*)fill->data, fill->size, fill_times);
        dst_off += fill->size * fill_times;
        // copy the prefix of pad string for the last round
        strings::memcpy_inlined((uint8_t*)dst_begin + dst_off, (uint8_t*)fill->data, fill_rest);
        dst_off += fill_rest;
        // put str to rightmost if pad_type if PAD_TYPE_LEFT
        if constexpr (pad_type == PAD_TYPE_LEFT) {
            strings::memcpy_inlined(dst_begin + dst_off, str.data, str.size);
            dst_off += str.size;
        }
        dst_offsets[i + 1] = dst_off;
    }
    DCHECK(dst_bytes.size() == dst_off);
    builder.data_column()->get_bytes().swap(reinterpret_cast<Bytes&>(dst_bytes));
    builder.set_has_null(has_null);
    return builder.build(ColumnHelper::is_all_const(columns));
}

template <bool pad_is_const, PadType pad_type>
ColumnPtr pad_not_const_check_ascii(const Columns& columns, [[maybe_unused]] const PadState* state) {
    auto src = ColumnHelper::get_binary_column(columns[0].get());
    auto& bytes = src->get_bytes();
    auto is_ascii = validate_ascii_fast((const char*)bytes.data(), bytes.size());
    if (is_ascii) {
        return pad_not_const<true, pad_is_const, pad_type>(columns, state);
    } else {
        return pad_not_const<false, pad_is_const, pad_type>(columns, state);
    }
}
// pad
template <PadType pad_type>
static ColumnPtr pad(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto state = (PadState*)context->get_function_state(FunctionContext::FRAGMENT_LOCAL);
    if (state != nullptr) {
        if (state->is_const) {
            return pad_const<pad_type>(columns, state);
        } else if (state->fill_is_const) {
            return pad_not_const_check_ascii<true, pad_type>(columns, state);
        } else {
            return pad_not_const_check_ascii<false, pad_type>(columns, nullptr);
        }
    } else {
        return pad_not_const_check_ascii<false, pad_type>(columns, nullptr);
    }
}

// lpad
StatusOr<ColumnPtr> StringFunctions::lpad(FunctionContext* context, const Columns& columns) {
    RETURN_COLUMN(pad<PAD_TYPE_LEFT>(context, columns), "lpad");
}

// rpad
StatusOr<ColumnPtr> StringFunctions::rpad(FunctionContext* context, const Columns& columns) {
    RETURN_COLUMN(pad<PAD_TYPE_RIGHT>(context, columns), "rpad");
}

// append_trailing_char_if_absent
StatusOr<ColumnPtr> StringFunctions::append_trailing_char_if_absent(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    size_t row_num = columns[0]->size();
    bool is_src_col_nullable = columns[0]->is_nullable();
    bool is_tailing_col_nullable = columns[1]->is_nullable();
    if (is_src_col_nullable) {
        if (columns[0]->size() == ColumnHelper::count_nulls(columns[0])) {
            return ColumnHelper::create_const_null_column(columns[0]->size());
        }
    } else if (is_tailing_col_nullable) {
        if (columns[1]->size() == ColumnHelper::count_nulls(columns[1])) {
            return ColumnHelper::create_const_null_column(columns[0]->size());
        }
    }

    bool is_src_col_const = columns[0]->is_constant();
    bool is_tailing_col_const = columns[1]->is_constant();
    if (!is_src_col_const && is_tailing_col_const) {
        // The most common scene for append_trailing_char_if_absent is
        // src_col(not constant), tailing_col(constant).
        // So I optimize this scene for memory copy.
        //   First allocate enough dst memory for dest column.
        //   Second memcpy tailing char to dest memory when conditions match.
        // The kernel is to optimized small memory copy using strings::memcpy_inlined
        // and eliminate the extra one memory copy.

        auto* const_tailing = ColumnHelper::as_raw_column<ConstColumn>(columns[1]);
        auto tailing_col = ColumnHelper::cast_to<TYPE_VARCHAR>(const_tailing->data_column());
        const Slice& slice = tailing_col->get_slice(0);
        if (slice.size != 1) {
            return ColumnHelper::create_const_null_column(columns[0]->size());
        }

        BinaryColumn* src = nullptr;
        ColumnPtr dst = nullptr;
        BinaryColumn* binary_dst = nullptr;
        if (columns[0]->is_nullable()) {
            auto* src_null = ColumnHelper::as_raw_column<NullableColumn>(columns[0]);
            src = ColumnHelper::as_raw_column<BinaryColumn>(src_null->data_column());

            ColumnPtr data = RunTimeColumnType<TYPE_VARCHAR>::create();
            dst = NullableColumn::create(data, src_null->null_column());
            binary_dst = ColumnHelper::as_raw_column<BinaryColumn>(data);
        } else {
            src = ColumnHelper::as_raw_column<BinaryColumn>(columns[0]);
            dst = RunTimeColumnType<TYPE_VARCHAR>::create();
            binary_dst = ColumnHelper::as_raw_column<BinaryColumn>(dst);
        }
        const auto& src_data = src->get_bytes();
        const auto& src_offsets = src->get_offset();

        auto& dst_data = binary_dst->get_bytes();
        auto& dst_offsets = binary_dst->get_offset();

        dst_data.resize(src_data.size() + row_num);
        dst_offsets.resize(row_num + 1);
        dst_offsets[0] = 0;

        size_t src_offset = 0;
        size_t dst_offset = 0;

        for (int row = 0; row < row_num; ++row) {
            size_t src_length = src_offsets[row + 1] - src_offsets[row];
            // memcpy_inlined is better to copy small memory
            // it can inlines the optimal realization for sizes 1 to 16.
            // In my test, when string is less than 20 bytes, it can
            // improve performance by 20% in one thousand million rows.
            strings::memcpy_inlined(&dst_data[dst_offset], &src_data[src_offset], src_length);

            src_offset = src_offsets[row + 1];
            dst_offset += src_length;
            // Slice size must be 1, because has checked before
            if (src_length > 0 && memcmp(&dst_data[dst_offset - 1], slice.data, 1) != 0) {
                memcpy(&dst_data[dst_offset], slice.data, 1);
                ++dst_offset;
            }

            dst_offsets[row + 1] = dst_offset;
        }
        if (!dst_data.empty()) {
            dst_data.resize(dst_offsets.back());
        }

        RETURN_COLUMN(dst, "append_trailing_char_if_absent");
    } else {
        ColumnViewer<TYPE_VARCHAR> src_viewer(columns[0]);
        ColumnViewer<TYPE_VARCHAR> tailing_viewer(columns[1]);

        ColumnBuilder<TYPE_VARCHAR> dst_builder(row_num);

        for (int row = 0; row < row_num; ++row) {
            if (src_viewer.is_null(row) || tailing_viewer.is_null(row) || tailing_viewer.value(row).size != 1) {
                dst_builder.append_null();
                continue;
            }

            auto str = src_viewer.value(row);
            auto tailing_char = tailing_viewer.value(row);

            if (str.size == 0 || str.data[str.size - 1] == tailing_char.data[0]) {
                dst_builder.append(str);
                continue;
            }

            std::string s(str.data, str.size);
            s.append(tailing_char.data, 1);
            dst_builder.append(Slice(s));
        }

        RETURN_COLUMN(dst_builder.build(ColumnHelper::is_all_const(columns)), "append_trailing_char_if_absent");
    }
}

// length
DEFINE_UNARY_FN_WITH_IMPL(lengthImpl, str) {
    return str.size;
}

StatusOr<ColumnPtr> StringFunctions::length(FunctionContext* context, const Columns& columns) {
    return VectorizedStrictUnaryFunction<lengthImpl>::evaluate<TYPE_VARCHAR, TYPE_INT>(columns[0]);
}

DEFINE_UNARY_FN_WITH_IMPL(utf8LengthImpl, str) {
    return utf8_len(str.data, str.data + str.size);
}

StatusOr<ColumnPtr> StringFunctions::utf8_length(FunctionContext* context, const starrocks::Columns& columns) {
    return VectorizedStrictUnaryFunction<utf8LengthImpl>::evaluate<TYPE_VARCHAR, TYPE_INT>(columns[0]);
}

template <char CA, char CZ>
static inline void vectorized_toggle_case(const Bytes* src, Bytes* dst) {
    const size_t size = src->size();
    // resize of raw::RawVectorPad16 is faster than std::vector because of
    // no initialization
    static_assert(sizeof(Bytes::value_type) == 1, "Underlying element type must be 8-bit width");
    static_assert(std::is_trivially_destructible_v<Bytes::value_type>,
                  "Underlying element type must have a trivial destructor");
    Bytes buffer;
    buffer.resize(size);
    uint8_t* dst_ptr = buffer.data();
    char* begin = (char*)(src->data());
    char* end = (char*)(begin + size);
    char* src_ptr = begin;
#if defined(__SSE2__)
    static constexpr int SSE2_BYTES = sizeof(__m128i);
    const char* sse2_end = begin + (size & ~(SSE2_BYTES - 1));
    const auto a_minus1 = _mm_set1_epi8(CA - 1);
    const auto z_plus1 = _mm_set1_epi8(CZ + 1);
    const auto flips = _mm_set1_epi8(32);

    for (; src_ptr > sse2_end; src_ptr += SSE2_BYTES, dst_ptr += SSE2_BYTES) {
        auto bytes = _mm_loadu_si128((const __m128i*)src_ptr);
        // the i-th byte of masks is set to 0xff if the corresponding byte is
        // between a..z when computing upper function (A..Z when computing lower function),
        // otherwise set to 0;
        auto masks = _mm_and_si128(_mm_cmpgt_epi8(bytes, a_minus1), _mm_cmpgt_epi8(z_plus1, bytes));
        // only flip 5th bit of lowcase(uppercase) byte, other bytes keep verbatim.
        _mm_storeu_si128((__m128i*)dst_ptr, _mm_xor_si128(bytes, _mm_and_si128(masks, flips)));
    }
#endif
    // only flip 5th bit of lowcase(uppercase) byte, other bytes keep verbatim.
    // i.e.  'a' and 'A' are 0b0110'0001 and 0b'0100'0001 respectively in binary form,
    // whether 'a' to 'A' or 'A' to 'a' conversion, just flip 5th bit(xor 32).
    for (; src_ptr < end; src_ptr += 1, dst_ptr += 1) {
        *dst_ptr = *src_ptr ^ (((CA <= *src_ptr) & (*src_ptr <= CZ)) << 5);
    }
    // move semantics
    dst->swap(reinterpret_cast<Bytes&>(buffer));
}

template <bool to_upper>
struct StringCaseToggleFunction {
public:
    template <LogicalType Type, LogicalType ResultType>
    static ColumnPtr evaluate(const ColumnPtr& v1) {
        auto* src = down_cast<BinaryColumn*>(v1.get());
        Bytes& src_bytes = src->get_bytes();
        Offsets& src_offsets = src->get_offset();
        auto dst = RunTimeColumnType<TYPE_VARCHAR>::create();
        auto& dst_offsets = dst->get_offset();
        auto& dst_bytes = dst->get_bytes();
        dst_offsets.assign(src_offsets.begin(), src_offsets.end());
        if constexpr (to_upper) {
            vectorized_toggle_case<'a', 'z'>(&src_bytes, &dst_bytes);
        } else {
            vectorized_toggle_case<'A', 'Z'>(&src_bytes, &dst_bytes);
        }
        return dst;
    }
};

// lower
DEFINE_STRING_UNARY_FN_WITH_IMPL(lowerImpl, str) {
    std::string v = str.to_string();
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) { return std::tolower(c); });
    return v;
}

StatusOr<ColumnPtr> StringFunctions::lower(FunctionContext* context, const Columns& columns) {
    return VectorizedUnaryFunction<StringCaseToggleFunction<false>>::evaluate<TYPE_VARCHAR>(columns[0]);
}

// upper
DEFINE_STRING_UNARY_FN_WITH_IMPL(upperImpl, str) {
    std::string v = str.to_string();
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) { return std::toupper(c); });
    return v;
}

StatusOr<ColumnPtr> StringFunctions::upper(FunctionContext* context, const Columns& columns) {
    return VectorizedUnaryFunction<StringCaseToggleFunction<true>>::evaluate<TYPE_VARCHAR>(columns[0]);
}

static inline void ascii_reverse_per_slice(const char* src_begin, const char* src_end, char* dst_curr) {
    auto src_curr = src_begin;
#if defined(__SSSE3__) && defined(__SSE2__)
    auto const size = src_end - src_begin;
    constexpr auto SSE2_SIZE = sizeof(__m128i);
    const auto ctrl_masks = _mm_set_epi64((__m64)0x00'01'02'03'04'05'06'07ull, (__m64)0x08'09'0a'0b'0c'0d'0e'0full);
    const auto sse2_end = src_begin + (size & ~(SSE2_SIZE - 1));
    for (; src_curr < sse2_end; src_curr += SSE2_SIZE) {
        dst_curr -= SSE2_SIZE;
        _mm_storeu_si128((__m128i*)dst_curr, _mm_shuffle_epi8(_mm_loadu_si128((__m128i*)src_curr), ctrl_masks));
    }
#endif
    for (; src_curr < src_end; ++src_curr) {
        --dst_curr;
        *dst_curr = *src_curr;
    }
}

static inline void utf8_reverse_per_slice(const char* src_begin, const char* src_end, char* dst_curr) {
    auto src_curr = src_begin;
    for (auto char_size = 0; src_curr < src_end; src_curr += char_size) {
        char_size = UTF8_BYTE_LENGTH_TABLE[(uint8_t)*src_curr];
        // utf8 chars are copied from src_curr to  dst_curr one by one reversely, an illegal utf8 char
        // would give a larger char_size than expected, which would cause dst_curr advance to exceed its lower,
        // so protect char_size to not exceeds src_end-src_curr.
        char_size = std::min<size_t>(src_end - src_curr, char_size);
        dst_curr -= char_size;
        strings::memcpy_inlined(dst_curr, src_curr, char_size);
    }
}

template <bool is_ascii>
static inline void reverse(BinaryColumn* src, Bytes* dst_bytes) {
    const auto num_rows = src->size();
    char* dst_curr = (char*)dst_bytes->data();
    for (auto i = 0; i < num_rows; ++i) {
        auto s = src->get_slice(i);
        auto begin = s.data;
        auto end = s.data + s.size;
        dst_curr += s.size;
        if constexpr (is_ascii) {
            ascii_reverse_per_slice(begin, end, dst_curr);
        } else {
            utf8_reverse_per_slice(begin, end, dst_curr);
        }
    }
}

struct ReverseFunction {
    template <LogicalType Type, LogicalType ResultType>
    static inline ColumnPtr evaluate(const ColumnPtr& column) {
        auto* src = down_cast<BinaryColumn*>(column.get());
        auto& src_bytes = src->get_bytes();
        auto& src_offsets = src->get_offset();

        auto result = BinaryColumn::create();
        auto& dst_bytes = result->get_bytes();
        auto& dst_offsets = result->get_offset();

        dst_offsets.assign(src_offsets.begin(), src_offsets.end());
        dst_bytes.resize(src_bytes.size());

        const auto is_ascii = validate_ascii_fast((const char*)src_bytes.data(), src_bytes.size());
        if (is_ascii) {
            reverse<true>(src, &dst_bytes);
        } else {
            reverse<false>(src, &dst_bytes);
        }
        return result;
    }
};

StatusOr<ColumnPtr> StringFunctions::reverse(FunctionContext* context, const Columns& columns) {
    return VectorizedUnaryFunction<ReverseFunction>::evaluate<TYPE_VARCHAR>(columns[0]);
}

// strings with little spaces can not benefit from simd optimization,
// simd_optimization = false, turn off SIMD optimization
template <bool simd_optimization, bool trim_single, bool trim_utf8>
static inline const char* skip_leading_spaces(const char* begin, const char* end, const std::string& remove,
                                              const std::vector<size_t>& utf8_index) {
    auto p = begin;
#if defined(__SSE2__)
    if constexpr (simd_optimization && trim_single) {
        const auto size = end - begin;
        const auto SSE2_BYTES = sizeof(__m128i);
        const auto sse2_end = begin + (size & ~(SSE2_BYTES - 1));
        const auto spaces = _mm_set1_epi8(remove[0]);
        for (; p < sse2_end; p += SSE2_BYTES) {
            uint32_t masks = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128((__m128i*)p), spaces));
            int pos = __builtin_ctz((1u << SSE2_BYTES) | ~masks);
            if (pos < SSE2_BYTES) {
                return p + pos;
            }
        }
    }
#endif
    if constexpr (trim_single) {
        if (remove[0] == ' ') {
            for (; p < end && *p == ' '; ++p) {
            }
        } else {
            for (; p < end && *p == remove[0]; ++p) {
            }
        }
    } else if constexpr (!trim_utf8) {
        for (; p < end && remove.find(*p) != remove.npos; ++p) {
        }
    } else {
        size_t char_size = UTF8_BYTE_LENGTH_TABLE[(uint8_t)*p];
        while (p < end && utf8_contains(remove, utf8_index, {p, char_size})) {
            char_size = UTF8_BYTE_LENGTH_TABLE[(uint8_t)*p];
            p += char_size;
        }
    }
    return p;
}

// strings with little spaces can not benefit from simd optimization,
// simd_optimization = false, turn off SIMD optimization
template <bool simd_optimization, bool trim_single, bool trim_utf8>
static const char* skip_trailing_spaces(const char* begin, const char* end, const std::string& remove,
                                        const std::vector<size_t>& utf8_index) {
    if (UNLIKELY(begin == nullptr)) {
        return begin;
    }
    auto p = end;
#if defined(__SSE2__)
    if constexpr (simd_optimization && trim_single && !trim_utf8) {
        const auto size = end - begin;
        const auto SSE2_BYTES = sizeof(__m128i);
        const auto sse2_begin = end - (size & ~(SSE2_BYTES - 1));
        const auto spaces = _mm_set1_epi8(remove[0]);
        for (p = end - SSE2_BYTES; p >= sse2_begin; p -= SSE2_BYTES) {
            uint32_t masks = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128((__m128i*)p), spaces));
            int pos = __builtin_clz(~(masks << SSE2_BYTES));
            if (pos < SSE2_BYTES) {
                return p + SSE2_BYTES - pos;
            }
        }
        p += SSE2_BYTES;
    }
#endif
    if constexpr (trim_single) {
        if (remove[0] == ' ') {
            for (--p; p >= begin && *p == ' '; --p) {
            }
        } else {
            for (--p; p >= begin && *p == remove[0]; --p) {
            }
        }
    } else if constexpr (!trim_utf8) {
        for (--p; p >= begin && remove.find(*p) != remove.npos; --p) {
        }
    } else {
        p--;
        while (true) {
            // TODO: Maybe we could also match the charcters in reverse order instead of find the start of utf-8 char
            Slice utf8_char = utf8_char_start(p);
            if (!utf8_contains(remove, utf8_index, utf8_char)) {
                break;
            }
            p = utf8_char.data - 1;
            if (p < begin) {
                break;
            }
        }
    }
    return p + 1;
}

enum TrimType { TRIM_LEFT, TRIM_RIGHT, TRIM_BOTH };
enum TrimSimdOptimization { TRIM_SIMD_NONE, TRIM_SIMD_LEFT, TRIM_SIMD_RIGHT, TRIM_SIMD_BOTH };

template <TrimType trim_type, TrimSimdOptimization trim_simd_optimization, bool compute_spaces_num, bool trim_single,
          bool trim_utf8>
static inline void trim_per_slice(const BinaryColumn* src, const size_t i, Bytes* bytes, Offsets* offsets,
                                  const std::string& remove, const std::vector<size_t>& utf8_index,
                                  [[maybe_unused]] size_t* leading_spaces_num,
                                  [[maybe_unused]] size_t* trailing_spaces_num) {
    auto s = src->get_slice(i);
    if (UNLIKELY(s.size == 0)) {
        (*offsets)[i + 1] = bytes->size();
        return;
    }

    const auto begin = s.data;
    const auto end = s.data + s.size;

    const char* from_ptr = begin;
    if constexpr (trim_type == TRIM_LEFT || trim_type == TRIM_BOTH) {
        constexpr auto simd_enable =
                trim_simd_optimization == TRIM_SIMD_LEFT || trim_simd_optimization == TRIM_SIMD_BOTH;
        from_ptr = skip_leading_spaces<simd_enable, trim_single, trim_utf8>(from_ptr, end, remove, utf8_index);
        if constexpr (compute_spaces_num) {
            *leading_spaces_num += from_ptr - begin;
        }
    }

    const char* to_ptr = end;
    if constexpr (trim_type == TRIM_RIGHT || trim_type == TRIM_BOTH) {
        constexpr auto simd_enable =
                trim_simd_optimization == TRIM_SIMD_RIGHT || trim_simd_optimization == TRIM_SIMD_BOTH;
        to_ptr = skip_trailing_spaces<simd_enable, trim_single, trim_utf8>(from_ptr, to_ptr, remove, utf8_index);
        if constexpr (compute_spaces_num) {
            *trailing_spaces_num += end - to_ptr;
        }
    }

    bytes->insert(bytes->end(), (uint8*)from_ptr, (uint8*)to_ptr);
    (*offsets)[i + 1] = bytes->size();
}

template <TrimType trim_type, size_t simd_threshold, bool trim_single, bool trim_utf8>
struct AdaptiveTrimFunction {
    template <LogicalType Type, LogicalType ResultType, class RemoveArg, class Utf8Index>
    static ColumnPtr evaluate(const ColumnPtr& column, RemoveArg&& remove, Utf8Index&& utf8_index) {
        auto* src = down_cast<BinaryColumn*>(column.get());

        auto dst = RunTimeColumnType<TYPE_VARCHAR>::create();
        auto& dst_offsets = dst->get_offset();
        auto& dst_bytes = dst->get_bytes();

        const auto num_rows = src->size();
        raw::make_room(&dst_offsets, num_rows + 1);
        dst_offsets[0] = 0;
        dst_bytes.reserve(dst_bytes.size());

        size_t i = 0;
        const auto sample_num = std::min(num_rows, 100ul);
        size_t leading_spaces_num = 0;
        size_t trailing_spaces_num = 0;

        for (; i < sample_num; ++i) {
            trim_per_slice<trim_type, TRIM_SIMD_NONE, true, trim_single, trim_utf8>(
                    src, i, &dst_bytes, &dst_offsets, remove, utf8_index, &leading_spaces_num, &trailing_spaces_num);
        }
        // when the average number of leading/trailing spaces in the sample is greater than
        // simd_threshold, SIMD optimization is enabled.
        bool leading_simd = leading_spaces_num > simd_threshold * sample_num;
        bool trailing_simd = trailing_spaces_num > simd_threshold * sample_num;
        if (leading_simd && trailing_simd) {
            for (; i < num_rows; ++i) {
                trim_per_slice<trim_type, TRIM_SIMD_BOTH, false, trim_single, trim_utf8>(
                        src, i, &dst_bytes, &dst_offsets, remove, utf8_index, &leading_spaces_num,
                        &trailing_spaces_num);
            }
        } else if (leading_simd) {
            for (; i < num_rows; ++i) {
                trim_per_slice<trim_type, TRIM_SIMD_LEFT, false, trim_single, trim_utf8>(
                        src, i, &dst_bytes, &dst_offsets, remove, utf8_index, &leading_spaces_num,
                        &trailing_spaces_num);
            }
        } else if (trailing_simd) {
            for (; i < num_rows; ++i) {
                trim_per_slice<trim_type, TRIM_SIMD_RIGHT, false, trim_single, trim_utf8>(
                        src, i, &dst_bytes, &dst_offsets, remove, utf8_index, &leading_spaces_num,
                        &trailing_spaces_num);
            }
        } else {
            for (; i < num_rows; ++i) {
                trim_per_slice<trim_type, TRIM_SIMD_NONE, false, trim_single, trim_utf8>(
                        src, i, &dst_bytes, &dst_offsets, remove, utf8_index, &leading_spaces_num,
                        &trailing_spaces_num);
            }
        }
        return dst;
    }
};

struct TrimState {
    std::string remove_chars;
    bool is_utf8;
    std::vector<size_t> utf8_index;
};

template <TrimType trim_type>
static StatusOr<ColumnPtr> trim_impl(FunctionContext* context, const starrocks::Columns& columns) {
    auto* state = reinterpret_cast<TrimState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    DCHECK(!!state);
    auto& remove_chars = state->remove_chars;
    DCHECK(remove_chars.size() > 0);
    if (!state->is_utf8) {
        if (remove_chars.size() == 1) {
            return VectorizedUnaryFunction<AdaptiveTrimFunction<trim_type, 8, true, false>>::template evaluate<
                    TYPE_VARCHAR, const std::string&>(columns[0], remove_chars, state->utf8_index);
        } else {
            return VectorizedUnaryFunction<AdaptiveTrimFunction<trim_type, 8, false, false>>::template evaluate<
                    TYPE_VARCHAR, const std::string&>(columns[0], remove_chars, state->utf8_index);
        }
    } else {
        return VectorizedUnaryFunction<AdaptiveTrimFunction<trim_type, 8, false, true>>::template evaluate<
                TYPE_VARCHAR, const std::string&>(columns[0], remove_chars, state->utf8_index);
    }
}

Status StringFunctions::trim_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }
    if (context->get_num_constant_columns() == 1) {
        auto* state = new TrimState();
        context->set_function_state(scope, state);
        state->remove_chars = " ";
        return Status::OK();
    }
    if (!context->is_constant_column(1)) {
        return Status::InvalidArgument("The second parameter of trim only accept literal value");
    }
    if (!context->is_notnull_constant_column(1)) {
        return Status::InvalidArgument("The second parameter should not be null");
    }
    auto remove_col = context->get_constant_column(1);
    const Slice chars = ColumnHelper::get_const_value<TYPE_VARCHAR>(remove_col);
    std::string remove(chars.get_data(), chars.get_size());
    if (remove.empty()) {
        return Status::InvalidArgument("The second parameter should not be empty string");
    }

    auto* state = new TrimState();
    context->set_function_state(scope, state);
    state->remove_chars = std::move(remove);
    size_t utf8_len = get_utf8_index(state->remove_chars, &state->utf8_index);
    state->is_utf8 = state->remove_chars.length() > utf8_len;

    return Status::OK();
}

Status StringFunctions::trim_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<TrimState*>(context->get_function_state(scope));
        delete state;
    }

    return Status::OK();
}

StatusOr<ColumnPtr> StringFunctions::trim(FunctionContext* context, const starrocks::Columns& columns) {
    return trim_impl<TRIM_BOTH>(context, columns);
}

StatusOr<ColumnPtr> StringFunctions::ltrim(FunctionContext* context, const starrocks::Columns& columns) {
    return trim_impl<TRIM_LEFT>(context, columns);
}

StatusOr<ColumnPtr> StringFunctions::rtrim(FunctionContext* context, const starrocks::Columns& columns) {
    return trim_impl<TRIM_RIGHT>(context, columns);
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(hex_intImpl, v) {
    // TODO: this is probably unreasonably slow
    std::stringstream ss;
    ss << std::hex << std::uppercase << v;
    return ss.str();
}

StatusOr<ColumnPtr> StringFunctions::hex_int(FunctionContext* context, const starrocks::Columns& columns) {
    return VectorizedStringStrictUnaryFunction<hex_intImpl>::evaluate<TYPE_BIGINT, TYPE_VARCHAR>(columns[0]);
}

static constexpr char const* alphabet = "0123456789ABCDEF";
DEFINE_STRING_UNARY_FN_WITH_IMPL(hex_stringImpl, str) {
    std::string s;
    raw::stl_string_resize_uninitialized(&s, str.size << 1);
    auto* p = s.data();
    const auto* q = str.data;
    const auto* end = str.data + str.size;
    while (q != end) {
        int ci = static_cast<unsigned char>(*q++);
        *p++ = alphabet[ci >> 4];
        *p++ = alphabet[ci & 0xf];
    }
    return s;
}

StatusOr<ColumnPtr> StringFunctions::hex_string(FunctionContext* context, const starrocks::Columns& columns) {
    return VectorizedStringStrictUnaryFunction<hex_stringImpl>::evaluate<TYPE_VARCHAR, TYPE_VARCHAR>(columns[0]);
}

#ifdef __AVX2__
static inline bool hexdigit_4chars(char ch0, char ch1, char ch2, char ch3, char* ret0, char* ret1) {
    const auto bs = _mm256_set_epi64x(0x6660'4640'392f'0000, 0x6660'4640'392f'0000, 0x6660'4640'392f'0000,
                                      0x6660'4640'392f'0000);
    auto chs = _mm256_set_epi8(ch3, ch3, ch3, ch3, ch3, ch3, '\1', '\1', ch2, ch2, ch2, ch2, ch2, ch2, '\1', '\1', ch1,
                               ch1, ch1, ch1, ch1, ch1, '\1', '\1', ch0, ch0, ch0, ch0, ch0, ch0, '\1', '\1');

    auto x = _mm256_sub_epi8(bs, chs);
    // mask         legal  bits  range
    // 11111111   N  6    'z' < ch
    // 01111111   Y  5    'a' <= ch <= 'z'
    // 00111111   N  4    'a' < ch < 'Z'
    // 00011111   Y  3    'Z' <= ch <= 'A'
    // 00001111   N  2    '9' < ch < 'A'
    // 00000111   Y  1    '0' <= ch <= '9'
    // 00000011   N  0     ch < '0'
    auto mask = _mm256_movemask_epi8(x);
    // if bits == 0x1; then t = -1;otherwise t = 9;
    // ch in 0..9; 1st byte is ('0' - 1 - ch), so obtain (ch - '0') from -1 - ('0' - 1 - ch)
    // ch in A..F; 3st byte is ('A' - 1 - ch), so obtain (ch - 'A' + 10) from -9 - ('A' - 1 - ch)
    // ch in a..f; 5st byte is ('a' - 1 - ch), so obtain (ch - 'a' + 10) from -9 - ('a' - 1 - ch)

    // process ch0
#define PROCESS_CHAR(i, stmt)                                                                           \
    do {                                                                                                \
        auto mask0 = mask & 0xff;                                                                       \
        auto bits = 30 - __builtin_clz(mask0);                                                          \
        if ((bits & 0x1) == 0) {                                                                        \
            return false;                                                                               \
        }                                                                                               \
        auto t = (10 & (0xff << ((bits == 0b1) << 3))) - 1;                                             \
        auto bytes_vec = (__v32qi)_mm256_srli_epi64(_mm256_permute4x64_epi64(x, (i)), (bits + 1) << 3); \
        auto delta = bytes_vec[0];                                                                      \
        stmt;                                                                                           \
    } while (0);

    // process ch0
    PROCESS_CHAR(0, *ret0 = static_cast<char>((t - delta) << 4));
    // process ch1
    mask >>= 8;
    PROCESS_CHAR(1, *ret0 += static_cast<char>(t - delta));
    // process ch2
    mask >>= 8;
    PROCESS_CHAR(2, *ret1 = static_cast<char>((t - delta) << 4));
    // process ch3
    mask >>= 8;
    PROCESS_CHAR(3, *ret1 += static_cast<char>(t - delta));
    return true;
}

static inline std::string unhex_4chars(Slice s) {
    const auto sz = s.size;
    if (sz == 0 || (sz & 0x1)) {
        return {};
    }
    std::string ret;
    raw::stl_string_resize_uninitialized(&ret, sz >> 1);
    const auto* q = s.data;
    const auto* end = s.data + sz;
    auto* p = ret.data();
    for (; q + 3 < end; q += 4, p += 2) {
        if (!hexdigit_4chars(q[0], q[1], q[2], q[3], &p[0], &p[1])) {
            return {};
        }
    }
    if (q == end) {
        return ret;
    }
    char dummy;
    if (!hexdigit_4chars(q[0], q[1], '0', '0', &p[0], &dummy)) {
        return {};
    }
    return ret;
}
#endif

static inline char hexdigit_1char(char ch) {
    if (int value = ch - '0'; value >= 0 && value <= ('9' - '0')) {
        return value;
    } else if (int value = ch - 'A'; value >= 0 && value <= ('F' - 'A')) {
        return value + 10;
    } else if (int value = ch - 'a'; value >= 0 && value <= ('f' - 'a')) {
        return value + 10;
    } else {
        return 0xff;
    }
}

static inline std::string unhex_1char(Slice str) {
    // For uneven number of chars return empty string like Hive does.
    if (str.size == 0 || (str.size & 0x1)) {
        return {};
    }

    std::string ret;
    raw::stl_string_resize_uninitialized(&ret, str.size >> 1);
    const auto* q = str.data;
    const auto* end = str.data + str.size;
    char* p = ret.data();
    while (q < end) {
        // first half of byte
        char ch0 = hexdigit_1char(*q++);
        if ((ch0 & 0xff) == 0xff) {
            return {};
        }
        char ch1 = hexdigit_1char(*q++);
        if ((ch1 & 0xff) == 0xff) {
            return {};
        }
        *p++ = static_cast<char>((ch0 << 4) + ch1);
    }
    return ret;
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(unhexImpl, str) {
#ifdef __AVX2__
    return unhex_4chars(str);
#else
    return unhex_1char(str);
#endif
}

StatusOr<ColumnPtr> StringFunctions::unhex(FunctionContext* context, const starrocks::Columns& columns) {
    return VectorizedStringStrictUnaryFunction<unhexImpl>::evaluate<TYPE_VARCHAR, TYPE_VARCHAR>(columns[0]);
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(url_encodeImpl, str) {
    return StringFunctions::url_encode_func(str.to_string());
}

std::string StringFunctions::url_encode_func(const std::string& value) {
    std::string escaped;
    raw::stl_string_resize_uninitialized(&escaped, value.size() * 3);
    char* p = escaped.data();
    for (auto c : value) {
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            *p++ = c;
            continue;
        }
        int ci = static_cast<unsigned char>(c);
        *p++ = '%';
        *p++ = alphabet[ci >> 4];
        *p++ = alphabet[ci & 0xf];
    }
    escaped.resize(p - escaped.data());
    return escaped;
}

StatusOr<ColumnPtr> StringFunctions::url_encode(FunctionContext* context, const starrocks::Columns& columns) {
    return VectorizedStringStrictUnaryFunction<url_encodeImpl>::evaluate<TYPE_VARCHAR, TYPE_VARCHAR>(columns[0]);
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(url_decodeImpl, str) {
    return StringFunctions::url_decode_func(str.to_string());
}

static Status url_decode_slice(const char* value, size_t len, std::string* to) {
    to->clear();
    to->reserve(len);
    for (size_t i = 0; i < len; i++) {
        if (value[i] == '%') {
            char l = value[i + 1];
            char r = value[i + 2];
            if ((l < 'A' || l > 'F') && (l < '0' || l > '9')) {
                return Status::RuntimeError(
                        strings::Substitute("decode string contains illegal hex chars: $0$1", l, r));
            }
            if ((r < 'A' || r > 'F') && (r < '0' || r > '9')) {
                return Status::RuntimeError(
                        strings::Substitute("decode string contains illegal hex chars: $0$1", l, r));
            }
            // if l in 'A'..'F', then l-'A' > 0; otherwise l-'A' < 0; we arithmetic shift right 8 bit
            // yields mask, so all bits of mask are 0 if l in 'A'..'F', all bits are 1 if l in '0'..'9'
            auto mask = (l - 'A') >> 8;
            // so mask is all zeros, we choose l - '0'; otherwise we choose l - 'A' + 10; the result is the
            // just the value that '0..9','A'..'F' represent in hexadecimal.
            auto ch = ((l - 'A' + 10) & (~mask)) + ((l - '0') & mask);
            // use the same way get the value of r in hexadecimal
            mask = (r - 'A') >> 8;
            // finally, high*16 + low is the value the string represent.
            ch = (ch << 4) + ((r - 'A' + 10) & (~mask)) + ((r - '0') & mask);
            to->push_back(ch);
            i = i + 2;
        } else {
            to->push_back(value[i]);
        }
    }
    return Status::OK();
}

std::string StringFunctions::url_decode_func(const std::string& value) {
    std::string ret;
    auto status = url_decode_slice(value.data(), value.size(), &ret);
    if (status.ok()) {
        return ret;
    } else {
        throw std::runtime_error(status.get_error_msg());
    }
}

StatusOr<ColumnPtr> StringFunctions::url_decode(FunctionContext* context, const starrocks::Columns& columns) {
    return VectorizedStringStrictUnaryFunction<url_decodeImpl>::evaluate<TYPE_VARCHAR, TYPE_VARCHAR>(columns[0]);
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(sm3Impl, str) {
    const Slice& input_str = str;
    std::stringstream result;

    const unsigned char* message = (unsigned char*)input_str.data;
    unsigned long message_len = input_str.size;
    if (message_len > 0) {
        unsigned char output[32];

        // output as 256 bits(32 bytes) result.
        Sm3::sm3_compute(message, message_len, output);
        result << std::hex << std::setfill('0');

        // first 4 bytes;
        for (int i = 0; i < 4; ++i) {
            result << std::setw(2) << (output[i] & 0xFF);
        }

        // remaining 4 bytes start with " ";
        for (int i = 4; i < 32; ++i) {
            if ((i % 4) == 0) {
                result << " ";
            }
            result << std::setw(2) << (output[i] & 0xFF);
        }
    }

    return result.str();
}

StatusOr<ColumnPtr> StringFunctions::sm3(FunctionContext* context, const starrocks::Columns& columns) {
    return VectorizedStringStrictUnaryFunction<sm3Impl>::evaluate<TYPE_VARCHAR, TYPE_VARCHAR>(columns[0]);
}

// ascii
DEFINE_UNARY_FN_WITH_IMPL(asciiImpl, str) {
    return str.size == 0 ? 0 : static_cast<uint8_t>(str.data[0]);
}

StatusOr<ColumnPtr> StringFunctions::ascii(FunctionContext* context, const Columns& columns) {
    return VectorizedStrictUnaryFunction<asciiImpl>::evaluate<TYPE_CHAR, TYPE_INT>(columns[0]);
}

DEFINE_UNARY_FN_WITH_IMPL(get_charImpl, value) {
    return std::string((char*)&value, 1);
}

StatusOr<ColumnPtr> StringFunctions::get_char(FunctionContext* context, const Columns& columns) {
    return VectorizedStringStrictUnaryFunction<get_charImpl>::evaluate<TYPE_INT, TYPE_CHAR>(columns[0]);
}

// strcmp
DEFINE_BINARY_FUNCTION_WITH_IMPL(strcmpImpl, lhs, rhs) {
    int ret = lhs.compare(rhs);
    if (ret == 0) {
        return 0;
    }
    return ret > 0 ? 1 : -1;
}

StatusOr<ColumnPtr> StringFunctions::strcmp(FunctionContext* context, const Columns& columns) {
    return VectorizedStrictBinaryFunction<strcmpImpl>::evaluate<TYPE_VARCHAR, TYPE_INT>(columns[0], columns[1]);
}

static inline ColumnPtr concat_const_not_null(Columns const& columns, BinaryColumn* src, const ConcatState* state) {
    NullableBinaryColumnBuilder builder;
    auto* binary = down_cast<BinaryColumn*>(builder.data_column().get());
    auto& nulls = builder.get_null_data();
    auto& dst_offsets = binary->get_offset();
    auto& dst_bytes = binary->get_bytes();
    auto is_null = false;

    const auto num_rows = src->size();
    raw::make_room(&dst_offsets, num_rows + 1);
    dst_offsets[0] = 0;
    nulls.resize(num_rows);

    auto& tail = state->tail;
    const auto tail_begin = (uint8_t*)tail.data();
    const auto tail_size = tail.size();
    size_t dst_off = 0;
    // first pass: compute offsets and nulls
    for (int i = 0; i < num_rows; ++i) {
        auto s = src->get_slice(i);
        const auto dst_slice_size = s.size + tail_size;
        if (LIKELY(dst_slice_size <= OLAP_STRING_MAX_LENGTH)) {
            dst_off += dst_slice_size;
            dst_offsets[i + 1] = dst_off;
        } else {
            // return NULL for an oversize result
            nulls[i] = 1;
            is_null = true;
            dst_offsets[i + 1] = dst_off;
        }
    }

    // second pass: reserve proper memory room for concatenation
    dst_bytes.resize(dst_off);
    auto dst_begin = dst_bytes.data();
    dst_off = 0;
    for (int i = 0; i < num_rows; ++i) {
        auto s = src->get_slice(i);
        const auto dst_slice_size = s.size + tail_size;
        if (LIKELY(dst_slice_size <= OLAP_STRING_MAX_LENGTH)) {
            strings::memcpy_inlined(dst_begin + dst_off, s.data, s.size);
            dst_off += s.size;
            strings::memcpy_inlined(dst_begin + dst_off, tail_begin, tail_size);
            dst_off += tail_size;
        }
    }
    builder.set_has_null(is_null);
    return builder.build(ColumnHelper::is_all_const(columns));
}

static inline ColumnPtr concat_const(Columns const& columns, const ConcatState* state) {
    return string_func_const(concat_const_not_null, columns, state);
}

static inline ColumnPtr concat_not_const_small(std::vector<ColumnViewer<TYPE_VARCHAR>> const& list,
                                               const size_t num_rows, const size_t dst_bytes_max_size,
                                               const bool is_const) {
    NullableBinaryColumnBuilder builder;
    auto& dst_nulls = builder.get_null_data();
    auto& dst_offsets = builder.data_column()->get_offset();
    auto& dst_bytes = builder.data_column()->get_bytes();
    dst_nulls.resize(num_rows);
    raw::make_room(&dst_offsets, num_rows + 1);
    dst_offsets[0] = 0;
    dst_bytes.resize(dst_bytes_max_size);

    auto* dst_begin = (uint8_t*)dst_bytes.data();
    size_t dst_off = 0;
    bool has_null = false;

    for (int i = 0; i < num_rows; i++) {
        bool is_null = false;
        for (auto& view : list) {
            if (view.is_null(i)) {
                is_null = true;
                break;
            }
        }
        if (is_null) {
            has_null = true;
            dst_nulls[i] = 1;
            dst_offsets[i + 1] = dst_off;
            continue;
        }
        size_t dst_slice_len = 0;
        bool oversize = false;
        for (auto& view : list) {
            auto v = view.value(i);
            if (UNLIKELY(dst_slice_len + v.size > OLAP_STRING_MAX_LENGTH)) {
                oversize = true;
                break;
            }
            strings::memcpy_inlined(dst_begin + dst_off, v.data, v.size);
            dst_slice_len += v.size;
            dst_off += v.size;
        }

        if (UNLIKELY(oversize)) {
            has_null = true;
            dst_nulls[i] = 1;
            dst_off -= dst_slice_len;
        }
        dst_offsets[i + 1] = dst_off;
    }
    dst_bytes.resize(dst_off);
    builder.set_has_null(has_null);
    return builder.build(is_const);
}

static inline ColumnPtr concat_not_const(Columns const& columns) {
    std::vector<ColumnViewer<TYPE_VARCHAR>> list;
    list.reserve(columns.size());
    for (const ColumnPtr& col : columns) {
        list.emplace_back(ColumnViewer<TYPE_VARCHAR>(col));
    }
    const auto num_rows = columns[0]->size();
    auto dst_bytes_max_size = ColumnHelper::compute_bytes_size(columns.begin(), columns.end());
    const bool is_const = ColumnHelper::is_all_const(columns);

    // small concatenation optimization
    if (dst_bytes_max_size <= CONCAT_SMALL_OPTIMIZE_THRESHOLD) {
        return concat_not_const_small(list, num_rows, dst_bytes_max_size, is_const);
    }

    NullableBinaryColumnBuilder builder;
    builder.resize(num_rows, std::min(dst_bytes_max_size, CONCAT_SMALL_OPTIMIZE_THRESHOLD));
    for (int i = 0; i < num_rows; i++) {
        bool is_null = false;
        for (auto& view : list) {
            if (view.is_null(i)) {
                is_null = true;
                builder.set_null(i);
                break;
            }
        }
        if (is_null) {
            continue;
        }
        size_t dst_slice_len = 0;
        bool oversize = false;
        for (auto& view : list) {
            auto v = view.value(i);
            if (UNLIKELY(dst_slice_len + v.size > OLAP_STRING_MAX_LENGTH)) {
                oversize = true;
                break;
            }
            builder.append_partial((uint8_t*)v.data, (uint8_t*)v.data + v.size);
            dst_slice_len += v.size;
        }

        if (UNLIKELY(oversize)) {
            builder.rewind(dst_slice_len);
            builder.set_null(i);
        } else {
            builder.append_complete(i);
        }
    }
    return builder.build(is_const);
}
/**
 * @param: [string_value, ......]
 * @paramType: [BinaryColumn, ......]`
 * @return: BinaryColumn
 */
StatusOr<ColumnPtr> StringFunctions::concat(FunctionContext* context, const Columns& columns) {
    if (columns.size() == 1) {
        return columns[0];
    }

    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto state = reinterpret_cast<ConcatState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state != nullptr && state->is_const) {
        if (state->is_oversize) {
            return ColumnHelper::create_const_null_column(columns[0]->size());
        } else {
            return concat_const(columns, state);
        }
    } else {
        return concat_not_const(columns);
    }
}

ColumnPtr concat_ws_small(ColumnViewer<TYPE_VARCHAR>& sep_viewer, std::vector<ColumnViewer<TYPE_VARCHAR>> const& list,
                          const size_t num_rows, const size_t dst_bytes_max_size, const bool is_const) {
    NullableBinaryColumnBuilder builder;
    auto& dst_nulls = builder.get_null_data();
    auto& dst_offsets = builder.data_column()->get_offset();
    auto& dst_bytes = builder.data_column()->get_bytes();
    dst_nulls.resize(num_rows);
    raw::make_room(&dst_offsets, num_rows + 1);
    dst_offsets[0] = 0;
    dst_bytes.resize(dst_bytes_max_size);
    auto* dst_begin = (uint8_t*)dst_bytes.data();
    size_t dst_off = 0;
    bool has_null = false;
    for (auto i = 0; i < num_rows; i++) {
        if (sep_viewer.is_null(i)) {
            has_null = true;
            dst_nulls[i] = 1;
            dst_offsets[i + 1] = dst_off;
            continue;
        }

        auto sep = sep_viewer.value(i);

        bool oversize = false;
        size_t dst_slice_size = 0;
        for (auto& view : list) {
            if (view.is_null(i)) {
                continue;
            }
            auto v = view.value(i);
            if (UNLIKELY(dst_slice_size + v.size > OLAP_STRING_MAX_LENGTH)) {
                oversize = true;
                break;
            }
            strings::memcpy_inlined(dst_begin + dst_off, v.data, v.size);
            dst_off += v.size;
            dst_slice_size += v.size;

            strings::memcpy_inlined(dst_begin + dst_off, sep.data, sep.size);
            dst_off += sep.size;
            dst_slice_size += sep.size;
        }

        if (UNLIKELY(oversize)) {
            // rewind all appended bytes if oversize.
            dst_off -= dst_slice_size;
            has_null = true;
            dst_nulls[i] = 1;
            dst_offsets[i + 1] = dst_off;
        } else if (LIKELY(dst_slice_size > 0)) {
            // just rewind last sep
            dst_off -= sep.size;
            dst_offsets[i + 1] = dst_off;
        } else {
            // empty result, no need to rewind
            dst_offsets[i + 1] = dst_off;
        }
    }
    dst_bytes.resize(dst_off);
    builder.set_has_null(has_null);
    return builder.build(is_const);
}
// concat_ws
StatusOr<ColumnPtr> StringFunctions::concat_ws(FunctionContext* context, const Columns& columns) {
    const auto column_num = columns.size();
    if (column_num <= 1 || columns[0]->only_null()) {
        return ColumnHelper::create_const_null_column(columns[0]->size());
    }

    if (columns.size() == 2) {
        return columns[1];
    }

    const auto sep_size = ColumnHelper::compute_bytes_size(columns.begin(), columns.begin() + 1);
    const auto rest_size = ColumnHelper::compute_bytes_size(columns.begin() + 1, columns.end());
    // need extra SIZE_LIMIT bytes of space for rewinding the appended separator.
    const auto dst_bytes_max_size = rest_size + sep_size * (column_num - 2) + OLAP_STRING_MAX_LENGTH;

    ColumnViewer<TYPE_VARCHAR> sep_viewer(columns[0]);
    std::vector<ColumnViewer<TYPE_VARCHAR>> list;
    list.reserve(columns.size());
    // skip only null
    for (int i = 1; i < columns.size(); ++i) {
        if (!columns[i]->only_null()) {
            list.emplace_back(ColumnViewer<TYPE_VARCHAR>(columns[i]));
        }
    }

    const auto num_rows = columns[0]->size();
    const auto is_const = ColumnHelper::is_all_const(columns);

    if (dst_bytes_max_size <= CONCAT_SMALL_OPTIMIZE_THRESHOLD) {
        return concat_ws_small(sep_viewer, list, num_rows, dst_bytes_max_size, is_const);
    }

    NullableBinaryColumnBuilder builder;
    // reserved extra max_sep_size bytes for interpolation of the last separator for each row.
    builder.resize(num_rows, std::min(dst_bytes_max_size, CONCAT_SMALL_OPTIMIZE_THRESHOLD));
    for (auto i = 0; i < num_rows; i++) {
        if (sep_viewer.is_null(i)) {
            builder.set_null(i);
            continue;
        }

        auto sep = sep_viewer.value(i);

        size_t dst_slice_size = 0;
        for (auto& view : list) {
            if (view.is_null(i)) {
                continue;
            }
            auto v = view.value(i);
            builder.append_partial((uint8_t*)v.data, (uint8_t*)v.data + v.size);
            dst_slice_size += v.size;
            builder.append_partial((uint8_t*)sep.data, (uint8_t*)sep.data + sep.size);
            dst_slice_size += sep.size;
        }
        // return NULL for oversize
        if (UNLIKELY(dst_slice_size > OLAP_STRING_MAX_LENGTH + sep.size)) {
            builder.rewind(dst_slice_size);
            builder.set_null(i);
        } else if (LIKELY(dst_slice_size > 0)) {
            // remove last separator
            builder.rewind(sep.size);
            builder.append_complete(i);
        } else {
            builder.append_complete(i);
        }
    }
    return builder.build(is_const);
}

/**
 * @param: [string_value]
 * @paramType: [BinaryColumn]
 * @return: BooleanColumn
 */
StatusOr<ColumnPtr> StringFunctions::null_or_empty(FunctionContext* context, const starrocks::Columns& columns) {
    DCHECK_EQ(columns.size(), 1);
    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; row++) {
        if (str_viewer.is_null(row)) {
            result.append(true);
            continue;
        }

        auto str_value = str_viewer.value(row);
        if (str_value.empty()) {
            result.append(true);
            continue;
        }

        result.append(false);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

int StringFunctions::index_of(const char* source, int source_count, const char* target, int target_count,
                              int from_index) {
    if (from_index >= source_count) {
        return (target_count == 0 ? source_count : -1);
    }
    if (from_index < 0) {
        from_index = 0;
    }
    if (target_count == 0) {
        return from_index;
    }

    const char first = *target;
    int max = source_count - target_count;
    for (int i = from_index; i <= max; i++) {
        while (i <= max && source[i] != first) {
            i++; // Look for first character
        }
        if (i <= max) { // Found first character, now look at the rest of v2
            int j = i + 1;
            int end = j + target_count - 1;
            for (int k = 1; j < end && source[j] == target[k]; j++, k++) {
                ;
            }
            if (j == end) {
                return i; // Found whole string.
            }
        }
    }
    return -1;
}

struct StringFunctionsState {
    using DriverMap = phmap::parallel_flat_hash_map<int32_t, std::unique_ptr<re2::RE2>, phmap::Hash<int32_t>,
                                                    phmap::EqualTo<int32_t>, phmap::Allocator<int32_t>,
                                                    NUM_LOCK_SHARD_LOG, std::mutex>;

    std::string pattern;
    std::unique_ptr<re2::RE2> regex;
    std::unique_ptr<re2::RE2::Options> options;
    bool const_pattern{false};
    DriverMap driver_regex_map; // regex for each pipeline_driver, to make it driver-local

    bool use_hyperscan = false;
    int size_of_pattern = -1;

    // a pointer to the generated database that responsible for parsed expression.
    hs_database_t* database = nullptr;
    // a type containing error details that is returned by the compile calls on failure.
    hs_compile_error_t* compile_err = nullptr;
    // A Hyperscan scratch space, Used to call hs_scan,
    // one scratch space per thread, or concurrent caller, is required
    hs_scratch_t* scratch = nullptr;

    StringFunctionsState() : regex(), options() {}

    // Implement a driver-local regex, to avoid lock contention on the RE2::cache_mutex
    re2::RE2* get_or_prepare_regex() {
        DCHECK(const_pattern);
        int32_t driver_id = CurrentThread::current().get_driver_id();
        if (driver_id == 0) {
            return regex.get();
        }
        re2::RE2* res = nullptr;
        driver_regex_map.lazy_emplace_l(
                driver_id, [&](auto& value) { res = value.get(); },
                [&](auto build) {
                    auto regex = std::make_unique<re2::RE2>(pattern, *options);
                    DCHECK(regex->ok());
                    res = regex.get();
                    build(driver_id, std::move(regex));
                });
        DCHECK(!!res);
        return res;
    }

    ~StringFunctionsState() {
        if (scratch != nullptr) {
            hs_free_scratch(scratch);
        }

        if (database != nullptr) {
            hs_free_database(database);
        }
    }
};

Status StringFunctions::hs_compile_and_alloc_scratch(const std::string& pattern, StringFunctionsState* state,
                                                     FunctionContext* context, const Slice& slice) {
    if (hs_compile(pattern.c_str(), HS_FLAG_ALLOWEMPTY | HS_FLAG_DOTALL | HS_FLAG_UTF8 | HS_FLAG_SOM_LEFTMOST,
                   HS_MODE_BLOCK, nullptr, &state->database, &state->compile_err) != HS_SUCCESS) {
        std::stringstream error;
        error << "Invalid regex expression: " << slice << ": " << state->compile_err->message;
        context->set_error(error.str().c_str());
        hs_free_compile_error(state->compile_err);
        return Status::InvalidArgument(error.str());
    }

    if (hs_alloc_scratch(state->database, &state->scratch) != HS_SUCCESS) {
        std::stringstream error;
        error << "ERROR: Unable to allocate scratch space.";
        context->set_error(error.str().c_str());
        hs_free_database(state->database);
        return Status::InvalidArgument(error.str());
    }

    return Status::OK();
}

Status StringFunctions::regexp_extract_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    auto* state = new StringFunctionsState();
    context->set_function_state(scope, state);

    state->options = std::make_unique<re2::RE2::Options>();
    state->options->set_log_errors(false);
    state->options->set_longest_match(false);
    state->options->set_dot_nl(true);

    // go row regex
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    state->const_pattern = true;
    auto column = context->get_constant_column(1);
    auto pattern = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    state->pattern = pattern.to_string();
    state->regex = std::make_unique<re2::RE2>(state->pattern, *(state->options));

    if (!state->regex->ok()) {
        std::stringstream error;
        error << "Invalid regex expression: " << pattern.to_string();
        context->set_error(error.str().c_str());
        return Status::InvalidArgument(error.str());
    }

    return Status::OK();
}

Status StringFunctions::regexp_replace_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }

    auto* state = new StringFunctionsState();
    context->set_function_state(scope, state);

    state->options = std::make_unique<re2::RE2::Options>();
    state->options->set_log_errors(false);
    state->options->set_longest_match(true);
    state->options->set_dot_nl(true);

    // go row regex
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    state->const_pattern = true;
    auto column = context->get_constant_column(1);
    auto pattern = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    std::string pattern_str = pattern.to_string();
    state->pattern = pattern_str;

    std::string search_string;
    if (pattern_str.size() && RE2::FullMatch(pattern_str, SUBSTRING_RE, &search_string)) {
        state->use_hyperscan = true;
        state->size_of_pattern = pattern.size;
        std::string re_pattern(pattern.data, pattern.size);
        RETURN_IF_ERROR(hs_compile_and_alloc_scratch(re_pattern, state, context, pattern));
    } else {
        state->use_hyperscan = false;
        state->regex = std::make_unique<re2::RE2>(state->pattern, *(state->options));

        if (!state->regex->ok()) {
            std::stringstream error;
            error << "Invalid regex expression: " << pattern.to_string();
            context->set_error(error.str().c_str());
            return Status::InvalidArgument(error.str());
        }
    }

    return Status::OK();
}

Status StringFunctions::regexp_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto* state =
                reinterpret_cast<StringFunctionsState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
    return Status::OK();
}

static ColumnPtr regexp_extract_general(FunctionContext* context, re2::RE2::Options* options, const Columns& columns) {
    auto content_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto ptn_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto field_viewer = ColumnViewer<TYPE_BIGINT>(columns[2]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (content_viewer.is_null(row) || ptn_viewer.is_null(row) || field_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto field_value = field_viewer.value(row);
        if (field_value < 0) {
            result.append(Slice("", 0));
            continue;
        }

        std::string ptn_value = ptn_viewer.value(row).to_string();
        re2::RE2 local_re(ptn_value, *options);
        if (!local_re.ok()) {
            context->set_error(strings::Substitute("Invalid regex: $0", ptn_value).c_str());
            result.append_null();
            continue;
        }

        int max_matches = 1 + local_re.NumberOfCapturingGroups();
        if (field_value >= max_matches) {
            result.append(Slice("", 0));
            continue;
        }

        auto str_value = content_viewer.value(row);
        re2::StringPiece str_sp(str_value.get_data(), str_value.get_size());
        std::vector<re2::StringPiece> matches(max_matches);
        bool success = local_re.Match(str_sp, 0, str_value.get_size(), re2::RE2::UNANCHORED, &matches[0], max_matches);
        if (!success) {
            result.append(Slice("", 0));
            continue;
        }

        const re2::StringPiece& match = matches[field_value];
        result.append(Slice(match.data(), match.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

static ColumnPtr regexp_extract_const(re2::RE2* const_re, const Columns& columns) {
    auto content_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto field_viewer = ColumnViewer<TYPE_BIGINT>(columns[2]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (content_viewer.is_null(row) || field_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto field_value = field_viewer.value(row);
        if (field_value < 0) {
            result.append(Slice("", 0));
            continue;
        }

        int max_matches = 1 + const_re->NumberOfCapturingGroups();
        if (field_value >= max_matches) {
            result.append(Slice("", 0));
            continue;
        }

        auto str_value = content_viewer.value(row);
        re2::StringPiece str_sp(str_value.get_data(), str_value.get_size());
        std::vector<re2::StringPiece> matches(max_matches);
        bool success = const_re->Match(str_sp, 0, str_value.get_size(), re2::RE2::UNANCHORED, &matches[0], max_matches);
        if (!success) {
            result.append(Slice("", 0));
            continue;
        }

        const re2::StringPiece& match = matches[field_value];
        result.append(Slice(match.data(), match.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> StringFunctions::regexp_extract(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto state = reinterpret_cast<StringFunctionsState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    if (state->const_pattern) {
        re2::RE2* const_re = state->get_or_prepare_regex();
        return regexp_extract_const(const_re, columns);
    }

    re2::RE2::Options* options = state->options.get();
    return regexp_extract_general(context, options, columns);
}

static ColumnPtr regexp_extract_all_general(FunctionContext* context, re2::RE2::Options* options,
                                            const Columns& columns) {
    auto content_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto ptn_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto group_viewer = ColumnViewer<TYPE_BIGINT>(columns[2]);

    auto size = columns[0]->size();

    auto str_col = BinaryColumn::create();
    auto offset_col = UInt32Column::create();
    auto nl_col = NullColumn::create();
    offset_col->append(0);
    uint32_t index = 0;

    for (int row = 0; row < size; ++row) {
        if (content_viewer.is_null(row) || ptn_viewer.is_null(row)) {
            offset_col->append(index);
            nl_col->append(1);
            continue;
        }

        std::string ptn_value = ptn_viewer.value(row).to_string();
        re2::RE2 local_re(ptn_value, *options);
        if (!local_re.ok()) {
            context->set_error(strings::Substitute("Invalid regex: $0", ptn_value).c_str());
            offset_col->append(index);
            nl_col->append(1);
            continue;
        }

        nl_col->append(0);
        auto group = group_viewer.value(row);
        if (group < 0) {
            offset_col->append(index);
            continue;
        }

        int max_matches = 1 + local_re.NumberOfCapturingGroups();
        if (group >= max_matches) {
            offset_col->append(index);
            continue;
        }

        auto str_value = content_viewer.value(row);
        re2::StringPiece str_sp(str_value.get_data(), str_value.get_size());

        re2::StringPiece find[group];
        const RE2::Arg* args[group];
        RE2::Arg argv[group];

        for (size_t i = 0; i < group; i++) {
            argv[i] = &find[i];
            args[i] = &argv[i];
        }
        while (re2::RE2::FindAndConsumeN(&str_sp, local_re, args, group)) {
            str_col->append(Slice(find[group - 1].data(), find[group - 1].size()));
            index += 1;
        }
        offset_col->append(index);
    }

    auto array =
            ArrayColumn::create(NullableColumn::create(str_col, NullColumn::create(str_col->size(), 0)), offset_col);
    return NullableColumn::create(array, nl_col);
}

static ColumnPtr regexp_extract_all_const_pattern(re2::RE2* const_re, const Columns& columns) {
    auto content_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto group_viewer = ColumnViewer<TYPE_BIGINT>(columns[2]);

    auto size = ColumnHelper::is_all_const(columns) ? 1 : columns[0]->size();

    auto str_col = BinaryColumn::create();
    auto offset_col = UInt32Column::create();
    auto nl_col = NullColumn::create();
    offset_col->append(0);
    uint32_t index = 0;

    for (int row = 0; row < size; ++row) {
        if (content_viewer.is_null(row)) {
            offset_col->append(index);
            nl_col->append(1);
            continue;
        }

        nl_col->append(0);
        auto group = group_viewer.value(row);
        if (group < 0) {
            offset_col->append(index);
            continue;
        }

        int max_matches = 1 + const_re->NumberOfCapturingGroups();
        if (group >= max_matches) {
            offset_col->append(index);
            continue;
        }

        auto str_value = content_viewer.value(row);
        re2::StringPiece str_sp(str_value.get_data(), str_value.get_size());

        re2::StringPiece find[group];
        const RE2::Arg* args[group];
        RE2::Arg argv[group];

        for (size_t i = 0; i < group; i++) {
            argv[i] = &find[i];
            args[i] = &argv[i];
        }
        while (re2::RE2::FindAndConsumeN(&str_sp, *const_re, args, group)) {
            str_col->append(Slice(find[group - 1].data(), find[group - 1].size()));
            index += 1;
        }
        offset_col->append(index);
    }

    auto array =
            ArrayColumn::create(NullableColumn::create(str_col, NullColumn::create(str_col->size(), 0)), offset_col);
    if (ColumnHelper::is_all_const(columns)) {
        return ConstColumn::create(array, columns[0]->size());
    }
    return NullableColumn::create(array, nl_col);
}

static ColumnPtr regexp_extract_all_const(re2::RE2* const_re, const Columns& columns) {
    auto content_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto group = ColumnHelper::get_const_value<TYPE_BIGINT>(columns[2]);

    auto size = ColumnHelper::is_all_const(columns) ? 1 : columns[0]->size();

    auto str_col = BinaryColumn::create();
    auto offset_col = UInt32Column::create();
    offset_col->append(0);

    NullColumnPtr nl_col;
    if (columns[0]->is_nullable()) {
        auto x = down_cast<NullableColumn*>(columns[0].get())->null_column();
        nl_col = ColumnHelper::as_column<NullColumn>(x->clone_shared());
    } else {
        nl_col = NullColumn::create(size, 0);
    }

    uint64_t index = 0;
    int max_matches = 1 + const_re->NumberOfCapturingGroups();
    if (group < 0 || group >= max_matches) {
        offset_col->append_value_multiple_times(&index, size);
        auto array = ArrayColumn::create(NullableColumn::create(str_col, NullColumn::create(0, 0)), offset_col);

        if (ColumnHelper::is_all_const(columns)) {
            return ConstColumn::create(array, columns[0]->size());
        }
        return NullableColumn::create(array, nl_col);
    }

    re2::StringPiece find[group];
    const RE2::Arg* args[group];
    RE2::Arg argv[group];

    for (size_t i = 0; i < group; i++) {
        argv[i] = &find[i];
        args[i] = &argv[i];
    }
    for (int row = 0; row < size; ++row) {
        if (content_viewer.is_null(row)) {
            offset_col->append(index);
            continue;
        }

        auto str_value = content_viewer.value(row);
        re2::StringPiece str_sp(str_value.get_data(), str_value.get_size());
        while (re2::RE2::FindAndConsumeN(&str_sp, *const_re, args, group)) {
            str_col->append(Slice(find[group - 1].data(), find[group - 1].size()));

            index += 1;
        }
        offset_col->append(index);
    }

    auto array =
            ArrayColumn::create(NullableColumn::create(str_col, NullColumn::create(str_col->size(), 0)), offset_col);

    if (ColumnHelper::is_all_const(columns)) {
        return ConstColumn::create(array, columns[0]->size());
    }
    return NullableColumn::create(array, nl_col);
}

StatusOr<ColumnPtr> StringFunctions::regexp_extract_all(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto state = reinterpret_cast<StringFunctionsState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    if (state->const_pattern) {
        re2::RE2* const_re = state->get_or_prepare_regex();
        if (columns[2]->is_constant()) {
            return regexp_extract_all_const(const_re, columns);
        } else {
            return regexp_extract_all_const_pattern(const_re, columns);
        }
    }

    re2::RE2::Options* options = state->options.get();
    return regexp_extract_all_general(context, options, columns);
}

static ColumnPtr regexp_replace_general(FunctionContext* context, re2::RE2::Options* options, const Columns& columns) {
    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto ptn_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto rpl_viewer = ColumnViewer<TYPE_VARCHAR>(columns[2]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (str_viewer.is_null(row) || ptn_viewer.is_null(row) || rpl_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        std::string ptn_value = ptn_viewer.value(row).to_string();
        re2::RE2 local_re(ptn_value, *options);
        if (!local_re.ok()) {
            context->set_error(strings::Substitute("Invalid regex: $0", ptn_value).c_str());
            result.append_null();
            continue;
        }

        auto rpl_value = rpl_viewer.value(row);
        re2::StringPiece rpl_str = re2::StringPiece(rpl_value.get_data(), rpl_value.get_size());
        auto str_value = str_viewer.value(row);
        std::string result_str(str_value.get_data(), str_value.get_size());
        re2::RE2::GlobalReplace(&result_str, local_re, rpl_str);
        result.append(Slice(result_str.data(), result_str.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

static ColumnPtr regexp_replace_const(re2::RE2* const_re, const Columns& columns) {
    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto rpl_viewer = ColumnViewer<TYPE_VARCHAR>(columns[2]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    std::string result_str;
    for (int row = 0; row < size; ++row) {
        if (str_viewer.is_null(row) || rpl_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto rpl_value = rpl_viewer.value(row);
        re2::StringPiece rpl_str = re2::StringPiece(rpl_value.get_data(), rpl_value.get_size());
        auto str_value = str_viewer.value(row);
        re2::StringPiece str_str = re2::StringPiece(str_value.get_data(), str_value.get_size());
        result_str.clear();
        re2::RE2::GlobalReplace(str_str, *const_re, rpl_str, result_str);
        result.append(Slice(result_str.data(), result_str.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

static StatusOr<ColumnPtr> regexp_replace_use_hyperscan(StringFunctionsState* state, const Columns& columns) {
    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto rpl_viewer = ColumnViewer<TYPE_VARCHAR>(columns[2]);

    hs_scratch_t* scratch = nullptr;
    hs_error_t status;
    if ((status = hs_clone_scratch(state->scratch, &scratch)) != HS_SUCCESS) {
        return Status::InternalError(strings::Substitute("Unable to clone scratch space. status: $0", status));
    }
    DeferOp op([&] {
        if (scratch != nullptr) {
            hs_error_t st;
            if ((st = hs_free_scratch(scratch)) != HS_SUCCESS) {
                LOG(ERROR) << "free scratch space failure. status: " << st;
            }
        }
    });

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

    MatchInfoChain match_info_chain;
    match_info_chain.info_chain.reserve(64);

    for (int row = 0; row < size; ++row) {
        if (str_viewer.is_null(row) || rpl_viewer.is_null(row)) {
            result.append_null();
            continue;
        }
        match_info_chain.info_chain.clear();

        auto rpl_value = rpl_viewer.value(row);

        auto value_size = str_viewer.value(row).size;
        const char* data =
                (value_size) ? str_viewer.value(row).data : &StringFunctions::_DUMMY_STRING_FOR_EMPTY_PATTERN;

        auto st = hs_scan(
                // Use &_DUMMY_STRING_FOR_EMPTY_PATTERN instead of nullptr to avoid crash.
                state->database, data, value_size, 0, scratch,
                [](unsigned int id, unsigned long long from, unsigned long long to, unsigned int flags,
                   void* ctx) -> int {
                    auto* value = (MatchInfoChain*)ctx;
                    if (value->info_chain.empty()) {
                        value->info_chain.emplace_back(MatchInfo{.from = from, .to = to});
                    } else if (value->info_chain.back().from == from) {
                        value->info_chain.back().to = to;
                    } else if (value->info_chain.back().to <= from) {
                        value->info_chain.emplace_back(MatchInfo{.from = from, .to = to});
                    }
                    return 0;
                },
                &match_info_chain);
        DCHECK(st == HS_SUCCESS || st == HS_SCAN_TERMINATED) << " status: " << st;

        std::string result_str;
        result_str.reserve(value_size);

        const char* start = str_viewer.value(row).data;
        size_t last_to = 0;
        for (const auto& info : match_info_chain.info_chain) {
            result_str.append(start + last_to, info.from - last_to);
            result_str.append(rpl_value.data, rpl_value.size);
            last_to = info.to;
        }
        result_str.append(start + last_to, value_size - last_to);

        result.append(Slice(result_str.data(), result_str.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> StringFunctions::regexp_replace(FunctionContext* context, const Columns& columns) {
    auto state = reinterpret_cast<StringFunctionsState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));

    if (state->const_pattern) {
        if (state->use_hyperscan) {
            return regexp_replace_use_hyperscan(state, columns);
        } else {
            re2::RE2* const_re = state->get_or_prepare_regex();
            return regexp_replace_const(const_re, columns);
        }
    }

    re2::RE2::Options* options = state->options.get();
    return regexp_replace_general(context, options, columns);
}

struct ReplaceState {
    bool only_null{false};

    bool const_pattern{false};
    bool const_repl{false};

    std::string pattern;
    std::string repl;
};

Status StringFunctions::replace_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = new ReplaceState();
    context->set_function_state(scope, state);

    if (!context->is_constant_column(1)) {
        return Status::OK();
    }

    const auto pattern_col = context->get_constant_column(1);
    if (pattern_col->only_null()) {
        state->only_null = true;
        return Status::OK();
    }

    state->const_pattern = true;
    const auto pattern = ColumnHelper::get_const_value<TYPE_VARCHAR>(pattern_col);
    state->pattern = pattern.to_string();

    if (!context->is_constant_column(2)) {
        return Status::OK();
    }

    const auto replace_col = context->get_constant_column(2);
    if (replace_col->only_null()) {
        state->only_null = true;
        return Status::OK();
    }

    state->const_repl = true;
    const auto repl = ColumnHelper::get_const_value<TYPE_VARCHAR>(replace_col);
    state->repl = repl.to_string();

    return Status::OK();
}

Status StringFunctions::replace_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = reinterpret_cast<ReplaceState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    delete state;

    return Status::OK();
}

static void replace_all(std::string& str, const std::string& ptn, const std::string& rpl) {
    if (ptn.empty()) {
        return;
    }

    for (auto found = str.find(ptn); found != std::string::npos; found = str.find(ptn, found + rpl.length())) {
        str.replace(found, ptn.length(), rpl);
    }
}

StatusOr<ColumnPtr> StringFunctions::replace(FunctionContext* context, const Columns& columns) {
    const ColumnPtr& arg0 = columns[0];
    if (arg0->only_null()) {
        return arg0;
    }

    const auto state =
            reinterpret_cast<const ReplaceState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state->const_pattern && state->pattern.empty()) {
        return arg0;
    }

    // NOTE: ColumnView's size is not equal to input column's size, use input column instead.
    const auto num_rows = arg0->size();
    const auto str_viewer = ColumnViewer<TYPE_VARCHAR>(arg0);
    if (state->only_null) {
        return ColumnHelper::create_const_null_column(num_rows);
    }

    const auto ptn_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    const auto rpl_viewer = ColumnViewer<TYPE_VARCHAR>(columns[2]);

    ColumnBuilder<TYPE_VARCHAR> result(num_rows);
    for (int row = 0; row < num_rows; ++row) {
        if (str_viewer.is_null(row) || (!state->const_pattern && ptn_viewer.is_null(row)) ||
            (!state->const_repl && rpl_viewer.is_null(row))) {
            result.append_null();
            continue;
        }

        const auto str_slice = str_viewer.value(row);
        if (str_slice.empty()) {
            result.append(str_slice);
            continue;
        }

        std::string str = str_slice.to_string();
        replace_all(str, state->const_pattern ? state->pattern : ptn_viewer.value(row).to_string(),
                    state->const_repl ? state->repl : rpl_viewer.value(row).to_string());
        result.append(Slice(str.data(), str.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> StringFunctions::money_format_double(FunctionContext* context, const starrocks::Columns& columns) {
    auto money_viewer = ColumnViewer<TYPE_DOUBLE>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (money_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        double cent_money = MathFunctions::double_round(money_viewer.value(row), 2, false, false) * 100;
        std::string concurr_format = transform_currency_format(context, std::to_string(cent_money));
        result.append(Slice(concurr_format.data(), concurr_format.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> StringFunctions::money_format_bigint(FunctionContext* context, const starrocks::Columns& columns) {
    auto money_viewer = ColumnViewer<TYPE_BIGINT>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (money_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        std::string cent_money = std::to_string(money_viewer.value(row)).append("00");
        std::string concurr_format = transform_currency_format(context, cent_money);
        result.append(Slice(concurr_format.data(), concurr_format.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> StringFunctions::money_format_largeint(FunctionContext* context,
                                                           const starrocks::Columns& columns) {
    auto money_viewer = ColumnViewer<TYPE_LARGEINT>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (money_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        std::stringstream cent_money;
        auto money_value = money_viewer.value(row);
        starrocks::operator<<(cent_money, money_value) << "00";
        std::string concurr_format = transform_currency_format(context, cent_money.str());
        result.append(Slice(concurr_format.data(), concurr_format.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> StringFunctions::money_format_decimalv2val(FunctionContext* context,
                                                               const starrocks::Columns& columns) {
    auto money_viewer = ColumnViewer<TYPE_DECIMALV2>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (money_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto money_value = money_viewer.value(row);
        DecimalV2Value rounded;
        money_value.round(&rounded, 2, HALF_UP);
        DecimalV2Value tmp(std::string("100"));
        DecimalV2Value cent_money = rounded * tmp;

        std::string concurr_format = transform_currency_format(context, cent_money.to_string());
        result.append(Slice(concurr_format.data(), concurr_format.size()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// regex method
Status StringFunctions::parse_url_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = new ParseUrlState();
    context->set_function_state(scope, state);

    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    state->const_pattern = true;
    auto column = context->get_constant_column(1);
    auto part = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    state->url_part = std::make_unique<UrlParser::UrlPart>();
    *(state->url_part) = UrlParser::get_url_part(StringValue::from_slice(part));

    if (*(state->url_part) == UrlParser::INVALID) {
        std::stringstream error;
        error << "Invalid URL part: " << part.to_string() << std::endl
              << "(Valid URL parts are 'PROTOCOL', 'HOST', 'PATH', 'REF', 'AUTHORITY', 'FILE', "
              << "'USERINFO', and 'QUERY')";
        context->set_error(error.str().c_str());
        return Status::InvalidArgument(error.str());
    }

    return Status::OK();
}

Status StringFunctions::parse_url_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<ParseUrlState*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

StatusOr<ColumnPtr> StringFunctions::parse_url_general(FunctionContext* context, const starrocks::Columns& columns) {
    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto part_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (str_viewer.is_null(row) || part_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto part = part_viewer.value(row);
        UrlParser::UrlPart url_part = UrlParser::get_url_part(StringValue::from_slice(part));

        if (url_part == UrlParser::INVALID) {
            std::stringstream ss;
            ss << "Invalid URL part: " << part.to_string();
            context->add_warning(ss.str().c_str());
            result.append_null();
            continue;
        }
        auto str_value = str_viewer.value(row);
        StringValue value;
        if (!UrlParser::parse_url(StringValue::from_slice(str_value), url_part, &value)) {
            std::stringstream ss;
            ss << "Could not parse URL: " << str_value.to_string();
            context->add_warning(ss.str().c_str());
            result.append_null();
            continue;
        }
        result.append(Slice(value.ptr, value.len));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> StringFunctions::parse_url_const(UrlParser::UrlPart* url_part, FunctionContext* context,
                                                     const starrocks::Columns& columns) {
    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (str_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto str_value = str_viewer.value(row);
        StringValue value;
        if (!UrlParser::parse_url(StringValue::from_slice(str_value), *url_part, &value)) {
            std::stringstream ss;
            ss << "Could not parse URL: " << str_value.to_string();
            context->add_warning(ss.str().c_str());
            result.append_null();
            continue;
        }

        result.append(Slice(value.ptr, value.len));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> StringFunctions::parse_url(FunctionContext* context, const starrocks::Columns& columns) {
    DCHECK_EQ(columns.size(), 2);
    auto* state = reinterpret_cast<ParseUrlState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (state->const_pattern) {
        UrlParser::UrlPart* url_part = state->url_part.get();
        return parse_url_const(url_part, context, columns);
    }

    return parse_url_general(context, columns);
}
static bool seek_param_key_in_query_params(const StringValue& query_params, const StringValue& param_key,
                                           std::string* param_value) {
    const StringSearch param_search(&param_key);
    auto pos = param_search.search(&query_params);
    auto* begin = query_params.ptr;
    auto* end = query_params.ptr + query_params.len;
    auto* p_prev_char = begin + pos - 1;
    auto* p_next_char = begin + pos + param_key.len;
    // NOT FOUND
    // case 1: just not found
    // case 2: suffix found, seek "k1" in "abck1=2", prev char must be '&' if it exists
    // case 3: prefix found, seek "k1" in "k1abc=2", next char must be '=' or '&' if it exists
    if (pos < 0 || (p_prev_char >= begin && *p_prev_char != '&') ||
        (p_next_char < end && *p_next_char != '=' && *p_next_char != '&')) {
        return false;
    }
    // no value; return empty string
    if (p_next_char >= end || *p_next_char == '&') {
        *param_value = "";
        return true;
    }
    // skip '='
    ++p_next_char;
    auto* p = p_next_char;
    // seek '&', the value is string between '=' and '&' if '&' exists, otherwise is remaining string following '='
    while (p < end && *p != '&') ++p;
    auto status = url_decode_slice(p_next_char, p - p_next_char, param_value);
    return status.ok();
}

static bool seek_param_key_in_url(const Slice& url, const Slice& param_key, std::string* param_value) {
    StringValue query_params;
    if (!UrlParser::parse_url(StringValue::from_slice(url), UrlParser::UrlPart::QUERY, &query_params)) {
        return false;
    }
    return seek_param_key_in_query_params(query_params, StringValue::from_slice(param_key), param_value);
}

static StatusOr<ColumnPtr> url_extract_parameter_const_param_key(const starrocks::Columns& columns,
                                                                 const std::string& param_key) {
    auto url_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto num_rows = columns[0]->size();
    Slice param_key_str(param_key);
    ColumnBuilder<TYPE_VARCHAR> result(num_rows);
    std::string param_value;
    for (auto i = 0; i < num_rows; ++i) {
        if (url_viewer.is_null(i)) {
            result.append_null();
            continue;
        }
        auto url = url_viewer.value(i);
        auto found = seek_param_key_in_url(url, param_key_str, &param_value);
        if (!found) {
            result.append_null();
        } else {
            result.append(param_value);
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

static StatusOr<ColumnPtr> url_extract_parameter_general(const starrocks::Columns& columns) {
    auto url_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto param_key_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto num_rows = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(num_rows);
    std::string param_value;
    for (auto i = 0; i < num_rows; ++i) {
        if (url_viewer.is_null(i) || param_key_viewer.is_null(i)) {
            result.append_null();
            continue;
        }
        auto url = url_viewer.value(i);
        auto param_key = param_key_viewer.value(i);
        bool ill_formed = param_key.size == 0 || std::any_of(param_key.data, param_key.data + param_key.size, isspace);
        if (ill_formed) {
            result.append_null();
            continue;
        }
        auto found = seek_param_key_in_url(url, param_key, &param_value);
        if (!found) {
            result.append_null();
        } else {
            result.append(param_value);
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

static StatusOr<ColumnPtr> url_extract_parameter_const_query_params(const starrocks::Columns& columns,
                                                                    const std::string& query_params) {
    auto param_key_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto num_rows = columns[1]->size();
    StringValue query_params_str(query_params);
    ColumnBuilder<TYPE_VARCHAR> result(num_rows);
    std::string param_value;
    for (auto i = 0; i < num_rows; ++i) {
        if (param_key_viewer.is_null(i)) {
            result.append_null();
            continue;
        }
        auto param_key = param_key_viewer.value(i);
        bool ill_formed = param_key.size == 0 || std::any_of(param_key.data, param_key.data + param_key.size, isspace);
        if (ill_formed) {
            result.append_null();
            continue;
        }
        auto found = seek_param_key_in_query_params(query_params_str, StringValue::from_slice(param_key), &param_value);
        if (!found) {
            result.append_null();
        } else {
            result.append(param_value);
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

Status StringFunctions::url_extract_parameter_prepare(starrocks::FunctionContext* context,
                                                      FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = new UrlExtractParameterState();
    context->set_function_state(scope, state);
    auto url_is_const = context->is_constant_column(0);
    auto param_is_const = context->is_constant_column(1);
    auto url_is_null = url_is_const && !context->is_notnull_constant_column(0);
    auto param_is_null = param_is_const && !context->is_notnull_constant_column(1);

    if (url_is_null || param_is_null) {
        state->opt_const_result = "";
        state->result_is_null = true;
        return Status::OK();
    }

    if (!url_is_const && !param_is_const) {
        return Status::OK();
    }

    bool ill_formed = false;
    if (param_is_const) {
        auto param_key_column = context->get_constant_column(1);
        auto param_key = ColumnHelper::get_const_value<TYPE_VARCHAR>(param_key_column);
        state->opt_const_param_key = param_key.to_string();
        ill_formed |= param_key.empty() || std::any_of(param_key.data, param_key.data + param_key.size, isspace);
    }

    if (url_is_const) {
        auto url_column = context->get_constant_column(0);
        auto url = ColumnHelper::get_const_value<TYPE_VARCHAR>(url_column);
        StringValue query_params;
        auto parse_success =
                UrlParser::parse_url(StringValue::from_slice(url), UrlParser::UrlPart::QUERY, &query_params);
        state->opt_const_query_params = query_params.to_string();
        ill_formed |= !parse_success || query_params.len == 0;
    }

    // result is const null is either url or param_key is ill-formed
    if (ill_formed) {
        state->opt_const_result = "";
        state->result_is_null = true;
        return Status::OK();
    }

    if (state->opt_const_query_params.has_value() && state->opt_const_param_key.has_value()) {
        StringValue query_params(state->opt_const_query_params.value());
        StringValue param_key(state->opt_const_param_key.value());
        std::string result;
        state->result_is_null = !seek_param_key_in_query_params(query_params, param_key, &result);
        state->opt_const_result = std::move(result);
    }
    return Status::OK();
}

Status StringFunctions::url_extract_parameter_close(starrocks::FunctionContext* context,
                                                    FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<UrlExtractParameterState*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}
StatusOr<ColumnPtr> StringFunctions::url_extract_parameter(starrocks::FunctionContext* context,
                                                           const starrocks::Columns& columns) {
    DCHECK_EQ(columns.size(), 2);
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto* state =
            reinterpret_cast<UrlExtractParameterState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    auto num_rows = columns[0]->size();
    if (state->opt_const_result.has_value()) {
        if (state->result_is_null) {
            return ColumnHelper::create_const_null_column(num_rows);
        } else {
            return ColumnHelper::create_const_column<TYPE_VARCHAR>(state->opt_const_result.value(), num_rows);
        }
    } else if (state->opt_const_param_key.has_value()) {
        return url_extract_parameter_const_param_key(columns, state->opt_const_param_key.value());
    } else if (state->opt_const_query_params.has_value()) {
        return url_extract_parameter_const_query_params(columns, state->opt_const_query_params.value());
    } else {
        return url_extract_parameter_general(columns);
    }
}

} // namespace starrocks
