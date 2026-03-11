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

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/struct_column.h"
#include "exec/sorting/sorting.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// Per-group state: serialized row buffer.
// Buffer is a flat concatenation of serialized rows.
// Row format: [null_bitmap (ceil(N/8) bytes)][non-null field values...]
struct MultiArrayAggAggregateState {
    std::string buffer;
};

// MULTI_ARRAY_AGG(col1, ..., colN ORDER BY o1, ...) -> STRUCT<ARRAY<col1>, ..., ARRAY<colN>>
// No DISTINCT support.
// Order-by columns are the trailing arguments (determined by ctx->get_is_asc_order().size()).
//
// Intermediate type: VARBINARY blob per group.
// Blob format: [row1][row2]... (flat concatenation, no header)
// Row format: [null_bitmap (ceil(N/8) bytes)][non-null field values...]
class MultiArrayAggAggregateFunction final
        : public AggregateFunctionBatchHelper<MultiArrayAggAggregateState, MultiArrayAggAggregateFunction> {
public:
    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        new (ptr) MultiArrayAggAggregateState;
    }

    void destroy(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        auto& state = this->data(ptr);
        ctx->add_mem_usage(-static_cast<int64_t>(state.buffer.capacity()));
        state.~MultiArrayAggAggregateState();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        auto& s = this->data(state);
        size_t num_fields = ctx->get_arg_types().size();
        size_t nbm = (num_fields + 7) / 8;

        // Compute serialized row size
        uint32_t row_size = static_cast<uint32_t>(nbm);
        for (size_t i = 0; i < num_fields; ++i) {
            if (!_is_null(columns[i], row_num)) {
                auto [dc, ar] = _unwrap(columns[i], row_num);
                row_size += dc->serialize_size(ar);
            }
        }

        size_t old_cap = s.buffer.capacity();
        size_t off = s.buffer.size();
        s.buffer.resize(off + row_size); // std::string zero-fills new bytes
        uint8_t* ptr = reinterpret_cast<uint8_t*>(s.buffer.data() + off);

        uint8_t* bitmap = ptr;
        uint8_t* pos = ptr + nbm;

        for (size_t i = 0; i < num_fields; ++i) {
            if (_is_null(columns[i], row_num)) {
                bitmap[i / 8] |= (1 << (i % 8));
            } else {
                auto [dc, ar] = _unwrap(columns[i], row_num);
                pos += dc->serialize(ar, pos);
            }
        }

        ctx->add_mem_usage(static_cast<int64_t>(s.buffer.capacity()) - static_cast<int64_t>(old_cap));
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state,
               size_t row_num) const override {
        auto& s = this->data(state);

        Slice slice;
        if (column->is_nullable()) {
            if (column->is_null(row_num)) return;
            auto* data_col = down_cast<const NullableColumn*>(column)->data_column().get();
            slice = down_cast<const BinaryColumn*>(data_col)->get_slice(row_num);
        } else {
            slice = down_cast<const BinaryColumn*>(column)->get_slice(row_num);
        }

        if (slice.size == 0) return;

        size_t old_cap = s.buffer.capacity();
        s.buffer.append(slice.data, slice.size);
        ctx->add_mem_usage(static_cast<int64_t>(s.buffer.capacity()) - static_cast<int64_t>(old_cap));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& s = this->data(const_cast<AggDataPtr>(state));

        auto* bin = ColumnHelper::get_binary_column(to);
        auto& bytes = bin->get_bytes();
        size_t old_size = bytes.size();
        bytes.resize(old_size + s.buffer.size());
        memcpy(bytes.data() + old_size, s.buffer.data(), s.buffer.size());

        bin->get_offset().push_back(bytes.size());
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& s = this->data(const_cast<AggDataPtr>(state));
        size_t num_fields = ctx->get_arg_types().size();
        size_t nbm = (num_fields + 7) / 8;
        size_t num_order_by = ctx->get_is_asc_order().size();
        size_t num_agg_cols = num_fields - num_order_by;

        // Deserialize rows from buffer
        MutableColumns tmp = _deserialize_buffer(ctx, s.buffer, num_fields, nbm);
        uint32_t count = tmp.empty() ? 0 : static_cast<uint32_t>(tmp[0]->size());

        // Apply ORDER BY sorting
        Buffer<uint32_t> index;
        if (num_order_by > 0 && count > 0) {
            Columns order_by_columns;
            SortDescs sort_desc(ctx->get_is_asc_order(), ctx->get_nulls_first());
            order_by_columns.assign(tmp.begin() + num_agg_cols, tmp.end());
            Permutation perm;
            Status st = sort_and_tie_columns(ctx->state()->cancelled_ref(), order_by_columns, sort_desc, &perm);
            order_by_columns.clear();
            if (UNLIKELY(ctx->state()->cancelled_ref())) {
                ctx->set_error("multi_array_agg detects cancelled.", false);
                to->append_default();
                return;
            }
            if (UNLIKELY(!st.ok())) {
                ctx->set_error(st.to_string().c_str(), false);
                to->append_default();
                return;
            }
            if (!perm.empty()) {
                index.resize(count);
                for (uint32_t i = 0; i < count; ++i) {
                    index[i] = perm[i].index_in_chunk;
                }
            }
        }

        auto* struct_column = down_cast<StructColumn*>(ColumnHelper::get_data_column(to));
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
        }

        for (size_t i = 0; i < num_agg_cols; ++i) {
            auto* field_column = struct_column->field_column_raw_ptr(i);
            auto* array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(field_column));
            if (field_column->is_nullable()) {
                down_cast<NullableColumn*>(field_column)->null_column_data().emplace_back(0);
            }
            auto* elements_col = array_col->elements_column_raw_ptr();
            auto* offsets_col = array_col->offsets_column_raw_ptr();
            if (index.empty()) {
                if (count > 0) {
                    elements_col->append(*tmp[i], 0, count);
                }
            } else {
                elements_col->append_selective(*tmp[i], index);
            }
            offsets_col->append(offsets_col->immutable_data().back() + (index.empty() ? count : index.size()));
        }

        if (UNLIKELY(_check_overflow(*to, ctx))) {
            return;
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        auto* bin = ColumnHelper::get_binary_column(dst.get());
        auto& bytes = bin->get_bytes();
        auto& offsets = bin->get_offset();

        size_t num_fields = src.size();
        size_t nbm = (num_fields + 7) / 8;

        for (size_t row = 0; row < chunk_size; ++row) {
            uint32_t row_size = static_cast<uint32_t>(nbm);
            for (size_t i = 0; i < num_fields; ++i) {
                if (!_is_null(src[i].get(), row)) {
                    auto [dc, ar] = _unwrap(src[i].get(), row);
                    row_size += dc->serialize_size(ar);
                }
            }

            size_t old_size = bytes.size();
            bytes.resize(old_size + row_size);
            uint8_t* dst_ptr = bytes.data() + old_size;

            memset(dst_ptr, 0, nbm);
            uint8_t* bitmap = dst_ptr;
            dst_ptr += nbm;

            for (size_t i = 0; i < num_fields; ++i) {
                if (_is_null(src[i].get(), row)) {
                    bitmap[i / 8] |= (1 << (i % 8));
                } else {
                    auto [dc, ar] = _unwrap(src[i].get(), row);
                    dst_ptr += dc->serialize(ar, dst_ptr);
                }
            }

            offsets.push_back(bytes.size());
        }

        if (dst->is_nullable()) {
            for (size_t i = 0; i < chunk_size; i++) {
                down_cast<NullableColumn*>(dst.get())->null_column_data().emplace_back(0);
            }
        }
    }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        auto& s = this->data(state);
        ctx->add_mem_usage(-static_cast<int64_t>(s.buffer.capacity()));
        s.buffer.clear();
    }

    std::string get_name() const override { return "multi_array_agg"; }

private:
    static MutableColumns _deserialize_buffer(FunctionContext* ctx, const std::string& buffer, size_t num_fields,
                                              size_t nbm) {
        MutableColumns cols;
        cols.reserve(num_fields);
        for (size_t i = 0; i < num_fields; ++i) {
            cols.emplace_back(FunctionHelper::create_column(*ctx->get_arg_type(i), true));
        }

        const uint8_t* pos = reinterpret_cast<const uint8_t*>(buffer.data());
        const uint8_t* end = pos + buffer.size();
        while (pos < end) {
            const uint8_t* bitmap = pos;
            pos += nbm;

            for (size_t i = 0; i < num_fields; ++i) {
                bool is_null = (bitmap[i / 8] >> (i % 8)) & 1;
                auto* nullable = down_cast<NullableColumn*>(cols[i].get());
                if (is_null) {
                    nullable->append_nulls(1);
                } else {
                    pos = nullable->data_column()->deserialize_and_append(pos);
                    nullable->null_column_data().emplace_back(0);
                }
            }
        }
        return cols;
    }

    static bool _is_null(const Column* col, size_t row_num) {
        return (col->is_nullable() && col->is_null(row_num)) || col->only_null();
    }

    static std::pair<const Column*, size_t> _unwrap(const Column* col, size_t row_num) {
        if (col->is_constant()) {
            col = down_cast<const ConstColumn*>(col)->data_column().get();
            row_num = 0;
        }
        if (col->is_nullable()) {
            col = down_cast<const NullableColumn*>(col)->data_column().get();
        }
        return {col, row_num};
    }

    static bool _check_overflow(const Column& col, FunctionContext* ctx) {
        Status st = col.capacity_limit_reached();
        if (!st.ok()) {
            ctx->set_error(
                    fmt::format("The column generated by multi_array_agg is overflow: {}", st.message()).c_str());
            return true;
        }
        return false;
    }
};

} // namespace starrocks
