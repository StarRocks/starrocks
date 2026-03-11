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

#include <numeric>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/struct_column.h"
#include "exec/sorting/sorting.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// Shared arena-based state for multi_array_agg, stored in FunctionContext::THREAD_LOCAL.
// All groups within one driver share a single arena of serialized row blobs.
// Format per row: [uint32_t body_len][null_bitmap (ceil(N/8) bytes)][non-null field values...]
struct MultiArrayAggSharedState {
    MemPool arena;
    std::vector<const uint8_t*> row_ptrs;
    std::vector<uint32_t> group_tags;
    int32_t ref_count = 0;
    uint32_t next_group_id = 0;
    size_t num_fields = 0;
    size_t null_bitmap_bytes = 0;

    // Built by prepare_for_output(): sorted permutation and per-group ranges
    std::vector<uint32_t> sorted_perm;
    std::vector<std::pair<uint32_t, uint32_t>> group_ranges;
    bool output_prepared = false;

    void initialize(FunctionContext* ctx) {
        num_fields = ctx->get_arg_types().size();
        null_bitmap_bytes = (num_fields + 7) / 8;
    }

    void prepare_for_output() {
        if (output_prepared) return;
        output_prepared = true;

        size_t n = group_tags.size();
        sorted_perm.resize(n);
        std::iota(sorted_perm.begin(), sorted_perm.end(), 0);
        std::sort(sorted_perm.begin(), sorted_perm.end(),
                  [this](uint32_t a, uint32_t b) { return group_tags[a] < group_tags[b]; });

        group_ranges.resize(next_group_id, {0, 0});
        if (n > 0) {
            uint32_t cur_group = group_tags[sorted_perm[0]];
            uint32_t start = 0;
            for (size_t i = 1; i <= n; ++i) {
                if (i == n || group_tags[sorted_perm[i]] != cur_group) {
                    group_ranges[cur_group] = {start, static_cast<uint32_t>(i - start)};
                    if (i < n) {
                        cur_group = group_tags[sorted_perm[i]];
                        start = static_cast<uint32_t>(i);
                    }
                }
            }
        }
    }

    const uint8_t* serialize_row(const Column** columns, size_t row_num) {
        uint32_t body_size = static_cast<uint32_t>(null_bitmap_bytes);
        for (size_t i = 0; i < num_fields; ++i) {
            if (!_is_null(columns[i], row_num)) {
                auto [dc, ar] = _unwrap(columns[i], row_num);
                body_size += dc->serialize_size(ar);
            }
        }

        uint8_t* ptr = arena.allocate_aligned(sizeof(uint32_t) + body_size, 1);
        memcpy(ptr, &body_size, sizeof(uint32_t));
        uint8_t* pos = ptr + sizeof(uint32_t);
        memset(pos, 0, null_bitmap_bytes);
        uint8_t* bitmap = pos;
        pos += null_bitmap_bytes;

        for (size_t i = 0; i < num_fields; ++i) {
            if (_is_null(columns[i], row_num)) {
                bitmap[i / 8] |= (1 << (i % 8));
            } else {
                auto [dc, ar] = _unwrap(columns[i], row_num);
                pos += dc->serialize(ar, pos);
            }
        }
        return ptr;
    }

    const uint8_t* serialize_merge_row(const std::vector<const Column*>& elem_cols,
                                       const std::vector<uint32_t>& offsets, uint32_t elem_idx) {
        uint32_t body_size = static_cast<uint32_t>(null_bitmap_bytes);
        for (size_t i = 0; i < num_fields; ++i) {
            uint32_t row = offsets[i] + elem_idx;
            if (!_is_null(elem_cols[i], row)) {
                auto [dc, ar] = _unwrap(elem_cols[i], row);
                body_size += dc->serialize_size(ar);
            }
        }

        uint8_t* ptr = arena.allocate_aligned(sizeof(uint32_t) + body_size, 1);
        memcpy(ptr, &body_size, sizeof(uint32_t));
        uint8_t* pos = ptr + sizeof(uint32_t);
        memset(pos, 0, null_bitmap_bytes);
        uint8_t* bitmap = pos;
        pos += null_bitmap_bytes;

        for (size_t i = 0; i < num_fields; ++i) {
            uint32_t row = offsets[i] + elem_idx;
            if (_is_null(elem_cols[i], row)) {
                bitmap[i / 8] |= (1 << (i % 8));
            } else {
                auto [dc, ar] = _unwrap(elem_cols[i], row);
                pos += dc->serialize(ar, pos);
            }
        }
        return ptr;
    }

    MutableColumns deserialize_group(FunctionContext* ctx, const uint32_t* perm, uint32_t count) const {
        MutableColumns cols;
        cols.reserve(num_fields);
        for (size_t i = 0; i < num_fields; ++i) {
            cols.emplace_back(FunctionHelper::create_column(*ctx->get_arg_type(i), true));
        }

        for (uint32_t r = 0; r < count; ++r) {
            const uint8_t* ptr = row_ptrs[perm[r]];
            const uint8_t* pos = ptr + sizeof(uint32_t);
            const uint8_t* bitmap = pos;
            pos += null_bitmap_bytes;

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

    int64_t mem_usage() const {
        return arena.total_allocated_bytes() +
               static_cast<int64_t>(row_ptrs.capacity() * sizeof(const uint8_t*)) +
               static_cast<int64_t>(group_tags.capacity() * sizeof(uint32_t)) +
               static_cast<int64_t>(sorted_perm.capacity() * sizeof(uint32_t)) +
               static_cast<int64_t>(group_ranges.capacity() * sizeof(std::pair<uint32_t, uint32_t>));
    }

private:
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
};

// Per-group state: only 4 bytes (group_id).
struct MultiArrayAggAggregateState {
    uint32_t group_id = 0;
};

// MULTI_ARRAY_AGG(col1, ..., colN ORDER BY o1, ...) -> STRUCT<ARRAY<col1>, ..., ARRAY<colN>>
// No DISTINCT support. All N columns are aggregation columns.
// Order-by columns are the trailing arguments (determined by ctx->get_is_asc_order().size()).
class MultiArrayAggAggregateFunction final
        : public AggregateFunctionBatchHelper<MultiArrayAggAggregateState, MultiArrayAggAggregateFunction> {
public:
    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        auto* state = new (ptr) MultiArrayAggAggregateState;
        auto* shared = reinterpret_cast<MultiArrayAggSharedState*>(
                ctx->get_function_state(FunctionContext::THREAD_LOCAL));
        if (shared == nullptr) {
            shared = new MultiArrayAggSharedState();
            shared->initialize(ctx);
            ctx->set_function_state(FunctionContext::THREAD_LOCAL, shared);
        }
        state->group_id = shared->next_group_id++;
        shared->ref_count++;
    }

    void destroy(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        auto* shared = reinterpret_cast<MultiArrayAggSharedState*>(
                ctx->get_function_state(FunctionContext::THREAD_LOCAL));
        if (shared != nullptr) {
            shared->ref_count--;
            if (shared->ref_count <= 0) {
                ctx->add_mem_usage(-shared->mem_usage());
                delete shared;
                ctx->set_function_state(FunctionContext::THREAD_LOCAL, nullptr);
            }
        }
        this->data(ptr).~MultiArrayAggAggregateState();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        auto* shared = reinterpret_cast<MultiArrayAggSharedState*>(
                ctx->get_function_state(FunctionContext::THREAD_LOCAL));
        DCHECK(shared != nullptr);
        auto& state_impl = this->data(state);

        for (size_t i = 0; i < shared->num_fields; ++i) {
            if (UNLIKELY(columns[i]->size() <= row_num && !columns[i]->only_null())) {
                ctx->set_error(std::string(get_name() + "'s update row number overflow").c_str(), false);
                return;
            }
        }

        int64_t old_mem = shared->mem_usage();
        const uint8_t* ptr = shared->serialize_row(columns, row_num);
        shared->row_ptrs.push_back(ptr);
        shared->group_tags.push_back(state_impl.group_id);
        ctx->add_mem_usage(shared->mem_usage() - old_mem);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state,
               size_t row_num) const override {
        auto* shared = reinterpret_cast<MultiArrayAggSharedState*>(
                ctx->get_function_state(FunctionContext::THREAD_LOCAL));
        DCHECK(shared != nullptr);
        auto& state_impl = this->data(state);

        const auto& input_columns =
                down_cast<const StructColumn*>(ColumnHelper::get_data_column(column))->fields();
        size_t nf = input_columns.size();

        std::vector<const Column*> elem_cols(nf);
        std::vector<uint32_t> offsets(nf);
        uint32_t num_rows = 0;
        for (size_t i = 0; i < nf; ++i) {
            auto* array_col =
                    down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(input_columns[i].get()));
            auto array_offsets = array_col->offsets().immutable_data();
            offsets[i] = array_offsets[row_num];
            uint32_t count = array_offsets[row_num + 1] - array_offsets[row_num];
            elem_cols[i] = &array_col->elements();
            if (i == 0) num_rows = count;
        }

        int64_t old_mem = shared->mem_usage();
        for (uint32_t j = 0; j < num_rows; ++j) {
            const uint8_t* ptr = shared->serialize_merge_row(elem_cols, offsets, j);
            shared->row_ptrs.push_back(ptr);
            shared->group_tags.push_back(state_impl.group_id);
        }
        ctx->add_mem_usage(shared->mem_usage() - old_mem);
    }

    // serialize: state -> struct{array[col0], array[col1], ..., array[colN]}
    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(const_cast<AggDataPtr>(state));
        auto* shared = reinterpret_cast<MultiArrayAggSharedState*>(
                ctx->get_function_state(FunctionContext::THREAD_LOCAL));
        DCHECK(shared != nullptr);
        DCHECK(shared->output_prepared);

        auto [offset, count] = shared->group_ranges[state_impl.group_id];
        const uint32_t* perm_ptr = shared->sorted_perm.data() + offset;

        MutableColumns tmp_cols = shared->deserialize_group(ctx, perm_ptr, count);

        auto* struct_column = down_cast<StructColumn*>(ColumnHelper::get_data_column(to));
        if (to->is_nullable()) {
            down_cast<NullableColumn*>(to)->null_column_data().emplace_back(0);
        }

        for (size_t i = 0; i < struct_column->fields_size(); ++i) {
            auto* field_column = struct_column->field_column_raw_ptr(i);
            auto* array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(field_column));
            if (field_column->is_nullable()) {
                down_cast<NullableColumn*>(field_column)->null_column_data().emplace_back(0);
            }
            auto* elements_col = array_col->elements_column_raw_ptr();
            auto* offsets_col = array_col->offsets_column_raw_ptr();
            if (count > 0) {
                elements_col->append(*tmp_cols[i], 0, count);
            }
            offsets_col->append(offsets_col->immutable_data().back() + count);
        }

        if (UNLIKELY(_check_overflow(*to, ctx))) {
            return;
        }
    }

    // finalize: state -> struct{array[col0], array[col1], ..., array[colN]}
    // multi_array_agg returns the same struct type for both intermediate and final results,
    // but finalize applies ORDER BY sorting.
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(const_cast<AggDataPtr>(state));
        auto* shared = reinterpret_cast<MultiArrayAggSharedState*>(
                ctx->get_function_state(FunctionContext::THREAD_LOCAL));
        DCHECK(shared != nullptr);
        DCHECK(shared->output_prepared);

        auto [offset, count] = shared->group_ranges[state_impl.group_id];
        const uint32_t* perm_ptr = shared->sorted_perm.data() + offset;

        MutableColumns tmp_columns = shared->deserialize_group(ctx, perm_ptr, count);

        // Determine how many of the trailing columns are order-by columns
        size_t num_order_by = ctx->get_is_asc_order().size();
        size_t num_agg_cols = shared->num_fields - num_order_by;

        // Apply ORDER BY sorting if order-by columns exist
        Buffer<uint32_t> index;
        if (num_order_by > 0 && count > 0) {
            Columns order_by_columns;
            SortDescs sort_desc(ctx->get_is_asc_order(), ctx->get_nulls_first());
            order_by_columns.assign(tmp_columns.begin() + num_agg_cols, tmp_columns.end());
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

        // Output is struct{array[col0], ..., array[col_{num_agg_cols-1}]}
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
                    elements_col->append(*tmp_columns[i], 0, count);
                }
            } else {
                elements_col->append_selective(*tmp_columns[i], index);
            }
            offsets_col->append(offsets_col->immutable_data().back() + (index.empty() ? count : index.size()));
        }

        if (UNLIKELY(_check_overflow(*to, ctx))) {
            return;
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        auto* struct_column = down_cast<StructColumn*>(ColumnHelper::get_data_column(dst.get()));
        if (dst->is_nullable()) {
            for (size_t i = 0; i < chunk_size; i++) {
                down_cast<NullableColumn*>(dst.get())->null_column_data().emplace_back(0);
            }
        }
        for (size_t j = 0; j < struct_column->fields_size(); ++j) {
            auto* field_column = struct_column->field_column_raw_ptr(j);
            auto* array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(field_column));
            if (field_column->is_nullable()) {
                for (size_t i = 0; i < chunk_size; i++) {
                    down_cast<NullableColumn*>(field_column)->null_column_data().emplace_back(0);
                }
            }
            auto* element_column = array_col->elements_column_raw_ptr();
            auto* offsets_col = array_col->offsets_column_raw_ptr();
            for (size_t i = 0; i < chunk_size; i++) {
                element_column->append_datum(src[j]->get(i));
                offsets_col->append(offsets_col->immutable_data().back() + 1);
            }
        }
    }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        // Reset is not meaningful for shared state groups since
        // group_id is fixed and shared columns are append-only.
    }

    void prepare_for_output(FunctionContext* ctx) const override {
        auto* shared = reinterpret_cast<MultiArrayAggSharedState*>(
                ctx->get_function_state(FunctionContext::THREAD_LOCAL));
        if (shared != nullptr) {
            int64_t old_mem = shared->mem_usage();
            shared->prepare_for_output();
            ctx->add_mem_usage(shared->mem_usage() - old_mem);
        }
    }

    std::string get_name() const override { return "multi_array_agg"; }

private:
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
