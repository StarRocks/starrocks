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

#include <algorithm>
#include <cstring>
#include <limits>

#include "base/coding.h"
#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/ai/ai_aggregate_state.h"
#include "exprs/function_context.h"

namespace starrocks {

struct AICollectTextState {
    uint64_t valid_count = 0;
    VectorWithAggStateAllocator<uint8_t> records;
};

// A synchronous, data-only aggregate. Provider policy and inference belong to AIProject.
class AICollectTextAggregateFunction final
        : public AggregateFunctionBatchHelper<AICollectTextState, AICollectTextAggregateFunction> {
public:
    explicit AICollectTextAggregateFunction(size_t max_encoded_bytes = kAiAggregateStateMaxBytes)
            : _max_encoded_bytes(std::min(max_encoded_bytes, kAiAggregateStateMaxBytes)) {}

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override { data(state) = {}; }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        Slice value;
        if (!_read_value(columns[0], row_num, &value)) return;
        auto& destination = data(state);
        // Bound the value before adding the record prefix, so the addition cannot overflow.
        if (value.size > _max_encoded_bytes) {
            ctx->set_error("AI aggregate state exceeds the encoded byte limit");
            return;
        }
        if (!_check_growth(ctx, destination, kAiAggregateRecordLengthBytes + value.size, 1)) return;
        size_t offset = destination.records.size();
        _reserve(destination.records, offset + kAiAggregateRecordLengthBytes + value.size);
        destination.records.resize(offset + kAiAggregateRecordLengthBytes + value.size);
        encode_fixed64_le(destination.records.data() + offset, value.size);
        if (!value.empty()) {
            memcpy(destination.records.data() + offset + kAiAggregateRecordLengthBytes, value.data, value.size);
        }
        ++destination.valid_count;
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row_num) const override {
        Slice encoded;
        if (!_read_value(column, row_num, &encoded)) return;
        auto parsed = parse_ai_aggregate_state(encoded, _max_encoded_bytes);
        if (!parsed.ok()) {
            ctx->set_error(parsed.status().to_string().c_str());
            return;
        }
        auto& destination = data(state);
        Slice payload = parsed->payload();
        if (!_check_growth(ctx, destination, payload.size, parsed->count())) return;
        if (!payload.empty()) {
            size_t offset = destination.records.size();
            _reserve(destination.records, offset + payload.size);
            destination.records.resize(offset + payload.size);
            memcpy(destination.records.data() + offset, payload.data, payload.size);
        }
        destination.valid_count += parsed->count();
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        const auto& source = data(state);
        if (!_check_growth(ctx, source, 0, 0)) return;
        auto* output = down_cast<BinaryColumn*>(to);
        auto& bytes = output->get_bytes();
        size_t offset = bytes.size();
        bytes.resize(offset + kAiAggregateStateHeaderBytes + source.records.size());
        encode_ai_aggregate_state_header(bytes.data() + offset, source.valid_count, source.records.size());
        if (!source.records.empty()) {
            memcpy(bytes.data() + offset + kAiAggregateStateHeaderBytes, source.records.data(), source.records.size());
        }
        output->get_offset().push_back(bytes.size());
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        auto* output = down_cast<BinaryColumn*>(dst.get());
        for (size_t i = 0; i < chunk_size; ++i) {
            Slice value;
            bool has_value = _read_value(src[0].get(), i, &value);
            size_t payload_size = has_value ? kAiAggregateRecordLengthBytes : 0;
            // Check by subtraction before adding either the header or the record length.
            if (_max_encoded_bytes < kAiAggregateStateHeaderBytes + payload_size ||
                value.size > _max_encoded_bytes - kAiAggregateStateHeaderBytes - payload_size) {
                ctx->set_error("AI aggregate state exceeds the encoded byte limit");
                return;
            }
            payload_size += value.size;
            auto& bytes = output->get_bytes();
            size_t offset = bytes.size();
            bytes.resize(offset + kAiAggregateStateHeaderBytes + payload_size);
            auto* header = bytes.data() + offset;
            encode_ai_aggregate_state_header(header, has_value ? 1 : 0, payload_size);
            if (has_value) {
                encode_fixed64_le(header + kAiAggregateStateHeaderBytes, value.size);
                if (!value.empty()) {
                    memcpy(header + kAiAggregateStateHeaderBytes + kAiAggregateRecordLengthBytes, value.data,
                           value.size);
                }
            }
            output->get_offset().push_back(bytes.size());
        }
    }

    std::string get_name() const override { return "__ai_collect_text"; }

private:
    static bool _read_value(const Column* column, size_t row_num, Slice* value) {
        if (column->is_constant()) {
            column = down_cast<const ConstColumn*>(column)->data_column().get();
            row_num = 0;
        }
        if (column->is_nullable()) {
            if (column->is_null(row_num)) return false;
            column = down_cast<const NullableColumn*>(column)->data_column().get();
        }
        *value = down_cast<const BinaryColumn*>(column)->get_slice(row_num);
        return true;
    }

    bool _check_growth(FunctionContext* ctx, const AICollectTextState& state, size_t added_bytes,
                       uint64_t added_count) const {
        if (_max_encoded_bytes < kAiAggregateStateHeaderBytes ||
            state.records.size() > _max_encoded_bytes - kAiAggregateStateHeaderBytes ||
            added_bytes > _max_encoded_bytes - kAiAggregateStateHeaderBytes - state.records.size()) {
            ctx->set_error("AI aggregate state exceeds the encoded byte limit");
            return false;
        }
        if (added_count > std::numeric_limits<uint64_t>::max() - state.valid_count) {
            ctx->set_error("AI aggregate state record count overflow");
            return false;
        }
        return true;
    }

    void _reserve(VectorWithAggStateAllocator<uint8_t>& records, size_t required) const {
        if (required <= records.capacity()) return;
        size_t ceiling = _max_encoded_bytes - kAiAggregateStateHeaderBytes;
        size_t capacity = std::max(records.capacity(), std::min(size_t{64}, ceiling));
        while (capacity < required) {
            capacity = capacity > ceiling / 2 ? ceiling : capacity * 2;
        }
        records.reserve(capacity);
    }

    const size_t _max_encoded_bytes;
};

} // namespace starrocks
