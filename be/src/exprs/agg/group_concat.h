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

#include <cmath>

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

namespace starrocks {
template <LogicalType LT, typename = guard::Guard>
inline constexpr LogicalType GroupConcatResultLT = TYPE_VARCHAR;

struct GroupConcatAggregateState {
    // intermediate_string.
    // concat with sep_length first.
    std::string intermediate_string{};
    // is initial
    bool initial{};
};

template <LogicalType LT, typename T = RunTimeCppType<LT>, LogicalType ResultLT = GroupConcatResultLT<LT>,
          typename TResult = RunTimeCppType<ResultLT>>
class GroupConcatAggregateFunction
        : public AggregateFunctionBatchHelper<GroupConcatAggregateState,
                                              GroupConcatAggregateFunction<LT, T, ResultLT, TResult>> {
public:
    using InputColumnType = RunTimeColumnType<ResultLT>;
    using ResultColumnType = InputColumnType;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).intermediate_string = {};
        this->data(state).initial = false;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(columns[0]->is_binary());
        if (ctx->get_num_args() > 1) {
            if (!ctx->is_notnull_constant_column(1)) {
                const auto* column_val = down_cast<const InputColumnType*>(columns[0]);
                const auto* column_sep = down_cast<const InputColumnType*>(columns[1]);

                std::string& result = this->data(state).intermediate_string;

                Slice val = column_val->get_slice(row_num);
                Slice sep = column_sep->get_slice(row_num);
                if (!this->data(state).initial) {
                    this->data(state).initial = true;

                    // separator's length;
                    uint32_t size = sep.get_size();
                    result.append(reinterpret_cast<const char*>(&size), sizeof(uint32_t))
                            .append(sep.get_data(), sep.get_size())
                            .append(val.get_data(), val.get_size());
                } else {
                    result.append(sep.get_data(), sep.get_size()).append(val.get_data(), val.get_size());
                }
            } else {
                auto const_column_sep = ctx->get_constant_column(1);
                const auto* column_val = down_cast<const InputColumnType*>(columns[0]);
                std::string& result = this->data(state).intermediate_string;

                Slice val = column_val->get_slice(row_num);
                Slice sep = ColumnHelper::get_const_value<TYPE_VARCHAR>(const_column_sep);

                if (!this->data(state).initial) {
                    this->data(state).initial = true;

                    // separator's length;
                    uint32_t size = sep.get_size();
                    result.append(reinterpret_cast<const char*>(&size), sizeof(uint32_t))
                            .append(sep.get_data(), sep.get_size())
                            .append(val.get_data(), val.get_size());
                } else {
                    result.append(sep.get_data(), sep.get_size()).append(val.get_data(), val.get_size());
                }
            }
        } else {
            const auto* column_val = down_cast<const InputColumnType*>(columns[0]);
            std::string& result = this->data(state).intermediate_string;

            Slice val = column_val->get_slice(row_num);
            //DEFAULT sep_length.
            if (!this->data(state).initial) {
                this->data(state).initial = true;

                // separator's length;
                uint32_t size = 2;
                result.append(reinterpret_cast<const char*>(&size), sizeof(uint32_t))
                        .append(", ")
                        .append(val.get_data(), val.get_size());
            } else {
                result.append(", ").append(val.get_data(), val.get_size());
            }
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        if (ctx->get_num_args() > 1) {
            const auto* column_val = down_cast<const InputColumnType*>(columns[0]);
            if (!ctx->is_notnull_constant_column(1)) {
                const auto* column_sep = down_cast<const InputColumnType*>(columns[1]);
                this->data(state).intermediate_string.reserve(column_val->get_bytes().size() +
                                                              column_sep->get_bytes().size());
            } else {
                auto const_column_sep = ctx->get_constant_column(1);
                Slice sep = ColumnHelper::get_const_value<TYPE_VARCHAR>(const_column_sep);
                this->data(state).intermediate_string.reserve(column_val->get_bytes().size() +
                                                              sep.get_size() * chunk_size);
            }
        } else {
            const auto* column_val = down_cast<const InputColumnType*>(columns[0]);
            this->data(state).intermediate_string.reserve(column_val->get_bytes().size() + 2 * chunk_size);
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        Slice slice = column->get(row_num).get_slice();
        char* data = slice.data;
        uint32_t size_value = *reinterpret_cast<uint32_t*>(data);
        data += sizeof(uint32_t);

        if (!this->data(state).initial) {
            this->data(state).initial = true;
            this->data(state).intermediate_string.append(data, size_value);
        } else {
            data += sizeof(uint32_t);

            this->data(state).intermediate_string.append(data, size_value - sizeof(uint32_t));
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());

        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();

        const std::string& value = this->data(state).intermediate_string;

        size_t old_size = bytes.size();
        size_t new_size = old_size + sizeof(uint32_t) + value.size();
        bytes.resize(new_size);

        uint32_t size_value = value.size();
        memcpy(bytes.data() + old_size, &size_value, sizeof(uint32_t));
        memcpy(bytes.data() + old_size + sizeof(uint32_t), value.data(), size_value);

        column->get_offset().emplace_back(new_size);
    }

    size_t serialize_sep_and_value(Bytes& bytes, size_t old_size, uint32_t size_value, uint32_t size_sep,
                                   const char* sep, const char* data) const {
        uint32_t size_total = sizeof(uint32_t) + size_value + size_sep;
        memcpy(bytes.data() + old_size, &size_total, sizeof(uint32_t));
        old_size += sizeof(uint32_t);

        memcpy(bytes.data() + old_size, &size_sep, sizeof(uint32_t));
        old_size += sizeof(uint32_t);

        memcpy(bytes.data() + old_size, sep, size_sep);
        old_size += size_sep;

        memcpy(bytes.data() + old_size, data, size_value);
        old_size += size_value;

        return old_size;
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        if (src.size() > 1) {
            auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
            Bytes& bytes = dst_column->get_bytes();
            const auto* column_value = down_cast<BinaryColumn*>(src[0].get());
            if (!src[1]->is_constant()) {
                const auto* column_sep = down_cast<BinaryColumn*>(src[1].get());
                if (chunk_size > 0) {
                    size_t old_size = bytes.size();
                    CHECK_EQ(old_size, 0);
                    size_t new_size = 2 * chunk_size * sizeof(uint32_t) + column_value->get_bytes().size() +
                                      column_sep->get_bytes().size();
                    bytes.resize(new_size);
                    dst_column->get_offset().resize(chunk_size + 1);

                    for (size_t i = 0; i < chunk_size; ++i) {
                        auto value = column_value->get_slice(i);
                        auto sep = column_sep->get_slice(i);

                        uint32_t size_value = value.get_size();
                        uint32_t size_sep = sep.get_size();

                        old_size = serialize_sep_and_value(bytes, old_size, size_value, size_sep, sep.get_data(),
                                                           value.get_data());
                        dst_column->get_offset()[i + 1] = old_size;
                    }
                }
            } else {
                Slice sep = ColumnHelper::get_const_value<TYPE_VARCHAR>(src[1]);
                if (chunk_size > 0) {
                    size_t old_size = bytes.size();
                    CHECK_EQ(old_size, 0);
                    size_t new_size = 2 * chunk_size * sizeof(uint32_t) + column_value->get_bytes().size() +
                                      chunk_size * sep.size;
                    bytes.resize(new_size);
                    dst_column->get_offset().resize(chunk_size + 1);

                    for (size_t i = 0; i < chunk_size; ++i) {
                        auto value = column_value->get_slice(i);

                        uint32_t size_value = value.get_size();
                        uint32_t size_sep = sep.size;

                        old_size = serialize_sep_and_value(bytes, old_size, size_value, size_sep, sep.get_data(),
                                                           value.get_data());
                        dst_column->get_offset()[i + 1] = old_size;
                    }
                }
            }
        } else { //", "
            auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
            Bytes& bytes = dst_column->get_bytes();
            const auto* column_value = down_cast<BinaryColumn*>(src[0].get());

            if (chunk_size > 0) {
                const char* sep = ", ";
                const uint32_t size_sep = 2;

                size_t old_size = bytes.size();
                CHECK_EQ(old_size, 0);
                size_t new_size =
                        2 * chunk_size * sizeof(uint32_t) + column_value->get_bytes().size() + size_sep * chunk_size;
                bytes.resize(new_size);
                dst_column->get_offset().resize(chunk_size + 1);

                for (size_t i = 0; i < chunk_size; ++i) {
                    auto value = column_value->get_slice(i);
                    uint32_t size_value = value.get_size();

                    old_size = serialize_sep_and_value(bytes, old_size, size_value, size_sep, sep, value.get_data());
                    dst_column->get_offset()[i + 1] = old_size;
                }
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        const std::string& value = this->data(state).intermediate_string;
        if (value.empty()) {
            return;
        }
        // Remove first sep_length.
        const char* data = value.data();
        uint32_t size = value.size();

        uint32_t sep_size = *reinterpret_cast<const uint32_t*>(data);
        uint32_t offset = sizeof(uint32_t) + sep_size;

        data = data + offset;
        size = size - offset;
        down_cast<ResultColumnType*>(to)->append(Slice(data, size));
    }

    std::string get_name() const override { return "group concat"; }
};

} // namespace starrocks
