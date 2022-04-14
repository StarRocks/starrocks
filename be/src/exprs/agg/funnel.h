// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <cstring>
#include <limits>
#include <type_traits>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gen_cpp/Data_types.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "thrift/protocol/TJSONProtocol.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"
#include "util/phmap/phmap_dump.h"
#include "util/slice.h"
#include "util/stack_util.h"

namespace starrocks::vectorized {

template <PrimitiveType PT>
struct FunnelState {
    FunnelState() { time_event_list.reserve(64); }

    // Use to identify timestamp(datetime/date)
    using TimeType = typename RunTimeTypeTraits<PT>::CppType;
    using TimeTypeColumn = typename RunTimeTypeTraits<PT>::ColumnType;

    // The timestamp-event list
    using TimeEvent = std::pair<TimeType, int8_t>;
    using TimeEventList = std::vector<TimeEvent>;

    mutable TimeEventList time_event_list;

    // Events' size in array.
    mutable int32_t events_size = 0;
    mutable int64_t window_value = 0;

    // mode for funnel(deduplication(001B)/order(010B)/increase(100B)), It could be combined.
    // TODO: It should not need to be passed through serialization
    mutable int8_t mode_value = 0;
    mutable bool has_set_property = false;
    mutable bool sort = false;

    void update(const Column** columns, size_t row_num) {
        auto time_column = down_cast<const TimeTypeColumn*>(columns[0]);

        auto window_column = columns[1];
        if (window_column->is_constant()) {
            window_value = ColumnHelper::get_const_value<TYPE_BIGINT>(window_column);
        } else {
            window_value = down_cast<const Int64Column*>(window_column)->get_data()[row_num];
        }

        auto mode_column = columns[2];
        if (mode_column->is_constant()) {
            mode_value = ColumnHelper::get_const_value<TYPE_TINYINT>(mode_column);
        } else {
            mode_value = down_cast<const Int8Column*>(mode_column)->get_data()[row_num];
        }

        auto array_column = down_cast<const ArrayColumn*>(columns[3]);
        const auto& ele_col = array_column->elements();
        const auto& offsets = array_column->offsets().get_data();

        if (ele_col.is_nullable()) {
            const auto& null_column = down_cast<const NullableColumn&>(ele_col);
            auto data_column = down_cast<BooleanColumn*>(null_column.data_column().get());
            size_t offset = offsets[row_num];
            size_t array_size = offsets[row_num + 1] - offset;

            events_size = array_size;

            for (size_t i = 0; i < array_size; ++i) {
                auto ele_offset = offset + i;
                if (!null_column.is_null(ele_offset) && data_column->get_data()[ele_offset]) {
                    // add time-event to list.
                    time_event_list.emplace_back(time_column->get_data()[row_num], i + 1);
                }
            }
        } else {
            const auto& data_column = down_cast<const BooleanColumn&>(ele_col);
            size_t offset = offsets[row_num];
            size_t array_size = offsets[row_num + 1] - offset;

            events_size = array_size;

            for (size_t i = 0; i < array_size; ++i) {
                if (data_column.get_data()[offset + i]) {
                    // add time-event to list.
                    time_event_list.emplace_back(time_column->get_data()[row_num], i + 1);
                }
            }
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const {
        DCHECK(to->is_binary());
        std::stable_sort(time_event_list.begin(), time_event_list.end());

        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();

        size_t old_size = bytes.size();

        size_t one_element_size = sizeof(bool) + sizeof(int64_t) + sizeof(int8_t) + sizeof(int32_t) + sizeof(size_t);
        size_t time_event_size = 0;
        if constexpr (PT == TYPE_DATETIME) {
            time_event_size = sizeof(int64_t) + sizeof(int8_t);
        } else if constexpr (PT == TYPE_DATE) {
            time_event_size = sizeof(int32_t) + sizeof(int8_t);
        }
        size_t new_size = old_size + one_element_size + time_event_list.size() * time_event_size;
        bytes.resize(new_size);

        bool sorted = true;
        memcpy(bytes.data() + old_size, &sorted, sizeof(bool));
        old_size += sizeof(bool);
        // TODO: use const_column instead and It should not need to be passed through serialization
        memcpy(bytes.data() + old_size, &window_value, sizeof(int64_t));
        old_size += sizeof(int64_t);
        // TODO: use const_column instead and It should not need to be passed through serialization
        memcpy(bytes.data() + old_size, &mode_value, sizeof(int8_t));
        old_size += sizeof(int8_t);
        memcpy(bytes.data() + old_size, &events_size, sizeof(int32_t));
        old_size += sizeof(int32_t);

        size_t size = time_event_list.size();
        memcpy(bytes.data() + old_size, &size, sizeof(size_t));
        old_size += sizeof(size_t);

        for (const auto& time_event : time_event_list) {
            if constexpr (PT == TYPE_DATETIME) {
                memcpy(bytes.data() + old_size, &time_event.first._timestamp, sizeof(int64_t));
                old_size += sizeof(int64_t);
            } else if constexpr (PT == TYPE_DATE) {
                memcpy(bytes.data() + old_size, &time_event.first._julian, sizeof(int32_t));
                old_size += sizeof(int32_t);
            }

            memcpy(bytes.data() + old_size, &time_event.second, sizeof(int8_t));
            old_size += sizeof(int8_t);
        }

        column->get_offset().emplace_back(new_size);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const {
        DCHECK(column->is_binary());
        Slice slice = column->get(row_num).get_slice();

        bool sorted = *reinterpret_cast<bool*>(slice.data);
        if (!has_set_property) {
            window_value = *reinterpret_cast<int64_t*>(slice.data + sizeof(bool));
            mode_value = *reinterpret_cast<int8_t*>(slice.data + sizeof(bool) + sizeof(int64_t));
            events_size = *reinterpret_cast<int32_t*>(slice.data + sizeof(bool) + sizeof(int64_t) + sizeof(int8_t));
            has_set_property = true;
        }

        size_t current_index = sizeof(bool) + sizeof(int64_t) + sizeof(int8_t) + sizeof(int32_t);
        size_t size = *reinterpret_cast<size_t*>(slice.data + current_index);

        current_index += sizeof(size_t);
        if (size > 0) {
            TimeEventList other_time_event_list(size);
            for (int i = 0; i < size; ++i) {
                TimeType time;
                if constexpr (PT == TYPE_DATETIME) {
                    int64_t time_value = *reinterpret_cast<int64_t*>(slice.data + current_index);
                    current_index += sizeof(int64_t);
                    time._timestamp = time_value;
                } else if constexpr (PT == TYPE_DATE) {
                    int32_t time_value = *reinterpret_cast<int32_t*>(slice.data + current_index);
                    current_index += sizeof(int32_t);
                    time._julian = time_value;
                }

                int8_t event = *reinterpret_cast<int8_t*>(slice.data + current_index);
                current_index += sizeof(int8_t);

                other_time_event_list[i] = TimeEvent(time, event);
            }

            if (!sorted) {
                std::stable_sort(other_time_event_list.begin(), other_time_event_list.end());
            }

            time_event_list.insert(time_event_list.end(), other_time_event_list.begin(), other_time_event_list.end());
            const auto start = std::begin(time_event_list);
            const auto middle = std::next(start, size);
            const auto end = std::end(time_event_list);

            std::inplace_merge(start, middle, end);
        }

        sort = true;
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const {
        if (time_event_list.empty()) {
            return;
        }

        if (events_size == 1) {
            down_cast<Int64Column*>(to)->append(1);
            return;
        }

        if (!sort) {
            std::stable_sort(time_event_list.begin(), time_event_list.end());
            sort = true;
        }

        int8_t max_level = 0;
        int8_t prev_event_idx = -1;
        int event_chiain_index = 0;
        // The outer std::pair is used to tag event chain.
        std::vector<std::optional<std::pair<std::pair<TimeType, TimeType>, int>>> event_timestamp(events_size);

        bool first_event = false;

        // TODO: It should take parameter mode outer, and reduce work in loop.
        for (size_t i = 0; i < time_event_list.size(); ++i) {
            const TimeType& timestamp = time_event_list[i].first;
            const int8_t event_idx = (int8_t)(time_event_list[i].second - 1);
            if (event_idx == 0) {
                event_timestamp[0] = std::make_pair(std::make_pair(timestamp, timestamp), event_chiain_index++);
                prev_event_idx = (event_idx > prev_event_idx) ? event_idx : prev_event_idx;
                first_event = true;
            } else if ((mode_value & MODE_FLAGS[0]) && event_timestamp[event_idx].has_value()) {
                max_level = (prev_event_idx > max_level) ? prev_event_idx : max_level;
                DCHECK(event_timestamp[event_idx]->second == event_timestamp[prev_event_idx]->second);

                // Eliminate last event chain
                reset_event_timestamp_list_and_change_prev_event_idx(&prev_event_idx, &event_timestamp,
                                                                     event_timestamp[prev_event_idx]->second);

                if (event_idx == 1 && event_timestamp[event_idx - 1].has_value()) {
                    if (promote_to_next_level(&event_timestamp, timestamp, event_idx, &prev_event_idx)) {
                        down_cast<Int64Column*>(to)->append(events_size);
                        return;
                    }
                }
            } else if ((mode_value & MODE_FLAGS[1]) && first_event && !event_timestamp[event_idx - 1].has_value()) {
                if (prev_event_idx >= 0) {
                    max_level = (prev_event_idx > max_level) ? prev_event_idx : max_level;

                    // Eliminate last event chain
                    reset_event_timestamp_list_and_change_prev_event_idx(&prev_event_idx, &event_timestamp,
                                                                         event_timestamp[prev_event_idx]->second);
                }
            } else if (event_timestamp[event_idx - 1].has_value()) {
                if (promote_to_next_level(&event_timestamp, timestamp, event_idx, &prev_event_idx)) {
                    down_cast<Int64Column*>(to)->append(events_size);
                    return;
                }
            }
        }

        down_cast<Int64Column*>(to)->append((prev_event_idx > max_level) ? prev_event_idx + 1 : max_level + 1);
    }

    static void reset_event_timestamp_list_and_change_prev_event_idx(
            int8_t* prev_event_idx,
            std::vector<std::optional<std::pair<std::pair<TimeType, TimeType>, int>>>* event_timestamp,
            const int local_event_chain_index) {
        for (; (*prev_event_idx) >= 0; --(*prev_event_idx)) {
            if ((*event_timestamp)[(*prev_event_idx)]->second == local_event_chain_index) {
                (*event_timestamp)[(*prev_event_idx)].reset();
            }
        }
    }

    bool promote_to_next_level(
            std::vector<std::optional<std::pair<std::pair<TimeType, TimeType>, int>>>* event_timestamp,
            const TimeType& timestamp, const int8_t event_idx, int8_t* prev_event_idx) const {
        auto first_timestamp = (*event_timestamp)[event_idx - 1]->first.first;
        bool time_matched = false;

        if constexpr (PT == TYPE_DATETIME) {
            time_matched = (timestamp.diff_microsecond(first_timestamp) / USECS_PER_SEC) <= window_value;
        } else if constexpr (PT == TYPE_DATE) {
            time_matched = (((DateValue)timestamp).julian() - ((DateValue)first_timestamp).julian()) <= window_value;
        }

        if ((mode_value & MODE_FLAGS[2])) {
            time_matched = time_matched && (*event_timestamp)[event_idx - 1]->first.second < timestamp;
        }

        if (time_matched) {
            (*event_timestamp)[event_idx] = std::make_pair(std::make_pair(first_timestamp, timestamp),
                                                           (*event_timestamp)[event_idx - 1]->second);
            (*prev_event_idx) = (event_idx > (*prev_event_idx)) ? event_idx : (*prev_event_idx);
            if (event_idx + 1 == events_size) {
                return true;
            }
        }

        return false;
    }

    // 1th bit for deduplication, 2th bit for order, 3th bit for increase.
    static inline int8_t MODE_FLAGS[] = {1 << 0, 1 << 1, 1 << 2};
};

template <PrimitiveType PT>
class FunnelAggregateFunction final
        : public AggregateFunctionBatchHelper<FunnelState<PT>, FunnelAggregateFunction<PT>> {
    using TimeTypeColumn = typename RunTimeTypeTraits<PT>::ColumnType;

public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        this->data(state).update(columns, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        this->data(state).merge(ctx, column, state, row_num);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        this->data(state).serialize_to_column(ctx, state, to);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        this->data(state).finalize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        if (chunk_size > 0) {
            int32_t events_size = 0;

            std::vector<size_t> num_of_time_event_per_row(chunk_size);
            for (int i = 0; i < chunk_size; ++i) {
                auto array_column = down_cast<const ArrayColumn*>(src[3].get());
                const auto& ele_col = array_column->elements();
                const auto& offsets = array_column->offsets().get_data();

                if (ele_col.is_nullable()) {
                    const auto& null_column = down_cast<const NullableColumn&>(ele_col);
                    auto data_column = down_cast<BooleanColumn*>(null_column.data_column().get());
                    size_t offset = offsets[i];
                    size_t array_size = offsets[i + 1] - offset;
                    events_size = array_size;

                    for (size_t j = 0; j < array_size; ++j) {
                        auto ele_offset = offset + j;
                        if (!null_column.is_null(ele_offset) && data_column->get_data()[ele_offset]) {
                            ++num_of_time_event_per_row[i];
                        }
                    }
                } else {
                    const auto& data_column = down_cast<const BooleanColumn&>(ele_col);
                    size_t offset = offsets[i];
                    size_t array_size = offsets[i + 1] - offset;
                    events_size = array_size;

                    for (size_t j = 0; j < array_size; ++j) {
                        if (data_column.get_data()[offset + j]) {
                            ++num_of_time_event_per_row[i];
                        }
                    }
                }
            }

            size_t num_of_time_event_list = 0;
            for (int i = 0; i < chunk_size; ++i) {
                num_of_time_event_list += num_of_time_event_per_row[i];
            }

            DCHECK((*dst)->is_binary());
            auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
            Bytes& bytes = dst_column->get_bytes();
            size_t old_size = bytes.size();

            size_t one_element_size =
                    sizeof(bool) + sizeof(int64_t) + sizeof(int8_t) + sizeof(int32_t) + sizeof(size_t);
            size_t time_event_size = 0;
            if constexpr (PT == TYPE_DATETIME) {
                time_event_size = sizeof(int64_t) + sizeof(int8_t);
            } else if constexpr (PT == TYPE_DATE) {
                time_event_size = sizeof(int32_t) + sizeof(int8_t);
            }
            bytes.resize(one_element_size * chunk_size + time_event_size * num_of_time_event_list);
            dst_column->get_offset().resize(chunk_size + 1);

            for (int i = 0; i < chunk_size; ++i) {
                bool sorted = false;
                memcpy(bytes.data() + old_size, &sorted, sizeof(bool));
                old_size += sizeof(bool);

                auto time_column = down_cast<const TimeTypeColumn*>(src[0].get());

                // TODO: use const_column instead and It should not need to be passed through serialization
                auto window_column = src[1].get();
                if (window_column->is_constant()) {
                    auto window_value = ColumnHelper::get_const_value<TYPE_BIGINT>(window_column);
                    memcpy(bytes.data() + old_size, &window_value, sizeof(int64_t));
                } else {
                    auto window_value = down_cast<const Int64Column*>(window_column)->get_data()[i];
                    memcpy(bytes.data() + old_size, &window_value, sizeof(int64_t));
                }
                old_size += sizeof(int64_t);

                // TODO: use const_column instead and It should not need to be passed through serialization
                auto mode_column = src[2].get();
                if (mode_column->is_constant()) {
                    auto mode_value = ColumnHelper::get_const_value<TYPE_TINYINT>(mode_column);
                    memcpy(bytes.data() + old_size, &mode_value, sizeof(int8_t));
                } else {
                    auto mode_value = down_cast<const Int8Column*>(mode_column)->get_data()[i];
                    memcpy(bytes.data() + old_size, &mode_value, sizeof(int8_t));
                }
                old_size += sizeof(int8_t);

                memcpy(bytes.data() + old_size, &events_size, sizeof(int32_t));
                old_size += sizeof(int32_t);

                memcpy(bytes.data() + old_size, &num_of_time_event_per_row[i], sizeof(size_t));
                old_size += sizeof(size_t);

                auto array_column = down_cast<const ArrayColumn*>(src[3].get());
                const auto& ele_col = array_column->elements();
                const auto& offsets = array_column->offsets().get_data();

                if (ele_col.is_nullable()) {
                    const auto& null_column = down_cast<const NullableColumn&>(ele_col);
                    auto data_column = down_cast<BooleanColumn*>(null_column.data_column().get());
                    size_t offset = offsets[i];
                    size_t array_size = offsets[i + 1] - offset;

                    for (size_t j = 0; j < array_size; ++j) {
                        auto ele_offset = offset + j;
                        if (!null_column.is_null(ele_offset) && data_column->get_data()[ele_offset]) {
                            auto data = time_column->get_data()[i];
                            if constexpr (PT == TYPE_DATETIME) {
                                memcpy(bytes.data() + old_size, &data._timestamp, sizeof(int64_t));
                                old_size += sizeof(int64_t);
                            } else if constexpr (PT == TYPE_DATE) {
                                memcpy(bytes.data() + old_size, &data._julian, sizeof(int32_t));
                                old_size += sizeof(int32_t);
                            }

                            int8_t event = j + 1;
                            memcpy(bytes.data() + old_size, &event, sizeof(int8_t));
                            old_size += sizeof(int8_t);
                        }
                    }
                } else {
                    const auto& data_column = down_cast<const BooleanColumn&>(ele_col);
                    size_t offset = offsets[i];
                    size_t array_size = offsets[i + 1] - offset;

                    for (size_t j = 0; j < array_size; ++j) {
                        if (data_column.get_data()[offset + j]) {
                            auto data = time_column->get_data()[i];
                            if constexpr (PT == TYPE_DATETIME) {
                                memcpy(bytes.data() + old_size, &data._timestamp, sizeof(int64_t));
                                old_size += sizeof(int64_t);
                            } else if constexpr (PT == TYPE_DATE) {
                                memcpy(bytes.data() + old_size, &data._julian, sizeof(int32_t));
                                old_size += sizeof(int32_t);
                            }

                            int8_t event = j + 1;
                            memcpy(bytes.data() + old_size, &event, sizeof(int8_t));
                            old_size += sizeof(int8_t);
                        }
                    }
                }

                dst_column->get_offset()[i + 1] = old_size;
            }
        }
    }

    std::string get_name() const override { return "funnel"; }
};

} // namespace starrocks::vectorized
