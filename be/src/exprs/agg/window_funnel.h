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

#include <cstring>
#include <limits>
#include <memory>
#include <memory_resource>
#include <type_traits>
#include <vector>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "exprs/agg/aggregate.h"
#include "gen_cpp/Data_types.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "runtime/memory/memory_resource.h"
#include "thrift/protocol/TJSONProtocol.h"
#include "util/phmap/phmap_dump.h"
#include "util/slice.h"

namespace starrocks {

struct ComparePairFirst final {
    template <typename T1, typename T2>
    bool operator()(const std::pair<T1, T2>& lhs, const std::pair<T1, T2>& rhs) const {
        return std::tie(lhs) < std::tie(rhs);
    }
};

enum FunnelMode : int { DEDUPLICATION = 1, FIXED = 2, DEDUPLICATION_FIXED = 3, INCREASE = 4 };

namespace InteralTypeOfFunnel {
template <LogicalType logical_type>
struct TypeTraits {};

template <>
struct TypeTraits<TYPE_INT> {
    using Type = typename RunTimeTypeTraits<TYPE_INT>::CppType;
    using ValueType = typename RunTimeTypeTraits<TYPE_INT>::CppType;
};
template <>
struct TypeTraits<TYPE_BIGINT> {
    using Type = typename RunTimeTypeTraits<TYPE_BIGINT>::CppType;
    using ValueType = typename RunTimeTypeTraits<TYPE_BIGINT>::CppType;
};
template <>
struct TypeTraits<TYPE_DATE> {
    using Type = typename RunTimeTypeTraits<TYPE_DATE>::CppType;
    using ValueType = typename RunTimeTypeTraits<TYPE_DATE>::CppType::type;
};
template <>
struct TypeTraits<TYPE_DATETIME> {
    using Type = typename RunTimeTypeTraits<TYPE_DATETIME>::CppType;
    using ValueType = typename RunTimeTypeTraits<TYPE_DATETIME>::CppType::type;
};
} // namespace InteralTypeOfFunnel

inline const constexpr int reserve_list_size = 4;

template <LogicalType LT>
struct WindowFunnelState {
    // Use to identify timestamp(datetime/date)
    using TimeType = typename InteralTypeOfFunnel::TypeTraits<LT>::Type;
    using TimeTypeColumn = typename RunTimeTypeTraits<LT>::ColumnType;
    using TimestampType = typename InteralTypeOfFunnel::TypeTraits<LT>::ValueType;

    // first args is timestamp, second is event position.
    using TimestampEvent = std::pair<TimestampType, uint8_t>;
    struct TimestampTypePair {
        TimestampType start_timestamp = -1;
        TimestampType last_timestamp = -1;
    };
    using TimestampVector = std::vector<TimestampTypePair>;
    int64_t window_size;
    mutable int32_t mode = 0;
    uint8_t events_size;
    bool sorted = true;
    char buffer[reserve_list_size * sizeof(TimestampEvent)];
    stack_memory_resource mr;
    mutable std::pmr::vector<TimestampEvent> events_list;
    WindowFunnelState() : mr(buffer, sizeof(buffer)), events_list(&mr) { events_list.reserve(reserve_list_size); }

    void sort() const { std::stable_sort(std::begin(events_list), std::end(events_list)); }

    void update(TimestampType timestamp, uint8_t event_level) {
        // keep only one as a placeholder for someone group.
        if (events_list.size() > 0 && event_level == 0) {
            return;
        }

        if (sorted && events_list.size() > 0) {
            if (events_list.back().first == timestamp)
                sorted = events_list.back().second <= event_level;
            else
                sorted = events_list.back().first <= timestamp;
        }
        events_list.emplace_back(std::make_pair(timestamp, event_level));
    }

    void deserialize_and_merge(FunctionContext* ctx, const int64_t* array, size_t length) {
        if (length == 0) {
            return;
        }

        std::vector<TimestampEvent> other_list;
        window_size = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(1));
        mode = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(2));

        events_size = (uint8_t)array[0];
        bool other_sorted = (uint8_t)array[1];

        for (size_t i = 2; i < length - 1; i += 2) {
            TimestampType timestamp = array[i];
            int64_t event_level = array[i + 1];
            other_list.emplace_back(std::make_pair(timestamp, uint8_t(event_level)));
        }

        const auto size = events_list.size();

        events_list.insert(std::end(events_list), std::begin(other_list), std::end(other_list));
        if (!sorted && !other_sorted)
            std::sort(std::begin(events_list), std::end(events_list), ComparePairFirst{});
        else {
            const auto begin = std::begin(events_list);
            const auto middle = std::next(begin, size);
            const auto end = std::end(events_list);

            if (!sorted) std::stable_sort(begin, middle, ComparePairFirst{});

            if (!other_sorted) std::stable_sort(middle, end, ComparePairFirst{});

            std::inplace_merge(begin, middle, end);
        }

        sorted = true;
    }

    static void serialize(int64_t* buffer, size_t length, ArrayColumn* array_column) {
        CHECK(array_column->elements_column()->append_numbers(buffer, sizeof(int64_t) * length) > 0);
        array_column->offsets_column()->append(array_column->offsets_column()->get_data().back() + length);
    }

    void serialize_to_array_column(ArrayColumn* array_column) const {
        if (!events_list.empty()) {
            size_t size = events_list.size();
            size_t serialize_size = size * 2 + 2;
            // TODO:
            std::unique_ptr<int64_t[]> buffer = std::make_unique<int64_t[]>(serialize_size);

            buffer[0] = (int64_t)events_size;
            buffer[1] = (int64_t)sorted;

            size_t write_idx = 2;
            for (auto [first, second] : events_list) {
                buffer[write_idx++] = first;
                buffer[write_idx++] = second;
            }

            serialize(buffer.get(), write_idx, array_column);
        } else {
            array_column->append_default();
        }
    }

    int32_t get_event_level() const {
        if (!sorted) {
            sort();
        }

        bool increase = (mode & INCREASE);
        mode &= DEDUPLICATION_FIXED;
        auto const& ordered_events_list = events_list;
        if (!mode) {
            TimestampVector events_timestamp(events_size);
            auto begin = ordered_events_list.begin();
            while (begin != ordered_events_list.end()) {
                TimestampType timestamp = (*begin).first;
                uint8_t event_idx = (*begin).second;

                if (event_idx == 0) {
                    ++begin;
                    continue;
                }

                event_idx -= 1;
                if (event_idx == 0) {
                    events_timestamp[0].start_timestamp = timestamp;
                    events_timestamp[0].last_timestamp = timestamp;
                } else if (events_timestamp[event_idx - 1].start_timestamp >= 0) {
                    bool matched = timestamp <= events_timestamp[event_idx - 1].start_timestamp + window_size;
                    if (increase) {
                        matched = matched && events_timestamp[event_idx - 1].last_timestamp < timestamp;
                    }
                    if (matched) {
                        events_timestamp[event_idx].start_timestamp = events_timestamp[event_idx - 1].start_timestamp;
                        events_timestamp[event_idx].last_timestamp = timestamp;
                        if (event_idx + 1 == events_size) {
                            return events_size;
                        }
                    }
                }
                ++begin;
            }

            for (size_t event = events_timestamp.size(); event > 0; --event) {
                if (events_timestamp[event - 1].start_timestamp >= 0) {
                    return event;
                }
            }
            return 0;
        }

        // max level when search event chains.
        int8_t max_level = -1;
        // curr_event_level is used to record max level in search for a event chain,
        // It will update max_level and reduce when encounter condition of deduplication or fixed mode.
        int8_t curr_event_level = -1;

        /*
         * EventTimestamp's element is the timestamp of event.
         */
        TimestampVector events_timestamp(events_size);
        auto begin = ordered_events_list.begin();
        switch (mode) {
        // mode: deduplication
        case DEDUPLICATION: {
            while (begin != ordered_events_list.end()) {
                TimestampType timestamp = (*begin).first;
                uint8_t event_idx = (*begin).second;

                if (event_idx == 0) {
                    ++begin;
                    continue;
                }

                event_idx -= 1;
                // begin a new event chain.
                if (event_idx == 0) {
                    events_timestamp[0].start_timestamp = timestamp;
                    if (event_idx > curr_event_level) {
                        curr_event_level = event_idx;
                    }
                    // encounter condition of deduplication: an existing event occurs.
                } else if (events_timestamp[event_idx].start_timestamp >= 0) {
                    if (curr_event_level > max_level) {
                        max_level = curr_event_level;
                    }

                    // Eliminate last event chain
                    eliminate_last_event_chains(&curr_event_level, &events_timestamp);

                } else if (events_timestamp[event_idx - 1].start_timestamp >= 0) {
                    if (promote_to_next_level(&events_timestamp, timestamp, event_idx, &curr_event_level, increase)) {
                        return events_size;
                    }
                }
                ++begin;
            }
        } break;
        // mode: fixed
        case FIXED: {
            bool first_event = false;
            while (begin != ordered_events_list.end()) {
                TimestampType timestamp = (*begin).first;
                uint8_t event_idx = (*begin).second;

                if (event_idx == 0) {
                    ++begin;
                    continue;
                }

                event_idx -= 1;
                if (event_idx == 0) {
                    events_timestamp[0].start_timestamp = timestamp;
                    if (event_idx > curr_event_level) {
                        curr_event_level = event_idx;
                    }
                    first_event = true;
                    // encounter condition of fixed: a leap event occurred.
                } else if (first_event && events_timestamp[event_idx - 1].start_timestamp < 0) {
                    if (curr_event_level >= 0) {
                        if (curr_event_level > max_level) {
                            max_level = curr_event_level;
                        }

                        // Eliminate last event chain
                        eliminate_last_event_chains(&curr_event_level, &events_timestamp);
                    }
                } else if (events_timestamp[event_idx - 1].start_timestamp >= 0) {
                    if (promote_to_next_level(&events_timestamp, timestamp, event_idx, &curr_event_level, increase)) {
                        return events_size;
                    }
                }
                ++begin;
            }
        } break;
        // mode: deduplication | fixed
        case DEDUPLICATION_FIXED: {
            bool first_event = false;
            while (begin != ordered_events_list.end()) {
                TimestampType timestamp = (*begin).first;
                uint8_t event_idx = (*begin).second;

                if (event_idx == 0) {
                    ++begin;
                    continue;
                }

                event_idx -= 1;

                if (event_idx == 0) {
                    events_timestamp[0].start_timestamp = timestamp;
                    if (event_idx > curr_event_level) {
                        curr_event_level = event_idx;
                    }
                    first_event = true;
                } else if (events_timestamp[event_idx].start_timestamp >= 0) {
                    if (curr_event_level > max_level) {
                        max_level = curr_event_level;
                    }

                    // Eliminate last event chain
                    eliminate_last_event_chains(&curr_event_level, &events_timestamp);

                } else if (first_event && events_timestamp[event_idx - 1].start_timestamp < 0) {
                    if (curr_event_level >= 0) {
                        if (curr_event_level > max_level) {
                            max_level = curr_event_level;
                        }

                        // Eliminate last event chain
                        eliminate_last_event_chains(&curr_event_level, &events_timestamp);
                    }
                } else if (events_timestamp[event_idx - 1].start_timestamp >= 0) {
                    if (promote_to_next_level(&events_timestamp, timestamp, event_idx, &curr_event_level, increase)) {
                        return events_size;
                    }
                }
                ++begin;
            }
        } break;

        default:
            DCHECK(false);
        }

        if (curr_event_level > max_level) {
            return curr_event_level + 1;
        } else {
            return max_level + 1;
        }
    }

    static void eliminate_last_event_chains(int8_t* curr_event_level, TimestampVector* events_timestamp) {
        for (; (*curr_event_level) >= 0; --(*curr_event_level)) {
            (*events_timestamp)[(*curr_event_level)].start_timestamp = -1;
        }
    }

    bool promote_to_next_level(TimestampVector* events_timestamp, const TimestampType& timestamp,
                               const int8_t event_idx, int8_t* curr_event_level, bool increase) const {
        auto first_timestamp = (*events_timestamp)[event_idx - 1].start_timestamp;
        bool time_matched = (timestamp <= (first_timestamp + window_size));
        if (increase) {
            time_matched = time_matched && (*events_timestamp)[event_idx - 1].last_timestamp < timestamp;
        }

        if (time_matched) {
            // use prev level event's event_chain_level to record with this event.
            (*events_timestamp)[event_idx].start_timestamp = first_timestamp;
            (*events_timestamp)[event_idx].last_timestamp = timestamp;
            // update curr_event_level to bigger one.
            if (event_idx > (*curr_event_level)) {
                (*curr_event_level) = event_idx;
            }

            if (event_idx + 1 == events_size) {
                return true;
            }
        }

        return false;
    }

    // 1th bit for deduplication, 2th bit for fixed.
    static inline int8_t MODE_FLAGS[] = {1 << 0, 1 << 1};
};

template <LogicalType LT>
class WindowFunnelAggregateFunction final
        : public AggregateFunctionBatchHelper<WindowFunnelState<LT>, WindowFunnelAggregateFunction<LT>> {
    using TimeTypeColumn = typename WindowFunnelState<LT>::TimeTypeColumn;
    using TimeType = typename WindowFunnelState<LT>::TimeType;
    using TimestampType = typename WindowFunnelState<LT>::TimestampType;

public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(columns[2]->is_constant());

        this->data(state).window_size = down_cast<const Int64Column*>(columns[0])->get_data()[0];
        this->data(state).mode = ColumnHelper::get_const_value<TYPE_INT>(columns[2]);

        // get timestamp
        TimeType tv;
        if (!columns[1]->is_constant()) {
            const auto timestamp_column = down_cast<const TimeTypeColumn*>(columns[1]);
            DCHECK(LT == TYPE_DATETIME || LT == TYPE_DATE || LT == TYPE_INT || LT == TYPE_BIGINT);
            tv = timestamp_column->get_data()[row_num];
        } else {
            tv = ColumnHelper::get_const_value<LT>(columns[1]);
        }

        // get event
        uint8_t event_level = 0;
        const auto* event_column = down_cast<const ArrayColumn*>(columns[3]);

        const UInt32Column& offsets = event_column->offsets();
        auto offsets_ptr = offsets.get_data().data();
        size_t offset = offsets_ptr[row_num];
        size_t array_size = offsets_ptr[row_num + 1] - offsets_ptr[row_num];

        const Column& elements = event_column->elements();
        if (elements.is_nullable()) {
            const auto& null_column = down_cast<const NullableColumn&>(elements);
            auto data_column = down_cast<BooleanColumn*>(null_column.data_column().get());
            const auto& null_vector = null_column.null_column()->get_data();
            for (int i = 0; i < array_size; ++i) {
                auto ele_offset = offset + i;
                if (!null_vector[ele_offset] && data_column->get_data()[ele_offset]) {
                    event_level = i + 1;
                    if constexpr (LT == TYPE_DATETIME) {
                        this->data(state).update(tv.to_unix_second(), event_level);
                    } else if constexpr (LT == TYPE_DATE) {
                        this->data(state).update(tv.julian(), event_level);
                    } else {
                        this->data(state).update(tv, event_level);
                    }
                }
            }
        } else {
            const auto& data_column = down_cast<const BooleanColumn&>(elements);
            for (int i = 0; i < array_size; ++i) {
                auto ele_offset = offset + i;
                if (data_column.get_data()[ele_offset]) {
                    event_level = i + 1;
                    if constexpr (LT == TYPE_DATETIME) {
                        this->data(state).update(tv.to_unix_second(), event_level);
                    } else if constexpr (LT == TYPE_DATE) {
                        this->data(state).update(tv.julian(), event_level);
                    } else {
                        this->data(state).update(tv, event_level);
                    }
                }
            }
        }
        this->data(state).events_size = array_size;
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_array());
        DCHECK(!column->is_nullable());
        const auto* input_column = down_cast<const ArrayColumn*>(column);
        const auto& offsets = input_column->offsets().get_data();
        const auto& elements = input_column->elements();
        const int64_t* raw_data;
        if (elements.is_nullable()) {
            auto data_elements = down_cast<const NullableColumn*>(&elements)->data_column().get();
            raw_data = down_cast<const Int64Column*>(data_elements)->get_data().data();
        } else {
            raw_data = down_cast<const Int64Column*>(&elements)->get_data().data();
        }

        size_t offset = offsets[row_num];
        size_t array_size = offsets[row_num + 1] - offsets[row_num];

        this->data(state).deserialize_and_merge(ctx, raw_data + offset, array_size);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* array_column = down_cast<ArrayColumn*>(to);
        this->data(state).serialize_to_array_column(array_column);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        down_cast<Int32Column*>(to)->append(this->data(state).get_event_level());
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* dst_column = down_cast<ArrayColumn*>((*dst).get());
        dst_column->reserve(chunk_size);

        const auto timestamp_column = down_cast<const TimeTypeColumn*>(src[1].get());
        const auto* bool_array_column = down_cast<const ArrayColumn*>(src[3].get());
        for (int i = 0; i < chunk_size; i++) {
            TimestampType tv;
            if constexpr (LT == TYPE_DATETIME) {
                tv = timestamp_column->get_data()[i].to_unix_second();
            } else if constexpr (LT == TYPE_DATE) {
                tv = timestamp_column->get_data()[i].julian();
            } else {
                tv = timestamp_column->get_data()[i];
            }

            // get 4th value: event cond array
            auto ele_vector = bool_array_column->get(i).get_array();
            uint8_t event_level = 0;
            for (uint8_t j = 0; j < ele_vector.size(); j++) {
                if (!ele_vector[j].is_null() && ele_vector[j].get_uint8() > 0) {
                    event_level = j + 1;
                    break;
                }
            }

            size_t events_size = ele_vector.size();
            bool sorted = false;
            int64_t buffer[4];
            buffer[0] = (int64_t)events_size;
            buffer[1] = (int64_t)sorted;
            buffer[2] = (int64_t)tv;
            buffer[3] = (int64_t)event_level;
            WindowFunnelState<LT>::serialize(buffer, 4, dst_column);
        }
    }

    std::string get_name() const override { return "window_funnel"; }
};

} // namespace starrocks
