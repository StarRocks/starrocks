// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <cstring>
#include <limits>
#include <type_traits>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "exprs/agg/aggregate.h"
#include "gen_cpp/Data_types.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "thrift/protocol/TJSONProtocol.h"
#include "udf/udf_internal.h"
#include "util/phmap/phmap_dump.h"
#include "util/slice.h"

namespace starrocks::vectorized {

struct ComparePairFirst final {
    template <typename T1, typename T2>
    bool operator()(const std::pair<T1, T2>& lhs, const std::pair<T1, T2>& rhs) const {
        return std::tie(lhs) < std::tie(rhs);
    }
};

enum FunnelMode : int { DEDUPLICATION = 1, FIXED = 2, DEDUPLICATION_FIXED = 3 };

template <PrimitiveType PT>
struct WindowFunnelState {
    // Use to identify timestamp(datetime/date)
    using TimeType = typename RunTimeTypeTraits<PT>::CppType;
    using TimeTypeColumn = typename RunTimeTypeTraits<PT>::ColumnType;
    using TimestampType = typename TimeType::type;

    // first args is timestamp, second is event position.
    using TimestampEvent = std::pair<TimestampType, uint8_t>;
    mutable std::vector<TimestampEvent> events_list;
    using TimestampVector = std::vector<TimestampType>;
    int64_t window_size;
    int32_t mode;
    uint8_t events_size;
    bool sorted = true;

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

    void deserialize_and_merge(FunctionContext* ctx, DatumArray& datum_array) {
        if (datum_array.size() == 0) {
            return;
        }

        std::vector<TimestampEvent> other_list;
        window_size = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(1));
        mode = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(2));

        events_size = (uint8_t)datum_array[0].get_int64();
        bool other_sorted = (uint8_t)datum_array[1].get_int64();

        for (size_t i = 2; i < datum_array.size() - 1; i += 2) {
            TimestampType timestamp = datum_array[i].get_int64();
            int64_t event_level = datum_array[i + 1].get_int64();
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

    void serialize_to_array_column(ArrayColumn* array_column) const {
        if (!events_list.empty()) {
            size_t size = events_list.size();
            DatumArray array;
            array.reserve(size * 2 + 2);
            array.emplace_back((int64_t)events_size);
            array.emplace_back((int64_t)sorted);
            auto curr = events_list.begin();
            while (curr != events_list.end()) {
                array.emplace_back((int64_t)(*curr).first);
                array.emplace_back((int64_t)(*curr).second);
                ++curr;
            }
            array_column->append_datum(array);
        }
    }

    int32_t get_event_level() const {
        if (!sorted) {
            sort();
        }

        auto const& ordered_events_list = events_list;
        if (!mode) {
            TimestampVector events_timestamp(events_size, -1);
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
                    events_timestamp[0] = timestamp;
                } else if (events_timestamp[event_idx - 1] >= 0 &&
                           timestamp <= events_timestamp[event_idx - 1] + window_size) {
                    events_timestamp[event_idx] = events_timestamp[event_idx - 1];
                    if (event_idx + 1 == events_size) {
                        return events_size;
                    }
                }
                ++begin;
            }

            for (size_t event = events_timestamp.size(); event > 0; --event) {
                if (events_timestamp[event - 1] >= 0) {
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
        TimestampVector events_timestamp(events_size, -1);
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
                    events_timestamp[0] = timestamp;
                    if (event_idx > curr_event_level) {
                        curr_event_level = event_idx;
                    }
                    // encounter condition of deduplication: an existing event occurs.
                } else if (events_timestamp[event_idx] >= 0) {
                    if (curr_event_level > max_level) {
                        max_level = curr_event_level;
                    }

                    // Eliminate last event chain
                    eliminate_last_event_chains(&curr_event_level, &events_timestamp);

                } else if (events_timestamp[event_idx - 1] >= 0) {
                    if (promote_to_next_level(&events_timestamp, timestamp, event_idx, &curr_event_level)) {
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
                    events_timestamp[0] = timestamp;
                    if (event_idx > curr_event_level) {
                        curr_event_level = event_idx;
                    }
                    first_event = true;
                    // encounter condition of fixed: a leap event occurred.
                } else if (first_event && events_timestamp[event_idx - 1] < 0) {
                    if (curr_event_level >= 0) {
                        if (curr_event_level > max_level) {
                            max_level = curr_event_level;
                        }

                        // Eliminate last event chain
                        eliminate_last_event_chains(&curr_event_level, &events_timestamp);
                    }
                } else if (events_timestamp[event_idx - 1] >= 0) {
                    if (promote_to_next_level(&events_timestamp, timestamp, event_idx, &curr_event_level)) {
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
                    events_timestamp[0] = timestamp;
                    if (event_idx > curr_event_level) {
                        curr_event_level = event_idx;
                    }
                    first_event = true;
                } else if (events_timestamp[event_idx] >= 0) {
                    if (curr_event_level > max_level) {
                        max_level = curr_event_level;
                    }

                    // Eliminate last event chain
                    eliminate_last_event_chains(&curr_event_level, &events_timestamp);

                } else if (first_event && events_timestamp[event_idx - 1] < 0) {
                    if (curr_event_level >= 0) {
                        if (curr_event_level > max_level) {
                            max_level = curr_event_level;
                        }

                        // Eliminate last event chain
                        eliminate_last_event_chains(&curr_event_level, &events_timestamp);
                    }
                } else if (events_timestamp[event_idx - 1] >= 0) {
                    if (promote_to_next_level(&events_timestamp, timestamp, event_idx, &curr_event_level)) {
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
            (*events_timestamp)[(*curr_event_level)] = -1;
        }
    }

    bool promote_to_next_level(TimestampVector* events_timestamp, const TimestampType& timestamp,
                               const int8_t event_idx, int8_t* curr_event_level) const {
        auto first_timestamp = (*events_timestamp)[event_idx - 1];
        bool time_matched = (timestamp <= (first_timestamp + window_size));

        if (time_matched) {
            // use prev level event's event_chain_level to record with this event.
            (*events_timestamp)[event_idx] = first_timestamp;

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

template <PrimitiveType PT>
class WindowFunnelAggregateFunction final
        : public AggregateFunctionBatchHelper<WindowFunnelState<PT>, WindowFunnelAggregateFunction<PT>> {
    using TimeTypeColumn = typename WindowFunnelState<PT>::TimeTypeColumn;
    using TimeType = typename WindowFunnelState<PT>::TimeType;
    using TimestampType = typename TimeType::type;

public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state, size_t row_num) const {
        this->data(state).window_size = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(0));
        this->data(state).mode = ColumnHelper::get_const_value<TYPE_INT>(ctx->get_constant_column(2));

        // get timestamp
        TimeType tv;
        if (!columns[1]->is_constant()) {
            const auto timestamp_column = down_cast<const TimeTypeColumn*>(columns[1]);
            if constexpr (PT == TYPE_DATETIME) {
                tv = timestamp_column->get(row_num).get_timestamp();
            } else if constexpr (PT == TYPE_DATE) {
                tv = timestamp_column->get(row_num).get_date();
            }
        } else {
            tv = ColumnHelper::get_const_value<PT>(columns[1]);
        }

        // get event
        uint8_t event_level = 0;
        const auto* event_column = down_cast<const ArrayColumn*>(columns[3]);
        auto ele_vector = event_column->get(row_num).get_array();
        for (uint8_t i = 0; i < ele_vector.size(); i++) {
            if (!ele_vector[i].is_null() && ele_vector[i].get_uint8() > 0) {
                event_level = i + 1;
                if constexpr (PT == TYPE_DATETIME) {
                    this->data(state).update(tv.to_unix_second(), event_level);
                } else if constexpr (PT == TYPE_DATE) {
                    this->data(state).update(tv.julian(), event_level);
                }
            }
        }
        this->data(state).events_size = ele_vector.size();
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* input_column = down_cast<const ArrayColumn*>(column);
        auto ele_vector = input_column->get(row_num).get_array();
        this->data(state).deserialize_and_merge(ctx, ele_vector);
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

        const TimeTypeColumn* timestamp_column = down_cast<const TimeTypeColumn*>(src[1].get());
        const auto* bool_array_column = down_cast<const ArrayColumn*>(src[3].get());
        for (int i = 0; i < chunk_size; i++) {
            TimestampType tv;
            if constexpr (PT == TYPE_DATETIME) {
                tv = timestamp_column->get(i).get_timestamp().to_unix_second();
            } else if constexpr (PT == TYPE_DATE) {
                tv = timestamp_column->get(i).get_date().julian();
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

            DatumArray array;
            size_t events_size = ele_vector.size();
            bool sorted = false;
            array.reserve(1 + 1 + 2);
            array.emplace_back((int64_t)events_size);
            array.emplace_back((int64_t)sorted);
            array.emplace_back((int64_t)tv);
            array.emplace_back((int64_t)event_level);
            dst_column->append_datum(array);
        }
    }

    std::string get_name() const override { return "window_funnel"; }
};

} // namespace starrocks::vectorized