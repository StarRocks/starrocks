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

#include "runtime/runtime_filter_factory.h"

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "common/system/cpu_info.h"
#include "runtime/runtime_in_filter.h"
#include "types/logical_type_infra.h"

namespace starrocks {

template <template <LogicalType> typename FilterType>
static RuntimeFilter* create_runtime_filter_helper(ObjectPool* pool, LogicalType type, int8_t join_mode) {
    RuntimeFilter* filter = type_dispatch_filter(type, static_cast<RuntimeFilter*>(nullptr), [&]<LogicalType LT>() {
        RuntimeFilter* rf = new FilterType<LT>();
        rf->get_membership_filter()->set_join_mode(join_mode);
        return rf;
    });

    if (pool != nullptr && filter != nullptr) {
        return pool->add(filter);
    } else {
        return filter;
    }
}

RuntimeFilter* RuntimeFilterFactory::to_empty_filter(ObjectPool* pool, RuntimeFilter* rf) {
    const auto* min_max_filter = rf->get_min_max_filter();
    const auto* membership_filter = rf->get_membership_filter();
    RuntimeFilter* filter = type_dispatch_filter(
            membership_filter->logical_type(), static_cast<RuntimeFilter*>(nullptr),
            [&]<LogicalType LT>() -> RuntimeFilter* {
                return new ComposedRuntimeEmptyFilter<LT>(*down_cast<const MinMaxRuntimeFilter<LT>*>(min_max_filter),
                                                          *membership_filter);
            });

    if (pool != nullptr && filter != nullptr) {
        return pool->add(filter);
    } else {
        return filter;
    }
}

RuntimeFilter* RuntimeFilterFactory::create_empty_filter(ObjectPool* pool, LogicalType type, int8_t join_mode) {
    return create_runtime_filter_helper<ComposedRuntimeEmptyFilter>(pool, type, join_mode);
}

RuntimeFilter* RuntimeFilterFactory::create_bloom_filter(ObjectPool* pool, LogicalType type, int8_t join_mode) {
    return create_runtime_filter_helper<ComposedRuntimeBloomFilter>(pool, type, join_mode);
}

RuntimeFilter* RuntimeFilterFactory::create_in_filter(ObjectPool* pool, LogicalType type, int8_t join_mode) {
    return scalar_type_dispatch(
            type, [pool]<LogicalType ltype>() -> RuntimeFilter* { return InRuntimeFilter<ltype>::create(pool); });
}

RuntimeFilter* RuntimeFilterFactory::create_bitset_filter(ObjectPool* pool, LogicalType type, int8_t join_mode) {
    RuntimeFilter* filter =
            type_dispatch_bitset_filter(type, static_cast<RuntimeFilter*>(nullptr), [&]<LogicalType LT>() {
                RuntimeFilter* rf = new ComposedRuntimeBitsetFilter<LT>();
                rf->get_membership_filter()->set_join_mode(join_mode);
                return rf;
            });

    if (pool != nullptr && filter != nullptr) {
        return pool->add(filter);
    } else {
        return filter;
    }
}

RuntimeFilter* RuntimeFilterFactory::create_filter(ObjectPool* pool, RuntimeFilterSerializeType rf_type,
                                                   LogicalType ltype, int8_t join_mode) {
    switch (rf_type) {
    case RuntimeFilterSerializeType::EMPTY_FILTER:
        return create_empty_filter(pool, ltype, join_mode);
    case RuntimeFilterSerializeType::BLOOM_FILTER:
        return create_bloom_filter(pool, ltype, join_mode);
    case RuntimeFilterSerializeType::BITSET_FILTER:
        return create_bitset_filter(pool, ltype, join_mode);
    case RuntimeFilterSerializeType::IN_FILTER:
        return create_in_filter(pool, ltype, join_mode);
    case RuntimeFilterSerializeType::NONE:
    default:
        return nullptr;
    }
}

template <LogicalType LT, typename CppType = RunTimeCppType<LT>>
static std::pair<CppType, CppType> calc_min_max_from_columns(const Columns& columns, size_t column_offset) {
    auto min_value = std::numeric_limits<CppType>::max();
    auto max_value = std::numeric_limits<CppType>::min();
    for (const auto& col : columns) {
        if (!col->is_nullable()) {
            const auto& values = GetContainer<LT>::get_data(col.get());
            for (size_t i = column_offset; i < values.size(); i++) {
                min_value = std::min(min_value, values[i]);
                max_value = std::max(max_value, values[i]);
            }
        } else {
            const auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(col);
            const auto& values = GetContainer<LT>::get_data(nullable_column->data_column().get());
            if (!nullable_column->has_null()) {
                for (size_t i = column_offset; i < values.size(); i++) {
                    min_value = std::min(min_value, values[i]);
                    max_value = std::max(max_value, values[i]);
                }
            } else {
                const auto& null_data = nullable_column->immutable_null_column_data();
                for (size_t i = column_offset; i < values.size(); i++) {
                    if (null_data[i] == 0) {
                        min_value = std::min(min_value, values[i]);
                        max_value = std::max(max_value, values[i]);
                    }
                }
            }
        }
    }

    return {min_value, max_value};
}

struct TryCreateRuntimeBitsetFilter {
    template <LogicalType LT>
    RuntimeFilter* operator()(const Columns& columns, size_t column_offset, size_t row_count) {
        static auto cache_sizes = [] {
            static constexpr size_t DEFAULT_L2_CACHE_SIZE = 1 * 1024 * 1024;
            static constexpr size_t DEFAULT_L3_CACHE_SIZE = 32 * 1024 * 1024;

            const auto& cache_sizes = CpuInfo::get_cache_sizes();
            const auto cur_l2_cache_size = cache_sizes[CpuInfo::L2_CACHE];
            const auto cur_l3_cache_size = cache_sizes[CpuInfo::L3_CACHE];
            return std::pair{cur_l2_cache_size ? cur_l2_cache_size : DEFAULT_L2_CACHE_SIZE,
                             cur_l3_cache_size ? cur_l3_cache_size : DEFAULT_L3_CACHE_SIZE};
        }();
        const auto& [l2_cache_size, l3_cache_size] = cache_sizes;

        const auto [min_value, max_value] = calc_min_max_from_columns<LT>(columns, column_offset);
        const size_t value_interval = static_cast<size_t>(RuntimeBitsetFilter<LT>::to_numeric(max_value)) -
                                      RuntimeBitsetFilter<LT>::to_numeric(min_value) + 1;

        VLOG_FILE << "[BITSET] min_value=" << min_value << ", max_value=" << max_value
                  << ", value_interval=" << value_interval;

        if (min_value > max_value || value_interval == 0) {
            return nullptr;
        }

        const size_t bitset_memory_usage = (value_interval + 7) / 8;
        const size_t bloom_memory_usage = row_count;
        if (bitset_memory_usage <= bloom_memory_usage ||
            (bitset_memory_usage <= bloom_memory_usage * 4 && bitset_memory_usage <= l2_cache_size) ||
            (bitset_memory_usage <= bloom_memory_usage * 2 && bitset_memory_usage <= l3_cache_size)) {
            auto* rf = new ComposedRuntimeBitsetFilter<LT>();
            rf->membership_filter().set_min_max(min_value, max_value);
            return rf;
        }

        return nullptr;
    }
};

RuntimeFilter* RuntimeFilterFactory::create_join_filter(ObjectPool* pool, LogicalType type, int8_t join_mode,
                                                        const Columns& columns, size_t column_offset,
                                                        size_t row_count) {
    RuntimeFilter* filter =
            type_dispatch_bitset_filter(type, static_cast<RuntimeFilter*>(nullptr), TryCreateRuntimeBitsetFilter(),
                                        columns, column_offset, row_count);
    if (filter == nullptr) {
        return create_bloom_filter(pool, type, join_mode);
    }
    if (pool != nullptr) {
        return pool->add(filter);
    }
    return filter;
}

} // namespace starrocks
