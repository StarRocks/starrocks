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

#include "exec/agg_runtime_filter_builder.h"

#include <functional>
#include <type_traits>
#include <variant>

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/aggregator.h"
#include "exprs/agg_in_runtime_filter.h"
#include "exprs/runtime_filter.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "util/heap.h"

namespace starrocks {
class HeapBuilder {
public:
    virtual ~HeapBuilder() = default;
};

template <LogicalType Type, class Compare>
class THeapBuilder final : public HeapBuilder {
public:
    using T = RunTimeCppType<Type>;

    THeapBuilder(Compare comp) : _heap(comp) {}

    const T& top() { return _heap.top(); }

    size_t size() { return _heap.size(); }

    bool empty() { return _heap.empty(); }

    void reserve(size_t reserve_sz) { _heap.reserve(reserve_sz); }

    void replace_top(T&& new_top) { _heap.replace_top(std::move(new_top)); }

    void push(T&& rowcur) { _heap.push(std::move(rowcur)); }

private:
    SortingHeap<T, std::vector<T>, Compare> _heap;
};

struct TopNBuildHeapBuilder {
    template <LogicalType ltype>
    std::pair<HeapBuilder*, RuntimeFilter*> operator()(ObjectPool* pool, Aggregator* aggregator, size_t limit,
                                                       bool asc) {
        using CppType = RunTimeCppType<ltype>;
        if (asc) {
            return build<ltype, std::less<CppType>>(pool, aggregator, limit, asc);
        } else {
            return build<ltype, std::greater<CppType>>(pool, aggregator, limit, asc);
        }
    }

    template <LogicalType ltype, class Comp>
    std::pair<HeapBuilder*, RuntimeFilter*> build(ObjectPool* pool, Aggregator* aggregator, size_t limit, bool asc) {
        auto heap_builder = new THeapBuilder<ltype, Comp>(Comp());
        RuntimeFilter* filter = MinMaxRuntimeFilter<ltype>::create_full_range_with_null(pool);
        return {heap_builder, filter};
    }
};

struct TopNBuildHeapUpdater {
    template <LogicalType ltype>
    void* operator()(HeapBuilder* heap, RuntimeFilter* rf, ObjectPool* pool, const Column* column, size_t limit,
                     bool asc) {
        using CppType = RunTimeCppType<ltype>;
        if (asc) {
            return build<ltype, std::less<CppType>, true>(heap, rf, pool, column, limit);
        } else {
            return build<ltype, std::greater<CppType>, false>(heap, rf, pool, column, limit);
        }

        return nullptr;
    }

    template <LogicalType ltype, class Comp, bool isAsc>
    void* build(HeapBuilder* heap, RuntimeFilter* rf, ObjectPool* pool, const Column* column, size_t limit) {
        auto heap_builder = down_cast<THeapBuilder<ltype, Comp>*>(heap);
        if (column->is_nullable()) {
            const auto& column_data = GetContainer<ltype>::get_data(column);
            size_t num_rows = column->size();
            for (size_t i = 0; i < num_rows; ++i) {
                auto val = column_data[i];
                if (heap_builder->size() < limit) {
                    heap_builder->push(std::move(val));
                } else if (Comp()(val, heap_builder->top())) {
                    heap_builder->replace_top(std::move(val));
                }
            }
        } else {
            // auto spec_column = ColumnHelper::cast_to_raw<ltype>(column);
            const auto& column_data = GetContainer<ltype>::get_data(column);
            size_t num_rows = column->size();
            for (size_t i = 0; i < num_rows; ++i) {
                auto val = column_data[i];
                if (heap_builder->size() < limit) {
                    heap_builder->push(std::move(val));
                } else if (Comp()(val, heap_builder->top())) {
                    heap_builder->replace_top(std::move(val));
                }
            }
        }
        down_cast<MinMaxRuntimeFilter<ltype>*>(rf)->template update_min_max<isAsc>(heap_builder->top());
        return nullptr;
    }
};

RuntimeFilter* AggTopNRuntimeFilterBuilder::init_build(Aggregator* aggretator, ObjectPool* pool) {
    auto [builder, rf] = type_dispatch_predicate<std::pair<HeapBuilder*, RuntimeFilter*>>(
            _type, false, TopNBuildHeapBuilder(), pool, aggretator, _build_desc->limit(), _build_desc->is_asc());
    _heap_builder.reset(builder);
    _runtime_filter = rf;
    return rf;
}

RuntimeFilter* AggTopNRuntimeFilterBuilder::update(const Column* column, ObjectPool* pool) {
    type_dispatch_predicate<void*>(_type, false, TopNBuildHeapUpdater(), _heap_builder.get(), _runtime_filter, pool,
                                   column, _build_desc->limit(), _build_desc->is_asc());
    return _runtime_filter;
}

void AggTopNRuntimeFilterBuilder::close() {}

struct AggInRuntimeFilterBuilderImpl {
    template <LogicalType ltype>
    RuntimeFilter* operator()(ObjectPool* pool, Aggregator* aggregator, size_t build_expr_order) {
        auto runtime_filter = InRuntimeFilter<ltype>::create(pool);
        auto& hash_map_variant = aggregator->hash_map_variant();
        hash_map_variant.visit([&](auto& variant_value) {
            auto& hash_map_with_key = *variant_value;
            using HashMapWithKey = std::remove_reference_t<decltype(hash_map_with_key)>;
            using ResultVector = HashMapWithKey::ResultVector;
            auto& hash_map = hash_map_with_key.hash_map;
            const size_t hash_map_size = hash_map_variant.size();
            Columns group_by_columns = aggregator->create_group_by_columns(hash_map_size);
            {
                ResultVector result_vector;
                result_vector.resize(hash_map_size);
                auto it = hash_map.begin();
                auto end = hash_map.end();
                size_t read_index = 0;
                while (it != end) {
                    result_vector[read_index++] = it->first;
                    ++it;
                }
                if (read_index > 0) {
                    hash_map_with_key.insert_keys_to_columns(result_vector, group_by_columns, read_index);
                }
                if constexpr (HashMapWithKey::has_single_null_key) {
                    if (hash_map_with_key.null_key_data != nullptr) {
                        DCHECK(group_by_columns.size() == 1);
                        group_by_columns[0]->append_default();
                    }
                }
                runtime_filter->build(group_by_columns[build_expr_order].get());
            }
        });

        return runtime_filter;
    }
};

RuntimeFilter* AggInRuntimeFilterBuilder::build(Aggregator* aggretator, ObjectPool* pool) {
    return type_dispatch_predicate<RuntimeFilter*>(_type, false, AggInRuntimeFilterBuilderImpl(), pool, aggretator,
                                                   _build_desc->build_expr_order());
}

bool AggInRuntimeFilterMerger::merge(size_t seq, RuntimeFilterBuildDescriptor* desc, RuntimeFilter* in_rf) {
    if (in_rf == nullptr) {
        _always_true = true;
        return false;
    }
    if (_always_true) {
        return false;
    }

    _target_filters[seq] = in_rf;

    if (--_merged == 0) {
        size_t total_size = 0;
        scalar_type_dispatch(desc->build_expr_type(), [this, &total_size]<LogicalType Type>() {
            for (size_t i = 0; i < _target_filters.size(); ++i) {
                total_size += down_cast<InRuntimeFilter<Type>*>(_target_filters[i])->size();
            }
        });
        if (total_size > 1024) {
            _always_true = true;
            return false;
        }
        for (size_t i = 1; i < _target_filters.size(); ++i) {
            _target_filters[0]->merge(_target_filters[i]);
        }

        return true;
    }

    return false;
}

} // namespace starrocks