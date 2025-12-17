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

namespace starrocks {

struct AggInRuntimeFilterBuilderImpl {
    template <LogicalType ltype>
    RuntimeFilter* operator()(ObjectPool* pool, Aggregator* aggregator, size_t build_expr_order) {
        auto runtime_filter = InRuntimeFilter<ltype>::create(pool);
        auto& hash_map_variant = aggregator->hash_map_variant();
        hash_map_variant.visit([&](auto& variant_value) {
            auto& hash_map_with_key = *variant_value;
            using HashMapWithKey = std::remove_reference_t<decltype(hash_map_with_key)>;
            using ResultVector = typename HashMapWithKey::ResultVector;
            auto& hash_map = hash_map_with_key.hash_map;
            const size_t hash_map_size = hash_map_variant.size();
            MutableColumns group_by_columns = aggregator->create_group_by_columns(hash_map_size);
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
        if (total_size > config::max_pushdown_conditions_per_column) {
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