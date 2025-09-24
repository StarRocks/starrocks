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

#include "join_hash_table_descriptor.h"

#include "exec/sorting/sort_helper.h"
#include "util/orlp/pdqsort.h"

namespace starrocks {

// if the same hash values are clustered, after the first probe, all related hash buckets are cached, without too many
// misses. So check time locality of probe keys here.
void HashTableProbeState::consider_probe_time_locality() {
    if (active_coroutines > 0) {
        // redo decision
        if ((probe_chunks & (detect_step - 1)) == 0) {
            int window_size = std::min(active_coroutines * 4, 50);
            if (probe_row_count > window_size) {
                phmap::flat_hash_map<uint32_t, uint32_t, StdHash<uint32_t>> occurrence;
                occurrence.reserve(probe_row_count);
                uint32_t unique_size = 0;
                bool enable_interleaving = true;
                uint32_t target = probe_row_count >> 3;
                for (auto i = 0; i < probe_row_count; i++) {
                    if (occurrence[next[i]] == 0) {
                        ++unique_size;
                        if (unique_size >= target) {
                            break;
                        }
                    }
                    occurrence[next[i]]++;
                    if (i >= window_size) {
                        occurrence[next[i - window_size]]--;
                    }
                }
                if (unique_size < target) {
                    active_coroutines = 0;
                    enable_interleaving = false;
                }
                // enlarge step if the decision is the same, otherwise reduce it
                if (enable_interleaving == last_enable_interleaving) {
                    detect_step = detect_step >= 1024 ? detect_step : (detect_step << 1);
                } else {
                    last_enable_interleaving = enable_interleaving;
                    detect_step = 1;
                }
            } else {
                active_coroutines = 0;
            }
        } else if (!last_enable_interleaving) {
            active_coroutines = 0;
        }
    }
    ++probe_chunks;
}

template <typename CppType, TExprOpcode::type OpCode>
void AsofIndex<CppType, OpCode>::sort() {
    auto comparator = [](const Entry& lhs, const Entry& rhs) {
        if constexpr (is_descending) {
            return SorterComparator<CppType>::compare(lhs.asof_value, rhs.asof_value) > 0;
        } else {
            return SorterComparator<CppType>::compare(lhs.asof_value, rhs.asof_value) < 0;
        }
    };

    ::pdqsort(_entries.begin(), _entries.end(), comparator);
}

template <typename CppType, TExprOpcode::type OpCode>
uint32_t AsofIndex<CppType, OpCode>::find_asof_match(CppType probe_value) const {
    if (_entries.empty()) {
        return 0;
    }

    size_t size = _entries.size();
    size_t low = 0;

#pragma GCC unroll 3
    while (size >= 8) {
        _bound_search_iteration(probe_value, low, size);
    }

    while (size > 0) {
        _bound_search_iteration(probe_value, low, size);
    }

    uint32_t result = (low < _entries.size()) ? _entries[low].row_index : 0;

    return result;
}

template <typename CppType, TExprOpcode::type OpCode>
void AsofIndex<CppType, OpCode>::_bound_search_iteration(CppType probe_value, size_t& low, size_t& size) const {
    size_t half = size / 2;
    size_t other_half = size - half;
    size_t probe_pos = low + half;
    size_t other_low = low + other_half;
    const CppType& entry_value = _entries[probe_pos].asof_value;

    size = half;

    bool condition_result;
    if constexpr (is_descending) {
        if constexpr (is_strict) {
            condition_result = (SorterComparator<CppType>::compare(probe_value, entry_value) <= 0);
            low = condition_result ? other_low : low;
        } else {
            condition_result = (SorterComparator<CppType>::compare(probe_value, entry_value) < 0);
            low = condition_result ? other_low : low;
        }
    } else {
        if constexpr (is_strict) {
            condition_result = (SorterComparator<CppType>::compare(probe_value, entry_value) >= 0);
            low = condition_result ? other_low : low;
        } else {
            condition_result = (SorterComparator<CppType>::compare(probe_value, entry_value) > 0);
            low = condition_result ? other_low : low;
        }
    }
}

#define INSTANTIATE_ASOF_INDEX(CppType)                 \
    template class AsofIndex<CppType, TExprOpcode::LT>; \
    template class AsofIndex<CppType, TExprOpcode::LE>; \
    template class AsofIndex<CppType, TExprOpcode::GT>; \
    template class AsofIndex<CppType, TExprOpcode::GE>;

INSTANTIATE_ASOF_INDEX(int64_t)
INSTANTIATE_ASOF_INDEX(DateValue)
INSTANTIATE_ASOF_INDEX(TimestampValue)

#undef INSTANTIATE_ASOF_INDEX

} // namespace starrocks