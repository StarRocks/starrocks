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

#include <type_traits>

#include "column/column.h"
#include "column/type_traits.h"
#include "util/phmap/phmap.h"
namespace starrocks {

template <LogicalType LT, typename = guard::Guard>
struct DetailStateMap {};

template <LogicalType LT>
struct DetailStateMap<LT, FixedLengthLTGuard<LT>> {
    using CppType = RunTimeCppValueType<LT>;
    using KeyType = CppType;
    using HashMap = phmap::flat_hash_map<KeyType, int64_t, StdHash<CppType>>;
};

template <LogicalType LT>
struct DetailStateMap<LT, StringLTGuard<LT>> {
    using CppType = RunTimeCppValueType<LT>;
    using KeyType = std::string;
    using HashMap = phmap::flat_hash_map<KeyType, int64_t, SliceHash>;
};

// TODO: Support detail agg state reusable between different agg stats.
// TODO: How to handle count=0's key-value?
template <LogicalType LT>
class StreamDetailState {
public:
    using CppType = RunTimeCppType<LT>;
    using StateHashMap = typename DetailStateMap<LT>::HashMap;
    using HashMapKeyType = typename DetailStateMap<LT>::KeyType;

    StreamDetailState() = default;
    ~StreamDetailState() = default;

    template <bool only_not_found = false>
    void update_rows(const CppType& v, int64_t num_rows) {
        auto value = _convert_to_key_type(v);
        auto iter = _detail_state.find(value);
        auto is_found = iter != _detail_state.end();
        if constexpr (only_not_found) {
            if (is_found) return;
            _detail_state.emplace(value, num_rows);
        } else {
            if (is_found) {
                iter->second += num_rows;
            } else {
                _detail_state.emplace(value, num_rows);
            }
        }
    }

    bool exists(const CppType& v) {
        auto value = _convert_to_key_type(v);
        return _detail_state.find(value) != _detail_state.end();
    }

    const StateHashMap& detail_state() const { return _detail_state; }
    const bool is_sync() const { return _is_sync; }
    void set_is_sync(bool sync) { this->_is_sync = sync; }
    const bool is_restore_incremental() const { return _is_restore_incremental; }
    void set_is_restore_incremental(bool sync) { this->_is_restore_incremental = sync; }
    void reset() {
        _is_sync = false;
        _detail_state.clear();
    }

private:
    HashMapKeyType _convert_to_key_type(CppType v) {
        if constexpr (lt_is_string<LT>) {
            return std::string(v.data, v.size);
        } else {
            return v;
        }
    }

    // Keep tracts with all details for a specific group by key to
    // be used for detail aggregation retracts.
    StateHashMap _detail_state;
    // Mark whether sync all detail data when generating output results,
    // when _is_sync=true, use all key-values in _detail_state to update
    // intermediate state, and generate the final results.
    bool _is_sync{false};
    // Make whether sync all incremental records(partial) in the map into agg state.
    bool _is_restore_incremental{false};
};

} // namespace starrocks
