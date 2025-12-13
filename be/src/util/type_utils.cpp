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

#include "util/type_utils.h"

#include <algorithm>

namespace starrocks {

size_t TypeUtils::compute_materialization_cost(const TypeDescriptor& type_desc, size_t average_array_size,
                                               size_t average_map_size) {
    if (type_desc.is_string_type()) {
        // String types use Slice (16 bytes) plus variable-length data
        // The actual data copy cost is hard to estimate, so we use Slice size as base
        return 16;
    } else if (type_desc.is_struct_type()) {
        // Struct: 8 bytes base overhead for null flags + sum of all children costs
        size_t cost = 8;
        for (const auto& child : type_desc.children) {
            cost += compute_materialization_cost(child, average_array_size, average_map_size);
        }
        return cost;
    } else if (type_desc.is_array_type()) {
        // Array: 4 bytes for offset column + element cost * average array size
        size_t element_cost = 8; // default for empty children
        if (!type_desc.children.empty()) {
            element_cost = compute_materialization_cost(type_desc.children[0], average_array_size, average_map_size);
        }
        return 4 + element_cost * average_array_size;
    } else if (type_desc.is_map_type()) {
        // Map: 4 bytes for offset column + (key cost + value cost) * average map size
        size_t key_cost = 8;   // default
        size_t value_cost = 8; // default
        if (type_desc.children.size() > 0) {
            key_cost = compute_materialization_cost(type_desc.children[0], average_array_size, average_map_size);
        }
        if (type_desc.children.size() > 1) {
            value_cost = compute_materialization_cost(type_desc.children[1], average_array_size, average_map_size);
        }
        return 4 + (key_cost + value_cost) * average_map_size;
    } else {
        // Scalar types: use get_slot_size with minimum of 1
        return std::max<size_t>(1, type_desc.get_slot_size());
    }
}

} // namespace starrocks
