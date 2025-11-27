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

#include <cstddef>

#include "runtime/types.h"

namespace starrocks {

// Utility functions for TypeDescriptor operations.
class TypeUtils {
public:
    // Compute the estimated materialization cost for a type.
    // This is used for cost-based decisions like late materialization in sorting.
    //
    // The cost represents the approximate bytes needed to copy/materialize one row
    // of this type during operations like merge sort.
    //
    // For complex types (struct/array/map), the cost is computed recursively:
    // - Struct: base overhead + sum of all children costs
    // - Array: offset column + element cost * average array size
    // - Map: offset column + (key cost + value cost) * average map size
    //
    // Parameters:
    //   type_desc: The type descriptor to compute cost for
    //   average_array_size: Assumed average number of elements in arrays (default: 10)
    //   average_map_size: Assumed average number of entries in maps (default: 5)
    //
    // Returns:
    //   Estimated materialization cost in bytes
    static size_t compute_materialization_cost(const TypeDescriptor& type_desc, size_t average_array_size = 10,
                                               size_t average_map_size = 5);
};

} // namespace starrocks
