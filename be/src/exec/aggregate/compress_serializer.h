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

#include <any>

#include "column/column.h"
#include "types/logical_type.h"

namespace starrocks {
class VectorizedLiteral;
/**
 * Calculates the number of bits used between a given range for a specified logical type.
 * 
 * This function calculates the number of bits required for a given logical type and a specified range
 * of start and end values. The result is an optional integer representing the calculated number of bits.
 * 
 * If we input a column that does not support bit compress, we will return an empty optional.
 */
std::optional<int> get_used_bits(LogicalType ltype, const VectorizedLiteral& begin, const VectorizedLiteral& end,
                                 std::any& base);

/**
 * serialize column data into a bit-compressed format.
 */
void bitcompress_serialize(const Columns& columns, const std::vector<std::any>& bases, const std::vector<int>& offsets,
                           size_t num_rows, size_t fixed_key_size, void* buffer);

/**
 * deserialize column data from a bit-compressed format.
 * 
 */
void bitcompress_deserialize(MutableColumns& columns, const std::vector<std::any>& bases, const std::vector<int>& offsets,
                             const std::vector<int>& used_bits, size_t num_rows, size_t fixed_key_size, void* buffer);

} // namespace starrocks
