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

#include "storage/chunk_variant_helper.h"

#include "column/chunk.h"
#include "column/schema.h"

namespace starrocks {

VariantTuple build_variant_tuple_from_chunk_row(const Chunk& chunk, size_t row_idx,
                                                const std::vector<uint32_t>& column_indexes) {
    const auto& schema = chunk.schema();
    DCHECK(schema != nullptr);

    VariantTuple tuple;
    tuple.reserve(column_indexes.size());
    for (uint32_t column_index : column_indexes) {
        DCHECK_LT(column_index, chunk.num_columns());
        tuple.emplace(schema->field(column_index)->type(), chunk.get_column_by_index(column_index)->get(row_idx));
    }
    return tuple;
}

} // namespace starrocks
