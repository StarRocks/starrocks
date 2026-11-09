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

#include <memory>
#include <string>
#include <vector>

#include "base/statusor.h"
#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "types/type_descriptor.h"

namespace arrow {
class MemoryPool;
class RecordBatch;
class Schema;
} // namespace arrow

namespace starrocks {

class ExprContext;

// Compile this implementation only in the isolated Paimon SDK Arrow shim. The schema, including
// nested names/order, nullability, timestamp timezone and extension metadata, is authoritative.
// No Paimon C++ types cross this interface. Columns and expressions are never mutated.
StatusOr<std::shared_ptr<arrow::RecordBatch>> paimon_write_chunk_to_arrow(
        const ChunkPtr& chunk, const std::vector<ExprContext*>& output_exprs,
        const std::shared_ptr<arrow::Schema>& target_schema, const std::string& time_zone, arrow::MemoryPool* pool);

// Evaluated-column entrypoint, also useful for tests. All columns must have equal logical size.
// A target string/binary field may carry "starrocks.paimon.length" (a positive decimal UTF-8 byte
// bound); otherwise the source TypeDescriptor.len is used. This is a conservative byte bound,
// not a claim that StarRocks byte lengths and Java VARCHAR UTF-16 lengths are interchangeable.
StatusOr<std::shared_ptr<arrow::RecordBatch>> paimon_write_columns_to_arrow(
        const Columns& columns, const std::vector<TypeDescriptor>& types,
        const std::shared_ptr<arrow::Schema>& target_schema, const std::string& time_zone, arrow::MemoryPool* pool);

} // namespace starrocks
