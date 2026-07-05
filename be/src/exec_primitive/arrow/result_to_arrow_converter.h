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

// This file contains code originally based on Apache Doris'
// be/src/util/arrow/row_batch.h and starrocks_column_to_arrow.h under the Apache License 2.0.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"

namespace arrow {
class MemoryPool;
class RecordBatch;
class Schema;
} // namespace arrow

namespace starrocks {

class Chunk;
class ExprContext;
class RowDescriptor;
struct TypeDescriptor;

Status convert_to_arrow_schema(const RowDescriptor& row_desc,
                               const std::unordered_map<int64_t, std::string>& id_to_col_name,
                               std::shared_ptr<arrow::Schema>* result,
                               const std::vector<ExprContext*>& output_expr_ctxs,
                               const std::vector<std::string>* output_column_names = nullptr,
                               int32_t flight_sql_version = 0);

Status convert_chunk_to_arrow_batch(Chunk* chunk, std::vector<ExprContext*>& output_expr_ctxs,
                                    const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool,
                                    std::shared_ptr<arrow::RecordBatch>* result);

// only used for UT test
Status convert_chunk_to_arrow_batch(Chunk* chunk, const std::vector<const TypeDescriptor*>& slot_types,
                                    const std::vector<SlotId>& slot_ids, const std::shared_ptr<arrow::Schema>& schema,
                                    arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result);

} // namespace starrocks
