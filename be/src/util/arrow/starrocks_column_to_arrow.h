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

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/visitor.h>
#include <arrow/visitor_inline.h>

#include <memory>

#include "column/chunk.h"
#include "common/global_types.h"
#include "runtime/descriptors.h"

namespace starrocks {

Status convert_chunk_to_arrow_batch(Chunk* chunk, std::vector<ExprContext*>& _output_expr_ctxs,
                                    const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool,
                                    std::shared_ptr<arrow::RecordBatch>* result);

// only used for UT test
Status convert_chunk_to_arrow_batch(Chunk* chunk, const std::vector<const TypeDescriptor*>& _slot_types,
                                    const std::vector<SlotId>& _slot_ids, const std::shared_ptr<arrow::Schema>& schema,
                                    arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result);
} // namespace starrocks
