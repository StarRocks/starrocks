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

#include <memory>
#include <string>

#include "column/column.h"
#include "common/status.h"

namespace arrow {
class MemoryPool;
class RecordBatch;
class Schema;
} // namespace arrow

namespace starrocks {

struct TypeDescriptor;

Status convert_columns_to_arrow_batch(size_t num_rows, const Columns& columns, arrow::MemoryPool* pool,
                                      const TypeDescriptor* type_descs, const std::shared_ptr<arrow::Schema>& schema,
                                      std::shared_ptr<arrow::RecordBatch>* result);

Status serialize_record_batch(const arrow::RecordBatch& record_batch, std::string* result);
Status serialize_arrow_schema(std::shared_ptr<arrow::Schema>* schema, std::string* result);

} // namespace starrocks
