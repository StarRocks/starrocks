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

// arrow dependencies
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

// project dependencies
#include "column_converter.h"

// starrocks dependencies
#include "column/chunk.h"
#include "column/schema.h"

namespace starrocks::lake::format {

class BaseArrowConverter {
public:
    BaseArrowConverter(const std::shared_ptr<Schema>& sr_schema, const std::shared_ptr<arrow::Schema>& arrow_schema,
                       arrow::MemoryPool* pool)
            : _sr_schema(std::move(sr_schema)), _arrow_schema(std::move(arrow_schema)), _pool(pool) {}

    arrow::Status init();

protected:
    arrow::Status checkSchema();

protected:
    const std::shared_ptr<Schema> _sr_schema;
    const std::shared_ptr<arrow::Schema> _arrow_schema;
    arrow::MemoryPool* _pool;
    std::vector<std::shared_ptr<ColumnConverter>> _converters;
};

/**
 * Arrow RecordBatch to StarRocks Chunk converter.
 */
class RecordBatchToChunkConverter : public BaseArrowConverter {
public:
    static arrow::Result<std::shared_ptr<RecordBatchToChunkConverter>> create(
            const std::shared_ptr<Schema>& sr_schema, const std::shared_ptr<arrow::Schema>& arrow_schema,
            arrow::MemoryPool* pool);

    virtual ~RecordBatchToChunkConverter() = default;

    virtual arrow::Result<std::unique_ptr<Chunk>> convert(std::shared_ptr<arrow::RecordBatch> recordBatch) = 0;

public:
    RecordBatchToChunkConverter(RecordBatchToChunkConverter&&) = delete;

    RecordBatchToChunkConverter& operator=(RecordBatchToChunkConverter&&) = delete;

protected:
    RecordBatchToChunkConverter(const std::shared_ptr<Schema>& sr_schema,
                                const std::shared_ptr<arrow::Schema>& arrow_schema, arrow::MemoryPool* pool)
            : BaseArrowConverter(sr_schema, arrow_schema, pool) {};
};

/**
 * StarRocks Chunk to Arrow RecordBatch converter.
 */
class ChunkToRecordBatchConverter : public BaseArrowConverter {
public:
    static arrow::Result<std::shared_ptr<ChunkToRecordBatchConverter>> create(
            const std::shared_ptr<Schema>& sr_schema, const std::shared_ptr<arrow::Schema>& arrow_schema,
            arrow::MemoryPool* pool);

    virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> convert(const Chunk* chunk) = 0;

    virtual ~ChunkToRecordBatchConverter() = default;

public:
    ChunkToRecordBatchConverter(ChunkToRecordBatchConverter&&) = delete;

    ChunkToRecordBatchConverter& operator=(ChunkToRecordBatchConverter&&) = delete;

protected:
    ChunkToRecordBatchConverter(const std::shared_ptr<Schema>& sr_schema,
                                const std::shared_ptr<arrow::Schema>& arrow_schema, arrow::MemoryPool* pool)
            : BaseArrowConverter(sr_schema, arrow_schema, pool) {};
};

} // namespace starrocks::lake::format