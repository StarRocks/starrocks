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

#include "formats/parquet/chunk_writer.h"

#include <parquet/arrow/writer.h>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr.h"
#include "formats/parquet/column_chunk_writer.h"
#include "formats/parquet/level_builder.h"
#include "util/defer_op.h"

namespace starrocks::parquet {

ChunkWriter::ChunkWriter(::parquet::RowGroupWriter* rg_writer, const std::vector<TypeDescriptor>& type_descs,
                         const std::shared_ptr<::parquet::schema::GroupNode>& schema,
                         const std::function<StatusOr<ColumnPtr>(Chunk*, size_t)>& eval_func)
        : _rg_writer(rg_writer), _type_descs(type_descs), _schema(schema), _eval_func(eval_func) {
    int num_columns = rg_writer->num_columns();
    _estimated_buffered_bytes.resize(num_columns);
    std::fill(_estimated_buffered_bytes.begin(), _estimated_buffered_bytes.end(), 0);
}

Status ChunkWriter::write(Chunk* chunk) {
    LevelBuilderContext ctx(chunk->num_rows());

    // Writes out all leaf parquet columns to the RowGroupWriter. Each leaf column is written fully before
    // the next column is written. Columns are written in DFS order.
    int leaf_column_idx = 0;

    auto write_leaf_column = [&](const LevelBuilderResult& result) {
        auto leaf_column_writer = ColumnChunkWriter(_rg_writer->column(leaf_column_idx));
        leaf_column_writer.write(result);
        _estimated_buffered_bytes[leaf_column_idx] = leaf_column_writer.estimated_buffered_value_bytes();
        ++leaf_column_idx;
    };

    for (size_t i = 0; i < _type_descs.size(); i++) {
        ASSIGN_OR_RETURN(auto col, _eval_func(chunk, i));
        auto level_builder = LevelBuilder(_type_descs[i], _schema->field(i));
        level_builder.write(ctx, col, write_leaf_column);
    }

    return Status::OK();
}

void ChunkWriter::close() {
    _rg_writer->Close();
}

// The current row group written bytes = total_bytes_written + total_compressed_bytes + estimated_bytes.
// total_bytes_written: total bytes written by the page writer
// total_compressed_types: total bytes still compressed but not written
// estimated_bytes: estimated size of all column chunk uncompressed values that are not written to a page yet. it
// mainly includes value buffer size and repetition buffer size and definition buffer value for each column.
int64_t ChunkWriter::estimated_buffered_bytes() const {
    if (_rg_writer == nullptr) {
        return 0;
    }

    auto buffered_bytes = std::accumulate(_estimated_buffered_bytes.begin(), _estimated_buffered_bytes.end(), 0);
    return _rg_writer->total_bytes_written() + _rg_writer->total_compressed_bytes() + buffered_bytes;
}

} // namespace starrocks::parquet
