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

#include "data_sink/file/parquet_builder.h"

#include "formats/parquet/file_writer.h"

namespace starrocks {

ParquetBuilder::ParquetBuilder(std::unique_ptr<WritableFile> writable_file,
                               std::shared_ptr<::parquet::WriterProperties> properties,
                               std::shared_ptr<::parquet::schema::GroupNode> schema,
                               const std::vector<ExprContext*>& output_expr_ctxs, int64_t row_group_max_size,
                               int64_t max_file_size, RuntimeState* state) {
    _writer = std::make_unique<starrocks::parquet::SyncFileWriter>(
            std::move(writable_file), std::move(properties), std::move(schema), output_expr_ctxs, max_file_size, state);
    _writer->set_max_row_group_size(row_group_max_size);
}

ParquetBuilder::~ParquetBuilder() = default;

Status ParquetBuilder::init() {
    return _writer->init();
}

Status ParquetBuilder::add_chunk(Chunk* chunk) {
    return _writer->write(chunk);
}

Status ParquetBuilder::finish() {
    return _writer->close();
}

std::size_t ParquetBuilder::file_size() {
    return _writer->file_size();
}

} // namespace starrocks
