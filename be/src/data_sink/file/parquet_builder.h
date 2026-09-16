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

#include <cstdint>
#include <memory>
#include <vector>

#include "data_sink/file/file_builder.h"
#include "fs/fs_fwd.h"
#include "runtime/runtime_fwd.h"

namespace parquet {
class WriterProperties;
namespace schema {
class GroupNode;
}
} // namespace parquet

namespace starrocks {

class ExprContext;

namespace parquet {
class SyncFileWriter;
}

class ParquetBuilder : public FileBuilder {
public:
    ParquetBuilder(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
                   std::shared_ptr<::parquet::schema::GroupNode> schema,
                   const std::vector<ExprContext*>& output_expr_ctxs, int64_t row_group_max_size, int64_t max_file_size,
                   RuntimeState* state);

    ~ParquetBuilder() override;

    Status init();

    Status add_chunk(Chunk* chunk) override;

    std::size_t file_size() override;

    Status finish() override;

private:
    std::unique_ptr<starrocks::parquet::SyncFileWriter> _writer;
};

} // namespace starrocks
