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

#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <cstdint>
#include <map>
#include <string>

#include "common/status.h"
#include "exec/file_builder.h"
#include "formats/parquet/parquet_file_writer.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/parquet_types.h"

namespace starrocks {

class ExprContext;
class FileWriter;

class ParquetBuilder : public FileBuilder {
public:
    ParquetBuilder(std::unique_ptr<WritableFile> writable_file, std::shared_ptr<::parquet::WriterProperties> properties,
                   std::shared_ptr<::parquet::schema::GroupNode> schema,
                   const std::vector<ExprContext*>& output_expr_ctxs, int64_t row_group_max_size,
                   int64_t max_file_size);

    ~ParquetBuilder() override = default;

    Status init();

    Status add_chunk(Chunk* chunk) override;

    std::size_t file_size() override;

    Status finish() override;

private:
    std::unique_ptr<starrocks::parquet::SyncFileWriter> _writer;
};

} // namespace starrocks
