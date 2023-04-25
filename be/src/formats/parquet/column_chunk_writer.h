
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

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <gen_cpp/DataSinks_types.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <utility>

#include "column/chunk.h"
#include "column/nullable_column.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::parquet {

class LevelBuilderResult;

// ColumnChunkWriter wraps parquet::ColumnWriter.
// The wrapper class provide runtime-polymorphism through type switch.
class ColumnChunkWriter {
public:
    ColumnChunkWriter(::parquet::ColumnWriter* column_writer);

    void write(const LevelBuilderResult& result);

    int64_t estimated_buffered_value_bytes();

private:
    ::parquet::ColumnWriter* _column_writer;
};

} // namespace starrocks::parquet
