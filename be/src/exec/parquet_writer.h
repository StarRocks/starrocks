
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
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include "common/logging.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/parquet/file_writer.h"
#include "fs/fs.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

struct TableInfo;
struct PartitionInfo;

struct TableInfo {
    std::string _table_location;
    std::string _file_format;
    TCompressionType::type _compress_type = TCompressionType::SNAPPY;
    bool _enable_dictionary = true;

    std::shared_ptr<::parquet::schema::GroupNode> _schema;
    ;
};

struct PartitionInfo {
    std::vector<std::string> _column_names;
    std::vector<std::string> _column_values;

    std::string partition_dir() const {
        std::stringstream ss;
        for (size_t i = 0; i < _column_names.size(); i++) {
            ss << _column_names[i];
            ss << "=";
            ss << _column_values[i];
            ss << "/";
        }
        return ss.str();
    }
};

class RollingAsyncParquetWriter {
public:
    RollingAsyncParquetWriter(const TableInfo& tableInfo, const PartitionInfo& partitionInfo,
                              const std::vector<ExprContext*>& output_expr_ctxs, RuntimeProfile* parent_profile);
    ~RollingAsyncParquetWriter() = default;

    Status append_chunk(Chunk* chunk, RuntimeState* state); //check if we need a new file, file_writer->write
    // init filesystem, init writeproperties, schema
    Status init_rolling_writer(const TableInfo& tableInfo, const PartitionInfo& partitionInfo);
    Status close(RuntimeState* state);
    bool writable() const { return _writer == nullptr || _writer->writable(); }
    bool closed();

    static void add_iceberg_commit_info(starrocks::parquet::AsyncFileWriter* writer, RuntimeState* state);

private:
    std::string get_new_file_name();
    Status new_file_writer();
    Status close_current_writer(RuntimeState* state);

    std::shared_ptr<FileSystem> _fs;
    std::shared_ptr<starrocks::parquet::AsyncFileWriter> _writer;
    std::shared_ptr<::parquet::WriterProperties> _properties;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;
    std::string _partition_dir;
    int32_t _cnt = 0;
    std::string _location;
    std::vector<std::shared_ptr<starrocks::parquet::AsyncFileWriter>> _pending_commits;
    int64_t _max_file_size = 512 * 1024 * 1024;
    std::vector<ExprContext*> _output_expr_ctxs;
    RuntimeProfile* _parent_profile;
};

} // namespace starrocks
