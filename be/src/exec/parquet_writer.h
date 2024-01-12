
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

struct TableInfo {
    TCompressionType::type compress_type = TCompressionType::SNAPPY;
    bool enable_dictionary = true;
    std::string partition_location = "";
    std::shared_ptr<::parquet::schema::GroupNode> schema;
    int64_t max_file_size = 1024 * 1024 * 1024; // 1GB
    TCloudConfiguration cloud_conf;
};

class RollingAsyncParquetWriter {
public:
    RollingAsyncParquetWriter(TableInfo tableInfo, const std::vector<ExprContext*>& output_expr_ctxs,
                              RuntimeProfile* parent_profile,
                              std::function<void(starrocks::parquet::AsyncFileWriter*, RuntimeState*)> _commit_func,
                              RuntimeState* state, int32_t driver_id);

    ~RollingAsyncParquetWriter() = default;

    Status append_chunk(Chunk* chunk, RuntimeState* state);
    Status init();
    Status close(RuntimeState* state);
    Status rollback(RuntimeState* state);
    bool writable() const { return _writer == nullptr || _writer->writable(); }
    bool closed();

    void set_io_status(const Status& status) {
        if (_io_status.ok()) {
            _io_status = status;
        }
    }

    Status get_io_status() const { return _io_status; }

private:
    std::string _new_file_location();

    Status _new_file_writer(RuntimeState* state);
    Status close_current_writer(RuntimeState* state);

private:
    std::unique_ptr<FileSystem> _fs;
    std::shared_ptr<starrocks::parquet::AsyncFileWriter> _writer;
    std::shared_ptr<::parquet::WriterProperties> _properties;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;
    std::string _partition_location;
    TableInfo _table_info;
    int32_t _file_cnt = 0;
    std::string _outfile_location;
    Status _io_status;
    std::vector<std::shared_ptr<starrocks::parquet::AsyncFileWriter>> _pending_commits;
    int64_t _max_file_size;
    std::vector<ExprContext*> _output_expr_ctxs;
    RuntimeProfile* _parent_profile;
    std::function<void(starrocks::parquet::AsyncFileWriter*, RuntimeState*)> _commit_func;
    RuntimeState* _state;
    int32_t _driver_id;
};

} // namespace starrocks
