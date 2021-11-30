// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/parquet_writer.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "parquet_builder.h"

#include <arrow/array.h>
#include <arrow/status.h>

#include <ctime>

#include "common/logging.h"
#include "exec/file_writer.h"
#include "gen_cpp/FileBrokerService_types.h"
#include "gen_cpp/TFileBrokerService.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/tuple.h"
#include "util/thrift_util.h"

namespace starrocks {

/// ParquetOutputStream

ParquetOutputStream::ParquetOutputStream(WritableFile* writable_file) : _writable_file(writable_file) {
    set_mode(arrow::io::FileMode::WRITE);
}

ParquetOutputStream::~ParquetOutputStream() {
    Close();
}

arrow::Status ParquetOutputStream::Write(const void* data, int64_t nbytes) {
    Status st = _writable_file->append({reinterpret_cast<const uint8_t*>(data), static_cast<std::size_t>(nbytes)});
    if (!st.ok()) {
        return arrow::Status::IOError(st.get_error_msg());
    }
    _cur_pos += nbytes;
    return arrow::Status::OK();
}

arrow::Result<int64_t> ParquetOutputStream::Tell() const {
    return _cur_pos;
}

arrow::Status ParquetOutputStream::Close() {
    Status st = _writable_file->close();
    if (!st.ok()) {
        return arrow::Status::IOError(st.get_error_msg());
    }
    _is_closed = true;
    return arrow::Status::OK();
}

/// ParquetBuilder
ParquetBuilder::ParquetBuilder(std::unique_ptr<WritableFile> writable_file,
                               const std::vector<ExprContext*>& output_expr_ctxs)
        : _outstream(new ParquetOutputStream(writable_file.get())),
          _writable_file(std::move(writable_file)),
          _output_expr_ctxs(output_expr_ctxs) {
    // TODO(cmy): implement
}

ParquetBuilder::~ParquetBuilder() = default;

Status ParquetBuilder::add_chunk(vectorized::Chunk* chunk) {
    // TODO(c1oudman): implement
    return Status::NotSupported("Parquest builder not supported yet");
}

Status ParquetBuilder::finish() {
    // TODO(cmy): implement
    return Status::NotSupported("Parquest builder not supported yet");
}

} // namespace starrocks
