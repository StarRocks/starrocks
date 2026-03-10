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

#include <arrow-adbc/adbc.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <memory>
#include <string>

#include "column/chunk.h"
#include "common/status.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class ADBCParallelReader;
class TupleDescriptor;

class ADBCScanner {
public:
    ADBCScanner(std::string driver, std::string uri, std::string username, std::string password, std::string token,
                std::string sql, const TupleDescriptor* tuple_desc, std::string ca_cert_file,
                std::string client_cert_file, std::string client_key_file, bool tls_verify);
    ~ADBCScanner();

    Status open(RuntimeState* state);
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos);
    void close(RuntimeState* state);

    int64_t rows_read() const { return _rows_read; }
    int64_t bytes_read() const { return _bytes_read; }
    int64_t connect_time_ms() const { return _connect_time_ms; }

private:
    Status _init_adbc();
    Status _try_parallel_read();
    Status _fallback_single_stream_read();
    Status _convert_batch_to_chunk(const std::shared_ptr<arrow::RecordBatch>& batch, ChunkPtr* chunk);
    static Status _read_file_to_string(const std::string& path, std::string* content);

    // Connection parameters
    std::string _driver;
    std::string _uri;
    std::string _username;
    std::string _password;
    std::string _token;
    std::string _sql;
    const TupleDescriptor* _tuple_desc;

    // TLS parameters
    std::string _ca_cert_file;
    std::string _client_cert_file;
    std::string _client_key_file;
    bool _tls_verify = true;

    // ADBC C API handles (must be released in reverse order)
    AdbcDatabase _database{};
    AdbcConnection _connection{};
    AdbcStatement _statement{};
    bool _database_initialized = false;
    bool _connection_initialized = false;
    bool _statement_initialized = false;

    // Arrow C stream (single-stream fallback) — managed directly instead of via
    // ImportRecordBatchReader to work around Go ADBC driver not nulling release callback.
    ArrowArrayStream _c_stream{};
    std::shared_ptr<arrow::Schema> _arrow_schema;
    bool _stream_initialized = false;

    // Parallel reader (multi-endpoint)
    std::unique_ptr<ADBCParallelReader> _parallel_reader;
    bool _use_parallel = false;

    // Re-chunking state
    std::shared_ptr<arrow::RecordBatch> _pending_batch;
    int64_t _pending_offset = 0;
    int64_t _max_chunk_size = 4096;

    // Stats
    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
    int64_t _connect_time_ms = 0;
    bool _opened = false;
    bool _closed = false;
};

} // namespace starrocks
