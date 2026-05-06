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

// connector/adbc_connector.h MUST come before arrow-adbc/adbc.h because
// adbc.h defines `#define ADBC` (empty macro) which clashes with
// ConnectorType::ADBC_CONN's predecessor name in connector.h.
#include <arrow-adbc/adbc.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <memory>
#include <string>

#include "column/chunk.h"
#include "common/status.h"
#include "connector/adbc_connector.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

class ADBCParallelReader;
class TupleDescriptor;

struct ADBCScannerProfile {
    RuntimeProfile::Counter* rows_read_counter = nullptr;
    RuntimeProfile::Counter* io_timer = nullptr;
    RuntimeProfile::Counter* io_counter = nullptr;
    RuntimeProfile::Counter* fill_chunk_timer = nullptr;
    RuntimeProfile::Counter* connect_timer = nullptr;
};

class ADBCScanner {
public:
    // Takes context by const ref. Scanner copies the context.
    ADBCScanner(const ADBCScanContext& ctx, const TupleDescriptor* tuple_desc, RuntimeProfile* runtime_profile);
    ~ADBCScanner();

    Status open(RuntimeState* state);
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos);
    void close(RuntimeState* state);

    int64_t rows_read() const { return _rows_read; }
    int64_t bytes_read() const { return _bytes_read; }
    int64_t connect_time_ms() const { return _connect_time_ms; }

private:
    void _init_profile();
    Status _init_adbc();
    Status _get_next_impl(RuntimeState* state, ChunkPtr* chunk, bool* eos);
    Status _try_parallel_read();
    Status _fallback_single_stream_read();
    Status _convert_batch_to_chunk(const std::shared_ptr<arrow::RecordBatch>& batch, ChunkPtr* chunk);

    // Context (copied, not borrowed, since caller may not outlive scanner)
    const ADBCScanContext _ctx;
    const TupleDescriptor* _tuple_desc;

    RuntimeProfile* _runtime_profile = nullptr;
    ADBCScannerProfile _profile;

    // ADBC C API handles (released in reverse order in close())
    AdbcDatabase _database{};
    AdbcConnection _connection{};
    AdbcStatement _statement{};
    bool _database_initialized = false;
    bool _connection_initialized = false;
    bool _statement_initialized = false;

    // Arrow C stream (released explicitly in close())
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
