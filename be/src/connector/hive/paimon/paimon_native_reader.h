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

#include <arrow/record_batch.h>
#include <paimon/memory/memory_pool.h>
#include <paimon/reader/batch_reader.h>

#include <memory>
#include <vector>

#include "column/arrow/arrow_to_starrocks_converter.h"
#include "common/object_pool.h"
#include "connector/hive/scanner/hdfs_scanner_context.h"

namespace starrocks {

class Expr;
class PaimonFileSystem;
class RuntimeState;

// Owns the paimon-cpp read context and BatchReader, and converts Paimon Arrow
// batches into materialized StarRocks columns. Hdfs scan side-column filling
// and residual predicate evaluation stay in PaimonScanner.
class PaimonNativeReader {
public:
    PaimonNativeReader(const HdfsScannerContext& scanner_ctx, RuntimeState* runtime_state,
                       FormatScannerStats* app_stats);
    ~PaimonNativeReader();

    Status open();
    Status get_next(ChunkPtr* chunk);
    void close() noexcept;

    std::shared_ptr<paimon::Metrics> get_reader_metrics() const {
        return _reader != nullptr ? _reader->GetReaderMetrics() : _reader_metrics;
    }

    std::shared_ptr<PaimonFileSystem> get_paimon_file_system() const { return _paimon_file_system; }

private:
    Status _next_batch();
    Status _initialize_converters();
    Status _append_batch_to_chunk();
    Status _fill_output_chunk(ChunkPtr* chunk);
    bool _batch_is_exhausted() const;

    const HdfsScannerContext& _scanner_ctx;
    RuntimeState* _runtime_state;
    FormatScannerStats* _app_stats;

    int64_t _max_chunk_size = 4096;
    int64_t _batch_start_idx = 0;
    int64_t _chunk_start_idx = 0;
    bool _scanner_eof = false;
    bool _converters_initialized = false;
    bool _closed = false;

    std::shared_ptr<paimon::MemoryPool> _memory_pool;
    std::shared_ptr<PaimonFileSystem> _paimon_file_system;
    std::unique_ptr<paimon::BatchReader> _reader;
    std::shared_ptr<paimon::Metrics> _reader_metrics;
    std::shared_ptr<arrow::RecordBatch> _arrow_batch;

    ObjectPool _pool;
    std::vector<std::unique_ptr<ConvertFuncTree>> _convert_functions;
    std::vector<Expr*> _cast_exprs;
    ChunkPtr _read_chunk;
    Filter _chunk_filter;
    ArrowConvertContext _convert_context;
};

} // namespace starrocks
