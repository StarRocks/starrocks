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
#include "connector/hive/scanner/hdfs_scanner.h"

namespace starrocks {

class Expr;
class PaimonFileSystem;

class PaimonScanner final : public HdfsScanner {
public:
    PaimonScanner() = default;
    ~PaimonScanner() override;

    Status do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) override;
    Status do_open(RuntimeState* runtime_state) override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    void do_update_counter(HdfsScannerProfile* profile) override;

private:
    Status _next_batch();
    Status _append_batch_to_chunk();
    Status _fill_dst_chunk(ChunkPtr* chunk);
    bool _chunk_is_full() const;
    bool _batch_is_exhausted() const;

    int64_t _max_chunk_size = 4096;
    int64_t _batch_start_idx = 0;
    int64_t _chunk_start_idx = 0;
    bool _scanner_eof = false;

    std::shared_ptr<paimon::MemoryPool> _memory_pool;
    std::shared_ptr<PaimonFileSystem> _paimon_file_system;
    std::unique_ptr<paimon::BatchReader> _reader;
    std::shared_ptr<arrow::RecordBatch> _arrow_batch;

    ObjectPool _pool;
    std::vector<std::unique_ptr<ConvertFuncTree>> _convert_functions;
    std::vector<Expr*> _cast_exprs;
    ChunkPtr _read_chunk;
    Filter _chunk_filter;
    Filter _conjunct_filter;
    ArrowConvertContext _convert_context;
};

} // namespace starrocks
