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

#include <memory>
#include <vector>

#include "column/arrow/arrow_to_starrocks_converter.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"

namespace arrow {
class RecordBatch;
}

struct SrLanceReader;

namespace starrocks {

class RuntimeState;

class LanceNativeReader {
public:
    LanceNativeReader(const HdfsScannerParams& scanner_params, const HdfsScannerContext& scanner_ctx,
                      RuntimeState* state, HdfsScanStats* app_stats);
    ~LanceNativeReader();

    Status open();
    StatusOr<size_t> get_next(ChunkPtr* chunk);
    void close();

private:
    Status _open_reader();
    Status _next_batch();
    Status _ensure_read_chunk();
    Status _append_batch_to_read_chunk();
    StatusOr<size_t> _fill_dst_chunk(ChunkPtr* dst);
    void _init_read_fields();

    bool _chunk_is_full() const;
    bool _batch_is_exhausted() const;

    const HdfsScannerParams& _scanner_params;
    const HdfsScannerContext& _scanner_ctx;
    RuntimeState* _state;
    HdfsScanStats* _app_stats;
    int _max_chunk_size;

    int64_t _batch_start_idx = 0;
    int64_t _chunk_start_idx = 0;
    bool _scanner_eof = false;
    bool _read_chunk_initialized = false;

    ::SrLanceReader* _reader = nullptr;
    std::vector<std::string> _field_names;
    std::vector<std::unique_ptr<ConvertFuncTree>> _conv_funcs;
    std::shared_ptr<arrow::RecordBatch> _arrow_batch;
    std::vector<Expr*> _cast_exprs;
    Filter _chunk_filter;
    ArrowConvertContext _conv_ctx;
    ObjectPool _pool;
    ChunkPtr _read_chunk;
};

} // namespace starrocks
