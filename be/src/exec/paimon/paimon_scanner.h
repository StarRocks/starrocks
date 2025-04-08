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

#include <exec/arrow_to_starrocks_converter.h>
#include <exec/hdfs_scanner.h>
#include <paimon/reader/batch_reader.h>

#include "common/statusor.h"
#include "exprs/expr.h"

using namespace paimon;

namespace starrocks {

struct PaimonScanStats {
    int64_t raw_rows_read = 0;
    int64_t rows_read = 0;
    int64_t late_materialize_skip_rows = 0;

    int64_t io_ns = 0;
    int64_t io_count = 0;
    int64_t bytes_read = 0;

    int64_t expr_filter_ns = 0;
    int64_t column_read_ns = 0;
    int64_t column_convert_ns = 0;
    int64_t reader_init_ns = 0;

    // late materialize round-by-round
    int64_t group_min_round_cost = 0;
};

struct PaimonScannerParams {
    // paimon table path
    std::string table_path;

    // paimon split info
    std::string paimon_split_info;

    // paimon schema id
    int64_t schema_id;

    // materialized columns.
    std::vector<SlotDescriptor*> materialize_slots;
    std::vector<int> materialize_index_in_chunk;
};

class PaimonScanner : public HdfsScanner {
public:
    PaimonScanner() = default;
    ~PaimonScanner() override = default;

    // std::shared_ptr<paimon::Predicate> convert_array_to_column(const std::unordered_map<int, std::vector<ExprContext*>>& pairs);
    Status do_open(RuntimeState* runtime_state) override;
    bool chunk_is_full() const;
    void do_close(RuntimeState* runtime_state) noexcept override;
    bool batch_is_exhausted() const;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    void do_update_counter(HdfsScanProfile* profile) override;

    int64_t num_bytes_read() const { return _app_stats.bytes_read; }
    int64_t raw_rows_read() const { return _app_stats.raw_rows_read; }
    int64_t num_rows_read() const { return _app_stats.rows_read; }
    int64_t cpu_time_spent() const { return _total_running_time - _app_stats.io_ns; }
    int64_t io_time_spent() const { return _app_stats.io_ns; }

private:
    RuntimeState* _runtime_state = nullptr;
    RuntimeProfile* _profile;
    int64_t _total_running_time = 0;

    std::unique_ptr<BatchReader> _reader = nullptr;

    ObjectPool _pool;
    size_t _max_chunk_size;
    size_t _batch_start_idx;
    size_t _chunk_start_idx;
    std::vector<std::unique_ptr<ConvertFuncTree>> _conv_funcs;
    Filter _chunk_filter;
    ArrowConvertContext _conv_ctx;
    Status next_batch();
    Status initialize_src_chunk(ChunkPtr* chunk);
    Status append_batch_to_src_chunk(ChunkPtr* chunk);
    Status finalize_src_chunk(ChunkPtr* chunk);
    StatusOr<ChunkPtr> materialize(const starrocks::ChunkPtr& src, starrocks::ChunkPtr& cast);
    size_t _max_batch_rows;
    std::shared_ptr<arrow::RecordBatch> _arrow_batch;
    std::vector<Expr*> _cast_exprs;
    bool _scanner_eof;
};
} // namespace starrocks
