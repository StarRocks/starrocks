// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/statusor.h"
#include "exec/vectorized/file_scanner.h"
#include "exec/vectorized/parquet_scanner.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks::vectorized {

class PushBrokerReader {
public:
    PushBrokerReader() = default;
    ~PushBrokerReader();

    Status init(const TBrokerScanRange& t_scan_range, const TPushReq& request);
    Status next_chunk(ChunkPtr* chunk);

    void print_profile();

    Status close() {
        _scanner->close();
        _ready = false;
        return Status::OK();
    }
    bool eof() const { return _eof; }

private:
    // convert chunk that read from parquet scanner to chunk that write to rowset writer
    // 1. column is nullable or not should be determined by schema.
    // 2. deserialize bitmap and hll column from varchar that read from parquet file.
    // 3. padding char column.
    Status _convert_chunk(const ChunkPtr& from, ChunkPtr* to);
    ColumnPtr _build_object_column(const ColumnPtr& column);
    ColumnPtr _build_hll_column(const ColumnPtr& column);
    ColumnPtr _padding_char_column(const ColumnPtr& column, const SlotDescriptor* slot_desc, size_t num_rows);

    bool _ready = false;
    bool _eof = false;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile* _runtime_profile = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    std::unique_ptr<ScannerCounter> _counter;
    std::unique_ptr<FileScanner> _scanner;
};
} // namespace starrocks::vectorized
