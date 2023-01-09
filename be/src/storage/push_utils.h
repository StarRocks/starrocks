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

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/statusor.h"
#include "exec/file_scanner.h"
#include "exec/parquet_scanner.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks {

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
} // namespace starrocks
