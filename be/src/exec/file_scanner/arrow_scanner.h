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

#include <fs/fs.h>

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "column/arrow/arrow_to_starrocks_converter.h"
#include "column/vectorized_fwd.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "exec/file_scanner/file_scanner.h"
#include "runtime/mem_pool.h"

#include <arrow/api.h>
#include <arrow/ipc/api.h>

namespace starrocks {

class ArrowScanner : public FileScanner {
public:
    ArrowScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                 ScannerCounter* counter, bool schema_only = false);

    ~ArrowScanner() override;

    Status open() override;

    StatusOr<ChunkPtr> get_next() override;

    Status get_schema(std::vector<SlotDescriptor>* schema) override;

    void close() override;

    static Status new_column(const arrow::DataType* arrow_type, const SlotDescriptor* slot_desc,
                             MutableColumnPtr* column, ConvertFuncTree* conv_func, Expr** expr, ObjectPool& pool,
                             bool strict_mode);

private:
    Status open_next_reader();
    Status next_batch();
    Status initialize_src_chunk(ChunkPtr* chunk);
    Status append_batch_to_src_chunk(ChunkPtr* chunk);
    bool chunk_is_full();
    bool batch_is_exhausted();
    Status finalize_src_chunk(ChunkPtr* chunk);

    const TBrokerScanRange& _scan_range;
    int _next_file{0};
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> _curr_file_reader;
    bool _scanner_eof{false};
    std::shared_ptr<arrow::RecordBatch> _batch;
    const size_t _max_chunk_size;
    size_t _batch_start_idx{0};
    size_t _chunk_start_idx{0};
    int _num_of_columns_from_file = 0;
    std::vector<std::unique_ptr<ConvertFuncTree>> _conv_funcs;
    std::vector<Expr*> _cast_exprs;
    ObjectPool _pool;
    Filter _chunk_filter;
    ArrowConvertContext _conv_ctx;
};

} // namespace starrocks
