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

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "exec/file_scanner.h"
#include "parquet_reader.h"
#include "parquet_scanner.h"
#include "runtime/mem_pool.h"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace starrocks {

// Broker scanner convert the data read from broker to starrocks's tuple.
class ParquetScanner : public FileScanner {
public:
    ParquetScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                   ScannerCounter* counter, bool schema_only = false);

    ~ParquetScanner() override;

    Status open() override;

    StatusOr<ChunkPtr> get_next() override;

    Status get_schema(std::vector<SlotDescriptor>* schema) override;

    void close() override;

    static Status convert_array_to_column(ConvertFuncTree* func, size_t num_elements, const arrow::Array* array,
                                          const ColumnPtr& column, size_t batch_start_idx, size_t column_start_idx,
                                          Filter* chunk_filter, ArrowConvertContext* conv_ctx);

    static Status new_column(const arrow::DataType* arrow_type, const SlotDescriptor* slot_desc, ColumnPtr* column,
                             ConvertFuncTree* conv_func, Expr** expr, ObjectPool& pool, bool strict_mode);

    static Status build_dest(const arrow::DataType* arrow_type, const TypeDescriptor* type_desc, bool is_nullable,
                             TypeDescriptor* raw_type_desc, ConvertFuncTree* conv_func, bool& need_cast,
                             bool strict_mode);

private:
    // Read next buffer from reader
    Status open_next_reader();
    Status next_batch();
    Status initialize_src_chunk(ChunkPtr* chunk);
    Status append_batch_to_src_chunk(ChunkPtr* chunk);
    bool chunk_is_full();
    bool batch_is_exhausted();
    Status finalize_src_chunk(ChunkPtr* chunk);

    const TBrokerScanRange& _scan_range;
    int _next_file;
    std::shared_ptr<ParquetChunkReader> _curr_file_reader;
    bool _scanner_eof;
    RecordBatchPtr _batch;
    const size_t _max_chunk_size;
    size_t _batch_start_idx;
    size_t _chunk_start_idx;
    int _num_of_columns_from_file = 0;
    std::vector<std::unique_ptr<ConvertFuncTree>> _conv_funcs;
    std::vector<Expr*> _cast_exprs;
    ObjectPool _pool;
    Filter _chunk_filter;
    ArrowConvertContext _conv_ctx;
    int64_t _last_file_size = 0;
    int64_t _last_range_size = 0;
    int64_t _last_file_scan_rows = 0;
    int64_t _last_file_scan_bytes = 0;
    int64_t _next_batch_counter = 0;
};

} // namespace starrocks
