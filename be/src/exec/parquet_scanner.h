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
                   ScannerCounter* counter);

    ~ParquetScanner() override;

    Status open() override;

    StatusOr<ChunkPtr> get_next() override;

    void close() override;

private:
    // Read next buffer from reader
    Status open_next_reader();
    Status next_batch();
    Status initialize_src_chunk(ChunkPtr* chunk);
    Status append_batch_to_src_chunk(ChunkPtr* chunk);
    bool chunk_is_full();
    bool batch_is_exhausted();
    Status finalize_src_chunk(ChunkPtr* chunk);
    Status convert_array_to_column(ConvertFunc func, size_t num_elements, const arrow::Array* array,
                                   const TypeDescriptor* type_desc, const ColumnPtr& column);

    Status new_column(const arrow::DataType* arrow_type, const SlotDescriptor* slot_desc, ColumnPtr* column,
                      ConvertFunc* conv_func, Expr** expr);

    const TBrokerScanRange& _scan_range;
    int _next_file;
    std::shared_ptr<ParquetChunkReader> _curr_file_reader;
    bool _scanner_eof;
    RecordBatchPtr _batch;
    const size_t _max_chunk_size;
    size_t _batch_start_idx;
    size_t _chunk_start_idx;
    int _num_of_columns_from_file = 0;
    std::vector<ConvertFunc> _conv_funcs;
    std::vector<Expr*> _cast_exprs;
    ObjectPool _pool;
    Filter _chunk_filter;
    ArrowConvertContext _conv_ctx;
};

} // namespace starrocks
