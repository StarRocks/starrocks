// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <env/env.h>

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/vectorized/arrow_to_starrocks_converter.h"
#include "exec/vectorized/file_scanner.h"
#include "parquet_reader.h"
#include "parquet_scanner.h"
#include "runtime/mem_pool.h"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace starrocks::vectorized {

// Broker scanner convert the data read from broker to starrocks's tuple.
class ParquetScanner : public FileScanner {
public:
    ParquetScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                   ScannerCounter* counter, const std::vector<TBrokerRangeDesc>& ranges,
                   std::vector<std::shared_ptr<RandomAccessFile>>&& files);

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
                                   const TypeDescriptor* type_desc, ColumnPtr column);

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
    Column::Filter _chunk_filter;
    ArrowConvertContext _conv_ctx;
};

} // namespace starrocks::vectorized
