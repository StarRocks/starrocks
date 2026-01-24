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
#include <string>
#include <vector>

#include "benchgen/generator_options.h"
#include "benchgen/record_batch_iterator.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "runtime/descriptors.h"

namespace arrow {
class RecordBatch;
class Schema;
} // namespace arrow

namespace starrocks {

class Expr;
class RuntimeState;

struct BenchmarkScannerParam {
    std::string db_name;
    std::string table_name;
    benchgen::GeneratorOptions options;
};

class BenchmarkScanner {
public:
    BenchmarkScanner(BenchmarkScannerParam param, const TupleDescriptor* tuple_desc);

    Status open(RuntimeState* state);
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos);
    void close(RuntimeState* state);

private:
    Status _init_converters(const std::shared_ptr<arrow::Schema>& schema);
    Status _next_batch(bool* eos);

    BenchmarkScannerParam _param;
    const TupleDescriptor* _tuple_desc;
    std::vector<SlotDescriptor*> _slot_descs;

    std::unique_ptr<benchgen::RecordBatchIterator> _iter;
    std::shared_ptr<arrow::Schema> _schema;
    std::shared_ptr<arrow::RecordBatch> _batch;
    size_t _batch_start_idx = 0;
    size_t _max_chunk_size = 0;

    std::vector<std::unique_ptr<TypeDescriptor>> _raw_type_descs;
    std::vector<std::unique_ptr<ConvertFuncTree>> _conv_funcs;
    std::vector<Expr*> _cast_exprs;

    ObjectPool _pool;
    ArrowConvertContext _conv_ctx;
    Filter _chunk_filter;
    bool _scanner_eof = false;
};

} // namespace starrocks
