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

#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "connector/lance/lance_rs_ffi.h"

namespace arrow {
class RecordBatch;
}
namespace starrocks {
class RuntimeState;
class MemTracker;
class SlotDescriptor;
class TupleDescriptor;
class Expr;
struct TCloudConfiguration;
struct ConvertFuncTree;

// Lance-owned adapter: neither Hive nor other connectors depend on the native reader.
class LanceNativeReader {
public:
    LanceNativeReader();
    virtual ~LanceNativeReader();
    virtual Status open(RuntimeState* state, const TupleDescriptor* tuple, const std::string& uri,
                        const TCloudConfiguration& cloud);
    virtual Status get_next(RuntimeState* state, ChunkPtr* chunk);
    virtual void close();
    int64_t cpu_time_spent() const { return _convert_time_ns; }
    int64_t io_time_spent() const { return _io_time_ns; }

    // Used by the scan adapter and Arrow conversion tests; no load-style row rejection.
    static Status convert_batch(RuntimeState* state, const TupleDescriptor* tuple,
                                const std::shared_ptr<arrow::RecordBatch>& batch, ChunkPtr* chunk);

private:
    Status _open_reader(const std::string& dataset_uri, const TCloudConfiguration& cloud);
    Status _next_batch();
    Status _append_batch_to_read_chunk(ChunkPtr* chunk);
    void _init_read_fields();
    bool _batch_is_exhausted() const;
    void _release_batch();

    RuntimeState* _state = nullptr;
    int _max_chunk_size = 0;
    std::vector<std::string> _field_names;
    std::shared_ptr<MemTracker> _mem_tracker;
    int64_t _batch_bytes = 0;
    SrLanceReader* _reader = nullptr;
    const TupleDescriptor* _tuple = nullptr;
    std::shared_ptr<arrow::RecordBatch> _arrow_batch;
    int64_t _batch_start_idx = 0;
    int64_t _convert_time_ns = 0;
    int64_t _io_time_ns = 0;
};
} // namespace starrocks
