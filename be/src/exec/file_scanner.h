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

#include "common/statusor.h"
#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "util/runtime_profile.h"

namespace starrocks {
class SequentialFile;
class RandomAccessFile;
} // namespace starrocks

namespace starrocks {

const int64_t MAX_ERROR_LINES_IN_FILE = 50;

struct ScannerCounter {
    int64_t num_rows_filtered = 0;
    int64_t num_rows_unselected = 0;
    int64_t filtered_rows_read = 0;
    int64_t num_rows_read = 0;
    int64_t num_bytes_read = 0;

    int64_t total_ns = 0;
    int64_t fill_ns = 0;
    int64_t read_batch_ns = 0;
    int64_t cast_chunk_ns = 0;
    int64_t materialize_ns = 0;

    int64_t init_chunk_ns = 0;

    int64_t file_read_ns = 0;
};

class FileScanner {
public:
    FileScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                ScannerCounter* counter, bool schema_only = false);
    virtual ~FileScanner();

    virtual Status init_expr_ctx();

    virtual Status open();

    virtual StatusOr<ChunkPtr> get_next() = 0;

    virtual void close();

    virtual Status get_schema(std::vector<SlotDescriptor>* schema) { return Status::NotSupported("not implemented"); }

    static Status sample_schema(RuntimeState* state, const TBrokerScanRange& scan_range,
                                std::vector<SlotDescriptor>* schema);

    Status create_random_access_file(const TBrokerRangeDesc& range_desc, const TNetworkAddress& address,
                                     const TBrokerScanRangeParams& params, CompressionTypePB compression,
                                     std::shared_ptr<RandomAccessFile>* file);

    Status create_sequential_file(const TBrokerRangeDesc& range_desc, const TNetworkAddress& address,
                                  const TBrokerScanRangeParams& params, std::shared_ptr<SequentialFile>* file);

    // only for test
    RuntimeState* TEST_runtime_state() { return _state; }

    static void merge_schema(const std::vector<std::vector<SlotDescriptor>>& input,
                             std::vector<SlotDescriptor>* output);

protected:
    void fill_columns_from_path(ChunkPtr& chunk, int slot_start, const std::vector<std::string>& columns_from_path,
                                int size);
    // materialize is used to transform source chunk depicted by src_slot_descriptors into destination
    // chunk depicted by dest_slot_descriptors
    StatusOr<ChunkPtr> materialize(const starrocks::ChunkPtr& src, starrocks::ChunkPtr& cast);

protected:
    RuntimeState* _state;
    RuntimeProfile* _profile;
    const TBrokerScanRangeParams& _params;
    ScannerCounter* _counter;

    std::unique_ptr<RowDescriptor> _row_desc;

    bool _strict_mode;
    int64_t _error_counter;

    // sources
    std::vector<SlotDescriptor*> _src_slot_descriptors;

    // destination
    const TupleDescriptor* _dest_tuple_desc;
    std::vector<ExprContext*> _dest_expr_ctx;

    // the map values of destination slot id to src slot desc
    // index: destination slot id
    // value: source slot desc
    std::vector<SlotDescriptor*> _dest_slot_desc_mappings;

    bool _case_sensitive = true;

    bool _schema_only;
};
} // namespace starrocks
