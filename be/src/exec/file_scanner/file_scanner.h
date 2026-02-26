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

#include <string>

#include "common/runtime_profile.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {
class SequentialFile;
class RandomAccessFile;
} // namespace starrocks

namespace starrocks {

const int64_t MAX_ERROR_LINES_IN_FILE = 50;

// Diagram of row counters relationship:
//
// num_raw_rows_read (Total Rows from Storage)
// |
// +-- num_rows_filtered (Invalid Format)
// |
// +-- filtered_rows_read (Valid Format)
//     |
//     +-- num_rows_unselected (Filtered by Predicates)
//     |
//     +-- num_rows_read (Rows Returned)
//
// Equations:
// 1. filtered_rows_read = num_raw_rows_read - num_rows_filtered
// 2. num_rows_read = filtered_rows_read - num_rows_unselected
struct ScannerCounter {
    // num of rows filtered by invalid data format
    int64_t num_rows_filtered = 0;
    // num of rows filtered by predicates
    int64_t num_rows_unselected = 0;
    // num of rows with valid format (after format validation, before predicate filtering)
    // filtered_rows_read = num_rows_read + num_rows_unselected
    int64_t filtered_rows_read = 0;
    // num of rows returned (after predicate filtering)
    // num_rows_read = filtered_rows_read - num_rows_unselected
    int64_t num_rows_read = 0;
    int64_t num_bytes_read = 0;

    // total time cost in scanner
    int64_t total_ns = 0;
    // time cost in fill chunk
    int64_t fill_ns = 0;
    // time cost in read batch from file
    int64_t read_batch_ns = 0;
    // time cost in cast chunk
    int64_t cast_chunk_ns = 0;
    // time cost in materialize
    int64_t materialize_ns = 0;

    // time cost in init chunk
    int64_t init_chunk_ns = 0;

    // time cost in read file
    int64_t file_read_ns = 0;
    // count of file read io
    int64_t file_read_count = 0;
    // count of files opened for reading
    int64_t num_files_read = 0;
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

    std::string scan_type() const {
        switch (_file_scan_type) {
        case TFileScanType::FILES_INSERT:
            return "insert";
        case TFileScanType::LOAD:
            return "load";
        case TFileScanType::FILES_QUERY:
            return "query";
        default:
            // Fallback for any other or future scan types; keep behavior compatible with queries.
            return "query";
        }
    }

    const std::string& file_format() const { return _file_format_str; }

    static Status sample_schema(RuntimeState* state, const TBrokerScanRange& scan_range,
                                std::vector<SlotDescriptor>* schema);

    Status create_random_access_file(const TBrokerRangeDesc& range_desc, const TNetworkAddress& address,
                                     const TBrokerScanRangeParams& params, CompressionTypePB compression,
                                     std::shared_ptr<RandomAccessFile>* file);

    Status create_sequential_file(const TBrokerRangeDesc& range_desc, const TNetworkAddress& address,
                                  const TBrokerScanRangeParams& params, std::shared_ptr<SequentialFile>* file);

    // only for test
    RuntimeState* TEST_runtime_state() { return _state; }
    ScannerCounter* TEST_scanner_counter() { return _counter; }

    static void merge_schema(const std::vector<std::vector<SlotDescriptor>>& input,
                             std::vector<SlotDescriptor>* output);

protected:
    void fill_columns_from_path(ChunkPtr& chunk, int slot_start, const std::vector<std::string>& columns_from_path,
                                int size);
    // materialize is used to transform source chunk depicted by src_slot_descriptors into destination
    // chunk depicted by dest_slot_descriptors
    StatusOr<ChunkPtr> materialize(const starrocks::ChunkPtr& src, starrocks::ChunkPtr& cast);

    static void sample_files(size_t total_file_count, int64_t sample_file_count,
                             std::vector<size_t>* sample_file_indexes);

protected:
    RuntimeState* _state;
    RuntimeProfile* _profile;
    const TBrokerScanRangeParams& _params;
    ScannerCounter* _counter;

    std::unique_ptr<RowDescriptor> _row_desc;

    bool _strict_mode;
    int64_t _error_counter;
    // When column mismatch, files query/load and other type load have different behaviors.
    // Query returns error, while load counts the filtered rows, and return error or not is based on max filter ratio,
    // files load will not filter rows if file column count is larger that the schema,
    // so need to check files query/load or other type load in scanner.
    // Currently only used in csv scanner.
    TFileScanType::type _file_scan_type;

    // string of _params.format_type: "avro", "csv", "json", "parquet", "orc", etc.
    // NOTE: remember to check the labels defined in be/src/util/metrics/file_scan_metrics.cpp, update the labels when necessary.
    std::string _file_format_str;

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
