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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cache/scan/cache_input_stream.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "exec/hdfs_scanner/hdfs_scanner_context.h"
#include "fs/fs.h"
#include "runtime/runtime_state_fwd.h"

namespace starrocks {

struct OpenFileOptions {
    FileSystem* fs = nullptr;
    std::string file_path;
    int64_t file_size = -1;
    FormatScannerStats* fs_stats = nullptr;
    FormatScannerStats* app_stats = nullptr;

    // for datacache
    DataCacheOptions datacache_options;

    // for compressed text file
    CompressionTypePB compression_type = CompressionTypePB::NO_COMPRESSION;
};

class HdfsScanner {
public:
    HdfsScanner() = default;
    virtual ~HdfsScanner() = default;

    Status init(RuntimeState* runtime_state, HdfsScannerContext* scanner_ctx);
    Status open(RuntimeState* runtime_state);
    Status get_next(RuntimeState* runtime_state, ChunkPtr* chunk);
    void close() noexcept;

    int64_t num_bytes_read() const { return _app_stats.bytes_read; }
    int64_t raw_rows_read() const { return _app_stats.raw_rows_read; }
    int64_t num_rows_read() const { return _app_stats.rows_read; }
    int64_t cpu_time_spent() const { return _total_running_time - _app_stats.io_ns; }
    int64_t io_time_spent() const { return _app_stats.io_ns; }
    virtual int64_t estimated_mem_usage() const;
    void update_counter();

    RuntimeState* runtime_state() { return _runtime_state; }

    virtual Status do_open(RuntimeState* runtime_state) = 0;
    virtual void do_close(RuntimeState* runtime_state) noexcept = 0;
    virtual Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) = 0;
    virtual Status do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) = 0;
    virtual void do_update_counter(HdfsScannerProfile* profile);
    virtual Status reinterpret_status(const Status& st);

    // ORC and Parquet push conjunct_ctxs_by_slot into their own column-level
    // readers (lazy materialisation, dict filter, etc.) and do NOT want the base
    // class to apply them a second time.  All other formats return false so the
    // base class handles by-slot evaluation uniformly in get_next().
    virtual bool scanner_handles_predicate_by_slot_internally() const { return false; }

    // True if the scanner evaluates multi-slot predicates (conjuncts->scanner_ctxs)
    // internally inside do_get_next(), so the base class must not apply them again.
    // Scanners that return true MUST guarantee row-level correctness; the ORC
    // search-argument / Parquet statistics pass is only an approximate skip; the
    // actual predicate must still be evaluated on every returned row.
    // Future expression-driven Parquet lazy materialisation will also set this true
    // once it can interleave predicate evaluation with column loading.
    virtual bool scanner_handles_multi_slot_conjuncts_internally() const { return false; }
    void move_split_tasks(std::vector<pipeline::ScanSplitContextPtr>* split_tasks);
    bool has_split_tasks() const { return _scanner_ctx != nullptr && _scanner_ctx->split.has_split_tasks; }

    static StatusOr<std::unique_ptr<RandomAccessFile>> create_random_access_file(
            std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream,
            std::shared_ptr<CacheInputStream>& cache_input_stream, const OpenFileOptions& options);

protected:
    Status open_random_access_file();
    static CompressionTypePB get_compression_type_from_path(const std::string& filename);

    void do_update_iceberg_v2_counter(RuntimeProfile* parquet_profile, const std::string& parent_name);
    void do_update_deletion_vector_build_counter(RuntimeProfile* parent_profile);
    void do_update_deletion_vector_filter_counter(RuntimeProfile* parent_profile);

private:
    bool _opened = false;
    std::atomic<bool> _closed = false;
    Status _build_scanner_context();
    void update_hdfs_counter(HdfsScannerProfile* profile);

protected:
    HdfsScannerContext* _scanner_ctx = nullptr;
    RuntimeState* _runtime_state = nullptr;
    FormatScannerStats _app_stats;
    FormatScannerStats _fs_stats;
    std::unique_ptr<RandomAccessFile> _file;
    // by default it's no compression.
    CompressionTypePB _compression_type = CompressionTypePB::NO_COMPRESSION;
    std::shared_ptr<CacheInputStream> _cache_input_stream = nullptr;
    std::shared_ptr<SharedBufferedInputStream> _shared_buffered_input_stream = nullptr;
    int64_t _total_running_time = 0;

public:
    static constexpr const char* ICEBERG_ROW_ID = "_row_id";
    static constexpr const char* ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER = "_last_updated_sequence_number";
    static constexpr const char* ICEBERG_ROW_POSITION = "_pos";
    // Iceberg v3 spec reserved field IDs for row lineage columns.
    // See: https://iceberg.apache.org/spec/#reserved-field-ids
    static constexpr int32_t ICEBERG_ROW_ID_COLUMN_ID = 2147483540;
    static constexpr int32_t ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_ID = 2147483539;
};

} // namespace starrocks
