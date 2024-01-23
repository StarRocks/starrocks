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
#include <boost/algorithm/string.hpp>

#include "exec/mor_processor.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "fs/fs.h"
#include "io/cache_input_stream.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

class RuntimeFilterProbeCollector;

struct HdfsScanStats {
    int64_t raw_rows_read = 0;
    int64_t rows_read = 0;
    int64_t late_materialize_skip_rows = 0;

    int64_t io_ns = 0;
    int64_t io_count = 0;
    int64_t bytes_read = 0;

    int64_t expr_filter_ns = 0;
    int64_t column_read_ns = 0;
    int64_t column_convert_ns = 0;
    int64_t reader_init_ns = 0;

    // parquet only!
    // read & decode
    int64_t request_bytes_read = 0;
    int64_t request_bytes_read_uncompressed = 0;
    int64_t level_decode_ns = 0;
    int64_t value_decode_ns = 0;
    int64_t page_read_ns = 0;
    // reader init
    int64_t footer_read_ns = 0;
    int64_t footer_cache_read_ns = 0;
    int64_t footer_cache_read_count = 0;
    int64_t footer_cache_write_count = 0;
    int64_t footer_cache_write_bytes = 0;
    int64_t column_reader_init_ns = 0;
    // dict filter
    int64_t group_chunk_read_ns = 0;
    int64_t group_dict_filter_ns = 0;
    int64_t group_dict_decode_ns = 0;
    // io coalesce
    int64_t group_active_lazy_coalesce_together = 0;
    int64_t group_active_lazy_coalesce_seperately = 0;
    // page statistics
    bool has_page_statistics = false;
    // page skip
    int64_t page_skip = 0;

    // late materialize round-by-round
    int64_t group_min_round_cost = 0;

    std::vector<int64_t> orc_stripe_sizes;
    // io coalesce
    int64_t orc_stripe_active_lazy_coalesce_together = 0;
    int64_t orc_stripe_active_lazy_coalesce_seperately = 0;

    // Iceberg v2 only!
    int64_t iceberg_delete_file_build_ns = 0;
    int64_t iceberg_delete_files_per_scan = 0;
    int64_t iceberg_delete_file_build_filter_ns = 0;
};

class HdfsParquetProfile;

struct HdfsScanProfile {
    RuntimeProfile* runtime_profile = nullptr;
    RuntimeProfile::Counter* raw_rows_read_counter = nullptr;
    RuntimeProfile::Counter* rows_read_counter = nullptr;
    RuntimeProfile::Counter* late_materialize_skip_rows_counter = nullptr;
    RuntimeProfile::Counter* scan_ranges_counter = nullptr;

    RuntimeProfile::Counter* reader_init_timer = nullptr;
    RuntimeProfile::Counter* open_file_timer = nullptr;
    RuntimeProfile::Counter* expr_filter_timer = nullptr;
    RuntimeProfile::Counter* column_read_timer = nullptr;
    RuntimeProfile::Counter* column_convert_timer = nullptr;

    RuntimeProfile::Counter* datacache_read_counter = nullptr;
    RuntimeProfile::Counter* datacache_read_bytes = nullptr;
    RuntimeProfile::Counter* datacache_read_mem_bytes = nullptr;
    RuntimeProfile::Counter* datacache_read_disk_bytes = nullptr;
    RuntimeProfile::Counter* datacache_read_timer = nullptr;
    RuntimeProfile::Counter* datacache_skip_read_counter = nullptr;
    RuntimeProfile::Counter* datacache_skip_read_bytes = nullptr;
    RuntimeProfile::Counter* datacache_write_counter = nullptr;
    RuntimeProfile::Counter* datacache_write_bytes = nullptr;
    RuntimeProfile::Counter* datacache_write_timer = nullptr;
    RuntimeProfile::Counter* datacache_write_fail_counter = nullptr;
    RuntimeProfile::Counter* datacache_write_fail_bytes = nullptr;
    RuntimeProfile::Counter* datacache_read_block_buffer_counter = nullptr;
    RuntimeProfile::Counter* datacache_read_block_buffer_bytes = nullptr;

    RuntimeProfile::Counter* shared_buffered_shared_io_count = nullptr;
    RuntimeProfile::Counter* shared_buffered_shared_io_bytes = nullptr;
    RuntimeProfile::Counter* shared_buffered_shared_io_timer = nullptr;
    RuntimeProfile::Counter* shared_buffered_direct_io_count = nullptr;
    RuntimeProfile::Counter* shared_buffered_direct_io_bytes = nullptr;
    RuntimeProfile::Counter* shared_buffered_direct_io_timer = nullptr;

    RuntimeProfile::Counter* app_io_bytes_read_counter = nullptr;
    RuntimeProfile::Counter* app_io_timer = nullptr;
    RuntimeProfile::Counter* app_io_counter = nullptr;
    RuntimeProfile::Counter* fs_bytes_read_counter = nullptr;
    RuntimeProfile::Counter* fs_io_timer = nullptr;
    RuntimeProfile::Counter* fs_io_counter = nullptr;
};

struct HdfsScannerParams {
    // one file split (parition_id, file_path, file_length, offset, length, file_format)
    std::vector<const THdfsScanRange*> scan_ranges;

    // runtime bloom filter.
    const RuntimeFilterProbeCollector* runtime_filter_collector = nullptr;

    // all conjuncts except `conjunct_ctxs_by_slot`
    std::vector<ExprContext*> conjunct_ctxs;
    std::unordered_set<SlotId> slots_in_conjunct;
    // slot used by conjunct_ctxs
    std::unordered_set<SlotId> slots_of_mutli_slot_conjunct;
    bool eval_conjunct_ctxs = true;

    // conjunct ctxs grouped by slot.
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // The FileSystem used to open the file to be scanned
    FileSystem* fs = nullptr;
    // The file to scan
    std::string path;
    // The file size. -1 means unknown.
    int64_t file_size = -1;

    // The file last modification time
    int64_t modification_time = 0;

    TupleDescriptor* tuple_desc = nullptr;

    // columns read from file
    std::vector<SlotDescriptor*> materialize_slots;
    std::vector<int> materialize_index_in_chunk;

    // columns of partition info
    std::vector<SlotDescriptor*> partition_slots;
    std::vector<int> partition_index_in_chunk;
    // partition index in hdfs partition columns.
    std::vector<int> _partition_index_in_hdfs_partition_columns;

    // partition conjunct, used to generate partition columns
    std::vector<ExprContext*> partition_values;

    // min max conjunct for filter row group or page
    // should clone in scanner
    std::vector<ExprContext*> min_max_conjunct_ctxs;

    const TupleDescriptor* min_max_tuple_desc = nullptr;

    std::vector<std::string>* hive_column_names = nullptr;

    bool case_sensitive = false;

    HdfsScanProfile* profile = nullptr;

    std::atomic<int32_t>* open_limit;

    std::vector<const TIcebergDeleteFile*> deletes;

    const TIcebergSchema* iceberg_schema = nullptr;

    bool is_lazy_materialization_slot(SlotId slot_id) const;

    bool use_datacache = false;
    bool enable_populate_datacache = false;

    std::atomic<int32_t>* lazy_column_coalesce_counter;
    bool can_use_any_column = false;
    bool can_use_min_max_count_opt = false;
    bool use_file_metacache = false;
    MORParams mor_params;
};

struct HdfsScannerContext {
    struct ColumnInfo {
        int col_idx;
        TypeDescriptor col_type;
        SlotId slot_id;
        std::string col_name;
        SlotDescriptor* slot_desc;
        bool decode_needed = true;

        std::string formated_col_name(bool case_sensitive) {
            return case_sensitive ? col_name : boost::algorithm::to_lower_copy(col_name);
        }
    };

    const TupleDescriptor* tuple_desc = nullptr;
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // materialized column read from parquet file
    std::vector<ColumnInfo> materialized_columns;

    // partition column
    std::vector<ColumnInfo> partition_columns;

    // partition column value which read from hdfs file path
    std::vector<ColumnPtr> partition_values;

    // scan ranges
    std::vector<const THdfsScanRange*> scan_ranges;

    // min max slots
    const TupleDescriptor* min_max_tuple_desc = nullptr;

    // min max conjunct
    std::vector<ExprContext*> min_max_conjunct_ctxs;

    // runtime filters.
    const RuntimeFilterProbeCollector* runtime_filter_collector = nullptr;

    std::vector<std::string>* hive_column_names = nullptr;

    bool case_sensitive = false;

    bool can_use_any_column = false;

    bool can_use_min_max_count_opt = false;

    bool use_file_metacache = false;

    std::string timezone;

    const TIcebergSchema* iceberg_schema = nullptr;

    HdfsScanStats* stats = nullptr;

    std::atomic<int32_t>* lazy_column_coalesce_counter;

    // update materialized column against data file.
    // and to update not_existed slots and conjuncts.
    // and to update `conjunct_ctxs_by_slot` field.
    void update_materialized_columns(const std::unordered_set<std::string>& names);
    // "not existed columns" are materialized columns not found in file
    // this usually happens when use changes schema. for example
    // user create table with 3 fields A, B, C, and there is one file F1
    // but user change schema and add one field like D.
    // when user select(A, B, C, D), then D is the non-existed column in file F1.
    void update_not_existed_columns_of_chunk(ChunkPtr* chunk, size_t row_count);
    // if we can skip this file by evaluating conjuncts of non-existed columns with default value.
    StatusOr<bool> should_skip_by_evaluating_not_existed_slots();
    std::vector<SlotDescriptor*> not_existed_slots;
    std::vector<ExprContext*> conjunct_ctxs_of_non_existed_slots;

    // other helper functions.
    bool can_use_dict_filter_on_slot(SlotDescriptor* slot) const;

    void append_not_existed_columns_to_chunk(ChunkPtr* chunk, size_t row_count);

    // If there is no partition column in the chunk，append partition column to chunk，otherwise update partition column in chunk
    void append_or_update_partition_column_to_chunk(ChunkPtr* chunk, size_t row_count);
    Status evaluate_on_conjunct_ctxs_by_slot(ChunkPtr* chunk, Filter* filter);
};

// if *lvalue == expect, swap(*lvalue,*rvalue)
inline bool atomic_cas(std::atomic_bool* lvalue, std::atomic_bool* rvalue, bool expect) {
    bool res = lvalue->compare_exchange_strong(expect, *rvalue);
    if (res) *rvalue = expect;
    return res;
}

class HdfsScanner {
public:
    HdfsScanner() = default;
    virtual ~HdfsScanner() = default;

    Status open(RuntimeState* runtime_state);
    void close(RuntimeState* runtime_state) noexcept;
    Status get_next(RuntimeState* runtime_state, ChunkPtr* chunk);
    Status init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params);
    void finalize();

    int64_t num_bytes_read() const { return _app_stats.bytes_read; }
    int64_t raw_rows_read() const { return _app_stats.raw_rows_read; }
    int64_t num_rows_read() const { return _app_stats.rows_read; }
    int64_t cpu_time_spent() const { return _total_running_time - _app_stats.io_ns; }
    int64_t io_time_spent() const { return _app_stats.io_ns; }
    virtual int64_t estimated_mem_usage() const;
    void set_keep_priority(bool v) { _keep_priority = v; }
    bool keep_priority() const { return _keep_priority; }
    void update_counter();

    RuntimeState* runtime_state() { return _runtime_state; }

    int32_t open_limit() { return _scanner_params.open_limit->load(std::memory_order_relaxed); }

    bool is_open() { return _opened; }

    bool acquire_pending_token(std::atomic_bool* token) {
        // acquire resource
        return atomic_cas(token, &_pending_token, true);
    }

    bool release_pending_token(std::atomic_bool* token) {
        if (_pending_token) {
            _pending_token = false;
            *token = true;
            return true;
        }
        return false;
    }

    bool has_pending_token() { return _pending_token; }

    virtual Status do_open(RuntimeState* runtime_state) = 0;
    virtual void do_close(RuntimeState* runtime_state) noexcept = 0;
    virtual Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) = 0;
    virtual Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) = 0;
    virtual void do_update_counter(HdfsScanProfile* profile);
    virtual bool is_jni_scanner() { return false; }

    void enter_pending_queue();
    // how long it stays inside pending queue.
    uint64_t exit_pending_queue();

protected:
    Status open_random_access_file();

    void do_update_iceberg_v2_counter(RuntimeProfile* parquet_profile, const std::string& parent_name);

private:
    bool _opened = false;
    std::atomic<bool> _closed = false;
    bool _keep_priority = false;
    Status _build_scanner_context();
    MonotonicStopWatch _pending_queue_sw;
    void update_hdfs_counter(HdfsScanProfile* profile);
    Status _init_mor_processor(RuntimeState* runtime_state, const MORParams& params);

protected:
    std::atomic_bool _pending_token = false;

    HdfsScannerContext _scanner_ctx;
    HdfsScannerParams _scanner_params;
    RuntimeState* _runtime_state = nullptr;
    HdfsScanStats _app_stats;
    HdfsScanStats _fs_stats;
    std::unique_ptr<RandomAccessFile> _file;
    // by default it's no compression.
    CompressionTypePB _compression_type = CompressionTypePB::NO_COMPRESSION;
    std::shared_ptr<io::CacheInputStream> _cache_input_stream = nullptr;
    std::shared_ptr<io::SharedBufferedInputStream> _shared_buffered_input_stream = nullptr;
    int64_t _total_running_time = 0;

    std::shared_ptr<DefaultMORProcessor> _mor_processor;
};

} // namespace starrocks
