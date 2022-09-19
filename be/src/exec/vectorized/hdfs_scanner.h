// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <utility>

#include "column/chunk.h"
#include "common/object_pool.h"
#include "env/env.h"
#include "env/env_hdfs.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"

namespace starrocks::parquet {
class FileReader;
}
namespace starrocks::vectorized {

class HdfsScanNode;
class RuntimeFilterProbeCollector;

struct HdfsScanStats {
    int64_t scan_ns = 0;
    int64_t raw_rows_read = 0;
    int64_t expr_filter_ns = 0;
    int64_t io_ns = 0;
    int64_t io_count = 0;
    int64_t bytes_read = 0;
    int64_t column_read_ns = 0;
    int64_t column_convert_ns = 0;
    int64_t reader_init_ns = 0;

    // parquet only!
    // read & decode
    int64_t level_decode_ns = 0;
    int64_t value_decode_ns = 0;
    int64_t page_read_ns = 0;
    // reader init
    int64_t footer_read_ns = 0;
    int64_t column_reader_init_ns = 0;
    // dict filter
    int64_t group_chunk_read_ns = 0;
    int64_t group_dict_filter_ns = 0;
    int64_t group_dict_decode_ns = 0;
};

struct HdfsScanProfile {
    RuntimeProfile* runtime_profile = nullptr;

    RuntimeProfile::Counter* rows_read_counter = nullptr;
    RuntimeProfile::Counter* bytes_read_counter = nullptr;
    RuntimeProfile::Counter* scan_timer = nullptr;
    RuntimeProfile::Counter* scanner_queue_timer = nullptr;
    RuntimeProfile::Counter* scan_ranges_counter = nullptr;
    RuntimeProfile::Counter* scan_files_counter = nullptr;
    RuntimeProfile::Counter* reader_init_timer = nullptr;
    RuntimeProfile::Counter* open_file_timer = nullptr;
    RuntimeProfile::Counter* expr_filter_timer = nullptr;

    RuntimeProfile::Counter* io_timer = nullptr;
    RuntimeProfile::Counter* io_counter = nullptr;
    RuntimeProfile::Counter* column_read_timer = nullptr;
    RuntimeProfile::Counter* column_convert_timer = nullptr;
};

struct HdfsScannerParams {
    // one file split (parition_id, file_path, file_length, offset, length, file_format)
    std::vector<const THdfsScanRange*> scan_ranges;

    // runtime bloom filter.
    RuntimeFilterProbeCollector* runtime_filter_collector;

    // should clone in scanner
    std::vector<ExprContext*> conjunct_ctxs;
    // conjunct group by slot
    // excluded from conjunct_ctxs.
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // The Env used to open the file to be scanned
    Env* env = nullptr;
    // The file to scan
    std::string path;

    const TupleDescriptor* tuple_desc;

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

    TupleDescriptor* min_max_tuple_desc;

    std::vector<std::string>* hive_column_names;

    HdfsScanProfile* profile = nullptr;

    std::atomic<int32_t>* open_limit;
};

struct HdfsFileReaderParam {
    struct ColumnInfo {
        int col_idx;
        TypeDescriptor col_type;
        SlotId slot_id;
        std::string col_name;
        SlotDescriptor* slot_desc;
    };

    const TupleDescriptor* tuple_desc;
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // materialized column read from parquet file
    std::vector<ColumnInfo> materialized_columns;

    // partition column
    std::vector<ColumnInfo> partition_columns;

    // partition column value which read from hdfs file path
    std::vector<vectorized::ColumnPtr> partition_values;

    // scan ranges
    std::vector<const THdfsScanRange*> scan_ranges;

    // min max slots
    TupleDescriptor* min_max_tuple_desc;

    // min max conjunct
    std::vector<ExprContext*> min_max_conjunct_ctxs;

    std::string timezone;

    vectorized::HdfsScanStats* stats = nullptr;

    // set column names from file.
    // and to update not_existed slots and conjuncts.
    // and to update `conjunct_ctxs_by_slot` field.
    void set_columns_from_file(const std::unordered_set<std::string>& names);
    // "not existed columns" are materialized columns not found in file
    // this usually happens when use changes schema. for example
    // user create table with 3 fields A, B, C, and there is one file F1
    // but user change schema and add one field like D.
    // when user select(A, B, C, D), then D is the non-existed column in file F1.
    void append_not_exised_columns_to_chunk(vectorized::ChunkPtr* chunk, size_t row_count);
    // if we can skip this file by evaluating conjuncts of non-existed columns with default value.
    StatusOr<bool> should_skip_by_evaluating_not_existed_slots();
    std::vector<SlotDescriptor*> not_existed_slots;
    std::vector<ExprContext*> conjunct_ctxs_of_non_existed_slots;

    // other helper functions.
    void append_partition_column_to_chunk(vectorized::ChunkPtr* chunk, size_t row_count);
    bool can_use_dict_filter_on_slot(SlotDescriptor* slot) const;
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
    void cleanup();

    int64_t raw_rows_read() const { return _stats.raw_rows_read; }
    void set_keep_priority(bool v) { _keep_priority = v; }
    bool keep_priority() const { return _keep_priority; }
    void update_counter();

    RuntimeState* runtime_state() { return _runtime_state; }

    int32_t open_limit() { return _scanner_params.open_limit->load(std::memory_order_relaxed); }

    bool is_open() { return _is_open; }

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

    void enter_pending_queue();
    // how long it stays inside pending queue.
    uint64_t exit_pending_queue();

private:
    bool _is_open = false;
    bool _is_closed = false;
    bool _keep_priority = false;
    Status _build_file_read_param();
    MonotonicStopWatch _pending_queue_sw;
    void update_hdfs_counter(HdfsScanProfile* profile);

protected:
    std::atomic_bool _pending_token = false;

    HdfsFileReaderParam _file_read_param;
    HdfsScannerParams _scanner_params;
    ObjectPool _pool;
    RuntimeState* _runtime_state = nullptr;
    HdfsScanStats _stats;
    // predicate collections.
    std::vector<ExprContext*> _conjunct_ctxs;
    // columns we want to fetch.
    std::vector<std::string> _scanner_columns;
    // predicates group by slot.
    std::unordered_map<SlotId, std::vector<ExprContext*>> _conjunct_ctxs_by_slot;
    // predicate which havs min/max
    std::vector<ExprContext*> _min_max_conjunct_ctxs;
    std::unique_ptr<RandomAccessFile> _file;
};

} // namespace starrocks::vectorized
