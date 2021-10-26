// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>

#include "env/env.h"
#include "exec/scan_node.h"
#include "exec/vectorized/hdfs_scanner.h"
#include "exec/vectorized/hdfs_scanner_orc.h"
#include "hdfs/hdfs.h"
#include "runtime/tuple.h"

namespace starrocks::vectorized {

struct HdfsFileDesc {
    hdfsFS hdfs_fs;
    hdfsFile hdfs_file;
    THdfsFileFormat::type hdfs_file_format;
    std::shared_ptr<RandomAccessFile> fs = nullptr;

    int partition_id = 0;
    std::string path;
    int64_t file_length = 0;
    std::vector<const THdfsScanRange*> splits;
};

class HdfsScanNode final : public starrocks::ScanNode {
public:
    HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~HdfsScanNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status close(RuntimeState* state) override;

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

private:
    friend HdfsScanner;
    friend HdfsParquetScanner;
    friend HdfsOrcScanner;
    constexpr static const int kMaxConcurrency = 50;

    template <typename T>
    class Stack {
    public:
        void reserve(size_t n) { _items.reserve(n); }

        void push(const T& p) { _items.push_back(p); }
        void push(T&& v) { _items.emplace_back(std::move(v)); }

        // REQUIRES: not empty.
        T pop() {
            DCHECK(!_items.empty());
            T v = _items.back();
            _items.pop_back();
            return v;
        }

        size_t size() const { return _items.size(); }
        bool empty() const { return _items.empty(); }
        void reverse() { std::reverse(_items.begin(), _items.end()); }

    private:
        std::vector<T> _items;
    };

    // create
    // 1. _scanner_conjunct_ctxs evaled in scanner.
    // 2. _conjunct_ctxs_by_slot evaled in parquet file reader or group reader.
    void _pre_process_conjunct_ctxs();

    Status _start_scan_thread(RuntimeState* state);
    void _init_partition_expr_map();
    bool _filter_partition(const std::vector<ExprContext*>& partition_exprs);
    Status _find_and_insert_hdfs_file(const THdfsScanRange& scan_range);
    Status _create_and_init_scanner(RuntimeState* state, const HdfsFileDesc& hdfs_file_desc);

    bool _submit_scanner(HdfsScanner* scanner, bool blockable);
    void _scanner_thread(HdfsScanner* scanner);
    void _update_status(const Status& status);
    Status _get_status();
    void _fill_chunk_pool(int count);
    void _close_pending_scanners();
    static int _compute_priority(int32_t num_submitted_tasks);

    void _init_counter(RuntimeState* state);

    static Status _get_name_node_from_path(const std::string& path, std::string* namenode);

    int _tuple_id = 0;
    const TupleDescriptor* _tuple_desc = nullptr;

    int _min_max_tuple_id = 0;
    TupleDescriptor* _min_max_tuple_desc = nullptr;
    RowDescriptor* _min_max_row_desc = nullptr;
    std::vector<ExprContext*> _min_max_conjunct_ctxs;

    // complex conjuncts, such as contains multi slot, are evaled in scanner.
    std::vector<ExprContext*> _scanner_conjunct_ctxs;
    // conjuncts that contains only one slot.
    // 1. conjuncts that column is not exist in file, are used to filter file in file reader.
    // 2. conjuncts that column is materialized, are evaled in group reader.
    std::unordered_map<SlotId, std::vector<ExprContext*>> _conjunct_ctxs_by_slot;

    // materialized columns.
    std::vector<SlotDescriptor*> _materialize_slots;
    std::vector<int> _materialize_index_in_chunk;

    // partition columns.
    std::vector<SlotDescriptor*> _partition_slots;
    // created like partition columns.
    ChunkPtr _partition_chunk;
    // partition column index in `tuple_desc`
    std::vector<int> _partition_index_in_chunk;
    // partition index in hdfs partition columns
    std::vector<int> _partition_index_in_hdfs_partition_columns;
    // partition id -> values of partition_slots, and they are constant for sure.
    std::unordered_map<int64_t, std::vector<ExprContext*>> _partition_values_map;
    // partition conjuncts of each partition slot.
    std::vector<ExprContext*> _partition_conjunct_ctxs;
    bool _has_partition_columns = false;
    bool _has_partition_conjuncts = false;

    std::vector<THdfsScanRange> _scan_ranges;
    std::vector<HdfsFileDesc*> _hdfs_files;
    const HdfsTableDescriptor* _hdfs_table = nullptr;
    std::vector<std::string> _hive_column_names;

    std::unique_ptr<MemPool> _mem_pool;

    bool _eos = false;

    std::mutex _mtx;
    Stack<ChunkPtr> _chunk_pool;
    Stack<HdfsScanner*> _pending_scanners;
    int _num_scanners = 0;
    int _chunks_per_scanner = 0;
    bool _start = false;
    mutable SpinLock _status_mutex;
    Status _status;
    RuntimeState* _runtime_state = nullptr;
    bool _is_hdfs_fs = true;

    std::atomic<int32_t> _scanner_submit_count = 0;
    std::atomic<int32_t> _running_threads = 0;
    std::atomic<int32_t> _closed_scanners = 0;

    UnboundedBlockingQueue<ChunkPtr> _result_chunks;

    RuntimeProfile::Counter* _scan_timer = nullptr;
    RuntimeProfile::Counter* _reader_init_timer = nullptr;
    RuntimeProfile::Counter* _open_file_timer = nullptr;
    RuntimeProfile::Counter* _raw_rows_counter = nullptr;
    RuntimeProfile::Counter* _expr_filter_timer = nullptr;

    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _io_counter = nullptr;
    RuntimeProfile::Counter* _bytes_read_from_disk_counter = nullptr;
    RuntimeProfile::Counter* _column_read_timer = nullptr;
    RuntimeProfile::Counter* _level_decode_timer = nullptr;
    RuntimeProfile::Counter* _value_decode_timer = nullptr;
    RuntimeProfile::Counter* _page_read_timer = nullptr;
    RuntimeProfile::Counter* _column_convert_timer = nullptr;

    RuntimeProfile::Counter* _bytes_total_read = nullptr;
    RuntimeProfile::Counter* _bytes_read_local = nullptr;
    RuntimeProfile::Counter* _bytes_read_short_circuit = nullptr;
    RuntimeProfile::Counter* _bytes_read_dn_cache = nullptr;
    RuntimeProfile::Counter* _bytes_read_remote = nullptr;

    // reader init
    RuntimeProfile::Counter* _footer_read_timer = nullptr;
    RuntimeProfile::Counter* _column_reader_init_timer = nullptr;

    // dict filter
    RuntimeProfile::Counter* _group_chunk_read_timer = nullptr;
    RuntimeProfile::Counter* _group_dict_filter_timer = nullptr;
    RuntimeProfile::Counter* _group_dict_decode_timer = nullptr;
};
} // namespace starrocks::vectorized
