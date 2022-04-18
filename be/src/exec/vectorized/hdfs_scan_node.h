// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <memory>

#include "env/env.h"
#include "exec/scan_node.h"
#include "exec/vectorized/hdfs_scanner.h"
#include "hdfs/hdfs.h"

namespace starrocks::vectorized {

struct HdfsFileDesc {
    THdfsFileFormat::type hdfs_file_format;
    Env* env; // The Env used to open |path|
    std::string path;

    int partition_id = 0;
    std::string scan_range_path;
    int64_t file_length = 0;
    std::vector<const THdfsScanRange*> splits;

    std::atomic<int32_t>* open_limit = nullptr;
};

class HdfsScanNode final : public starrocks::ScanNode {
public:
    HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~HdfsScanNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status close(RuntimeState* state) override;

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

    const THdfsScanNode& thrift_hdfs_scan_node() const { return _hdfs_scan_node; }

private:
    int kMaxConcurrency = config::max_hdfs_scanner_num;

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
    void _decompose_conjunct_ctxs();

    Status _start_scan_thread(RuntimeState* state);
    Status _init_partition_values_map();
    StatusOr<bool> _filter_partition(const std::vector<ExprContext*>& partition_exprs);
    Status _find_and_insert_hdfs_file(const THdfsScanRange& scan_range);
    Status _create_and_init_scanner(RuntimeState* state, const HdfsFileDesc& hdfs_file_desc);

    bool _submit_scanner(HdfsScanner* scanner, bool blockable);
    void _scanner_thread(HdfsScanner* scanner);
    void _release_scanner(HdfsScanner* scanner);
    void _update_status(const Status& status);
    Status _get_status();
    void _fill_chunk_pool(int count);
    void _close_pending_scanners();
    void _push_pending_scanner(HdfsScanner* scanner);
    HdfsScanner* _pop_pending_scanner();
    static int _compute_priority(int32_t num_submitted_tasks);

    void _init_counter();

    Status _init_table();

    THdfsScanNode _hdfs_scan_node;
    const TupleDescriptor* _tuple_desc = nullptr;

    int _min_max_tuple_id = 0;
    TupleDescriptor* _min_max_tuple_desc = nullptr;
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
    const LakeTableDescriptor* _lake_table = nullptr;
    std::vector<std::string> _hive_column_names;

    std::mutex _mtx;
    Stack<ChunkPtr> _chunk_pool;
    Stack<HdfsScanner*> _pending_scanners;
    int _num_scanners = 0;
    int _chunks_per_scanner = 0;
    bool _start = false;
    mutable SpinLock _status_mutex;
    Status _status;
    RuntimeState* _runtime_state = nullptr;
    std::atomic_bool _pending_token = true;

    std::atomic<int32_t> _scanner_submit_count = 0;
    std::atomic<int32_t> _running_threads = 0;
    std::atomic<int32_t> _closed_scanners = 0;

    UnboundedBlockingQueue<ChunkPtr> _result_chunks;

    HdfsScanProfile _profile;
};
} // namespace starrocks::vectorized
