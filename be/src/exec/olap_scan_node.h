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

#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "column/chunk.h"
#include "exec/olap_common.h"
#include "exec/olap_scan_prepare.h"
#include "exec/scan_node.h"
#include "exec/tablet_scanner.h"
#include "runtime/global_dict/parser.h"

namespace starrocks {
class DescriptorTbl;
class SlotDescriptor;
class TupleDescriptor;

class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class Tablet;
using TabletSharedPtr = std::shared_ptr<Tablet>;
} // namespace starrocks

namespace starrocks {

// OlapScanNode fetch records from storage engine and pass them to the parent node.
// It will submit many TabletScanner to a global-shared thread pool to execute concurrently.
//
// Execution flow:
// 1. OlapScanNode creates many empty chunks and put them into _chunk_pool.
// 2. OlapScanNode submit many OlapScanners to a global-shared thread pool.
// 3. TabletScanner fetch an empty Chunk from _chunk_pool and fill it with the records retrieved
//    from storage engine.
// 4. TabletScanner put the non-empty Chunk into _result_chunks.
// 5. OlapScanNode receive chunk from _result_chunks and put an new empty chunk into _chunk_pool.
//
// If _chunk_pool is empty, OlapScanners will quit the thread pool and put themself to the
// _pending_scanners. After enough chunks has been placed into _chunk_pool, OlapScanNode will
// resubmit OlapScanners to the thread pool.
class OlapScanNode final : public starrocks::ScanNode {
public:
    OlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~OlapScanNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status close(RuntimeState* statue) override;

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;
    StatusOr<pipeline::MorselQueuePtr> convert_scan_range_to_morsel_queue(
            const std::vector<TScanRangeParams>& scan_ranges, int node_id, int32_t pipeline_dop,
            bool enable_tablet_internal_parallel, TTabletInternalParallelMode::type tablet_internal_parallel_mode,
            size_t num_total_scan_ranges) override;

    void debug_string(int indentation_level, std::stringstream* out) const override { *out << "OlapScanNode"; }
    Status collect_query_statistics(QueryStatistics* statistics) override;

    Status set_scan_ranges(const std::vector<TInternalScanRange>& ranges);

    Status set_scan_range(const TInternalScanRange& range);

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

    const TOlapScanNode& thrift_olap_scan_node() const { return _olap_scan_node; }

    int estimated_max_concurrent_chunks() const;

    static StatusOr<TabletSharedPtr> get_tablet(const TInternalScanRange* scan_range);
    static int compute_priority(int32_t num_submitted_tasks);

    int io_tasks_per_scan_operator() const override {
        if (_sorted_by_keys_per_tablet) {
            return 1;
        }
        return starrocks::ScanNode::io_tasks_per_scan_operator();
    }

    bool is_asc_hint() const override { return _output_asc_hint; }
    std::optional<bool> partition_order_hint() const override { return _partition_order_hint; }

    const std::vector<ExprContext*>& bucket_exprs() const { return _bucket_exprs; }

private:
    friend class TabletScanner;

    constexpr static const int kMaxConcurrency = 50;
    constexpr static const int kMaxScannerPerRange = 64;

    template <typename T>
    class Stack {
    public:
        void reserve(size_t n) { _items.reserve(n); }

        void push(const T& p) { _items.push_back(p); }

        void push(T&& v) { _items.emplace_back(std::move(v)); }

        void clear() { _items.clear(); }

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

private:
    Status _start_scan(RuntimeState* state);
    Status _start_scan_thread(RuntimeState* state);
    void _scanner_thread(TabletScanner* scanner);

    void _init_counter(RuntimeState* state);

    void _update_status(const Status& status);
    Status _get_status();

    void _fill_chunk_pool(int count, bool force_column_pool);
    bool _submit_scanner(TabletScanner* scanner, bool blockable);
    void _close_pending_scanners();

    // Reference the row sets into _tablet_rowsets in the preparation phase to avoid
    // the row sets being deleted. Should be called after set_scan_ranges.
    Status _capture_tablet_rowsets();

    // scanner concurrency
    size_t _scanner_concurrency() const;
    void _estimate_scan_and_output_row_bytes();

    StatusOr<bool> _could_tablet_internal_parallel(const std::vector<TScanRangeParams>& scan_ranges,
                                                   int32_t pipeline_dop, size_t num_total_scan_ranges,
                                                   TTabletInternalParallelMode::type tablet_internal_parallel_mode,
                                                   int64_t* scan_dop, int64_t* splitted_scan_rows) const;
    StatusOr<bool> _could_split_tablet_physically(const std::vector<TScanRangeParams>& scan_ranges) const;

private:
    TOlapScanNode _olap_scan_node;
    std::vector<std::unique_ptr<TInternalScanRange>> _scan_ranges;
    TupleDescriptor* _tuple_desc = nullptr;
    OlapScanConjunctsManager _conjuncts_manager;
    DictOptimizeParser _dict_optimize_parser;
    const Schema* _chunk_schema = nullptr;

    int32_t _num_scanners = 0;
    int32_t _chunks_per_scanner = 10;
    size_t _estimated_scan_row_bytes = 0;
    size_t _estimated_output_row_bytes = 0;
    bool _start = false;

    mutable SpinLock _status_mutex;
    Status _status;

    // _mtx protects _chunk_pool and _pending_scanners.
    std::mutex _mtx;
    Stack<ChunkPtr> _chunk_pool;
    Stack<TabletScanner*> _pending_scanners;

    UnboundedBlockingQueue<ChunkPtr> _result_chunks;

    // used to compute task priority.
    std::atomic<int32_t> _scanner_submit_count{0};
    std::atomic<int32_t> _running_threads{0};
    std::atomic<int32_t> _closed_scanners{0};

    std::vector<std::string> _unused_output_columns;

    // The row sets of tablets will become stale and be deleted, if compaction occurs
    // and these row sets aren't referenced, which will typically happen when the tablets
    // of the left table are compacted at building the right hash table. Therefore, reference
    // the row sets into _tablet_rowsets in the preparation phase to avoid the row sets being deleted.
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;

    bool _sorted_by_keys_per_tablet = false;
    bool _output_asc_hint = true;
    std::optional<bool> _partition_order_hint;

    std::vector<ExprContext*> _bucket_exprs;

    // profile
    RuntimeProfile* _scan_profile = nullptr;

    RuntimeProfile::Counter* _scan_timer = nullptr;
    RuntimeProfile::Counter* _create_seg_iter_timer = nullptr;
    RuntimeProfile::Counter* _tablet_counter = nullptr;
    RuntimeProfile::Counter* _io_task_counter = nullptr;
    RuntimeProfile::Counter* _task_concurrency = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompress_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;
    RuntimeProfile::Counter* _raw_rows_counter = nullptr;
    RuntimeProfile::Counter* _pred_filter_counter = nullptr;
    RuntimeProfile::Counter* _del_vec_filter_counter = nullptr;
    RuntimeProfile::Counter* _pred_filter_timer = nullptr;
    RuntimeProfile::Counter* _chunk_copy_timer = nullptr;
    RuntimeProfile::Counter* _get_rowsets_timer = nullptr;
    RuntimeProfile::Counter* _get_delvec_timer = nullptr;
    RuntimeProfile::Counter* _seg_init_timer = nullptr;
    RuntimeProfile::Counter* _seg_zm_filtered_counter = nullptr;
    RuntimeProfile::Counter* _seg_rt_filtered_counter = nullptr;
    RuntimeProfile::Counter* _zm_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bf_filtered_counter = nullptr;
    RuntimeProfile::Counter* _sk_filtered_counter = nullptr;
    RuntimeProfile::Counter* _block_seek_timer = nullptr;
    RuntimeProfile::Counter* _block_seek_counter = nullptr;
    RuntimeProfile::Counter* _block_load_timer = nullptr;
    RuntimeProfile::Counter* _block_load_counter = nullptr;
    RuntimeProfile::Counter* _block_fetch_timer = nullptr;
    RuntimeProfile::Counter* _read_pages_num_counter = nullptr;
    RuntimeProfile::Counter* _cached_pages_num_counter = nullptr;
    RuntimeProfile::Counter* _bi_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bi_filter_timer = nullptr;
    RuntimeProfile::Counter* _pushdown_predicates_counter = nullptr;
    RuntimeProfile::Counter* _rowsets_read_count = nullptr;
    RuntimeProfile::Counter* _segments_read_count = nullptr;
    RuntimeProfile::Counter* _total_columns_data_page_count = nullptr;
};

} // namespace starrocks
