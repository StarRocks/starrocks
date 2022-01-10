// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "exec/scan_node.h"
#include "exec/vectorized/file_scanner.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks {

class RuntimeState;
struct ScannerCounter;

namespace vectorized {

class FileScanNode final : public ScanNode {
public:
    FileScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~FileScanNode() override;

    // Called after create this scan node
    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    // Prepare partition infos & set up timer
    Status prepare(RuntimeState* state) override;

    // Start broker scan using ParquetScanner or BrokerScanner.
    Status open(RuntimeState* state) override;

    // Fill the next row batch by calling next() on the scanner,
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    // Close the scanner, and report errors.
    Status close(RuntimeState* state) override;

    // No use
    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    // Write debug string of this into out.
    void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    // Update process status to one failed status,
    // NOTE: Must hold the mutex of this scan node
    bool _update_status(const Status& new_status) {
        if (_process_status.ok()) {
            _process_status = new_status;
            return true;
        }
        return false;
    }

    // Create scanners to do scan job
    Status _start_scanners();

    // One scanner worker, This scanner will handle 'length' ranges start from start_idx
    void _scanner_worker(int start_idx, int length);

    // Scan one range
    Status _scanner_scan(const TBrokerScanRange& scan_range, const std::vector<ExprContext*>& conjunct_ctxs,
                         ScannerCounter* counter);

    std::unique_ptr<FileScanner> _create_scanner(const TBrokerScanRange& scan_range, ScannerCounter* counter);

    TupleId _tuple_id;
    RuntimeState* _runtime_state = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    std::vector<TScanRangeParams> _scan_ranges;

    std::mutex _chunk_queue_lock;
    std::condition_variable _queue_reader_cond;
    std::condition_variable _queue_writer_cond;

    std::deque<ChunkPtr> _chunk_queue;

    int64_t _cur_mem_usage = 0;

    static const int _max_queue_size = 32;
    static const int64_t _max_mem_usage = 64 * 1024 * 1024;

    int _num_running_scanners = 0;

    std::atomic<bool> _scan_finished{false};

    Status _process_status;

    std::vector<std::thread> _scanner_threads;

    // Profile information
    RuntimeProfile::Counter* _wait_scanner_timer = nullptr;
    RuntimeProfile::Counter* _scanner_total_timer = nullptr;
    RuntimeProfile::Counter* _scanner_fill_timer = nullptr;
    RuntimeProfile::Counter* _scanner_read_timer = nullptr;
    RuntimeProfile::Counter* _scanner_cast_chunk_timer = nullptr;
    RuntimeProfile::Counter* _scanner_materialize_timer = nullptr;
    RuntimeProfile::Counter* _scanner_init_chunk_timer = nullptr;
    RuntimeProfile::Counter* _scanner_file_reader_timer = nullptr;
};

} // namespace vectorized
} // namespace starrocks
