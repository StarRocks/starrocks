// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "exec/decompressor.h"
#include "exec/scan_node.h"
#include "exec/vectorized/file_scanner.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks {

class RuntimeState;
class PartRangeKey;
class PartitionInfo;
class RandomAccessFile;
struct ScannerCounter;
class SequentialFile;
class RandomAccessFile;

namespace vectorized {

class FileScanNode : public ScanNode {
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
    bool update_status(const Status& new_status) {
        if (_process_status.ok()) {
            _process_status = new_status;
            return true;
        }
        return false;
    }

    // Create scanners to do scan job
    Status start_scanners();

    // One scanner worker, This scanner will handle 'length' ranges start from start_idx
    void scanner_worker(int start_idx, int length);

    // Scan one range
    Status scanner_scan(const TBrokerScanRange& scan_range, const std::vector<ExprContext*>& conjunct_ctxs,
                        ScannerCounter* counter);

    std::unique_ptr<FileScanner> create_scanner(const TBrokerScanRange& scan_range, ScannerCounter* counter);

private:
    TupleId _tuple_id;
    TupleDescriptor* _tuple_desc;
    std::vector<TScanRangeParams> _scan_ranges;

    std::mutex _chunk_queue_lock;
    std::condition_variable _queue_reader_cond;
    std::condition_variable _queue_writer_cond;

    std::deque<ChunkPtr> _chunk_queue;

    int _max_queue_size;

    int _num_running_scanners;

    std::atomic<bool> _scan_finished;

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
