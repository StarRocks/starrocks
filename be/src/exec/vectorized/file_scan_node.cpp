// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/file_scan_node.h"

#include <chrono>
#include <sstream>

#include "column/chunk.h"
#include "common/object_pool.h"
#include "env/compressed_file.h"
#include "env/env.h"
#include "env/env_broker.h"
#include "env/env_stream_pipe.h"
#include "env/env_util.h"
#include "exec/vectorized/csv_scanner.h"
#include "exec/vectorized/json_scanner.h"
#include "exec/vectorized/orc_scanner.h"
#include "exec/vectorized/parquet_scanner.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

FileScanNode::FileScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _tuple_id(tnode.file_scan_node.tuple_id),
          _runtime_state(nullptr),
          _tuple_desc(nullptr),
          _max_queue_size(32),
          _num_running_scanners(0),
          _scan_finished(false) {}

FileScanNode::~FileScanNode() = default;

Status FileScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::init(tnode));
    return Status::OK();
}

Status FileScanNode::prepare(RuntimeState* state) {
    VLOG_QUERY << "FileScanNode prepare";
    RETURN_IF_ERROR(ScanNode::prepare(state));
    // get tuple desc
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Failed to get tuple descriptor, _tuple_id=" << _tuple_id;
        return Status::InternalError(ss.str());
    }

    // Profile
    _wait_scanner_timer = ADD_TIMER(runtime_profile(), "WaitScannerTime");
    _scanner_total_timer = ADD_TIMER(runtime_profile(), "ScannerTotalTimer");

    RuntimeProfile* p = runtime_profile()->create_child("FileScanner", true, true);

    _scanner_fill_timer = ADD_TIMER(p, "FillTimer");
    _scanner_read_timer = ADD_TIMER(p, "ReadTimer");
    _scanner_cast_chunk_timer = ADD_TIMER(p, "CastChunkTimer");
    _scanner_materialize_timer = ADD_TIMER(p, "MaterializeTimer");
    _scanner_init_chunk_timer = ADD_TIMER(p, "CreateChunkTimer");

    _scanner_file_reader_timer = ADD_TIMER(p->create_child("FilePRead", true, true), "FileReadTimer");

    return Status::OK();
}

Status FileScanNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);

    RETURN_IF_ERROR(start_scanners());

    return Status::OK();
}

Status FileScanNode::start_scanners() {
    {
        std::unique_lock<std::mutex> l(_chunk_queue_lock);

        _num_running_scanners = 1;
        _scanner_threads.emplace_back(&FileScanNode::scanner_worker, this, 0, _scan_ranges.size());
    }
    return Status::OK();
}

Status FileScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::InternalError("Not support");
}

Status FileScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    // check if CANCELLED.
    if (state->is_cancelled()) {
        std::unique_lock<std::mutex> l(_chunk_queue_lock);
        if (update_status(Status::Cancelled("Cancelled FileScanNode::get_next"))) {
            // Notify all scanners
            _queue_writer_cond.notify_all();
            return _process_status;
        }
    }

    if (_scan_finished.load()) {
        *eos = true;
        return Status::OK();
    }

    ChunkPtr temp_chunk;
    {
        std::unique_lock<std::mutex> l(_chunk_queue_lock);
        while (_process_status.ok() && !_runtime_state->is_cancelled() && _num_running_scanners > 0 &&
               _chunk_queue.empty()) {
            SCOPED_TIMER(_wait_scanner_timer);
            _queue_reader_cond.wait_for(l, std::chrono::seconds(1));
        }
        if (!_process_status.ok()) {
            // Some scanner process failed.
            return _process_status;
        }
        if (_runtime_state->is_cancelled()) {
            if (update_status(Status::Cancelled("Cancelled FileScanNode::get_next"))) {
                _queue_writer_cond.notify_all();
            }
            return _process_status;
        }
        if (!_chunk_queue.empty()) {
            temp_chunk = _chunk_queue.front();
            _chunk_queue.pop_front();
            mem_tracker()->release(temp_chunk->memory_usage());
        }
    }

    // All scanner has been finished, and all cached batch has been read
    if (temp_chunk == nullptr) {
        _scan_finished.store(true);
        *eos = true;
        return Status::OK();
    }

    // notify one scanner
    _queue_writer_cond.notify_one();

    *chunk = temp_chunk;
    _num_rows_returned += temp_chunk->num_rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);

    // This is first time reach limit.
    // Only valid when query 'select * from table1 limit 20'
    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
        _num_rows_returned -= num_rows_over;
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);

        _scan_finished.store(true);
        _queue_writer_cond.notify_all();
    }
    *eos = false;

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

Status FileScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _scan_finished.store(true);
    _queue_writer_cond.notify_all();
    _queue_reader_cond.notify_all();
    for (auto& _scanner_thread : _scanner_threads) {
        _scanner_thread.join();
    }

    while (!_chunk_queue.empty()) {
        mem_tracker()->release(_chunk_queue.front()->memory_usage());
        _chunk_queue.pop_front();
    }

    return ExecNode::close(state);
}

// This function is called after plan node has been prepared.
Status FileScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    _scan_ranges = scan_ranges;
    return Status::OK();
}

void FileScanNode::debug_string(int ident_level, std::stringstream* out) const {
    (*out) << "FileScanNode";
}

std::unique_ptr<FileScanner> FileScanNode::create_scanner(const TBrokerScanRange& scan_range, ScannerCounter* counter) {
    if (scan_range.ranges[0].format_type == TFileFormatType::FORMAT_ORC) {
        return std::make_unique<ORCScanner>(_runtime_state, runtime_profile(), scan_range, counter);
    } else if (scan_range.ranges[0].format_type == TFileFormatType::FORMAT_PARQUET) {
        return std::make_unique<ParquetScanner>(_runtime_state, runtime_profile(), scan_range, counter);
    } else if (scan_range.ranges[0].format_type == TFileFormatType::FORMAT_JSON) {
        return std::make_unique<JsonScanner>(_runtime_state, runtime_profile(), scan_range, counter);
    } else {
        return std::make_unique<CSVScanner>(_runtime_state, runtime_profile(), scan_range, counter);
    }
}

Status FileScanNode::scanner_scan(const TBrokerScanRange& scan_range, const std::vector<ExprContext*>& conjunct_ctxs,
                                  ScannerCounter* counter) {
    if (scan_range.ranges.empty()) {
        return Status::EndOfFile("scan range is empty");
    }
    //create scanner object and open
    std::unique_ptr<FileScanner> scanner = create_scanner(scan_range, counter);
    if (scanner == nullptr) {
        return Status::InternalError("Failed to create scanner");
    }
    RETURN_IF_ERROR(scanner->open());

    while (true) {
        RETURN_IF_CANCELLED(_runtime_state);
        // If we have finished all works
        if (_scan_finished.load()) {
            return Status::OK();
        }

        auto res = scanner->get_next();
        if (!res.ok()) {
            return res.status();
        }
        ChunkPtr temp_chunk = std::move(res.value());

        // eval conjuncts
        size_t before = temp_chunk->num_rows();
        eval_conjuncts(conjunct_ctxs, temp_chunk.get());
        counter->num_rows_unselected += (before - temp_chunk->num_rows());

        // Row batch has been filled, push this to the queue
        if (temp_chunk->num_rows() > 0) {
            std::unique_lock<std::mutex> l(_chunk_queue_lock);
            while (_process_status.ok() && !_scan_finished.load() && !_runtime_state->is_cancelled() &&
                   // stop pushing more batch if
                   // 1. too many batches in queue, or
                   // 2. at least one batch in queue and memory exceed limit.
                   (_chunk_queue.size() >= _max_queue_size ||
                    (mem_tracker()->any_limit_exceeded() && !_chunk_queue.empty()))) {
                _queue_writer_cond.wait_for(l, std::chrono::seconds(1));
            }
            // Process already set failed, so we just return OK
            if (!_process_status.ok()) {
                return Status::OK();
            }
            // Scan already finished, just return
            if (_scan_finished.load()) {
                return Status::OK();
            }
            // Runtime state is canceled, just return cancel
            if (_runtime_state->is_cancelled()) {
                return Status::Cancelled("Cancelled FileScanNode::scanner_scan");
            }
            // Queue size Must be smaller than _max_queue_size
            _chunk_queue.push_back(std::move(temp_chunk));
            mem_tracker()->consume(_chunk_queue.back()->memory_usage());

            // Notify reader to
            _queue_reader_cond.notify_one();
        }
    }

    return Status::OK();
}

void FileScanNode::scanner_worker(int start_idx, int length) {
    // Clone expr context
    std::vector<ExprContext*> scanner_expr_ctxs;
    auto status = Expr::clone_if_not_exists(_conjunct_ctxs, _runtime_state, &scanner_expr_ctxs);
    if (!status.ok()) {
        LOG(WARNING) << "Clone conjuncts failed.";
    } else {
        ScannerCounter counter;
        for (int i = 0; i < length; ++i) {
            const TBrokerScanRange& scan_range = _scan_ranges[start_idx + i].scan_range.broker_scan_range;

            // remove range desc with empty file
            TBrokerScanRange new_scan_range(scan_range);
            new_scan_range.ranges.clear();
            for (const TBrokerRangeDesc& range_desc : scan_range.ranges) {
                // file_size is optional, and is not set in stream load and routine load,
                // so we should check file size is set firstly.
                if (range_desc.__isset.file_size && range_desc.file_size == 0) {
                    continue;
                }
                new_scan_range.ranges.emplace_back(range_desc);
            }
            status = scanner_scan(new_scan_range, scanner_expr_ctxs, &counter);

            // todo: break if failed ?
            if (!status.ok() && !status.is_end_of_file()) {
                LOG(WARNING) << "FileScanner[" << start_idx + i
                             << "] process failed. status=" << status.get_error_msg();
                break;
            }
        }

        // Update stats
        _runtime_state->update_num_rows_load_filtered(counter.num_rows_filtered);
        _runtime_state->update_num_rows_load_unselected(counter.num_rows_unselected);

        COUNTER_UPDATE(_scanner_total_timer, counter.total_ns);
        COUNTER_UPDATE(_scanner_fill_timer, counter.fill_ns);
        COUNTER_UPDATE(_scanner_read_timer, counter.read_batch_ns);
        COUNTER_UPDATE(_scanner_cast_chunk_timer, counter.cast_chunk_ns);
        COUNTER_UPDATE(_scanner_materialize_timer, counter.materialize_ns);
        COUNTER_UPDATE(_scanner_init_chunk_timer, counter.init_chunk_ns);

        COUNTER_UPDATE(_scanner_file_reader_timer, counter.file_read_ns);
    }

    // scanner is going to finish
    {
        std::lock_guard<std::mutex> l(_chunk_queue_lock);
        if (!status.ok() && !status.is_end_of_file()) {
            update_status(status);
        }
        // This scanner will finish
        _num_running_scanners--;
    }
    _queue_reader_cond.notify_all();
    // If one scanner failed, others don't need scan any more
    if (!status.ok() && !status.is_end_of_file()) {
        _queue_writer_cond.notify_all();
    }
    Expr::close(scanner_expr_ctxs, _runtime_state);
}

} // namespace starrocks::vectorized
