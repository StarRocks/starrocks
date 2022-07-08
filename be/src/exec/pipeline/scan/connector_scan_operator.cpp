// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/connector_scan_operator.h"

#include "column/chunk.h"
#include "exec/vectorized/connector_scan_node.h"
#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// ==================== ConnectorScanOperatorFactory ====================

ConnectorScanOperatorFactory::ConnectorScanOperatorFactory(int32_t id, ScanNode* scan_node)
        : ScanOperatorFactory(id, scan_node) {}

Status ConnectorScanOperatorFactory::do_prepare(RuntimeState* state) {
    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    RETURN_IF_ERROR(Expr::prepare(conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(conjunct_ctxs, state));

    return Status::OK();
}

void ConnectorScanOperatorFactory::do_close(RuntimeState* state) {
    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    Expr::close(conjunct_ctxs, state);
}

OperatorPtr ConnectorScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<ConnectorScanOperator>(this, _id, driver_sequence, _scan_node, _num_committed_scan_tasks);
}

// ==================== ConnectorScanOperator ====================

ConnectorScanOperator::ConnectorScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence,
                                             ScanNode* scan_node, std::atomic<int>& num_committed_scan_tasks)
        : ScanOperator(factory, id, driver_sequence, scan_node, num_committed_scan_tasks) {}

Status ConnectorScanOperator::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void ConnectorScanOperator::do_close(RuntimeState* state) {}

ChunkSourcePtr ConnectorScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    auto* scan_node = down_cast<vectorized::ConnectorScanNode*>(_scan_node);
    return std::make_shared<ConnectorChunkSource>(_chunk_source_profiles[chunk_source_index].get(), std::move(morsel),
                                                  this, scan_node);
}

// ==================== ConnectorChunkSource ====================
ConnectorChunkSource::ConnectorChunkSource(RuntimeProfile* runtime_profile, MorselPtr&& morsel, ScanOperator* op,
                                           vectorized::ConnectorScanNode* scan_node)
        : ChunkSource(runtime_profile, std::move(morsel)),
          _scan_node(scan_node),
          _limit(scan_node->limit()),
          _runtime_in_filters(op->runtime_in_filters()),
          _runtime_bloom_filters(op->runtime_bloom_filters()) {
    _conjunct_ctxs = scan_node->conjunct_ctxs();
    _conjunct_ctxs.insert(_conjunct_ctxs.end(), _runtime_in_filters.begin(), _runtime_in_filters.end());
    ScanMorsel* scan_morsel = (ScanMorsel*)_morsel.get();
    const TScanRange* scan_range = scan_morsel->get_scan_range();
    _data_source = scan_node->data_source_provider()->create_data_source(*scan_range);
    _data_source->set_predicates(_conjunct_ctxs);
    _data_source->set_runtime_filters(_runtime_bloom_filters);
    _data_source->set_read_limit(_limit);
    _data_source->set_runtime_profile(runtime_profile);
}

ConnectorChunkSource::~ConnectorChunkSource() {
    if (_runtime_state != nullptr) {
        close(_runtime_state);
    }
}

Status ConnectorChunkSource::prepare(RuntimeState* state) {
    _runtime_state = state;
    return Status::OK();
}

void ConnectorChunkSource::close(RuntimeState* state) {
    if (_closed) return;
    _closed = true;
    _data_source->close(state);
    _chunk_buffer.shutdown();
    _chunk_buffer.clear();
}

bool ConnectorChunkSource::has_next_chunk() const {
    // If we need and could get next chunk from storage engine,
    // the _status must be ok.
    return _status.ok();
}

bool ConnectorChunkSource::has_output() const {
    return !_chunk_buffer.empty();
}

size_t ConnectorChunkSource::get_buffer_size() const {
    return _chunk_buffer.get_size();
}

StatusOr<vectorized::ChunkPtr> ConnectorChunkSource::get_next_chunk_from_buffer() {
    vectorized::ChunkPtr chunk = nullptr;
    _chunk_buffer.try_get(&chunk);
    return chunk;
}

Status ConnectorChunkSource::buffer_next_batch_chunks_blocking(size_t batch_size, RuntimeState* state) {
    if (!_status.ok()) {
        return _status;
    }

    for (size_t i = 0; i < batch_size && !state->is_cancelled(); ++i) {
        vectorized::ChunkPtr chunk;
        _status = _read_chunk(&chunk);
        if (!_status.ok()) {
            // end of file is normal case, need process chunk
            if (_status.is_end_of_file()) {
                _chunk_buffer.put(std::move(chunk));
            }
            break;
        }
        _chunk_buffer.put(std::move(chunk));
    }
    return _status;
}
Status ConnectorChunkSource::buffer_next_batch_chunks_blocking_for_workgroup(size_t batch_size, RuntimeState* state,
                                                                             size_t* num_read_chunks, int worker_id,
                                                                             workgroup::WorkGroupPtr running_wg) {
    if (!_status.ok()) {
        return _status;
    }

    int64_t time_spent = 0;
    for (size_t i = 0; i < batch_size && !state->is_cancelled(); ++i) {
        {
            SCOPED_RAW_TIMER(&time_spent);

            vectorized::ChunkPtr chunk;
            _status = _read_chunk(&chunk);
            if (!_status.ok()) {
                // end of file is normal case, need process chunk
                if (_status.is_end_of_file()) {
                    ++(*num_read_chunks);
                    _chunk_buffer.put(std::move(chunk));
                }
                break;
            }

            ++(*num_read_chunks);
            _chunk_buffer.put(std::move(chunk));
        }

        if (time_spent >= YIELD_MAX_TIME_SPENT) {
            break;
        }

        if (time_spent >= YIELD_PREEMPT_MAX_TIME_SPENT &&
            workgroup::WorkGroupManager::instance()->get_owners_of_scan_worker(workgroup::TypeHdfsScanExecutor,
                                                                               worker_id, running_wg)) {
            break;
        }
    }

    return _status;
}

Status ConnectorChunkSource::_read_chunk(vectorized::ChunkPtr* chunk) {
    RuntimeState* state = _runtime_state;
    if (!_opened) {
        RETURN_IF_ERROR(_data_source->open(state));
        _opened = true;
    }

    if (state->is_cancelled()) {
        return Status::Cancelled("canceled state");
    }

    // Improve for select * from table limit x, x is small
    if (_limit != -1 && _rows_read >= _limit) {
        return Status::EndOfFile("limit reach");
    }

    do {
        RETURN_IF_ERROR(_data_source->get_next(state, chunk));
    } while ((*chunk)->num_rows() == 0);

    _rows_read += (*chunk)->num_rows();
    _scan_rows_num = _data_source->raw_rows_read();
    _scan_bytes = _data_source->num_bytes_read();

    return Status::OK();
}

} // namespace starrocks::pipeline
