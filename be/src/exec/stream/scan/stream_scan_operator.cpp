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

#include "stream_scan_operator.h"

#include "exec/connector_scan_node.h"

namespace starrocks::pipeline {

StreamScanOperatorFactory::StreamScanOperatorFactory(int32_t id, ScanNode* scan_node, RuntimeState* state, size_t dop,
                                                     ChunkBufferLimiterPtr buffer_limiter, bool is_stream_pipeline)
        : ConnectorScanOperatorFactory(id, scan_node, state, dop, std::move(buffer_limiter)),
          _is_stream_pipeline(is_stream_pipeline) {}

OperatorPtr StreamScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<StreamScanOperator>(this, _id, driver_sequence, dop, _scan_node, _is_stream_pipeline);
}

StreamScanOperator::StreamScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                                       ScanNode* scan_node, bool is_stream_pipeline)
        : ConnectorScanOperator(factory, id, driver_sequence, dop, scan_node),
          _is_stream_pipeline(is_stream_pipeline) {}

StreamScanOperator::~StreamScanOperator() {
    auto* state = runtime_state();
    if (state == nullptr) {
        return;
    }

    while (!_closed_chunk_sources.empty()) {
        ChunkSourcePtr chunk_source_ptr = _closed_chunk_sources.front();
        _closed_chunk_sources.pop();
        std::lock_guard guard(_task_mutex);
        chunk_source_ptr->close(state);
        chunk_source_ptr = nullptr;
    }

    // defensive programming,_chunk_sources should have been closed normally
    for (size_t i = 0; i < _chunk_sources.size(); i++) {
        if (_chunk_sources[i] != nullptr) {
            _chunk_sources[i]->close(state);
            _chunk_sources[i] = nullptr;
            detach_chunk_source(i);
        }
    }
}

Status StreamScanOperator::_reset_chunk_source(RuntimeState* state, int chunk_source_index) {
    //auto tablet_id = _chunk_sources[chunk_source_index]->get_lane_owner();
    StreamChunkSource* chunk_source = down_cast<StreamChunkSource*>(_chunk_sources[chunk_source_index].get());
    auto tablet_id = chunk_source->get_lane_owner();
    auto binlog_offset = _stream_epoch_manager->get_binlog_offset(state->fragment_instance_id(), _id, tablet_id);

    DCHECK(binlog_offset != nullptr);
    RETURN_IF_ERROR(chunk_source->set_stream_offset(binlog_offset->tablet_version, binlog_offset->lsn));
    chunk_source->set_epoch_limit(_current_epoch_info.max_scan_rows, _current_epoch_info.max_exec_millis);
    return chunk_source->reset_status();
}

// In order to not modify the code of ScanOperator,
// there is lots of repetition with the method of _pickup_morsel in ScanOperator,
// maybe can abstract a method contains the same code later
Status StreamScanOperator::_pickup_morsel(RuntimeState* state, int chunk_source_index) {
    DCHECK(_morsel_queue != nullptr);
    _close_chunk_source(state, chunk_source_index);

    // NOTE: attach an active source before really creating it, to avoid the race condition
    bool need_detach = true;
    attach_chunk_source(chunk_source_index);
    DeferOp defer([&]() {
        if (need_detach) {
            detach_chunk_source(chunk_source_index);
        }
    });

    ASSIGN_OR_RETURN(auto morsel, _morsel_queue->try_get());
    if (morsel != nullptr) {
        COUNTER_UPDATE(_morsels_counter, 1);

        _chunk_sources[chunk_source_index] = create_chunk_source(std::move(morsel), chunk_source_index);
        auto status = _chunk_sources[chunk_source_index]->prepare(state);
        if (!status.ok()) {
            _chunk_sources[chunk_source_index] = nullptr;
            static_cast<void>(set_finishing(state));
            return status;
        }
        need_detach = false;
        if (_is_stream_pipeline) {
            RETURN_IF_ERROR(_reset_chunk_source(state, chunk_source_index));
        }

        RETURN_IF_ERROR(_trigger_next_scan(state, chunk_source_index));
    } else if (_is_stream_pipeline) {
        if (!_old_chunk_sources.empty()) {
            _chunk_sources[chunk_source_index] = _old_chunk_sources.front();
            _old_chunk_sources.pop();
            RETURN_IF_ERROR(_reset_chunk_source(state, chunk_source_index));

            need_detach = false;
            RETURN_IF_ERROR(_trigger_next_scan(state, chunk_source_index));
        }
    }

    return Status::OK();
}

ChunkSourcePtr StreamScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    auto* scan_node = down_cast<ConnectorScanNode*>(_scan_node);
    auto* factory = down_cast<StreamScanOperatorFactory*>(_factory);
    return std::make_shared<StreamChunkSource>(this, _chunk_source_profiles[chunk_source_index].get(),
                                               std::move(morsel), scan_node, factory->get_chunk_buffer());
}

bool StreamScanOperator::is_finished() const {
    if (_is_stream_pipeline) {
        return _stream_epoch_manager && _stream_epoch_manager->is_finished();
    }
    return ConnectorScanOperator::is_finished();
}

// In order to not modify the code of ScanOperator,
// there is lots of repetition with the method of has_output in ScanOperator,
// maybe can abstract a method contains the same code later
bool StreamScanOperator::has_output() const {
    if (_is_stream_pipeline) {
        if (_is_epoch_finished) {
            return false;
        }

        if (_is_epoch_start) {
            return true;
        }
        // if storage layer returns an error, we should make sure `pull_chunk` has a chance to get it
        if (!_get_scan_status().ok()) {
            return true;
        }

        // Try to buffer enough chunks for exec thread, to reduce scheduling overhead.
        // It's like the Linux Block-Scheduler's Unplug algorithm, so we just name it unplug.
        // The default threshould of unpluging is BufferCapacity/DOP/4, and its range is [1, 16]
        // The overall strategy:
        // 1. If enough buffered chunks: pull_chunk, so return true
        // 2. If not enough buffered chunks (plug mode):
        //   2.1 Buffer full: return true if has any chunk, else false
        //   2.2 Enough running io-tasks: wait some time for more chunks, so return false
        //   2.3 Not enough running io-tasks: submit io tasks, so return true
        //   2.4 No more tasks: pull_chunk, so return true

        size_t chunk_number = num_buffered_chunks();
        if (_unpluging) {
            if (chunk_number > 0) {
                return true;
            }
            _unpluging = false;
        }
        if (chunk_number >= _buffer_unplug_threshold()) {
            _unpluging = true;
            return true;
        }

        DCHECK(!_unpluging);
        bool buffer_full = is_buffer_full();
        if (buffer_full) {
            return chunk_number > 0;
        }
        if (_num_running_io_tasks >= _io_tasks_per_scan_operator) {
            return false;
        }
        // Can pick up more morsels or submit more tasks
        // support stream need to check _old_chunk_source
        if (!_morsel_queue->empty() || !_old_chunk_sources.empty()) {
            return true;
        }
        for (int i = 0; i < _io_tasks_per_scan_operator; ++i) {
            std::shared_lock guard(_task_mutex);
            if (_chunk_sources[i] != nullptr && !_is_io_task_running[i] && _chunk_sources[i]->has_next_chunk()) {
                return true;
            }
        }
        return num_buffered_chunks() > 0;
    }
    return ConnectorScanOperator::has_output();
}

Status StreamScanOperator::reset_epoch(RuntimeState* state) {
    DCHECK(_is_stream_pipeline);
    _is_epoch_finished = false;
    _is_finished = false;
    _run_time = 0;
    _chunk_num = 0;

    _is_epoch_start = true;
    _current_epoch_info = _stream_epoch_manager->epoch_info();

    // reset chunkSource status
    // swap chunk queue
    int chunk_source_size = _closed_chunk_sources.size();
    for (int i = 0; i < chunk_source_size; i++) {
        ChunkSourcePtr chunk_source_ptr = _closed_chunk_sources.front();
        _closed_chunk_sources.pop();
        StreamChunkSource* chunk_source = down_cast<StreamChunkSource*>(chunk_source_ptr.get());

        RETURN_IF_ERROR(chunk_source->reset_status());
        _old_chunk_sources.push(chunk_source_ptr);

        auto tablet_id = chunk_source->get_lane_owner();
        auto binlog_offset = _stream_epoch_manager->get_binlog_offset(state->fragment_instance_id(), _id, tablet_id);

        DCHECK(binlog_offset != nullptr);
        RETURN_IF_ERROR(chunk_source->set_stream_offset(binlog_offset->tablet_version, binlog_offset->lsn));
        chunk_source->set_epoch_limit(_current_epoch_info.max_scan_rows, _current_epoch_info.max_exec_millis);
    }

    return Status::OK();
}

// In order to not modify the code of ScanOperator,
// there is lots of repetition with the method of is_finished in ScanOperator,
// maybe can abstract a method contains the same code later
bool StreamScanOperator::is_epoch_finished() const {
    if (_is_epoch_finished) {
        return true;
    }
    // if storage layer returns an error, we should make sure `pull_chunk` has a chance to get it
    if (!_get_scan_status().ok()) {
        return false;
    }

    // Any io task is running or needs to run.
    if (_num_running_io_tasks > 0 || !_morsel_queue->empty() || !_old_chunk_sources.empty()) {
        return false;
    }

    // Any shared chunk source from other ScanOperator
    if (has_shared_chunk_source()) {
        return false;
    }

    // Remain some data in the buffer
    if (num_buffered_chunks() > 0) {
        return false;
    }

    // This scan operator is epoch finished, if no more io tasks are running
    // or need to run, and all the read chunks are consumed.
    return true;
}

Status StreamScanOperator::set_epoch_finished(RuntimeState* state) {
    // todo report every chunk source with real offset
    int chunk_source_size = _closed_chunk_sources.size();
    for (int i = 0; i < chunk_source_size; i++) {
        BinlogOffset binlog_offset{0, 2, 2};
        RETURN_IF_ERROR(
                _stream_epoch_manager->update_binlog_offset(state->fragment_instance_id(), _id, 0, binlog_offset));
    }
    return Status::OK();
}

StatusOr<ChunkPtr> StreamScanOperator::_mark_mock_data_finished() {
    if (_is_stream_pipeline) {
        _is_epoch_finished = true;
    } else {
        _is_finished = true;
    }
    return Status::EndOfFile(fmt::format(""));
}

StatusOr<ChunkPtr> StreamScanOperator::pull_chunk(RuntimeState* state) {
    _is_epoch_start = false;
    auto status_or = ConnectorScanOperator::pull_chunk(state);

    if (status_or.ok()) {
        auto chunk = status_or.value();
        if (chunk && chunk->num_rows() > 0) {
            int32_t num = _chunk_num.fetch_add(1, std::memory_order_relaxed);
            if (num >= 1) {
                return _mark_mock_data_finished();
            } else {
                return status_or;
            }
        }
    }

    _run_time++;
    if (_run_time > 100) {
        return _mark_mock_data_finished();
    }
    return status_or;
}

// In order to not modify the code of ScanOperator,
// there is lots of repetition with the method of _finish_chunk_source_task in scanOperator,
// maybe can abstract a method contains the same code later
void StreamScanOperator::_finish_chunk_source_task(RuntimeState* state, int chunk_source_index, int64_t cpu_time_ns,
                                                   int64_t scan_rows, int64_t scan_bytes) {
    _last_growth_cpu_time_ns += cpu_time_ns;
    _last_scan_rows_num += scan_rows;
    _last_scan_bytes += scan_bytes;
    _num_running_io_tasks--;

    DCHECK(_chunk_sources[chunk_source_index] != nullptr);
    {
        // - close() closes the chunk source which is not running.
        // - _finish_chunk_source_task() closes the chunk source conditionally and then make it as not running.
        // Therefore, closing chunk source and storing/loading `_is_finished` and `_is_io_task_running`
        // must be protected by lock
        std::lock_guard guard(_task_mutex);
        if (!_chunk_sources[chunk_source_index]->has_next_chunk() || _is_finished ||
            (_is_epoch_finished && _is_stream_pipeline)) {
            _close_chunk_source_unlocked(state, chunk_source_index);
        }
        _is_io_task_running[chunk_source_index] = false;
    }
}

void StreamScanOperator::_close_chunk_source_unlocked(RuntimeState* state, int chunk_source_index) {
    if (_chunk_sources[chunk_source_index] != nullptr) {
        _closed_chunk_sources.push(_chunk_sources[chunk_source_index]);
        _chunk_sources[chunk_source_index] = nullptr;
        detach_chunk_source(chunk_source_index);
    }
}

StreamChunkSource::StreamChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                                     ConnectorScanNode* scan_node, BalancedChunkBuffer& chunk_buffer)
        : ConnectorChunkSource(op, runtime_profile, std::move(morsel), scan_node, chunk_buffer) {}

Status StreamChunkSource::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ConnectorChunkSource::prepare(state));
    // open data source eagerly rather than delay until to read data so that
    // it can interact with stream pipeline engine normally, such as set_offset()
    RETURN_IF_ERROR(_open_data_source(state));
    return Status::OK();
}

Status StreamChunkSource::set_stream_offset(int64_t table_version, int64_t changelog_id) {
    return _get_stream_data_source()->set_offset(table_version, changelog_id);
}

void StreamChunkSource::set_epoch_limit(int64_t epoch_rows_limit, int64_t epoch_time_limit) {
    _epoch_rows_limit = epoch_rows_limit;
    _epoch_time_limit = epoch_time_limit;
}

Status StreamChunkSource::reset_status() {
    _status = Status::OK();
    return _get_stream_data_source()->reset_status();
}

bool StreamChunkSource::_reach_eof() const {
    connector::StreamDataSource* data_source = _get_stream_data_source();
    return (_epoch_rows_limit != -1 && data_source->num_rows_read_in_epoch() >= _epoch_rows_limit) ||
           (_epoch_time_limit != -1 && data_source->cpu_time_spent_in_epoch() >= _epoch_time_limit);
}

} // namespace starrocks::pipeline