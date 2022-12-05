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


namespace starrocks::pipeline {


StreamScanOperator::StreamScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                                             ScanNode* scan_node)
        : ConnectorScanOperator(factory, id, driver_sequence, dop, scan_node) {}

void StreamScanOperator::star_epoch(const EpochInfo& epoch) {
    is_epoch_start = true;
    _curren_epoch = epoch;
    // reset chunkSource status
    // swap chunk queue
    for (int i = 0; i < _closed_chunk_sources.size(); i++) {
        ChunkSourcePtr chunk_source_ptr = _closed_chunk_sources.front();
        _closed_chunk_sources.pop();
        _closed_chunk_sources->reset_status();
        _closed_chunk_sources->set_limit(epoch.max_rows);
        _closed_chunk_sources->set_time_limit(epoch.max_time);
        _old_chunk_sources.push(chunk_source_ptr)
    }
}

bool StreamScanOperator::is_epoch_finished() {
    if (_is_epoch_finished) {
        return true;
    }
    // if storage layer returns an error, we should make sure `pull_chunk` has a chance to get it
    if (!_get_scan_status().ok()) {
        return false;
    }

    // Any io task is running or needs to run.
    if (_num_running_io_tasks > 0 || !_morsel_queue->empty()) {
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

    // This scan operator is finished, if no more io tasks are running
    // or need to run, and all the read chunks are consumed.
    return true;
}

Status StreamScanOperator::set_epoch_finishing(RuntimeState* state) {
    std::lock_guard guard(_task_mutex);
    _is_finished = true;
    return Status::OK();
}

CommitOffset StreamScanOperator::get_latest_offset() {
    // todo traverse all _old_chunks to get_latest_offset through binlog reader
    CommitOffset offset;
    return offset;
}

bool StreamScanOperator::is_finished() {
    return false;
}

bool StreamScanOperatior::has_output() {
    if (_is_finished || _is_epoch_finished) {
        return false;
    }

    if (_is_epoch_start) {
        _is_epoch_start = false;
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
        COUNTER_UPDATE(_buffer_unplug_counter, 1);
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
    if (!_morsel_queue->empty() || !_old_chunk_source.empty()) {
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

void StreamScanOperator::_close_chunk_source(RuntimeState* state, int chunk_source_index) {
    std::lock_guard guard(_task_mutex);

    _close_chunk_source_unlocked(state, chunk_source_index);
}

void StreamScanOperator::_close_chunk_source_unlocked(RuntimeState* state, int chunk_source_index) {
    if (_chunk_sources[chunk_source_index] != nullptr) {
        //_chunk_sources[chunk_source_index]->close(state);
        _closed_chunk_sources.push(_chunk_sources[chunk_source_index]);
        _chunk_sources[chunk_source_index] = nullptr;
        detach_chunk_source(chunk_source_index);
    }
}

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
        _chunk_sources[chunk_source_index]->set_stream_offset(_current_epoch.table_version, _current_epoch.get_start_offset);
        _chunk_sources[chunk_source_index]->set_epoch_limit(_current_epoch.max_batch_rows, _current_epoch.max_time);
        if (!status.ok()) {
            _chunk_sources[chunk_source_index] = nullptr;
            set_finishing(state);
            return status;
        }
        need_detach = false;
        RETURN_IF_ERROR(_trigger_next_scan(state, chunk_source_index));
    } else {
        _chunk_sources[chunk_source_index] = _old_chunk_sources.front();
        if (_chunk_sources[chunk_source_index] != null) {
            _old_chunk_sources.pop();
            _chunk_sources[chunk_source_index]->set_stream_offset(_current_epoch.table_version, _current_epoch.get_start_offset);
            _chunk_sources[chunk_source_index]->set_epoch_limit(_current_epoch.max_batch_rows, _current_epoch.max_time);
        }
    }

    return Status::OK();
}



} // namespace starrocks::pipeline
