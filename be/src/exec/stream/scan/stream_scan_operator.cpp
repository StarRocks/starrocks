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

StreamScanOperatorFactory::StreamScanOperatorFactory(int32_t id, ScanNode* scan_node, size_t dop,
                                                     ChunkBufferLimiterPtr buffer_limiter, bool is_stream_pipeline)
        : ConnectorScanOperatorFactory(id, scan_node, dop, std::move(buffer_limiter)),
          _is_stream_pipeline(is_stream_pipeline) {}

OperatorPtr StreamScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<StreamScanOperator>(this, _id, driver_sequence, dop, _scan_node, _is_stream_pipeline);
}

StreamScanOperator::StreamScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                                       ScanNode* scan_node, bool is_stream_pipeline)
        : ConnectorScanOperator(factory, id, driver_sequence, dop, scan_node, is_stream_pipeline) {}

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
    }
}

void StreamScanOperator::_reset_chunk_source(RuntimeState* state, int chunk_source_index) {
    auto tablet_id = _chunk_sources[chunk_source_index]->get_lane_owner();
    auto binlog_offset =
            _stream_epoch_manager->get_binlog_offset(state->fragment_instance_id(), _id, tablet_id);

    DCHECK(binlog_offset != nullptr);
    _chunk_sources[chunk_source_index]->set_stream_offset(binlog_offset->tablet_version, binlog_offset->lsn);
    _chunk_sources[chunk_source_index]->set_epoch_limit(_current_epoch_info.max_scan_rows, _current_epoch_info.max_exec_millis);
    _chunk_sources[chunk_source_index]->reset_status();
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
    if (_lane_arbiter != nullptr) {
        while (morsel != nullptr) {
            auto [lane_owner, version] = morsel->get_lane_owner_and_version();
            auto acquire_result = _lane_arbiter->try_acquire_lane(lane_owner);
            if (acquire_result == query_cache::AR_BUSY) {
                _morsel_queue->unget(std::move(morsel));
                return Status::OK();
            } else if (acquire_result == query_cache::AR_PROBE) {
                auto hit = _cache_operator->probe_cache(lane_owner, version);
                RETURN_IF_ERROR(_cache_operator->reset_lane(state, lane_owner));
                if (!hit) {
                    break;
                }
                auto [delta_version, delta_rowsets] = _cache_operator->delta_version_and_rowsets(lane_owner);
                if (!delta_rowsets.empty()) {
                    // We must reset rowsets of Morsel to captured delta rowsets, because TabletReader now
                    // created from rowsets passed in to itself instead of capturing it from TabletManager again.
                    morsel->set_from_version(delta_version);
                    morsel->set_rowsets(delta_rowsets);
                    break;
                } else {
                    ASSIGN_OR_RETURN(morsel, _morsel_queue->try_get());
                }
            } else if (acquire_result == query_cache::AR_SKIP) {
                ASSIGN_OR_RETURN(morsel, _morsel_queue->try_get());
            } else if (acquire_result == query_cache::AR_IO) {
                // When both intra-tablet parallelism and multi-version cache mechanisms take effects, we must
                // use delta rowsets instead of the ensemble of rowsets to fetch rows from disk for all of the
                // morsels originated from the identical tablet.
                auto [delta_verrsion, delta_rowsets] = _cache_operator->delta_version_and_rowsets(lane_owner);
                if (!delta_rowsets.empty()) {
                    morsel->set_from_version(delta_verrsion);
                    morsel->set_rowsets(delta_rowsets);
                }
                break;
            }
        }
    }
    if (morsel != nullptr) {
        COUNTER_UPDATE(_morsels_counter, 1);

        _chunk_sources[chunk_source_index] = create_chunk_source(std::move(morsel), chunk_source_index);
        auto status = _chunk_sources[chunk_source_index]->prepare(state);
        if (!status.ok()) {
            _chunk_sources[chunk_source_index] = nullptr;
            set_finishing(state);
            return status;
        }
        need_detach = false;
        if (_is_stream_pipeline) {
            _reset_chunk_source(state, chunk_source_index);
        }

        RETURN_IF_ERROR(_trigger_next_scan(state, chunk_source_index));
    } else if (_is_stream_pipeline) {
        if (!_old_chunk_sources.empty()) {
            _chunk_sources[chunk_source_index] = _old_chunk_sources.front();
            _old_chunk_sources.pop();
            _reset_chunk_source(state, chunk_source_index);

            need_detach = false;
            RETURN_IF_ERROR(_trigger_next_scan(state, chunk_source_index));
        }
    }

    return Status::OK();
}

ChunkSourcePtr StreamScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    auto* scan_node = down_cast<ConnectorScanNode*>(_scan_node);
    auto* factory = down_cast<StreamScanOperatorFactory*>(_factory);
    return std::make_shared<StreamChunkSource>(_driver_sequence, _chunk_source_profiles[chunk_source_index].get(),
                                               std::move(morsel), this, scan_node, factory->get_chunk_buffer());
}

bool StreamScanOperator::is_finished() const {
    if (_is_stream_pipeline) {
        return _stream_epoch_manager && _stream_epoch_manager->is_finished();
    }
    return ConnectorScanOperator::is_finished();
}

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
        chunk_source_ptr->reset_status();
        _old_chunk_sources.push(chunk_source_ptr);

        auto tablet_id = chunk_source_ptr->get_lane_owner();
        auto binlog_offset = _stream_epoch_manager->get_binlog_offset(state->fragment_instance_id(), _id, tablet_id);

        DCHECK(binlog_offset != nullptr);
        chunk_source_ptr->set_stream_offset(binlog_offset->tablet_version, binlog_offset->lsn);
        chunk_source_ptr->set_epoch_limit(_current_epoch_info.max_scan_rows, _current_epoch_info.max_exec_millis);
    }

    return Status::OK();
}

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
        _stream_epoch_manager->update_binlog_offset(state->fragment_instance_id(), _id, 0, binlog_offset);
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

StreamChunkSource::StreamChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                                     ScanOperator* op, ConnectorScanNode* scan_node, BalancedChunkBuffer& chunk_buffer)
        : ConnectorChunkSource(scan_operator_id, runtime_profile, std::move(morsel), op, scan_node, chunk_buffer) {}

} // namespace starrocks::pipeline