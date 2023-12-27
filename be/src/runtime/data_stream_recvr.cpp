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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/data_stream_recvr.cc

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/data_stream_recvr.h"

#include <util/time.h>

#include <condition_variable>
#include <deque>
#include <utility>

#include "column/chunk.h"
#include "exec/sort_exec_exprs.h"
#include "gen_cpp/data.pb.h"
#include "runtime/chunk_cursor.h"
#include "runtime/current_thread.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/sender_queue.h"
#include "runtime/sorted_chunks_merger.h"
#include "util/compression/block_compression.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
#include "util/faststring.h"
#include "util/logging.h"
#include "util/phmap/phmap.h"
#include "util/runtime_profile.h"

using std::list;
using std::vector;
using std::pair;
using std::make_pair;

namespace starrocks {

Status DataStreamRecvr::create_merger(RuntimeState* state, RuntimeProfile* profile, const SortExecExprs* exprs,
                                      const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first) {
    DCHECK(_is_merging);
    _chunks_merger = std::make_unique<SortedChunksMerger>(state, _keep_order);
    ChunkSuppliers chunk_suppliers;
    for (SenderQueue* q : _sender_queues) {
        // we use chunk_supplier in non-pipeline.
        auto f = [q](Chunk** chunk) -> Status { return q->get_chunk(chunk); };
        chunk_suppliers.emplace_back(std::move(f));
    }
    ChunkProbeSuppliers chunk_probe_suppliers;
    for ([[maybe_unused]] auto _ : _sender_queues) {
        // we willn't use chunk_probe_supplier in non-pipeline.
        auto f = [](Chunk** chunk) -> bool { return false; };
        chunk_probe_suppliers.emplace_back(std::move(f));
    }
    ChunkHasSuppliers chunk_has_suppliers;
    for ([[maybe_unused]] auto _ : _sender_queues) {
        // we willn't use chunk_has_supplier in non-pipeline.
        auto f = []() -> bool { return false; };
        chunk_has_suppliers.emplace_back(std::move(f));
    }

    RETURN_IF_ERROR(_chunks_merger->init(chunk_suppliers, chunk_probe_suppliers, chunk_has_suppliers,
                                         &(exprs->lhs_ordering_expr_ctxs()), is_asc, is_null_first));
    _chunks_merger->set_profile(profile);
    return Status::OK();
}

Status DataStreamRecvr::create_merger_for_pipeline(RuntimeState* state, const SortExecExprs* exprs,
                                                   const std::vector<bool>* is_asc,
                                                   const std::vector<bool>* is_null_first) {
    DCHECK(_is_merging);
    _chunks_merger = nullptr;
    if (exprs->is_constant_lhs_ordering()) {
        _cascade_merger = std::make_unique<ConstChunkMerger>(state);
    } else {
        _cascade_merger = std::make_unique<CascadeChunkMerger>(state);
    }

    std::vector<ChunkProvider> providers;
    for (SenderQueue* q : _sender_queues) {
        ChunkProvider provider = [q](ChunkUniquePtr* out_chunk, bool* eos) -> bool {
            // data ready
            if (out_chunk == nullptr || eos == nullptr) {
                return q->has_chunk();
            }
            if (!q->has_chunk()) {
                return false;
            }
            Chunk* chunk;
            if (q->try_get_chunk(&chunk)) {
                out_chunk->reset(chunk);
                return true;
            }
            *eos = true;
            return false;
        };
        providers.push_back(std::move(provider));
    }
    RETURN_IF_ERROR(_cascade_merger->init(providers, &(exprs->lhs_ordering_expr_ctxs()), is_asc, is_null_first));
    return Status::OK();
}

std::vector<merge_path::MergePathChunkProvider> DataStreamRecvr::create_merge_path_chunk_providers() {
    DCHECK(_is_merging);
    std::vector<merge_path::MergePathChunkProvider> chunk_providers;
    for (SenderQueue* q : _sender_queues) {
        chunk_providers.emplace_back([q](bool only_check_if_has_data, ChunkPtr* chunk, bool* eos) {
            if (!q->has_chunk()) {
                return false;
            }

            if (!only_check_if_has_data) {
                Chunk* chunk_ptr;
                if (q->try_get_chunk(&chunk_ptr)) {
                    chunk->reset(chunk_ptr);
                } else {
                    *eos = true;
                }
            }
            return true;
        });
    }
    return chunk_providers;
}

DataStreamRecvr::DataStreamRecvr(DataStreamMgr* stream_mgr, RuntimeState* runtime_state, const RowDescriptor& row_desc,
                                 const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
                                 bool is_merging, int total_buffer_limit,
                                 std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr,
                                 bool is_pipeline, int32_t degree_of_parallelism, bool keep_order,
                                 PassThroughChunkBuffer* pass_through_chunk_buffer)
        : _mgr(stream_mgr),
          _fragment_instance_id(fragment_instance_id),
          _dest_node_id(dest_node_id),
          _total_buffer_limit(total_buffer_limit),
          _row_desc(row_desc),
          _is_merging(is_merging),
          _num_buffered_bytes(0),
          _instance_profile(runtime_state->runtime_profile_ptr()),
          _query_mem_tracker(runtime_state->query_mem_tracker_ptr()),
          _instance_mem_tracker(runtime_state->instance_mem_tracker_ptr()),
          _sub_plan_query_statistics_recvr(std::move(sub_plan_query_statistics_recvr)),
          _is_pipeline(is_pipeline),
          _keep_order(keep_order),
          _pass_through_context(pass_through_chunk_buffer, fragment_instance_id, dest_node_id) {
    // Create one queue per sender if is_merging is true.
    int num_queues = is_merging ? num_senders : 1;
    _sender_queues.reserve(num_queues);
    int num_sender_per_queue = is_merging ? 1 : num_senders;
    DCHECK_GE(degree_of_parallelism, 1);
    for (int i = 0; i < num_queues; ++i) {
        SenderQueue* queue = nullptr;
        if (_is_pipeline) {
            queue = _sender_queue_pool.add(
                    new PipelineSenderQueue(this, num_sender_per_queue, _is_merging ? 1 : degree_of_parallelism));
        } else {
            queue = _sender_queue_pool.add(new NonPipelineSenderQueue(this, num_sender_per_queue));
        }
        _sender_queues.push_back(queue);
    }

    _metrics.resize(degree_of_parallelism);

    _pass_through_context.init();
    if (runtime_state->query_options().__isset.transmission_encode_level) {
        _encode_level = runtime_state->query_options().transmission_encode_level;
    }
}

void DataStreamRecvr::bind_profile(int32_t driver_sequence, const std::shared_ptr<RuntimeProfile>& profile) {
    DCHECK(profile != nullptr);
    DCHECK_GE(driver_sequence, 0);
    DCHECK_LT(driver_sequence, _metrics.size());
    auto& statistics = _metrics[driver_sequence];

    statistics.runtime_profile = profile;
    statistics.bytes_received_counter = ADD_COUNTER(profile, "BytesReceived", TUnit::BYTES);
    statistics.bytes_pass_through_counter = ADD_COUNTER(profile, "BytesPassThrough", TUnit::BYTES);
    statistics.request_received_counter = ADD_COUNTER(profile, "RequestReceived", TUnit::UNIT);
    statistics.closure_block_timer = ADD_TIMER(profile, "ClosureBlockTime");
    statistics.closure_block_counter = ADD_COUNTER(profile, "ClosureBlockCount", TUnit::UNIT);
    statistics.deserialize_chunk_timer = ADD_TIMER(profile, "DeserializeChunkTime");
    statistics.decompress_chunk_timer = ADD_TIMER(profile, "DecompressChunkTime");
    statistics.process_total_timer = ADD_TIMER(profile, "ReceiverProcessTotalTime");
    statistics.wait_lock_timer = ADD_TIMER(profile, "WaitLockTime");
    statistics.buffer_unplug_counter = ADD_COUNTER(profile, "BufferUnplugCount", TUnit::UNIT);
    statistics.peak_buffer_mem_bytes = profile->AddHighWaterMarkCounter(
            "PeakBufferMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
}

Status DataStreamRecvr::get_next(ChunkPtr* chunk, bool* eos) {
    DCHECK(_chunks_merger.get() != nullptr);
    return _chunks_merger->get_next(chunk, eos);
}

Status DataStreamRecvr::get_next_for_pipeline(ChunkPtr* chunk, std::atomic<bool>* eos, bool* should_exit) {
    DCHECK(_cascade_merger);
    ChunkUniquePtr chunk_ptr;
    RETURN_IF_ERROR(_cascade_merger->get_next(&chunk_ptr, eos, should_exit));
    *chunk = std::move(chunk_ptr);
    return Status::OK();
}

bool DataStreamRecvr::is_data_ready() {
    if (_chunks_merger) {
        return _chunks_merger->is_data_ready();
    } else {
        return _cascade_merger->is_data_ready();
    }
}

Status DataStreamRecvr::add_chunks(const PTransmitChunkParams& request, ::google::protobuf::Closure** done) {
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_instance_mem_tracker.get());
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    auto& metrics = get_metrics_round_robin();
    SCOPED_TIMER(metrics.process_total_timer);
    COUNTER_UPDATE(metrics.request_received_counter, 1);
    int use_sender_id = _is_merging ? request.sender_id() : 0;
    // Add all batches to the same queue if _is_merging is false.

    if (_keep_order) {
        DCHECK(_is_pipeline);
        return _sender_queues[use_sender_id]->add_chunks_and_keep_order(request, metrics, done);
    } else {
        return _sender_queues[use_sender_id]->add_chunks(request, metrics, done);
    }
}

void DataStreamRecvr::remove_sender(int sender_id, int be_number) {
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->decrement_senders(be_number);
}

void DataStreamRecvr::cancel_stream() {
    for (auto& _sender_queue : _sender_queues) {
        _sender_queue->cancel();
    }
}

void DataStreamRecvr::close() {
    if (_closed) {
        return;
    }

    for (auto& _sender_queue : _sender_queues) {
        _sender_queue->close();
    }
    // Remove this receiver from the DataStreamMgr that created it.
    // TODO: log error msg
    _mgr->deregister_recvr(fragment_instance_id(), dest_node_id());
    _mgr = nullptr;
    _chunks_merger.reset();
    _cascade_merger.reset();
    _closed = true;
}

DataStreamRecvr::~DataStreamRecvr() {
    if (!_closed) {
        close();
    }
    DCHECK(_mgr == nullptr) << "Must call close()";
}

Status DataStreamRecvr::get_chunk(std::unique_ptr<Chunk>* chunk) {
    DCHECK(!_is_merging);
    DCHECK_EQ(_sender_queues.size(), 1);
    Chunk* tmp_chunk = nullptr;
    Status status = _sender_queues[0]->get_chunk(&tmp_chunk);
    chunk->reset(tmp_chunk);
    return status;
}

Status DataStreamRecvr::get_chunk_for_pipeline(std::unique_ptr<Chunk>* chunk, const int32_t driver_sequence) {
    DCHECK(!_is_merging);
    DCHECK_EQ(_sender_queues.size(), 1);
    Chunk* tmp_chunk = nullptr;
    Status status = _sender_queues[0]->get_chunk(&tmp_chunk, driver_sequence);
    chunk->reset(tmp_chunk);
    return status;
}

void DataStreamRecvr::short_circuit_for_pipeline(const int32_t driver_sequence) {
    DCHECK(_is_pipeline);
    auto* sender_queue = static_cast<PipelineSenderQueue*>(_sender_queues[0]);
    return sender_queue->short_circuit(driver_sequence);
}

bool DataStreamRecvr::has_output_for_pipeline(const int32_t driver_sequence) const {
    DCHECK(!_is_merging);
    DCHECK(_is_pipeline);
    auto* sender_queue = static_cast<PipelineSenderQueue*>(_sender_queues[0]);
    return sender_queue->has_output(driver_sequence);
}

bool DataStreamRecvr::is_finished() const {
    DCHECK(!_is_merging);
    DCHECK(_is_pipeline);
    auto* sender_queue = static_cast<PipelineSenderQueue*>(_sender_queues[0]);
    return sender_queue->is_finished();
}

} // namespace starrocks
