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

#include <memory>
#include <queue>
#include <vector>

#include "bthread/mutex.h"
#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "exec/pipeline/fetch_sink_operator.h"
#include "exec/sorting/sort_permute.h"
#include "runtime/descriptors.h"
#include "util/bthreads/bthread_shared_mutex.h"
#include "util/phmap/phmap.h"
#include "util/raw_container.h"

namespace starrocks::pipeline {

class FetchOperator;

using RequestColumns = phmap::flat_hash_map<uint32_t, std::pair<ColumnPtr, ColumnPtr>>;
using RequestColumnsPtr = std::shared_ptr<RequestColumns>;

struct FetchContext {
    // row_id_slot => row_id, position
    RequestColumnsPtr request_columns;
    // slot id => column
    mutable phmap::flat_hash_map<SlotId, ColumnPtr> response_columns;
    int64_t send_ts = 0;
    uint32_t be_id;
};
using FetchContextPtr = std::shared_ptr<FetchContext>;

class FetchProcessor {
private:
    struct BatchUnit {
        std::vector<ChunkPtr> input_chunks;
        phmap::flat_hash_map<uint32_t, FetchContextPtr> fetch_ctxs;
        int32_t total_request_num = 0;
        std::atomic_int32_t finished_request_num = 0;
        size_t next_output_idx = 0;
        bool build_output_done = false;
        // null rows' position
        phmap::flat_hash_map<uint32_t, ColumnPtr> missing_positions;
        std::string debug_string() const;

        bool all_fetch_done() const { return total_request_num == finished_request_num; }

        bool reach_end() const { return next_output_idx >= input_chunks.size(); }
        ChunkPtr get_next_chunk() { return input_chunks[next_output_idx++]; }
    };
    using BatchUnitPtr = std::shared_ptr<BatchUnit>;

public:
    FetchProcessor(int32_t target_node_id, const phmap::flat_hash_map<TupleId, SlotId>& row_id_slots,
                   const phmap::flat_hash_map<SlotId, SlotDescriptor*>& slot_id_to_desc,
                   std::shared_ptr<StarRocksNodesInfo> nodes_info, std::shared_ptr<LookUpDispatcher> local_dispatcher)
            : _target_node_id(target_node_id),
              _row_id_slots(row_id_slots),
              _slot_id_to_desc(slot_id_to_desc),
              _nodes_info(std::move(nodes_info)),
              _local_dispatcher(std::move(local_dispatcher)) {
        _current_unit = std::make_shared<BatchUnit>();
    }

    ~FetchProcessor() = default;

    Status prepare(RuntimeState* state, RuntimeProfile* runtime_profile);

    void close() {}
    bool need_input() const;
    bool has_output() const;
    bool is_finished() const;

    Status set_sink_finishing(RuntimeState* state);

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk);
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state);

    bool is_sink_complete() const { return _is_sink_complete; }

private:
    Status _fetch_data(RuntimeState* state, BatchUnitPtr& unit);
    StatusOr<ChunkPtr> _build_row_id_chunk(RuntimeState* state, const BatchUnitPtr& unit);
    Status _gen_request_chunks(RuntimeState* state, const ChunkPtr& chunk,
                               phmap::flat_hash_map<uint32_t, RequestColumnsPtr>* request_chunks,
                               // row_id_slot -> null rows' position
                               phmap::flat_hash_map<uint32_t, ColumnPtr>* null_position_columns = nullptr);
    Status _send_fetch_request(RuntimeState* state, const BatchUnitPtr& unit,
                               const phmap::flat_hash_map<uint32_t, RequestColumnsPtr>& request_chunks);
    Status _send_local_request(RuntimeState* state, const BatchUnitPtr& unit, const FetchContextPtr& fetch_ctx);
    Status _send_remote_request(RuntimeState* state, const BatchUnitPtr& unit, const FetchContextPtr& fetch_ctx);

    StatusOr<ChunkPtr> _sort_chunk(RuntimeState* state, const ChunkPtr& chunk, const Columns& order_by_columns);
    Status _build_output_chunk(RuntimeState* state, const BatchUnitPtr& unit);

    StatusOr<ChunkPtr> _get_output_chunk(RuntimeState* state);

    void _set_io_task_status(const Status& status) {
        std::unique_lock l(_status_mu);
        if (_io_task_status.ok()) {
            _io_task_status = status;
        }
    }
    Status _get_io_task_status() const {
        std::shared_lock l(_status_mu);
        return _io_task_status;
    }

    const int32_t _target_node_id;
    const phmap::flat_hash_map<TupleId, SlotId>& _row_id_slots;
    const phmap::flat_hash_map<SlotId, SlotDescriptor*>& _slot_id_to_desc;
    const std::shared_ptr<StarRocksNodesInfo> _nodes_info;
    const std::shared_ptr<LookUpDispatcher> _local_dispatcher;
    int32_t _local_be_id = 0;

    BatchUnitPtr _current_unit;
    // @TODO(silverbullet233): we can use a lock-free ring buffer
    mutable std::shared_mutex _queue_mu;
    std::queue<BatchUnitPtr> _queue;

    Permutation _permutation;
    raw::RawString _serialize_buffer;

    mutable bthreads::BThreadSharedMutex _status_mu;
    Status _io_task_status;

    std::atomic_bool _is_sink_complete = false;

    RuntimeProfile::Counter* _build_row_id_chunk_timer = nullptr;
    RuntimeProfile::Counter* _gen_request_chunk_timer = nullptr;
    RuntimeProfile::Counter* _serialize_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_timer = nullptr;
    RuntimeProfile::Counter* _build_output_chunk_timer = nullptr;

    RuntimeProfile::Counter* _rpc_count = nullptr;
    RuntimeProfile::Counter* _network_timer = nullptr;
    RuntimeProfile::Counter* _local_request_count = nullptr;
    RuntimeProfile::Counter* _local_request_timer = nullptr;
    static const int kPositionColumnSlotId = INT32_MAX;
};

using FetchProcessorPtr = std::shared_ptr<FetchProcessor>;

class FetchProcessorFactory {
public:
    FetchProcessorFactory(int32_t target_node_id, phmap::flat_hash_map<TupleId, SlotId> row_id_slots,
                          phmap::flat_hash_map<SlotId, SlotDescriptor*> slot_id_to_desc,
                          std::shared_ptr<StarRocksNodesInfo> nodes_info,
                          std::shared_ptr<LookUpDispatcher> local_dispatcher);

    ~FetchProcessorFactory() = default;

    std::shared_ptr<FetchProcessor> get_or_create(int32_t driver_sequence);

private:
    int32_t _target_node_id;
    phmap::flat_hash_map<TupleId, SlotId> _row_id_slots;
    phmap::flat_hash_map<SlotId, SlotDescriptor*> _slot_id_to_desc;
    std::shared_ptr<StarRocksNodesInfo> _nodes_info;
    std::shared_ptr<LookUpDispatcher> _local_dispatcher;

    typedef phmap::parallel_flat_hash_map<int32_t, FetchProcessorPtr, phmap::Hash<int32_t>, phmap::EqualTo<int32_t>,
                                          phmap::Allocator<int32_t>, 4, bthread::Mutex>
            FetchProcessorMap;

    FetchProcessorMap _processor_map; // driver_sequence -> processor
};

} // namespace starrocks::pipeline