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

#include "common/status.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group_fwd.h"
#include "runtime/descriptors.h"
#include "runtime/lookup_stream_mgr.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"

namespace starrocks::pipeline {

class LookUpProcessor;
using LookUpProcessorPtr = std::shared_ptr<LookUpProcessor>;

class LookUpOperator final : public SourceOperator {
public:
    LookUpOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                   const phmap::flat_hash_map<TupleId, RowPositionDescriptor*>& row_pos_descs,
                   std::shared_ptr<LookUpDispatcher> dispatcher, int32_t max_io_tasks);

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    bool pending_finish() const override;

    Status set_finishing(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override {
        return Status::InternalError("LookUpOperator does not support push_chunk");
    }

    auto defer_notify() {
        return DeferOp([this]() { observer()->source_trigger(); });
    }

private:
    friend class LookUpProcessor;
    inline static const std::string IO_TASK_EXEC_TIMER_NAME = "IOTaskExecTime";
    void _init_counter(RuntimeState* state);

    void _set_io_task_status(const Status& status) {
        std::unique_lock l(_lock);
        if (_io_task_status.ok()) {
            _io_task_status = status;
        }
    }
    Status _get_io_task_status() const {
        std::shared_lock l(_lock);
        return _io_task_status;
    }

    Status _try_to_trigger_io_task(RuntimeState* state);

    Status _clean_request_queue(RuntimeState* state);

    const phmap::flat_hash_map<TupleId, RowPositionDescriptor*>& _row_pos_descs;
    std::shared_ptr<LookUpDispatcher> _dispatcher;
    const int32_t _max_io_tasks;

    std::atomic_int32_t _num_running_io_tasks = 0;

    mutable std::shared_mutex _lock;
    Status _io_task_status;
    std::atomic_bool _is_finished = false;
    mutable std::vector<LookUpProcessorPtr> _processors;
    std::weak_ptr<QueryContext> _query_ctx;
    // // io task
    RuntimeProfile::Counter* _submit_io_task_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_scan_task_queue_size_counter = nullptr;
    RuntimeProfile::Counter* _io_task_wait_timer = nullptr;

public:
    RuntimeProfile::Counter* _calculate_row_id_range_timer = nullptr;
    RuntimeProfile::Counter* _get_data_from_storage_timer = nullptr;
    RuntimeProfile::Counter* _fill_response_timer = nullptr;
    RuntimeProfile::Counter* _build_row_id_filter_timer = nullptr;
};

class LookUpOperatorFactory final : public SourceOperatorFactory {
public:
    LookUpOperatorFactory(int32_t id, int32_t plan_node_id,
                          phmap::flat_hash_map<TupleId, RowPositionDescriptor*> row_pos_descs,
                          std::shared_ptr<LookUpDispatcher> dispatcher, int32_t max_io_tasks);

    ~LookUpOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LookUpOperator>(this, _id, _plan_node_id, driver_sequence, _row_pos_descs, _dispatcher,
                                                _max_io_tasks);
    }
    std::shared_ptr<workgroup::ScanTaskGroup> io_task_group() const { return _io_task_group; }
    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }
    bool support_event_scheduler() const override { return true; }

private:
    phmap::flat_hash_map<TupleId, RowPositionDescriptor*> _row_pos_descs;
    std::shared_ptr<LookUpDispatcher> _dispatcher;
    int32_t _max_io_tasks = 0;

    std::shared_ptr<workgroup::ScanTaskGroup> _io_task_group;
};
} // namespace starrocks::pipeline