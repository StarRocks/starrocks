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

#include "exec/pipeline/lookup_operator.h"

#include <memory>
#include <variant>

#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/global_types.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/olap_scan_node.h"
#include "exec/pipeline/fetch_processor.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/lookup_request.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/scan/olap_scan_context.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group.h"
#include "gutil/integral_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "runtime/lookup_stream_mgr.h"
#include "serde/column_array_serde.h"
#include "serde/protobuf_serde.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/common.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "util/defer_op.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"
#include "util/time.h"

namespace starrocks::pipeline {

class LookUpProcessor {
public:
    LookUpProcessor(LookUpOperator* parent) : _parent(parent) {}
    ~LookUpProcessor() = default;

    void close();

    bool is_running() const { return _is_running; }
    void set_running(bool running) { _is_running = running; }

    bool need_input() const { return false; }

    Status process(RuntimeState* state);

    void set_ctx(LookUpTaskContextPtr ctx) {
        DCHECK(ctx != nullptr && !ctx->request_ctxs.empty()) << "request_ctxs should not be empty";
        _ctx = std::move(ctx);
    }

private:
    // collect input columns into one chunk
    Status _collect_input_columns(RuntimeState* state, const ChunkPtr& request_chunk);
    
    // @TODO bind profile to task?
    StatusOr<LookUpTaskPtr> _create_task(const LookUpTaskContextPtr& ctx);

    LookUpTaskContextPtr _ctx;
    std::atomic_bool _is_running = false;

    Permutation _permutation;
    raw::RawString _serialize_buffer;

    LookUpOperator* _parent = nullptr;
};

void LookUpProcessor::close() {
    // update parent counter
}
// collect all input columns into one chunk
Status LookUpProcessor::_collect_input_columns(RuntimeState* state, const ChunkPtr& request_chunk) {
    DCHECK(_parent->_row_pos_descs.contains(_ctx->request_tuple_id)) << "missing request tuple id: " << _ctx->request_tuple_id;
    auto row_pos_desc = _parent->_row_pos_descs.at(_ctx->request_tuple_id);
    for (const auto& slot_id: row_pos_desc->get_fetch_ref_slot_ids()) {
        // @TODO use lookup slot ids?
        auto slot_desc = state->desc_tbl().get_slot_descriptor(slot_id);
        auto col = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        request_chunk->append_column(std::move(col), slot_desc->id());
    }
    auto slot_desc = state->desc_tbl().get_slot_descriptor(row_pos_desc->get_row_source_slot_id());
    // row source column from fetch node won't be nullable 
    auto row_source_col = ColumnHelper::create_column(slot_desc->type(), false);
    DLOG(INFO) << "LookUpProcessor _collect_input_columns, append source node id column, slot_id: " << slot_desc->id() << ", column: " << row_source_col->get_name();
    request_chunk->append_column(std::move(row_source_col), slot_desc->id());
    // add source node id
    DLOG(INFO) << "LookUpProcessor _collect_input_columns, request_chunk: " << request_chunk->debug_columns();

    for (auto& request_ctx : _ctx->request_ctxs) {
        RETURN_IF_ERROR(request_ctx->collect_input_columns(request_chunk));
    }
    return Status::OK();
}

StatusOr<LookUpTaskPtr> LookUpProcessor::_create_task(const LookUpTaskContextPtr& ctx) {
    auto tuple_id = ctx->request_tuple_id;
    auto row_pos_desc = _parent->_row_pos_descs.at(tuple_id);
    switch (row_pos_desc->type()) {
        case RowPositionDescriptor::Type::ICEBERG_V3: {
            return std::make_shared<IcebergV3LookUpTask>(ctx);
        }
        default:
            return Status::InternalError("unknown row position descriptor type: " + std::to_string(row_pos_desc->type()));
    }
}

Status LookUpProcessor::process(RuntimeState* state) {
    // SCOPED_TIMER(_parent->_process_time);
    Status status;
    DeferOp op([&]() {
        for (auto& request_ctx : _ctx->request_ctxs) {
            request_ctx->callback(status);
        }
    });
    auto request_chunk = std::make_shared<Chunk>();
    RETURN_IF_ERROR(status = _collect_input_columns(state, request_chunk));
    auto st = _create_task(_ctx);
    if(!st.ok()) {
        status = st.status();
        return status;
    }
    auto task = st.value();
    RETURN_IF_ERROR(status = task->process(state, request_chunk));
    return Status::OK();
}

LookUpOperator::LookUpOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                               const phmap::flat_hash_map<TupleId, RowPositionDescriptor*>& row_pos_descs,
                               std::shared_ptr<LookUpDispatcher> dispatcher, int32_t max_io_tasks)
        : SourceOperator(factory, id, "look_up", plan_node_id, true, driver_sequence),
          _row_pos_descs(row_pos_descs),
          _dispatcher(std::move(dispatcher)),
          _max_io_tasks(max_io_tasks) {
    for (int32_t i = 0; i < _max_io_tasks; i++) {
        _processors.emplace_back(std::make_shared<LookUpProcessor>(this));
    }
    // @TODO create multiple runtime_profile
}

Status LookUpOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _query_ctx = state->query_ctx()->get_shared_ptr();
    // init counter
    _init_counter(state);
    _dispatcher->attach_query_ctx(state->query_ctx());
    _dispatcher->attach_observer(state, observer());

    return Status::OK();
}

void LookUpOperator::_init_counter(RuntimeState* state) {
    _submit_io_task_counter = ADD_COUNTER(_unique_metrics, "SubmitIOTaskCount", TUnit::UNIT);
    _peak_scan_task_queue_size_counter = _unique_metrics->AddHighWaterMarkCounter(
            "PeakScanTaskQueueSize", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
    _io_task_wait_timer = ADD_TIMER(_unique_metrics, "IOTaskWaitTime");

    _calculate_row_id_range_timer = ADD_TIMER(_unique_metrics, "CalculateRowIdRangeTime");
    _get_data_from_storage_timer = ADD_TIMER(_unique_metrics, "GetDataFromStorageTime");
    _fill_response_timer = ADD_TIMER(_unique_metrics, "FillResponseTime");
    _build_row_id_filter_timer = ADD_TIMER(_unique_metrics, "BuildRowIdFilterTime");
}

void LookUpOperator::close(RuntimeState* state) {
    VLOG_ROW << "[GLM] LookUpOperator::close, " << (void*)this << ", target_node_id: " << _plan_node_id;
    for (auto& processor : _processors) {
        processor->close();
    }
    Operator::close(state);
}

bool LookUpOperator::has_output() const {
    if (_is_finished) {
        return false;
    }
    if (!_get_io_task_status().ok()) {
        return true;
    }
    if (_num_running_io_tasks >= _max_io_tasks) {
        return false;
    }
    for (size_t i = 0; i < _max_io_tasks; i++) {
        auto& processor = _processors[i];
        if (!processor->is_running() && _dispatcher->has_data(_driver_sequence)) {
            // we can trigger io task
            return true;
        }
    }

    return false;
}

bool LookUpOperator::is_finished() const {
    return _is_finished && _num_running_io_tasks == 0;
}

bool LookUpOperator::pending_finish() const {
    return !is_finished();
}

Status LookUpOperator::set_finishing(RuntimeState* state) {
    VLOG_ROW << "[GLM] LookUpOperator::set_finishing, " << (void*)this << ", target_node_id: " << _plan_node_id
             << ", dispatcher: " << (void*)_dispatcher.get();
    _is_finished = true;
    return _clean_request_queue(state);
}

StatusOr<ChunkPtr> LookUpOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_get_io_task_status());
    RETURN_IF_ERROR(_try_to_trigger_io_task(state));
    return nullptr;
}

Status LookUpOperator::_try_to_trigger_io_task(RuntimeState* state) {
    for (int32_t i = 0; i < _max_io_tasks; i++) {
        auto processor = _processors[i];

        // @TODO create task
        auto lookup_task_ctx = std::make_shared<LookUpTaskContext>();
        bool is_running = processor->is_running();
        if (!is_running && _dispatcher->try_get(_driver_sequence, config::max_lookup_batch_request, lookup_task_ctx.get())) {
            // @TODO
            lookup_task_ctx->row_source_slot_id = _row_pos_descs.at(lookup_task_ctx->request_tuple_id)->get_row_source_slot_id();
            lookup_task_ctx->lookup_ref_slot_ids = _row_pos_descs.at(lookup_task_ctx->request_tuple_id)->get_lookup_ref_slot_ids();
            lookup_task_ctx->fetch_ref_slot_ids = _row_pos_descs.at(lookup_task_ctx->request_tuple_id)->get_fetch_ref_slot_ids();
            lookup_task_ctx->profile = _unique_metrics.get();
            lookup_task_ctx->parent = this;
            processor->set_ctx(lookup_task_ctx);
            // @TODO create task
            COUNTER_UPDATE(_submit_io_task_counter, 1);
            workgroup::ScanTask task;
            task.workgroup = state->fragment_ctx()->workgroup();
            task.priority = OlapScanNode::compute_priority(_submit_io_task_counter->double_value());
            task.task_group = down_cast<const LookUpOperatorFactory*>(_factory)->io_task_group();
            task.peak_scan_task_queue_size_counter = _peak_scan_task_queue_size_counter;
            task.work_function = [wp = _query_ctx, this, state, idx = i, create_ts = MonotonicNanos()](auto& ctx) {
                if (auto sp = wp.lock()) {
                    auto& processor = _processors[idx];
                    [[maybe_unused]] int64_t start_time = MonotonicNanos();
                    DeferOp defer([&] {
                        _num_running_io_tasks--;
                        processor->set_running(false);
                        [[maybe_unused]] int64_t end_time = MonotonicNanos();
                        VLOG_ROW << "[GLM] LookUpOperator::process, num_running_io_tasks: " << _num_running_io_tasks
                                 << ", " << (void*)this << ", process time: " << (end_time - start_time) / 1000000
                                 << "ms"
                                 << ", queue time: " << (start_time - create_ts) / 1000000 << "ms"
                                 << ", dispatcher: " << (void*)_dispatcher.get();
                        this->defer_notify();
                    });
                    COUNTER_UPDATE(_io_task_wait_timer, MonotonicNanos() - create_ts);
                    Status status = processor->process(state);
                    if (!status.ok()) {
                        LOG(WARNING) << "process error: " << status.to_string();
                        _set_io_task_status(status);
                    }
                }
            };
            _num_running_io_tasks++;
            VLOG_ROW << "[GLM] LookUpOperator::submit_io_task, num_running_io_tasks: " << _num_running_io_tasks << ", "
                     << (void*)this << ", dispatcher: " << (void*)_dispatcher.get() << ", idx: " << i;
            processor->set_running(true);
            // @TODO choose executor
            task.workgroup->executors()->scan_executor()->submit(std::move(task));
        }
    }

    return Status::OK();
}

Status LookUpOperator::_clean_request_queue(RuntimeState* state) {
    DCHECK(_is_finished) << "LookUpOperator should be finished before clean request queue, " << (void*)this;
    do {
        auto ctx = std::make_shared<LookUpTaskContext>();
        if (!_dispatcher->try_get(_driver_sequence, config::max_lookup_batch_request, ctx.get())) {
            // no more request
            break;
        }
        for (auto& request_ctx : ctx->request_ctxs) {
            request_ctx->callback(Status::Cancelled("LookUpOperator is finished"));
        }
    } while (true);
    return Status::OK();
}

LookUpOperatorFactory::LookUpOperatorFactory(int32_t id, int32_t plan_node_id,
                                             phmap::flat_hash_map<TupleId, RowPositionDescriptor*> row_pos_descs,
                                             std::shared_ptr<LookUpDispatcher> dispatcher, int32_t max_io_tasks)
        : SourceOperatorFactory(id, "lookup", plan_node_id),
          _row_pos_descs(std::move(row_pos_descs)),
          _dispatcher(std::move(dispatcher)),
          _max_io_tasks(max_io_tasks),
          _io_task_group(std::make_shared<workgroup::ScanTaskGroup>()) {}

} // namespace starrocks::pipeline