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

#include "exec/pipeline/sink/remote_scan_result_sink_operator.h"

#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>

#include "base/statusor.h"
#include "base/uid_util.h"
#include "column/chunk.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/pipeline/fragment_context.h"
#include "exec_primitive/arrow/result_to_arrow_converter.h"
#include "exec_primitive/pipeline/primitives/driver_executor.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/remote_scan_token_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/serde/protobuf_chunk_serde.h"
#include "runtime/service_contexts.h"

namespace starrocks::pipeline {

namespace {

constexpr const char* kCancelReason = "Set cancelled by RemoteScanResultSinkOperator";

// Common blocking-queue mechanics shared by both transports. Concrete queues only add the
// per-wire transform (append) and the owning-manager cancel; everything else (backpressure,
// EOS marker, failure poisoning, enqueue/shutdown-status handling) lives here once.
template <typename QueuePtr, typename Element>
class BlockingResultQueue : public RemoteScanResultQueue {
public:
    BlockingResultQueue(QueuePtr queue, std::vector<ExprContext*> output_expr_ctxs, TUniqueId fragment_instance_id,
                        std::string shutdown_message)
            : _queue(std::move(queue)),
              _output_expr_ctxs(std::move(output_expr_ctxs)),
              _fragment_instance_id(fragment_instance_id),
              _shutdown_message(std::move(shutdown_message)) {}

    bool is_full() const final { return _queue->is_full(); }
    Status put_eos() final { return enqueue(Element{}); } // a null element marks EOS
    void fail(const Status& status) final {
        _queue->update_status(status);
        _queue->shutdown();
    }

protected:
    // Enqueue an already-transformed element; maps a shut-down queue to its failure/Cancelled
    // status so the producer never silently drops the element.
    Status enqueue(Element element) {
        if (!_queue->put(std::move(element))) {
            Status status = _queue->status();
            return status.ok() ? Status::Cancelled(_shutdown_message) : status;
        }
        return Status::OK();
    }

    QueuePtr _queue;
    std::vector<ExprContext*> _output_expr_ctxs;
    TUniqueId _fragment_instance_id;
    std::string _shutdown_message;
};

// arrow_flight transport: evaluate the output exprs into an Arrow RecordBatch.
class ArrowResultQueue final
        : public BlockingResultQueue<RemoteArrowQueueSharedPtr, std::shared_ptr<arrow::RecordBatch>> {
public:
    ArrowResultQueue(RemoteArrowQueueSharedPtr queue, std::vector<ExprContext*> output_expr_ctxs,
                     std::shared_ptr<arrow::Schema> schema, TUniqueId fragment_instance_id)
            : BlockingResultQueue(std::move(queue), std::move(output_expr_ctxs), fragment_instance_id,
                                  "remote scan arrow result queue has been shutdown"),
              _schema(std::move(schema)) {}

    Status append(const ChunkPtr& chunk) override {
        std::shared_ptr<arrow::RecordBatch> batch;
        Status status = convert_chunk_to_arrow_batch(chunk.get(), _output_expr_ctxs, _schema,
                                                     arrow::default_memory_pool(), &batch);
        if (!status.ok()) {
            fail(status);
            return status;
        }
        return enqueue(std::move(batch));
    }

    void cancel(RuntimeState* state) override {
        _queue->update_status(Status::Cancelled(kCancelReason));
        WARN_IF_ERROR(state->query_execution_services()->runtime->remote_arrow_queue_mgr->cancel(_fragment_instance_id),
                      "Failed to cancel remote scan arrow queue");
    }

private:
    std::shared_ptr<arrow::Schema> _schema;
};

// brpc_chunk transport: evaluate the output exprs into a native Chunk and serialize to ChunkPB.
class BrpcChunkResultQueue final : public BlockingResultQueue<RemoteChunkQueueSharedPtr, std::shared_ptr<ChunkPB>> {
public:
    BrpcChunkResultQueue(RemoteChunkQueueSharedPtr queue, std::vector<ExprContext*> output_expr_ctxs,
                         std::vector<SlotId> output_slot_ids, TUniqueId fragment_instance_id)
            : BlockingResultQueue(std::move(queue), std::move(output_expr_ctxs), fragment_instance_id,
                                  "remote scan chunk queue has been shutdown"),
              _output_slot_ids(std::move(output_slot_ids)) {}

    Status append(const ChunkPtr& chunk) override {
        if (_output_slot_ids.size() < _output_expr_ctxs.size()) {
            Status status = Status::InternalError("remote scan result sink output expr count exceeds output slot ids");
            fail(status);
            return status;
        }
        Chunk output_chunk;
        for (size_t i = 0; i < _output_expr_ctxs.size(); ++i) {
            auto column_or = _output_expr_ctxs[i]->evaluate(chunk.get());
            if (!column_or.ok()) {
                fail(column_or.status());
                return column_or.status();
            }
            output_chunk.append_column(std::move(column_or).value(), _output_slot_ids[i]);
        }
        auto chunk_pb_or = serde::ProtobufChunkSerde::serialize_without_meta(output_chunk);
        if (!chunk_pb_or.ok()) {
            fail(chunk_pb_or.status());
            return chunk_pb_or.status();
        }
        return enqueue(std::make_shared<ChunkPB>(std::move(chunk_pb_or).value()));
    }

    void cancel(RuntimeState* state) override {
        _queue->update_status(Status::Cancelled(kCancelReason));
        WARN_IF_ERROR(state->query_execution_services()->runtime->remote_chunk_queue_mgr->cancel(_fragment_instance_id),
                      "Failed to cancel remote scan chunk queue");
    }

private:
    std::vector<SlotId> _output_slot_ids;
};

} // namespace

RemoteScanResultSinkOperator::RemoteScanResultSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                           int32_t driver_sequence, RemoteScanResultQueuePtr queue,
                                                           std::string instance_scan_token,
                                                           std::atomic<int32_t>& num_sinkers,
                                                           std::atomic<bool>& eos_published)
        : Operator(factory, id, "remote_scan_result_sink", plan_node_id, false, driver_sequence),
          _queue(std::move(queue)),
          _instance_scan_token(std::move(instance_scan_token)),
          _num_sinkers(num_sinkers),
          _eos_published(eos_published) {}

Status RemoteScanResultSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void RemoteScanResultSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool RemoteScanResultSinkOperator::need_input() const {
    return !_is_finished.load(std::memory_order_acquire) && !_queue->is_full();
}

Status RemoteScanResultSinkOperator::set_finishing(RuntimeState* state) {
    // The driver cancel path runs set_finishing before set_cancelled
    // (_mark_operator_cancelled marks the operator finished first). The input
    // has not genuinely ended there, so don't publish a clean EOS or flip
    // _is_finished: leaving the flag unset keeps the full cancel handling in
    // set_cancelled reachable, which must publish a failure before waking the
    // consumer — otherwise a cancelled producer would hand the remote consumer
    // a truncated result with a clean EOS.
    if (state != nullptr && state->is_cancelled()) {
        return Status::OK();
    }
    if (_is_finished.exchange(true, std::memory_order_acq_rel)) {
        return Status::OK();
    }
    if (_num_sinkers.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        auto* fragment_ctx = state == nullptr ? nullptr : state->fragment_ctx();
        std::shared_ptr<workgroup::WorkGroup> workgroup;
        if (fragment_ctx != nullptr) {
            workgroup = fragment_ctx->workgroup();
        }
        auto* executors = workgroup == nullptr ? nullptr : workgroup->executors();
        auto* driver_executor = executors == nullptr ? nullptr : executors->driver_executor();
        if (driver_executor != nullptr) {
            driver_executor->report_audit_statistics(state->query_ctx(), fragment_ctx);
        }
        // Mark the scan token completed before publishing the EOS marker. Ordered before the
        // publish so completion is visible before any consumer can drain the eos and erase
        // the queue; a later fetch that then finds the queue gone is recognized as a legitimate
        // EOS rather than a missing-queue anomaly.
        if (state != nullptr) {
            auto* runtime = state->query_execution_services()->runtime;
            WARN_IF_ERROR(runtime->remote_scan_token_mgr->mark_completed(_instance_scan_token),
                          "Failed to mark remote scan token completed");
        }
        return publish_eos_once();
    }
    return Status::OK();
}

bool RemoteScanResultSinkOperator::pending_finish() const {
    return false;
}

Status RemoteScanResultSinkOperator::set_cancelled(RuntimeState* state) {
    // INVARIANT: once set_finishing has run, the instance scan token must remain
    // registered so a late consumer's lookup can still complete and drain the
    // trailing EOS marker from the queue. The expiry sweeper reclaims it.
    // See unit test `finished_result_sink_cancel_keeps_token_until_consumer_fetches_eos`.
    if (_is_finished.exchange(true, std::memory_order_acq_rel)) {
        return Status::OK();
    }
    // Keep the shared sinker count balanced even when this operator is cancelled
    // before normal finishing. The cancellation path below owns failure cleanup.
    _num_sinkers.fetch_sub(1, std::memory_order_acq_rel);
    Status status = terminal_status(state);
    if (!status.ok()) {
        publish_failure(status);
        return Status::OK();
    }
    // Plain cancel (no producer failure): poison and reap the queue via its manager, and drop
    // the token so a consumer retry fails fast instead of waiting for expiry.
    _queue->cancel(state);
    auto* runtime = state->query_execution_services()->runtime;
    WARN_IF_ERROR(runtime->remote_scan_token_mgr->remove(_instance_scan_token), "Failed to remove remote scan token");
    return Status::OK();
}

StatusOr<ChunkPtr> RemoteScanResultSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from remote scan result sink operator");
}

Status RemoteScanResultSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
    if (chunk == nullptr || chunk->num_rows() == 0) {
        return Status::OK();
    }
    return _queue->append(chunk);
}

Status RemoteScanResultSinkOperator::terminal_status(RuntimeState* state) const {
    if (state == nullptr) {
        return Status::OK();
    }
    if (state->fragment_ctx() != nullptr) {
        Status status = state->fragment_ctx()->final_status();
        if (!status.ok()) {
            return status;
        }
    }
    Status status = state->query_status();
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

void RemoteScanResultSinkOperator::publish_failure(const Status& status) {
    if (status.ok()) {
        return;
    }
    _queue->fail(status);
}

Status RemoteScanResultSinkOperator::publish_eos_once() {
    if (_eos_published.exchange(true, std::memory_order_acq_rel)) {
        return Status::OK();
    }
    return _queue->put_eos();
}

RemoteScanResultSinkOperatorFactory::RemoteScanResultSinkOperatorFactory(int32_t id, const RowDescriptor& row_desc,
                                                                         std::vector<TExpr> t_output_expr,
                                                                         TRemoteScanResultSink sink)
        : OperatorFactory(id, "remote_scan_result_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
          _row_desc(row_desc),
          _t_output_expr(std::move(t_output_expr)),
          _sink(std::move(sink)) {
    if (_sink.__isset.transport) {
        _transport = _sink.transport;
    }
    if (_sink.__isset.scan_token) {
        _scan_token = _sink.scan_token;
    }
    if (_sink.__isset.expire_ms) {
        _expire_ms = _sink.expire_ms;
    }
}

Status RemoteScanResultSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(ExprFactory::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(ExprExecutor::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(ExprExecutor::open(_output_expr_ctxs, state));
    _prepare_id_to_col_name_map();
    _prepare_output_slot_ids();
    if (_transport == TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT) {
        RETURN_IF_ERROR(convert_to_arrow_schema(_row_desc, _id_to_col_name, &_arrow_schema, _output_expr_ctxs));
    }
    if (_scan_token.empty()) {
        return Status::InvalidArgument("remote scan result sink missing scan token");
    }

    _fragment_instance_id = state->fragment_instance_id();
    _instance_scan_token = _scan_token + ":" + print_id(_fragment_instance_id);
    auto* runtime = state->query_execution_services()->runtime;
    RETURN_IF_ERROR(runtime->remote_scan_token_mgr->register_token(_instance_scan_token, _fragment_instance_id,
                                                                   _transport, _expire_ms));
    // Build the transport-specific result queue once; the operator only sees the abstract handle.
    if (_transport == TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT) {
        RemoteArrowQueueSharedPtr arrow_queue;
        runtime->remote_arrow_queue_mgr->create_queue(_fragment_instance_id, &arrow_queue);
        runtime->remote_arrow_queue_mgr->set_arrow_schema(_fragment_instance_id, _arrow_schema);
        _result_queue = std::make_shared<ArrowResultQueue>(std::move(arrow_queue), _output_expr_ctxs, _arrow_schema,
                                                           _fragment_instance_id);
    } else {
        RemoteChunkQueueSharedPtr chunk_queue;
        runtime->remote_chunk_queue_mgr->create_queue(_fragment_instance_id, &chunk_queue);
        _result_queue = std::make_shared<BrpcChunkResultQueue>(std::move(chunk_queue), _output_expr_ctxs,
                                                               _output_slot_ids, _fragment_instance_id);
    }
    return Status::OK();
}

void RemoteScanResultSinkOperatorFactory::close(RuntimeState* state) {
    // Remote scan result queues outlive fragment close. Consumers may fetch after
    // the producing fragment has finished, so token cleanup is owned by the
    // consumer path, operator cancellation, and expiry cleanup.
    ExprExecutor::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}

void RemoteScanResultSinkOperatorFactory::_prepare_id_to_col_name_map() {
    for (auto* tuple_desc : _row_desc.tuple_descriptors()) {
        auto& slots = tuple_desc->slots();
        int64_t tuple_id = tuple_desc->id();
        for (auto slot : slots) {
            int64_t slot_id = slot->id();
            int64_t id = tuple_id << 32 | slot_id;
            _id_to_col_name.emplace(id, slot->col_name());
        }
    }
}

void RemoteScanResultSinkOperatorFactory::_prepare_output_slot_ids() {
    if (_row_desc.tuple_descriptors().empty()) {
        return;
    }
    for (auto* slot : _row_desc.tuple_descriptors()[0]->slots()) {
        _output_slot_ids.emplace_back(slot->id());
    }
}

} // namespace starrocks::pipeline
