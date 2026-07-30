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

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/global_types.h"
#include "exec_primitive/pipeline/operator_factory.h"
#include "gen_cpp/DataSinks_types.h"
#include "runtime/remote_arrow_queue_mgr.h"
#include "runtime/remote_chunk_queue_mgr.h"

namespace arrow {
class RecordBatch;
class Schema;
} // namespace arrow

namespace starrocks {
class ExprContext;

namespace pipeline {

// Transport-agnostic handle over a fragment's remote-scan result queue. It hides the
// brpc-chunk vs arrow-flight differences (element type, serialization, owning manager) so the
// sink operator never branches on transport. Built once per fragment by the factory and shared
// by all sibling sink operators of that fragment.
class RemoteScanResultQueue {
public:
    virtual ~RemoteScanResultQueue() = default;

    // True when the soft watermark is exceeded; drives need_input backpressure.
    virtual bool is_full() const = 0;
    // Transform one chunk for the wire and enqueue it. On a transform error, poisons the queue
    // and returns the error; on enqueue after the queue was shut down, returns the queue status.
    virtual Status append(const ChunkPtr& chunk) = 0;
    // Enqueue the end-of-stream (eos) marker (caller guarantees once-ness).
    virtual Status put_eos() = 0;
    // Poison the queue with a failure and shut it down so the consumer observes the error.
    virtual void fail(const Status& status) = 0;
    // Poison and reap the queue through its owning manager (cancel path).
    virtual void cancel(RuntimeState* state) = 0;
};
using RemoteScanResultQueuePtr = std::shared_ptr<RemoteScanResultQueue>;

class RemoteScanResultSinkOperator final : public Operator {
public:
    RemoteScanResultSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                 RemoteScanResultQueuePtr queue, std::string instance_scan_token,
                                 std::atomic<int32_t>& num_sinkers, std::atomic<bool>& eos_published);
    ~RemoteScanResultSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }
    bool need_input() const override;
    bool is_finished() const override { return _is_finished.load(std::memory_order_acquire); }
    Status set_finishing(RuntimeState* state) override;
    bool pending_finish() const override;
    Status set_cancelled(RuntimeState* state) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    Status terminal_status(RuntimeState* state) const;
    void publish_failure(const Status& status);
    Status publish_eos_once();

    RemoteScanResultQueuePtr _queue;
    std::string _instance_scan_token;
    std::atomic<int32_t>& _num_sinkers;
    std::atomic<bool>& _eos_published;
    std::atomic<bool> _is_finished = false;
};

class RemoteScanResultSinkOperatorFactory final : public OperatorFactory {
public:
    RemoteScanResultSinkOperatorFactory(int32_t id, RecordDescriptor record_desc, std::vector<TExpr> t_output_expr,
                                        TRemoteScanResultSink sink);
    ~RemoteScanResultSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        _increment_num_sinkers_no_barrier();
        return std::make_shared<RemoteScanResultSinkOperator>(this, _id, _plan_node_id, driver_sequence, _result_queue,
                                                              _instance_scan_token, _num_sinkers, _eos_published);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    void _increment_num_sinkers_no_barrier() { _num_sinkers.fetch_add(1, std::memory_order_relaxed); }
    void _prepare_id_to_col_name_map();
    void _prepare_output_slot_ids();

    const RecordDescriptor _record_desc;
    std::vector<TExpr> _t_output_expr;
    TRemoteScanResultSink _sink;
    TStarRocksScanTransport::type _transport = TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT;
    std::string _scan_token;
    std::string _instance_scan_token;
    TUniqueId _fragment_instance_id;
    int64_t _expire_ms = 0;

    std::shared_ptr<arrow::Schema> _arrow_schema;
    std::vector<SlotId> _output_slot_ids;
    std::vector<ExprContext*> _output_expr_ctxs;
    std::unordered_map<int64_t, std::string> _id_to_col_name;
    RemoteScanResultQueuePtr _result_queue;
    std::atomic<int32_t> _num_sinkers = 0;
    std::atomic<bool> _eos_published = false;
};

} // namespace pipeline
} // namespace starrocks
