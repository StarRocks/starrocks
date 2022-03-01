// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <optional>

#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/work_group_fwd.h"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "runtime/global_dicts.h"
#include "util/blocking_queue.hpp"
#include "util/priority_thread_pool.hpp"

namespace starrocks {

class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

namespace vectorized {
class RuntimeFilterProbeCollector;
}

namespace pipeline {

class OlapScanOperator final : public SourceOperator {
public:
    OlapScanOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const TOlapScanNode& olap_scan_node,
                     const std::vector<ExprContext*>& conjunct_ctxs, int64_t limit)
            : SourceOperator(factory, id, "olap_scan", plan_node_id),
              _olap_scan_node(olap_scan_node),
              _conjunct_ctxs(conjunct_ctxs),
              _limit(limit),
              _is_io_task_running(MAX_IO_TASKS_PER_OP),
              _chunk_sources(MAX_IO_TASKS_PER_OP) {}

    ~OlapScanOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override;

    bool pending_finish() const override;

    bool is_finished() const override;

    void set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    void set_io_threads(PriorityThreadPool* io_threads) { _io_threads = io_threads; }
    void set_workgroup(workgroup::WorkGroupPtr wg);

private:
    static constexpr int MAX_IO_TASKS_PER_OP = 4;

    const size_t _buffer_size = config::pipeline_io_buffer_size;

    // This method is only invoked when current morsel is reached eof
    // and all cached chunk of this morsel has benn read out
    Status _pickup_morsel(RuntimeState* state, int chunk_source_index);
    Status _trigger_next_scan(RuntimeState* state, int chunk_source_index);
    Status _try_to_trigger_next_scan(RuntimeState* state);

    Status _capture_tablet_rowsets();

private:
    // TODO(hcf) ugly, remove this later
    RuntimeState* _state = nullptr;

    bool _is_finished = false;
    int32_t _io_task_retry_cnt = 0;

    // The row sets of tablets will become stale and be deleted, if compaction occurs
    // and these row sets aren't referenced, which will typically happen when the tablets
    // of the left table are compacted at building the right hash table. Therefore, reference
    // the row sets into _tablet_rowsets in the preparation phase to avoid the row sets being deleted.
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;
    const TOlapScanNode& _olap_scan_node;
    const std::vector<ExprContext*>& _conjunct_ctxs;
    PriorityThreadPool* _io_threads = nullptr;
    std::vector<std::string> _unused_output_columns;
    // Pass limit info to scan operator in order to improve sql:
    // select * from table limit x;
    int64_t _limit; // -1: no limit

    std::atomic<int> _num_running_io_tasks = 0;
    std::vector<std::atomic<bool>> _is_io_task_running;
    std::vector<ChunkSourcePtr> _chunk_sources;

    workgroup::WorkGroupPtr _workgroup = nullptr;
};

class OlapScanOperatorFactory final : public SourceOperatorFactory {
public:
    OlapScanOperatorFactory(int32_t id, int32_t plan_node_id, const TOlapScanNode& olap_scan_node,
                            std::vector<ExprContext*>&& conjunct_ctxs, int64_t limit)
            : SourceOperatorFactory(id, "olap_scan", plan_node_id),
              _olap_scan_node(olap_scan_node),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _limit(limit) {}

    ~OlapScanOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<OlapScanOperator>(this, _id, _plan_node_id, _olap_scan_node, _conjunct_ctxs, _limit);
    }

    // OlapScanOperator needs to attach MorselQueue.
    bool with_morsels() const override { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    const TOlapScanNode& _olap_scan_node;
    std::vector<ExprContext*> _conjunct_ctxs;
    // Pass limit info to scan operator in order to improve sql:
    // select * from table limit x;
    int64_t _limit; // -1: no limit
};

} // namespace pipeline
} // namespace starrocks
