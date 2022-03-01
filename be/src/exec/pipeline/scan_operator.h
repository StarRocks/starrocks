// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks {

class PriorityThreadPool;
class ScanNode;

namespace pipeline {

class ScanOperator;
class ScanNodeInOperator {
public:
    ScanNodeInOperator(ScanNode* scan_node) : _scan_node(scan_node) {}
    ScanNode* node() const { return _scan_node; }
    virtual Status do_prepare(ScanOperator* op) = 0;
    virtual Status do_close(ScanOperator* op) = 0;
    virtual Status do_prepare_in_factory(RuntimeState* state) = 0;
    virtual ChunkSourcePtr create_chunk_source(MorselPtr morsel, ScanOperator* op) = 0;

protected:
    ScanNode* _scan_node;
};

class ScanOperator final : public SourceOperator {
public:
    ScanOperator(OperatorFactory* factory, int32_t id, std::shared_ptr<ScanNodeInOperator> scan_node);

    ~ScanOperator() override = default;

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

private:
    std::shared_ptr<ScanNodeInOperator> _scan_node = nullptr;
    // TODO(hcf) ugly, remove this later
    RuntimeState* _state = nullptr;
    bool _is_finished = false;

    int32_t _io_task_retry_cnt = 0;
    PriorityThreadPool* _io_threads = nullptr;
    std::atomic<int> _num_running_io_tasks = 0;
    std::vector<std::atomic<bool>> _is_io_task_running;
    std::vector<ChunkSourcePtr> _chunk_sources;

    workgroup::WorkGroupPtr _workgroup = nullptr;
};

class ScanOperatorFactory final : public SourceOperatorFactory {
public:
    ScanOperatorFactory(int32_t id, std::shared_ptr<ScanNodeInOperator> scan_node);

    ~ScanOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    bool with_morsels() const override { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::shared_ptr<ScanNodeInOperator> _scan_node;
};

} // namespace pipeline
} // namespace starrocks
