// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "connector/connector.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/workgroup/work_group_fwd.h"
#include "storage/chunk_helper.h"

namespace starrocks {

class ScanNode;

namespace pipeline {

struct ConnectorScanOperatorIOTasksMemLimiter;

class ConnectorScanOperatorFactory final : public ScanOperatorFactory {
public:
    using ActiveInputKey = std::pair<int32_t, int32_t>;
    using ActiveInputSet = phmap::parallel_flat_hash_set<
            ActiveInputKey, typename phmap::Hash<ActiveInputKey>, typename phmap::EqualTo<ActiveInputKey>,
            typename std::allocator<ActiveInputKey>, NUM_LOCK_SHARD_LOG, std::mutex, true>;

    ConnectorScanOperatorFactory(int32_t id, ScanNode* scan_node, RuntimeState* state, size_t dop,
                                 ChunkBufferLimiterPtr buffer_limiter);

    ~ConnectorScanOperatorFactory() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;
    BalancedChunkBuffer& get_chunk_buffer() { return _chunk_buffer; }
    ActiveInputSet& get_active_inputs() { return _active_inputs; }

    TPartitionType::type partition_type() const override { return TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED; }
    void set_estimated_mem_usage_per_chunk_source(int64_t mem_usage);
    void set_scan_mem_limit(int64_t mem_limit);

private:
    // TODO: refactor the OlapScanContext, move them into the context
    BalancedChunkBuffer _chunk_buffer;
    ActiveInputSet _active_inputs;

public:
    ConnectorScanOperatorIOTasksMemLimiter* _io_tasks_mem_limiter;
};

struct ConnectorScanOperatorAdaptiveProcessor;

class ConnectorScanOperator final : public ScanOperator {
public:
    ConnectorScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                          ScanNode* scan_node);

    ~ConnectorScanOperator() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;
    connector::ConnectorType connector_type();

    void attach_chunk_source(int32_t source_index) override;
    void detach_chunk_source(int32_t source_index) override;
    bool has_shared_chunk_source() const override;
    ChunkPtr get_chunk_from_buffer() override;
    size_t num_buffered_chunks() const override;
    size_t buffer_size() const override;
    size_t buffer_capacity() const override;
    size_t default_buffer_capacity() const override;
    ChunkBufferTokenPtr pin_chunk(int num_chunks) override;
    bool is_buffer_full() const override;
    void set_buffer_finished() override;

    int available_pickup_morsel_count() override;
    void begin_driver_process() override;
    void end_driver_process(PipelineDriver* driver) override;
    bool is_running_all_io_tasks() const override;

public:
    mutable ConnectorScanOperatorAdaptiveProcessor* _adaptive_processor;
    bool _enable_adaptive_io_tasks = true;
};

class ConnectorChunkSource : public ChunkSource {
public:
    ConnectorChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                         vectorized::ConnectorScanNode* scan_node, BalancedChunkBuffer& chunk_buffer);

    ~ConnectorChunkSource() override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool reach_limit() override { return _limit != -1 && _reach_limit.load(); }

private:
    Status _read_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    const workgroup::WorkGroupScanSchedEntity* _scan_sched_entity(const workgroup::WorkGroup* wg) const override;

    connector::DataSourcePtr _data_source;
    [[maybe_unused]] vectorized::ConnectorScanNode* _scan_node;
    ConnectorScanOperatorIOTasksMemLimiter* _get_io_tasks_mem_limiter() const;

    const int64_t _limit; // -1: no limit
    const std::vector<ExprContext*>& _runtime_in_filters;
    const vectorized::RuntimeFilterProbeCollector* _runtime_bloom_filters;

    // copied from scan node and merge predicates from runtime filter.
    std::vector<ExprContext*> _conjunct_ctxs;

    // =========================
    RuntimeState* _runtime_state = nullptr;
    ChunkPipelineAccumulator _ck_acc;
    Status _status = Status::OK();
    bool _opened = false;
    bool _closed = false;
    uint64_t _rows_read = 0;
    ConnectorScanOperator* _op = nullptr;
};

} // namespace pipeline
} // namespace starrocks
