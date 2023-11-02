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

class ConnectorScanOperatorFactory : public ScanOperatorFactory {
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

    const std::vector<ExprContext*>& partition_exprs() const override;
    void set_estimated_mem_usage_per_chunk_source(int64_t mem_usage);
    void set_scan_mem_limit(int64_t mem_limit);

private:
    void _init_partition_type(ScanNode* scan_node, RuntimeState* state);
    // TODO: refactor the OlapScanContext, move them into the context
    BalancedChunkBuffer _chunk_buffer;
    ActiveInputSet _active_inputs;

public:
    ConnectorScanOperatorIOTasksMemLimiter* _io_tasks_mem_limiter;
};

struct ConnectorScanOperatorAdaptiveProcessor;
class ConnectorScanOperator : public ScanOperator {
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
    size_t buffer_memory_usage() const override;
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
                         ConnectorScanNode* scan_node, BalancedChunkBuffer& chunk_buffer);

    ~ConnectorChunkSource() override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    const std::string get_custom_coredump_msg() const override;

    bool reach_limit() override { return _limit != -1 && _reach_limit.load(); }

protected:
    virtual bool _reach_eof() const { return _limit != -1 && _rows_read >= _limit; }
    Status _open_data_source(RuntimeState* state);

    connector::DataSourcePtr _data_source;
    [[maybe_unused]] ConnectorScanNode* _scan_node;

private:
    Status _read_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    const workgroup::WorkGroupScanSchedEntity* _scan_sched_entity(const workgroup::WorkGroup* wg) const override;

    ConnectorScanOperatorIOTasksMemLimiter* _get_io_tasks_mem_limiter() const;

    const int64_t _limit; // -1: no limit
    const std::vector<ExprContext*>& _runtime_in_filters;
    RuntimeFilterProbeCollector* _runtime_bloom_filters;

    // copied from scan node and merge predicates from runtime filter.
    std::vector<ExprContext*> _conjunct_ctxs;

    // =========================
    RuntimeState* _runtime_state = nullptr;
    ChunkPipelineAccumulator _ck_acc;
    bool _opened = false;
    bool _closed = false;
    uint64_t _rows_read = 0;
};

} // namespace pipeline
} // namespace starrocks
