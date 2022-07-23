// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "connector/connector.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks {

class ScanNode;

namespace pipeline {

class ChunkBufferToken;
using ChunkBufferTokenPtr = std::unique_ptr<ChunkBufferToken>;
class ChunkBufferLimiter;

class ConnectorScanOperatorFactory final : public ScanOperatorFactory {
public:
    ConnectorScanOperatorFactory(int32_t id, ScanNode* scan_node, ChunkBufferLimiterPtr buffer_limiter);

    ~ConnectorScanOperatorFactory() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;
};

class ConnectorScanOperator final : public ScanOperator {
public:
<<<<<<< HEAD
    ConnectorScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, ScanNode* scan_node,
                          ChunkBufferLimiter* buffer_limiter);
=======
    ConnectorScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                          ScanNode* scan_node);
>>>>>>> 509a3b786 ([Enhance] introduce unplug mechanism to improve scalability (#8979))

    ~ConnectorScanOperator() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;
<<<<<<< HEAD

private:
=======
    connector::ConnectorType connector_type();

    // TODO: refactor it into the base class
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
>>>>>>> 509a3b786 ([Enhance] introduce unplug mechanism to improve scalability (#8979))
};

class ConnectorChunkSource final : public ChunkSource {
public:
    ConnectorChunkSource(RuntimeProfile* runtime_profile, MorselPtr&& morsel, ScanOperator* op,
                         vectorized::ConnectorScanNode* scan_node, ChunkBufferLimiter* const buffer_limiter);

    ~ConnectorChunkSource() override;

    Status prepare(RuntimeState* state) override;

    Status set_finished(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_next_chunk() const override;

    bool has_output() const override;

    virtual size_t get_buffer_size() const override;

    StatusOr<vectorized::ChunkPtr> get_next_chunk_from_buffer() override;

    Status buffer_next_batch_chunks_blocking(size_t chunk_size, RuntimeState* state) override;
    Status buffer_next_batch_chunks_blocking_for_workgroup(size_t chunk_size, RuntimeState* state,
                                                           size_t* num_read_chunks, int worker_id,
                                                           workgroup::WorkGroupPtr running_wg) override;

private:
    using ChunkWithToken = std::pair<vectorized::ChunkPtr, ChunkBufferTokenPtr>;

    Status _read_chunk(vectorized::ChunkPtr* chunk);

    // Yield scan io task when maximum time in nano-seconds has spent in current execution round.
    static constexpr int64_t YIELD_MAX_TIME_SPENT = 100'000'000L;
    // Yield scan io task when maximum time in nano-seconds has spent in current execution round,
    // if it runs in the worker thread owned by other workgroup, which has running drivers.
    static constexpr int64_t YIELD_PREEMPT_MAX_TIME_SPENT = 20'000'000L;

    // ========================
    connector::DataSourcePtr _data_source;
    vectorized::ConnectorScanNode* _scan_node;
    const int64_t _limit; // -1: no limit
    const std::vector<ExprContext*>& _runtime_in_filters;
    const vectorized::RuntimeFilterProbeCollector* _runtime_bloom_filters;

    // copied from scan node and merge predicates from runtime filter.
    std::vector<ExprContext*> _conjunct_ctxs;

    // =========================
    RuntimeState* _runtime_state = nullptr;
    Status _status = Status::OK();
    bool _opened = false;
    bool _closed = false;
    uint64_t _rows_read = 0;
    uint64_t _bytes_read = 0;

    UnboundedBlockingQueue<ChunkWithToken> _chunk_buffer;
    ChunkBufferLimiter* const _buffer_limiter;
};

} // namespace pipeline
} // namespace starrocks
