// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "exec/pipeline/chunk_source.h"
#include "exec/vectorized/hdfs_scan_node.h"
#include "exec/vectorized/hdfs_scanner.h"
#include "exec/workgroup/work_group_fwd.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class SlotDescriptor;

namespace vectorized {
class RuntimeFilterProbeCollector;
class HdfsScanner;
} // namespace vectorized

namespace pipeline {

class ScanOperator;
class HdfsChunkSource final : public ChunkSource {
public:
    HdfsChunkSource(RuntimeProfile* runtime_profile, MorselPtr&& morsel, ScanOperator* op,
                    vectorized::HdfsScanNode* scan_node);

    ~HdfsChunkSource() override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_next_chunk() const override;

    bool has_output() const override;

    virtual size_t get_buffer_size() const override;

    StatusOr<vectorized::ChunkPtr> get_next_chunk_from_buffer() override;

    Status buffer_next_batch_chunks_blocking(size_t chunk_size, bool& can_finish) override;
    Status buffer_next_batch_chunks_blocking_for_workgroup(size_t chunk_size, bool& can_finish, size_t* num_read_chunks,
                                                           int worker_id, workgroup::WorkGroupPtr running_wg) override;

private:
    // Yield scan io task when maximum time in nano-seconds has spent in current execution round.
    static constexpr int64_t YIELD_MAX_TIME_SPENT = 100'000'000L;
    // Yield scan io task when maximum time in nano-seconds has spent in current execution round,
    // if it runs in the worker thread owned by other workgroup, which has running drivers.
    static constexpr int64_t YIELD_PREEMPT_MAX_TIME_SPENT = 20'000'000L;

    // ============= init func =============
    Status _init_conjunct_ctxs(RuntimeState* state);
    void _decompose_conjunct_ctxs();
    void _init_tuples_and_slots(RuntimeState* state);
    void _init_counter(RuntimeState* state);

    void _init_partition_values();
    Status _init_scanner(RuntimeState* state);
    void _init_chunk(ChunkPtr* chunk);

    // =====================================
    Status _read_chunk_from_storage([[maybe_unused]] RuntimeState* state, vectorized::ChunkPtr* chunk);

    // =====================================
    vectorized::HdfsScanNode* _scan_node;
    const int64_t _limit; // -1: no limit
    const std::vector<ExprContext*>& _runtime_in_filters;
    const vectorized::RuntimeFilterProbeCollector* _runtime_bloom_filters;
    THdfsScanRange* _scan_range;

    Status _status = Status::OK();
    bool _closed = false;
    UnboundedBlockingQueue<vectorized::ChunkPtr> _chunk_buffer;

    ObjectPool _obj_pool;
    ObjectPool* _pool = &_obj_pool;
    RuntimeState* _runtime_state = nullptr;
    vectorized::HdfsScanner* _scanner = nullptr;

    // =============== conjuncts =====================
    // copied from scan node and merge predicates from runtime filter.
    std::vector<ExprContext*> _conjunct_ctxs;

    std::vector<ExprContext*> _min_max_conjunct_ctxs;

    // complex conjuncts, such as contains multi slot, are evaled in scanner.
    std::vector<ExprContext*> _scanner_conjunct_ctxs;
    // conjuncts that contains only one slot.
    // 1. conjuncts that column is not exist in file, are used to filter file in file reader.
    // 2. conjuncts that column is materialized, are evaled in group reader.
    std::unordered_map<SlotId, std::vector<ExprContext*>> _conjunct_ctxs_by_slot;

    // partition conjuncts of each partition slot.
    std::vector<ExprContext*> _partition_conjunct_ctxs;
    std::vector<ExprContext*> _partition_values;
    bool _has_partition_conjuncts = false;
    bool _filter_by_eval_partition_conjuncts = false;
    bool _no_data = false;

    // ============ tuples and slots ==========
    const TupleDescriptor* _tuple_desc = nullptr;

    int _min_max_tuple_id = 0;
    TupleDescriptor* _min_max_tuple_desc = nullptr;

    // materialized columns.
    std::vector<SlotDescriptor*> _materialize_slots;
    std::vector<int> _materialize_index_in_chunk;

    // partition columns.
    std::vector<SlotDescriptor*> _partition_slots;

    // partition column index in `tuple_desc`
    std::vector<int> _partition_index_in_chunk;
    // partition index in hdfs partition columns
    std::vector<int> _partition_index_in_hdfs_partition_columns;
    bool _has_partition_columns = false;

    std::vector<std::string> _hive_column_names;
    const LakeTableDescriptor* _lake_table = nullptr;

    // ======================================
    // The following are profile metrics
    vectorized::HdfsScanProfile _profile;
};
} // namespace pipeline
} // namespace starrocks
