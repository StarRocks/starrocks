// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/olap_scan_prepare.h"
#include "exec/stream/lookupjoin/lookup_join_context.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_reader.h"

namespace starrocks::pipeline {
using namespace vectorized;

using PredicatePtr = std::unique_ptr<vectorized::ColumnPredicate>;

struct TabletReaderState {
    ObjectPool pool;
    vectorized::OlapScanConjunctsManager conjuncts_manager;
    vectorized::ConjunctivePredicates not_push_down_predicates;
    std::vector<ExprContext*> conjunct_ctxs;
    std::vector<std::unique_ptr<OlapScanRange>> key_ranges;
    std::vector<ExprContext*> not_push_down_conjuncts;
    std::vector<PredicatePtr> predicate_free_pool;
    vectorized::TabletReaderParams params;
};

// LookupJoinSeekOperator
class LookupJoinSeekOperator final : public SourceOperator {
public:
    LookupJoinSeekOperator(OperatorFactory* factory, int32_t id,
                           int32_t plan_node_id, const int32_t driver_sequence,
                           const TOlapScanNode& olap_scan_node,
                           const vectorized::RuntimeFilterProbeCollector& runtime_filter_collector,
                           std::shared_ptr<LookupJoinContext> lookup_join_context);

    ~LookupJoinSeekOperator() override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    bool has_output() const override { return _lookup_join_context != nullptr && !_is_finished; }
    bool need_input() const override { return !is_finished(); }
    bool is_finished() const override { return _is_finished; }
    Status set_finishing(RuntimeState* state) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    Status _get_tablet(const TInternalScanRange* scan_range);
    void _init_row_desc();
    Status _init_scanner_columns(std::vector<uint32_t>& scanner_columns);

    Status _prepare_tablet_reader(RuntimeState* state);
    Status _init_tablet_reader(RuntimeState* state, TabletReaderState& reader_state);
    Status _init_reader_state(RuntimeState* state, TabletReaderState& reader_state);
    Status _init_conjuncts_manager(RuntimeState* state, TabletReaderState& reader_state);
    Status _init_conjunct_ctxs(TabletReaderState& reader_state, uint32_t row_id);

    Expr* _create_eq_conjunct_expr(ObjectPool* pool, PrimitiveType ptype);
    Expr* _create_literal_expr(ObjectPool* pool, const ColumnPtr& input_column, uint32_t row_id,
                               const TypeDescriptor& type_desc) ;

    vectorized::ChunkPtr _init_ouput_chunk(RuntimeState* state);
    void _permute_output_chunk(RuntimeState* state, vectorized::ChunkPtr* result);
    Status _seek_row(RuntimeState* state);

private:
    bool _is_finished = false;
    vectorized::ChunkPtr _cur_chunk = nullptr;

    const TOlapScanNode& _olap_scan_node;
    const vectorized::RuntimeFilterProbeCollector& _runtime_filter_collector;
    const std::shared_ptr<LookupJoinContext> _lookup_join_context;
    const std::vector<LookupJoinKeyDesc> _join_key_descs;
    const std::vector<ExprContext*>& _other_join_conjuncts;

    TabletSharedPtr _tablet;
    const TupleDescriptor* _tuple_desc;
    int64_t _version;
    ObjectPool _obj_pool;

    std::shared_ptr<TabletReaderState> _reader_state;
    // NOTE: _reader may reference the _predicate_free_pool, it should be released before the _predicate_free_pool
    std::shared_ptr<vectorized::TabletReader> _reader;
    // output columns of `this` OlapScanner, i.e, the final output columns of `get_chunk`.
    std::vector<uint32_t> _scanner_columns;
    std::vector<uint32_t> _reader_columns;
    starrocks::vectorized::Schema _child_schema;

    uint32_t _row_id{0};
    ChunkPtr _output_chunk = nullptr;
    ChunkPtr _probe_chunk = nullptr;

    vectorized::Buffer<SlotDescriptor*> _col_types;
    uint32_t _left_column_count;
    uint32_t _right_column_count;
    bool _cur_eos = true;
};

class LookupJoinSeekOperatorFactory final : public SourceOperatorFactory {
public:
    LookupJoinSeekOperatorFactory(int32_t id, int32_t plan_node_id,
                                  const TOlapScanNode& olap_scan_node,
                                  const vectorized::RuntimeFilterProbeCollector& runtime_filter_collector)
            : SourceOperatorFactory(id, "index_seek", plan_node_id),
              _olap_scan_node(olap_scan_node),
              _runtime_filter_collector(runtime_filter_collector){}

    ~LookupJoinSeekOperatorFactory() override = default;

    bool with_morsels() const override { return true; }

    // With lookup join dependency.
    void with_lookup_join_context(std::shared_ptr<LookupJoinContext> lookup_join_context) {
        VLOG(1) << "set lookup join context";
        _lookup_join_context = lookup_join_context;
    }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LookupJoinSeekOperator>(this, _id,
                                                        _plan_node_id, driver_sequence,
                                                        _olap_scan_node,
                                                        _runtime_filter_collector,
                                                        _lookup_join_context);
    }

private:

    const TOlapScanNode& _olap_scan_node;
    const vectorized::RuntimeFilterProbeCollector& _runtime_filter_collector;
    std::shared_ptr<LookupJoinContext> _lookup_join_context;
};

} // namespace starrocks::pipeline
