// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/scan/olap_scan_context.h"

#include "exec/vectorized/olap_scan_node.h"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "storage/tablet.h"

namespace starrocks::pipeline {

using namespace vectorized;

Status OlapScanContext::prepare(RuntimeState* state) {
    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    RETURN_IF_ERROR(Expr::prepare(conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(conjunct_ctxs, state));

    return Status::OK();
}

void OlapScanContext::close(RuntimeState* state) {
    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    Expr::close(conjunct_ctxs, state);

    for (const auto& rowsets_per_tablet : _tablet_rowsets) {
        Rowset::release_readers(rowsets_per_tablet);
    }
}

Status OlapScanContext::capture_tablet_rowsets(const std::vector<TInternalScanRange*>& olap_scan_ranges) {
    _tablet_rowsets.resize(olap_scan_ranges.size());
    _tablets.resize(olap_scan_ranges.size());
    for (int i = 0; i < olap_scan_ranges.size(); ++i) {
        auto* scan_range = olap_scan_ranges[i];

        int64_t version = strtoul(scan_range->version.c_str(), nullptr, 10);
        ASSIGN_OR_RETURN(TabletSharedPtr tablet, vectorized::OlapScanNode::get_tablet(scan_range));

        // Capture row sets of this version tablet.
        {
            std::shared_lock l(tablet->get_header_lock());
            RETURN_IF_ERROR(tablet->capture_consistent_rowsets(Version(0, version), &_tablet_rowsets[i]));
            Rowset::acquire_readers(_tablet_rowsets[i]);
        }

        _tablets[i] = std::move(tablet);
    }

    return Status::OK();
}

Status OlapScanContext::parse_conjuncts(RuntimeState* state, const std::vector<ExprContext*>& runtime_in_filters,
                                        RuntimeFilterProbeCollector* runtime_bloom_filters) {
    const TOlapScanNode& thrift_olap_scan_node = _scan_node->thrift_olap_scan_node();
    const TupleDescriptor* tuple_desc = state->desc_tbl().get_tuple_descriptor(thrift_olap_scan_node.tuple_id);

    // Get _conjunct_ctxs.
    _conjunct_ctxs = _scan_node->conjunct_ctxs();
    _conjunct_ctxs.insert(_conjunct_ctxs.end(), runtime_in_filters.begin(), runtime_in_filters.end());

    // eval_const_conjuncts.
    Status status;
    RETURN_IF_ERROR(vectorized::OlapScanConjunctsManager::eval_const_conjuncts(_conjunct_ctxs, &status));
    if (!status.ok()) {
        return status;
    }

    // Init _conjuncts_manager.
    vectorized::OlapScanConjunctsManager& cm = _conjuncts_manager;
    cm.conjunct_ctxs_ptr = &_conjunct_ctxs;
    cm.tuple_desc = tuple_desc;
    cm.obj_pool = &_obj_pool;
    cm.key_column_names = &thrift_olap_scan_node.key_column_name;
    cm.runtime_filters = runtime_bloom_filters;
    cm.runtime_state = state;

    const TQueryOptions& query_options = state->query_options();
    int32_t max_scan_key_num;
    if (query_options.__isset.max_scan_key_num && query_options.max_scan_key_num > 0) {
        max_scan_key_num = query_options.max_scan_key_num;
    } else {
        max_scan_key_num = config::doris_max_scan_key_num;
    }
    bool enable_column_expr_predicate = false;
    if (thrift_olap_scan_node.__isset.enable_column_expr_predicate) {
        enable_column_expr_predicate = thrift_olap_scan_node.enable_column_expr_predicate;
    }

    // Parse conjuncts via _conjuncts_manager.
    RETURN_IF_ERROR(cm.parse_conjuncts(true, max_scan_key_num, enable_column_expr_predicate));

    // Get key_ranges and not_push_down_conjuncts from _conjuncts_manager.
    RETURN_IF_ERROR(_conjuncts_manager.get_key_ranges(&_key_ranges));
    _conjuncts_manager.get_not_push_down_conjuncts(&_not_push_down_conjuncts);

    _dict_optimize_parser.set_mutable_dict_maps(state, state->mutable_query_global_dict_map());
    _dict_optimize_parser.rewrite_conjuncts<false>(&_not_push_down_conjuncts, state);

    return Status::OK();
}

<<<<<<< HEAD
=======
/// OlapScanContextFactory.
OlapScanContextPtr OlapScanContextFactory::get_or_create(int32_t driver_sequence) {
    DCHECK_LT(driver_sequence, _dop);
    // ScanOperators sharing one morsel use the same context.
    int32_t idx = _shared_morsel_queue ? 0 : driver_sequence;
    DCHECK_LT(idx, _contexts.size());

    if (_contexts[idx] == nullptr) {
        _contexts[idx] =
                std::make_shared<OlapScanContext>(_scan_node, _scan_table_id, _dop, _shared_scan, _chunk_buffer);
    }
    return _contexts[idx];
}

>>>>>>> 4265212f40 ([BugFix] fix incorrect scan metrics in FE (#27779))
} // namespace starrocks::pipeline
