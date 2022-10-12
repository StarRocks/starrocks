// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/scan/olap_scan_prepare_operator.h"

#include "exec/vectorized/olap_scan_node.h"
#include "storage/storage_engine.h"

namespace starrocks::pipeline {

/// OlapScanPrepareOperator
OlapScanPrepareOperator::OlapScanPrepareOperator(OperatorFactory* factory, int32_t id, const string& name,
                                                 int32_t plan_node_id, int32_t driver_sequence, OlapScanContextPtr ctx)
        : SourceOperator(factory, id, name, plan_node_id, driver_sequence), _ctx(std::move(ctx)) {
    _ctx->ref();
}

OlapScanPrepareOperator::~OlapScanPrepareOperator() {
    auto* state = runtime_state();
    if (state == nullptr) {
        return;
    }

    _ctx->unref(state);
}

Status OlapScanPrepareOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));

    RETURN_IF_ERROR(_capture_tablet_rowsets());
    return _ctx->prepare(state);
}

void OlapScanPrepareOperator::close(RuntimeState* state) {
    SourceOperator::close(state);
}

bool OlapScanPrepareOperator::has_output() const {
    return !is_finished();
}

bool OlapScanPrepareOperator::is_finished() const {
    return _ctx->is_prepare_finished() || _ctx->is_finished();
}

StatusOr<vectorized::ChunkPtr> OlapScanPrepareOperator::pull_chunk(RuntimeState* state) {
    Status status = _ctx->parse_conjuncts(state, runtime_in_filters(), runtime_bloom_filters());

    _morsel_queue->set_key_ranges(_ctx->key_ranges());
    _morsel_queue->set_tablets(_tablets);
    _morsel_queue->set_tablet_rowsets(_tablet_rowsets);

    _ctx->set_prepare_finished();
    if (!status.ok()) {
        _ctx->set_finished();
        return status;
    }

    return nullptr;
}

Status OlapScanPrepareOperator::_capture_tablet_rowsets() {
    auto olap_scan_ranges = _morsel_queue->olap_scan_ranges();
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
        }

        _tablets[i] = std::move(tablet);
    }

    return Status::OK();
}

/// OlapScanPrepareOperatorFactory
OlapScanPrepareOperatorFactory::OlapScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id,
                                                               vectorized::OlapScanNode* const scan_node,
                                                               OlapScanContextFactoryPtr ctx_factory)
        : SourceOperatorFactory(id, "olap_scan_prepare", plan_node_id),
          _scan_node(scan_node),
          _ctx_factory(std::move(ctx_factory)) {}

Status OlapScanPrepareOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperatorFactory::prepare(state));

    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    const auto& tolap_scan_node = _scan_node->thrift_olap_scan_node();
    auto tuple_desc = state->desc_tbl().get_tuple_descriptor(tolap_scan_node.tuple_id);

    vectorized::DictOptimizeParser::rewrite_descriptor(state, conjunct_ctxs, tolap_scan_node.dict_string_id_to_int_ids,
                                                       &(tuple_desc->decoded_slots()));

    RETURN_IF_ERROR(Expr::prepare(conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(conjunct_ctxs, state));

    return Status::OK();
}

void OlapScanPrepareOperatorFactory::close(RuntimeState* state) {
    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    Expr::close(conjunct_ctxs, state);

    SourceOperatorFactory::close(state);
}

OperatorPtr OlapScanPrepareOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<OlapScanPrepareOperator>(this, _id, _name, _plan_node_id, driver_sequence,
                                                     _ctx_factory->get_or_create(driver_sequence));
}

} // namespace starrocks::pipeline
