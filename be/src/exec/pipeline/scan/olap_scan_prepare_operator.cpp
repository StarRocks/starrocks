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
    TEST_SUCC_POINT("OlapScanPrepareOperator::prepare");

    RETURN_IF_ERROR(SourceOperator::prepare(state));

    RETURN_IF_ERROR(_ctx->prepare(state));
    RETURN_IF_ERROR(_ctx->capture_tablet_rowsets(_morsel_queue->olap_scan_ranges()));

    return Status::OK();
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
    _morsel_queue->set_tablets(_ctx->tablets());
    _morsel_queue->set_tablet_rowsets(_ctx->tablet_rowsets());

<<<<<<< HEAD
    _ctx->set_prepare_finished();
    if (!status.ok()) {
        _ctx->set_finished();
        return status;
    }
=======
    DeferOp defer([&]() {
        _ctx->set_prepare_finished();
        TEST_SYNC_POINT("OlapScnPrepareOperator::pull_chunk::after_set_prepare_finished");
    });
>>>>>>> d9d9c70453 ([BugFix] Fix the has_output check bug of scan operator (#42994))

    if (!status.ok()) {
        TEST_SYNC_POINT("OlapScnPrepareOperator::pull_chunk::before_set_finished");
        // OlapScanOperator::has_output() will `use !_ctx->is_prepare_finished() || _ctx->is_finished()` to
        // determine whether OlapScanOperator::pull_chunk() needs to be executed.
        // When _ctx->parse_conjuncts returns EOF, if set_prepare_finished first, and then set_finished.
        // Between calling set_finished, OlapScanOperator::has_output maybe return true,
        // causing OlapScanOperator::pull_chunk to be executed, which is invalid, maybe cause crash.
        // So we will set_finished first and set_prepare_finished()
        static_cast<void>(_ctx->set_finished());
        TEST_SYNC_POINT("OlapScnPrepareOperator::pull_chunk::after_set_finished");
        return status;
    } else {
        return nullptr;
    }
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
