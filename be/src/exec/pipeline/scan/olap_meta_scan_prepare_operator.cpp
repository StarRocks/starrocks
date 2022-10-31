// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/scan/olap_meta_scan_prepare_operator.h"

#include <utility>

#include "exec/pipeline/scan/olap_meta_scan_context.h"
#include "exec/pipeline/scan/olap_meta_scan_operator.h"
#include "exec/vectorized/olap_meta_scanner.h"
#include "gen_cpp/Types_types.h"
#include "storage/olap_common.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"

namespace starrocks {
namespace pipeline {

OlapMetaScanPrepareOperator::OlapMetaScanPrepareOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                         int32_t driver_sequence,
                                                         vectorized::OlapMetaScanNode* const scan_node,
                                                         OlapMetaScanContextPtr scan_ctx)
        : SourceOperator(factory, id, "olap_meta_scan_prepare", plan_node_id, driver_sequence),
          _scan_node(scan_node),
          _scan_ctx(std::move(scan_ctx)) {}

OlapMetaScanPrepareOperator::~OlapMetaScanPrepareOperator() = default;

Status OlapMetaScanPrepareOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    RETURN_IF_ERROR(_prepare_scan_context(state));
    _scan_ctx->set_prepare_finished();
    return Status::OK();
}

void OlapMetaScanPrepareOperator::close(RuntimeState* state) {
    SourceOperator::close(state);
}

bool OlapMetaScanPrepareOperator::has_output() const {
    return false;
}

bool OlapMetaScanPrepareOperator::is_finished() const {
    return true;
}

StatusOr<vectorized::ChunkPtr> OlapMetaScanPrepareOperator::pull_chunk(RuntimeState* state) {
    return nullptr;
}

Status OlapMetaScanPrepareOperator::_prepare_scan_context(RuntimeState* state) {
    auto meta_scan_ranges = _morsel_queue->olap_scan_ranges();
    for (auto& scan_range : meta_scan_ranges) {
        vectorized::OlapMetaScannerParams params;
        params.scan_range = scan_range;
        auto scanner = std::make_shared<vectorized::OlapMetaScanner>(_scan_node);
        RETURN_IF_ERROR(scanner->init(state, params));
        TTabletId tablet_id = scan_range->tablet_id;
        _scan_ctx->add_scanner(tablet_id, scanner);
    }
    return Status::OK();
}

OlapMetaScanPrepareOperatorFactory::OlapMetaScanPrepareOperatorFactory(
        int32_t id, int32_t plan_node_id, vectorized::OlapMetaScanNode* const scan_node,
        std::shared_ptr<OlapMetaScanContextFactory> scan_ctx_factory)
        : SourceOperatorFactory(id, "olap_meta_scan_prepare", plan_node_id),
          _scan_node(scan_node),
          _scan_ctx_factory(std::move(scan_ctx_factory)) {}

Status OlapMetaScanPrepareOperatorFactory::prepare(RuntimeState* state) {
    return Status::OK();
}

void OlapMetaScanPrepareOperatorFactory::close(RuntimeState* state) {}

OperatorPtr OlapMetaScanPrepareOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<OlapMetaScanPrepareOperator>(this, _id, _plan_node_id, driver_sequence, _scan_node,
                                                         _scan_ctx_factory->get_or_create(driver_sequence));
}

} // namespace pipeline
} // namespace starrocks
