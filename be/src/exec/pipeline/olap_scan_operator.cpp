// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/olap_scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/olap_chunk_source.h"
#include "exec/pipeline/scan_operator.h"
#include "exec/vectorized/olap_scan_node.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
namespace starrocks::pipeline {

class OlapScanOperatorFactory final : public ScanOperatorFactory {
public:
    OlapScanOperatorFactory(int32_t id, ScanNode* scan_node);

    ~OlapScanOperatorFactory() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;
};

class OlapScanOperator final : public ScanOperator {
public:
    OlapScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node);

    ~OlapScanOperator() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    ChunkSourcePtr create_chunk_source(MorselPtr morsel) override;

private:
    Status _capture_tablet_rowsets();

    // The row sets of tablets will become stale and be deleted, if compaction occurs
    // and these row sets aren't referenced, which will typically happen when the tablets
    // of the left table are compacted at building the right hash table. Therefore, reference
    // the row sets into _tablet_rowsets in the preparation phase to avoid the row sets being deleted.
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;
};

// ==================== OlapScanOperatorFactory ====================

OlapScanOperatorFactory::OlapScanOperatorFactory(int32_t id, ScanNode* scan_node)
        : ScanOperatorFactory(id, scan_node) {}

Status OlapScanOperatorFactory::do_prepare(RuntimeState* state) {
    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    vectorized::OlapScanNode* olap_scan_node = down_cast<vectorized::OlapScanNode*>(_scan_node);
    const auto& tolap_scan_node = olap_scan_node->thrift_olap_scan_node();
    auto tuple_desc = state->desc_tbl().get_tuple_descriptor(tolap_scan_node.tuple_id);
    vectorized::DictOptimizeParser::rewrite_descriptor(state, conjunct_ctxs, tolap_scan_node.dict_string_id_to_int_ids,
                                                       &(tuple_desc->decoded_slots()));
    return Status::OK();
}

void OlapScanOperatorFactory::do_close(RuntimeState*) {}

OperatorPtr OlapScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<OlapScanOperator>(this, _id, _scan_node);
}

// ==================== OlapScanOperator ====================

OlapScanOperator::OlapScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node)
        : ScanOperator(factory, id, scan_node) {}

Status OlapScanOperator::do_prepare(RuntimeState*) {
    RETURN_IF_ERROR(_capture_tablet_rowsets());
    return Status::OK();
}

void OlapScanOperator::do_close(RuntimeState*) {}

Status OlapScanOperator::_capture_tablet_rowsets() {
    const auto& morsels = this->morsel_queue()->morsels();
    _tablet_rowsets.resize(morsels.size());
    for (int i = 0; i < morsels.size(); ++i) {
        ScanMorsel* scan_morsel = (ScanMorsel*)morsels[i].get();
        auto* scan_range = scan_morsel->get_olap_scan_range();

        // Get version.
        int64_t version = strtoul(scan_range->version.c_str(), nullptr, 10);

        // Get tablet.
        TTabletId tablet_id = scan_range->tablet_id;
        std::string err;
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
        if (!tablet) {
            std::stringstream ss;
            SchemaHash schema_hash = strtoul(scan_range->schema_hash.c_str(), nullptr, 10);
            ss << "failed to get tablet. tablet_id=" << tablet_id << ", with schema_hash=" << schema_hash
               << ", reason=" << err;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // Capture row sets of this version tablet.
        {
            std::shared_lock l(tablet->get_header_lock());
            RETURN_IF_ERROR(tablet->capture_consistent_rowsets(Version(0, version), &_tablet_rowsets[i]));
        }
    }

    return Status::OK();
}

ChunkSourcePtr OlapScanOperator::create_chunk_source(MorselPtr morsel) {
    vectorized::OlapScanNode* olap_scan_node = down_cast<vectorized::OlapScanNode*>(_scan_node);
    return std::make_shared<OlapChunkSource>(std::move(morsel), this, olap_scan_node);
}

// ===========================================================

OpFactories decompose_olap_scan_node_to_pipeline(ScanNode* scan_node, PipelineBuilderContext* context) {
    OpFactories operators;
    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector =
            std::make_shared<RcRfProbeCollector>(1, std::move(scan_node->runtime_filter_collector()));
    auto scan_operator = std::make_shared<OlapScanOperatorFactory>(context->next_operator_id(), scan_node);
    // Initialize OperatorFactory's fields involving runtime filters.
    scan_node->init_runtime_filter_for_operator(scan_operator.get(), context, rc_rf_probe_collector);
    auto& morsel_queues = context->fragment_context()->morsel_queues();
    auto source_id = scan_operator->plan_node_id();
    DCHECK(morsel_queues.count(source_id));
    auto& morsel_queue = morsel_queues[source_id];
    // ScanOperator's degree_of_parallelism is not more than the number of morsels
    // If table is empty, then morsel size is zero and we still set degree of parallelism to 1
    const auto degree_of_parallelism =
            std::min<size_t>(std::max<size_t>(1, morsel_queue->num_morsels()), context->degree_of_parallelism());
    scan_operator->set_degree_of_parallelism(degree_of_parallelism);
    operators.emplace_back(std::move(scan_operator));
    size_t limit = scan_node->limit();
    if (limit != -1) {
        operators.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), scan_node->id(), limit));
    }
    return operators;
}

} // namespace starrocks::pipeline
